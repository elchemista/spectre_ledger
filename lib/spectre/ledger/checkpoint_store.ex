defmodule Spectre.Ledger.CheckpointStore do
  @moduledoc """
  Spectre Instance Checkpoint Store backed by an append-only Ledger chain.

  Spectre remains the only owner and scheduler. This adapter validates the
  existing canonical checkpoint, atomically advances one Ledger stream, and
  returns stable CAS errors to the core reconciliation boundary.
  """

  @behaviour Spectre.Instance.CheckpointStore

  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.Write

  @impl Spectre.Instance.CheckpointStore
  def load(%Ref{} = ref, opts) do
    with {:ok, config} <- Config.new(opts),
         :ok <- backend_callback(config.backend, :load, 2) do
      config.backend.load(config, ref)
    end
  end

  @impl Spectre.Instance.CheckpointStore
  def compare_and_swap(%Ref{} = ref, checkpoint, expected, revision, opts) do
    with {:ok, config} <- Config.new(opts),
         {:ok, write} <- prepare(ref, checkpoint, expected, revision, opts, config),
         :ok <- backend_callback(config.backend, :compare_and_swap, 2) do
      config.backend.compare_and_swap(config, write)
    end
  end

  @impl Spectre.Instance.CheckpointStore
  def migrate_instance_key(
        %Ref{} = legacy_ref,
        %Ref{} = stable_ref,
        legacy_checkpoint,
        migrated_checkpoint,
        opts
      ) do
    with {:ok, config} <- Config.new(opts),
         :ok <- backend_callback(config.backend, :migrate, 5),
         {:ok, source_digest} <- source_digest(config, legacy_ref),
         {:ok, report} <- Foundation.verify_instance_checkpoint(migrated_checkpoint),
         {:ok, write} <-
           prepare(
             stable_ref,
             migrated_checkpoint,
             0,
             report.revision,
             Keyword.put(opts, :source_entry_digest, source_digest),
             config,
             :migration
           ) do
      config.backend.migrate(config, legacy_ref, stable_ref, legacy_checkpoint, write)
    end
  end

  @doc false
  @spec prepare(
          Ref.t(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          keyword(),
          Config.t(),
          :checkpoint | :migration
        ) :: {:ok, Write.t()} | {:error, term()}
  def prepare(ref, checkpoint, expected, revision, opts, config, kind \\ :checkpoint)

  def prepare(
        %Ref{} = ref,
        checkpoint,
        expected,
        revision,
        opts,
        %Config{} = config,
        kind
      )
      when is_binary(checkpoint) and is_integer(expected) and expected >= 0 and
             is_integer(revision) and revision >= 0 and is_list(opts) and
             kind in [:checkpoint, :migration] do
    with :ok <- checkpoint_size(checkpoint, config.max_checkpoint_bytes),
         :ok <- advancing_revision(expected, revision),
         {:ok, report} <- Foundation.verify_instance_checkpoint(checkpoint),
         :ok <- matching_revision(report.revision, revision),
         {:ok, owner_fencing_token} <-
           owner_fencing_token(Keyword.get(opts, :owner_fencing_token)),
         {:ok, source_entry_digest} <-
           optional_digest(Keyword.get(opts, :source_entry_digest)) do
      {:ok,
       %Write{
         ref: ref,
         checkpoint: checkpoint,
         expected_revision: expected,
         revision: revision,
         checkpoint_digest: report.digest,
         blob_digest: sha256(checkpoint),
         byte_size: byte_size(checkpoint),
         owner_fencing_token: owner_fencing_token,
         source_entry_digest: source_entry_digest,
         kind: kind
       }}
    end
  end

  def prepare(_ref, _checkpoint, _expected, _revision, _opts, _config, _kind),
    do: {:error, :invalid_ledger_checkpoint_write}

  @spec source_digest(Config.t(), Ref.t()) :: {:ok, String.t() | nil} | {:error, term()}
  defp source_digest(config, legacy_ref) do
    with :ok <- backend_callback(config.backend, :head, 2) do
      case config.backend.head(config, legacy_ref.key) do
        {:ok, entry} -> {:ok, entry.entry_digest}
        :not_found -> {:ok, nil}
        {:error, _reason} = error -> error
      end
    end
  end

  defp checkpoint_size(checkpoint, max_bytes) do
    size = byte_size(checkpoint)

    cond do
      size == 0 -> {:error, :empty_ledger_checkpoint}
      size > max_bytes -> {:error, {:ledger_checkpoint_too_large, size, max_bytes}}
      true -> :ok
    end
  end

  defp advancing_revision(expected, revision) when revision > expected, do: :ok
  defp advancing_revision(_expected, _revision), do: {:error, :non_advancing_ledger_revision}

  defp matching_revision(revision, revision), do: :ok

  defp matching_revision(_checkpoint_revision, _argument_revision),
    do: {:error, :ledger_checkpoint_revision_mismatch}

  defp owner_fencing_token(nil), do: {:ok, nil}

  defp owner_fencing_token(value) when is_integer(value) and value >= 0,
    do: {:ok, value}

  defp owner_fencing_token(_value), do: {:error, :invalid_owner_fencing_token}

  defp optional_digest(nil), do: {:ok, nil}

  defp optional_digest(value) when is_binary(value) do
    if Regex.match?(~r/\A[0-9a-f]{64}\z/, value),
      do: {:ok, value},
      else: {:error, :invalid_source_entry_digest}
  end

  defp optional_digest(_value), do: {:error, :invalid_source_entry_digest}

  defp backend_callback(module, function, arity) do
    cond do
      not Code.ensure_loaded?(module) ->
        {:error, {:ledger_backend_not_loaded, module}}

      not function_exported?(module, function, arity) ->
        {:error, {:ledger_backend_callback_missing, module, function, arity}}

      true ->
        :ok
    end
  end

  defp sha256(bytes),
    do: bytes |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
end
