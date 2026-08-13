defmodule Spectre.Ledger do
  @moduledoc """
  Append-only durable checkpoint ledger for Spectre 0.3.1.

  Ledger implements the existing `Spectre.Instance.CheckpointStore` boundary.
  It archives the checkpoints that Spectre persists; it does not claim every
  runtime revision, deterministic replay, or exactly-once side effects.
  """

  use Spectre.Stack.Installable,
    id: :spectre_ledger,
    version: "0.1.0",
    contract: 1,
    spectre: "~> 0.3.1",
    provides: [
      {:contract, {:spectre, :instance_checkpoint_store, 1}},
      {:service, {:spectre_ledger, :checkpoint_archive, 1}}
    ],
    metadata: %{
      ledger_contract: 1,
      entry_contract: 1,
      bundle_contract: 1,
      capture: :persisted_checkpoints,
      every_revision: false,
      deterministic_replay: false
    }

  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Bundle
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.Config

  @version "0.1.0"

  @doc "Returns the Ledger package version."
  @spec version() :: String.t()
  def version, do: @version

  @doc "Builds the configuration consumed by `Spectre.Instance`."
  @spec checkpoint_store(keyword()) :: {module(), keyword()}
  def checkpoint_store(opts \\ []) when is_list(opts),
    do: {Spectre.Ledger.CheckpointStore, opts}

  @doc "Returns the durable head entry for a Ref or opaque stream key."
  @spec head(Ref.t() | String.t(), keyword()) ::
          :not_found | {:ok, Spectre.Ledger.Entry.t()} | {:error, term()}
  def head(ref_or_key, opts \\ []) do
    with {:ok, config} <- Config.new(opts),
         {:ok, stream_key} <- stream_key(ref_or_key),
         :ok <- backend_callback(config.backend, :head, 2) do
      config.backend.head(config, stream_key)
    end
  end

  @doc "Lists a stream's immutable entries in ascending revision order."
  @spec entries(Ref.t() | String.t(), keyword()) ::
          {:ok, [Spectre.Ledger.Entry.t()]} | {:error, term()}
  def entries(ref_or_key, opts \\ []) do
    with {:ok, config} <- Config.new(opts),
         {:ok, stream_key} <- stream_key(ref_or_key),
         :ok <- backend_callback(config.backend, :entries, 3) do
      query = Keyword.take(opts, [:after_revision, :limit])
      config.backend.entries(config, stream_key, query)
    end
  end

  @doc "Verifies all links and entry digests in one complete stream."
  @spec verify(Ref.t() | String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def verify(ref_or_key, opts \\ []) do
    with :ok <- complete_stream_options(opts),
         {:ok, entries} <- entries(ref_or_key, opts) do
      Chain.verify(entries)
    end
  end

  @doc "Exports one complete stream as a bounded, verified Ledger bundle."
  @spec export_bundle(Ref.t() | String.t(), keyword()) ::
          {:ok, binary()} | {:error, term()}
  def export_bundle(ref_or_key, opts \\ []) do
    with {:ok, config} <- Config.new(opts),
         {:ok, stream_key} <- stream_key(ref_or_key),
         :ok <- backend_callback(config.backend, :entries, 3),
         :ok <- backend_callback(config.backend, :objects, 3),
         {:ok, entries} <- config.backend.entries(config, stream_key, []),
         {:ok, objects} <- config.backend.objects(config, stream_key, []) do
      Bundle.export(entries, objects, bundle_options(opts))
    end
  end

  @doc "Verifies a bundle fully before atomically importing its single stream."
  @spec import_bundle(binary() | Bundle.t(), keyword()) ::
          {:ok, :imported | :idempotent, map()} | {:error, term()}
  def import_bundle(bundle, opts \\ []) do
    with {:ok, config} <- Config.new(opts),
         :ok <- backend_callback(config.backend, :put_stream, 4),
         {:ok, decoded} <- decode_bundle(bundle, bundle_options(opts)),
         {:ok, report} <- Bundle.verify(decoded, bundle_options(opts)),
         stream_key <- hd(decoded.entries).stream_key,
         {:ok, status} <-
           config.backend.put_stream(config, stream_key, decoded.entries, decoded.objects) do
      {:ok, status, report}
    end
  end

  defp decode_bundle(%Bundle{} = bundle, _opts), do: {:ok, bundle}
  defp decode_bundle(encoded, opts) when is_binary(encoded), do: Bundle.decode(encoded, opts)
  defp decode_bundle(_bundle, _opts), do: {:error, :invalid_ledger_bundle}

  defp bundle_options(opts), do: Keyword.get(opts, :bundle, [])

  defp complete_stream_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and
         not Keyword.has_key?(opts, :after_revision) and not Keyword.has_key?(opts, :limit),
       do: :ok,
       else: {:error, :partial_ledger_stream_not_verifiable}
  end

  defp complete_stream_options(_opts), do: {:error, :invalid_ledger_options}

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

  defp stream_key(%Ref{key: key}), do: {:ok, key}
  defp stream_key(key) when is_binary(key) and key != "", do: {:ok, key}
  defp stream_key(_value), do: {:error, :invalid_ledger_stream_key}
end
