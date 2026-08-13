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
         {:ok, stream_key} <- stream_key(ref_or_key) do
      config.backend.head(config, stream_key)
    end
  end

  @doc "Lists a stream's immutable entries in ascending revision order."
  @spec entries(Ref.t() | String.t(), keyword()) ::
          {:ok, [Spectre.Ledger.Entry.t()]} | {:error, term()}
  def entries(ref_or_key, opts \\ []) do
    with {:ok, config} <- Config.new(opts),
         {:ok, stream_key} <- stream_key(ref_or_key) do
      config.backend.entries(config, stream_key, opts)
    end
  end

  @doc "Verifies all links and entry digests in one complete stream."
  @spec verify(Ref.t() | String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def verify(ref_or_key, opts \\ []) do
    with {:ok, entries} <- entries(ref_or_key, opts) do
      Chain.verify(entries)
    end
  end

  defp stream_key(%Ref{key: key}), do: {:ok, key}
  defp stream_key(key) when is_binary(key) and key != "", do: {:ok, key}
  defp stream_key(_value), do: {:error, :invalid_ledger_stream_key}
end
