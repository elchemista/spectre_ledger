defmodule Spectre.Ledger do
  @moduledoc """
  Append-only durable checkpoint and boundary-receipt ledger for Spectre 0.3.2.

  Ledger implements `Spectre.Instance.CheckpointStore` and
  `Spectre.Receipt.Sink`. It archives checkpoints that Spectre persists and
  boundary receipts that Spectre emits; it does not claim every runtime
  revision, deterministic replay, or exactly-once side effects.
  """

  use Spectre.Stack.Installable,
    id: :spectre_ledger,
    version: "0.1.0",
    contract: 1,
    spectre: "~> 0.3.2",
    provides: [
      {:contract, {:spectre, :instance_checkpoint_store, 1}},
      {:contract, {:spectre, :receipt_sink, 1}},
      {:service, {:spectre_ledger, :checkpoint_archive, 1}},
      {:service, {:spectre_ledger, :boundary_receipt_archive, 1}}
    ],
    metadata: %{
      ledger_contract: 2,
      entry_contract: 1,
      receipt_entry_contract: 1,
      bundle_contract: 1,
      capture: [:persisted_checkpoints, :nondeterministic_boundaries],
      state_digest_linkage: true,
      every_revision: false,
      deterministic_replay: false,
      exactly_once_external_effects: false
    }

  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Backend.Capabilities
  alias Spectre.Ledger.Bundle
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.ReceiptChain
  alias Spectre.Ledger.ReceiptCodec
  alias Spectre.Ledger.ReceiptEntry
  alias Spectre.Ledger.ReceiptSink

  @version "0.1.0"

  @doc "Returns the Ledger package version."
  @spec version() :: String.t()
  def version, do: @version

  @doc "Builds the configuration consumed by `Spectre.Instance`."
  @spec checkpoint_store(keyword()) :: {module(), keyword()}
  def checkpoint_store(opts \\ []) when is_list(opts),
    do: {Spectre.Ledger.CheckpointStore, opts}

  @doc "Builds the configuration consumed by `Spectre.Instance` for boundary receipts."
  @spec receipt_sink(keyword()) :: {module(), keyword()}
  def receipt_sink(opts \\ []) when is_list(opts), do: {ReceiptSink, opts}

  @doc "Looks up one persisted boundary receipt by deterministic receipt id."
  @spec receipt(String.t(), keyword()) ::
          {:ok, Spectre.Receipt.Envelope.t()} | :not_found | {:error, term()}
  def receipt(id, opts \\ [])

  def receipt(id, opts) when is_binary(id) and id != "" and is_list(opts),
    do: ReceiptSink.lookup(id, opts)

  def receipt(_id, _opts), do: {:error, :invalid_ledger_receipt_id}

  @doc "Reads one content-addressed receipt payload, including a staged payload."
  @spec receipt_payload(String.t(), keyword()) ::
          {:ok, Spectre.Receipt.Envelope.t()} | :not_found | {:error, term()}
  def receipt_payload(ref, opts \\ [])

  def receipt_payload(ref, opts) when is_binary(ref) and ref != "" and is_list(opts),
    do: ReceiptSink.get_payload(ref, opts)

  def receipt_payload(_ref, _opts), do: {:error, :invalid_ledger_receipt_payload_ref}

  @doc "Lists immutable receipt-chain entries in physical append order."
  @spec receipt_entries(Ref.t() | String.t(), keyword()) ::
          {:ok, [ReceiptEntry.t()]} | {:error, term()}
  def receipt_entries(ref_or_key, opts \\ []) do
    with {:ok, config} <- Config.new(opts),
         {:ok, stream_key} <- stream_key(ref_or_key),
         :ok <- Capabilities.validate(config.backend, :receipt_archive) do
      query = Keyword.take(opts, [:after_sequence, :limit])
      config.backend.receipt_entries(config, stream_key, query)
    end
  end

  @doc "Lists validated receipt envelopes in physical append order."
  @spec receipts(Ref.t() | String.t(), keyword()) ::
          {:ok, [Spectre.Receipt.Envelope.t()]} | {:error, term()}
  def receipts(ref_or_key, opts \\ []) do
    with {:ok, config} <- Config.new(opts),
         {:ok, stream_key} <- stream_key(ref_or_key),
         :ok <- Capabilities.validate(config.backend, :receipt_archive),
         {:ok, entries} <-
           config.backend.receipt_entries(
             config,
             stream_key,
             Keyword.take(opts, [:after_sequence, :limit])
           ),
         {:ok, objects} <- config.backend.receipt_objects(config, stream_key, []) do
      decode_receipts(entries, objects, config.max_receipt_bytes)
    end
  end

  @doc "Verifies one complete receipt chain and every referenced envelope object."
  @spec verify_receipts(Ref.t() | String.t(), keyword()) :: {:ok, map()} | {:error, term()}
  def verify_receipts(ref_or_key, opts \\ []) do
    with :ok <- complete_receipt_options(opts),
         {:ok, config} <- Config.new(opts),
         {:ok, stream_key} <- stream_key(ref_or_key),
         :ok <- Capabilities.validate(config.backend, :receipt_archive),
         {:ok, entries} <- config.backend.receipt_entries(config, stream_key, []),
         {:ok, chain} <- ReceiptChain.verify(entries),
         {:ok, objects} <- config.backend.receipt_objects(config, stream_key, []),
         :ok <- exact_receipt_objects(entries, objects),
         {:ok, envelopes} <- decode_receipts(entries, objects, config.max_receipt_bytes) do
      {:ok,
       chain
       |> Map.put(:object_count, map_size(objects))
       |> Map.put(:linked_state_count, linked_state_count(envelopes))
       |> Map.put(:capture, :nondeterministic_boundaries)
       |> Map.put(:state_digest_linkage, true)
       |> Map.put(:deterministic_replay_claim, false)
       |> Map.put(:exactly_once_external_effects, false)}
    end
  end

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

  defp complete_receipt_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and not Keyword.has_key?(opts, :after_sequence) and
         not Keyword.has_key?(opts, :limit),
       do: :ok,
       else: {:error, :partial_ledger_receipt_stream_not_verifiable}
  end

  defp complete_receipt_options(_opts), do: {:error, :invalid_ledger_options}

  defp decode_receipts(entries, objects, max_bytes) do
    Enum.reduce_while(entries, {:ok, []}, fn entry, {:ok, envelopes} ->
      result =
        with {:ok, encoded} <- fetch_receipt_object(objects, entry.payload_ref),
             {:ok, envelope} <-
               ReceiptCodec.verify(
                 encoded,
                 entry.receipt_id,
                 entry.envelope_digest,
                 max_bytes
               ),
             :ok <- ReceiptEntry.verify_envelope(entry, envelope) do
          {:ok, envelope}
        end

      case result do
        {:ok, envelope} -> {:cont, {:ok, [envelope | envelopes]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, envelopes} -> {:ok, Enum.reverse(envelopes)}
      {:error, _reason} = error -> error
    end
  end

  defp fetch_receipt_object(objects, ref) do
    case Map.fetch(objects, ref) do
      {:ok, encoded} when is_binary(encoded) -> {:ok, encoded}
      {:ok, _invalid} -> {:error, :invalid_ledger_receipt_object}
      :error -> {:error, {:ledger_receipt_object_missing, ref}}
    end
  end

  defp exact_receipt_objects(entries, objects) do
    expected = entries |> Enum.map(& &1.payload_ref) |> MapSet.new()
    supplied = objects |> Map.keys() |> MapSet.new()

    if expected == supplied,
      do: :ok,
      else: {:error, :ledger_receipt_object_set_mismatch}
  end

  defp linked_state_count(envelopes) do
    Enum.count(envelopes, fn envelope ->
      is_binary(envelope.pre_state_digest) and is_binary(envelope.post_state_digest)
    end)
  end

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
