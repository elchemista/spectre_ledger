defmodule Spectre.Ledger.ReceiptBackend.Conformance do
  @moduledoc """
  ExUnit-independent contract suite for receipt-capable Ledger backends.

  The runner verifies the complete `Spectre.Receipt.Sink` and Ledger archive
  surface against an isolated stream: content-addressed staging, concurrent
  initial append, exact retry, lookup, physical chain ordering, pagination,
  immutable-object readback, and end-to-end verification.

  Receipt callbacks are an optional backend capability. A checkpoint-only
  backend remains valid, but must not claim this conformance contract. Callers
  must provide a fresh namespace and stream key for every run.
  """

  alias Spectre.Ledger
  alias Spectre.Ledger.Backend.Capabilities
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.ReceiptChain
  alias Spectre.Receipt.Envelope
  alias Spectre.Receipt.Sink

  @contract_version 1
  @concurrency 8

  @type report :: %{
          required(:contract_version) => 1,
          required(:sink) => :verified,
          required(:concurrent_append) => :single_winner,
          required(:entry_count) => 2,
          required(:head_sequence) => 2,
          required(:payload_count) => 2,
          required(:archive) => :verified
        }

  @doc "Runs receipt sink and archive conformance for one isolated stream."
  @spec run(keyword(), String.t()) :: {:ok, report()} | {:error, term()}
  def run(opts, stream_key)
      when is_list(opts) and is_binary(stream_key) and stream_key != "" do
    with {:ok, config} <- normalize_config(opts),
         :ok <- capabilities(config.backend),
         {:ok, sink} <- normalize_sink(opts),
         first <- fixture(stream_key, 1),
         second <- fixture(stream_key, 2),
         :ok <- stage_and_read(sink, first),
         :ok <- concurrent_append(sink, first),
         {:ok, :idempotent} <- Sink.append(sink, first, []),
         {:ok, :appended} <- Sink.append(sink, second, []),
         :ok <- sink_readback(sink, first, second),
         {:ok, entries} <- archive_entries(config, stream_key),
         {:ok, chain} <- ReceiptChain.verify(entries),
         :ok <- paginated_readback(config, stream_key, entries),
         {:ok, objects} <- archive_objects(config, stream_key),
         :ok <- exact_objects(entries, objects),
         {:ok, verification} <- Ledger.verify_receipts(stream_key, opts),
         :ok <- matching_reports(chain, verification, objects) do
      {:ok,
       %{
         contract_version: @contract_version,
         sink: :verified,
         concurrent_append: :single_winner,
         entry_count: chain.entry_count,
         head_sequence: chain.head_sequence,
         payload_count: map_size(objects),
         archive: :verified
       }}
    else
      {:error, {:ledger_receipt_backend_conformance_failed, _phase, _code}} = error -> error
      {:error, _reason} -> failure(:contract, :failed)
      _other -> failure(:contract, :invalid_reply)
    end
  rescue
    _exception -> failure(:callback, :raised)
  catch
    _kind, _reason -> failure(:callback, :failed)
  end

  def run(_opts, _stream_key), do: failure(:options, :invalid)

  defp normalize_config(opts) do
    case Config.new(opts) do
      {:ok, config} -> {:ok, config}
      {:error, _reason} -> failure(:configuration, :invalid)
    end
  end

  defp capabilities(backend) do
    with :ok <- Capabilities.validate(backend, :receipt_sink),
         :ok <- Capabilities.validate(backend, :receipt_archive) do
      :ok
    else
      {:error, _reason} -> failure(:configuration, :callback_missing)
    end
  end

  defp normalize_sink(opts) do
    case Sink.normalize(Ledger.receipt_sink(opts)) do
      {:ok, sink} -> {:ok, sink}
      {:error, _reason} -> failure(:configuration, :invalid_sink)
    end
  end

  defp stage_and_read(sink, envelope) do
    expected_ref = Sink.payload_ref(envelope)

    case Sink.put_payload(sink, envelope, []) do
      {:ok, ^expected_ref} ->
        case Sink.get_payload(sink, expected_ref, []) do
          {:ok, ^envelope} -> :ok
          _other -> failure(:payload_store, :invalid_readback)
        end

      _other ->
        failure(:payload_store, :put_failed)
    end
  end

  defp concurrent_append(sink, envelope) do
    results =
      1..@concurrency
      |> Task.async_stream(fn _index -> Sink.append(sink, envelope, []) end,
        max_concurrency: @concurrency,
        ordered: false,
        timeout: 15_000
      )
      |> Enum.map(fn
        {:ok, result} -> result
        _task_failure -> :task_failed
      end)

    appended = Enum.count(results, &(&1 == {:ok, :appended}))
    idempotent = Enum.count(results, &(&1 == {:ok, :idempotent}))

    if appended == 1 and idempotent == @concurrency - 1,
      do: :ok,
      else: failure(:concurrency, :not_single_winner)
  end

  defp sink_readback(sink, first, second) do
    with {:ok, ^first} <- Sink.lookup(sink, first.id, []),
         {:ok, ^second} <- Sink.lookup(sink, second.id, []),
         :not_found <- Sink.lookup(sink, second.id <> ":missing", []) do
      :ok
    else
      _other -> failure(:lookup, :invalid_readback)
    end
  end

  defp archive_entries(config, stream_key) do
    case config.backend.receipt_entries(config, stream_key, []) do
      {:ok, [first, second] = entries}
      when first.sequence == 1 and second.sequence == 2 and
             second.previous_entry_digest == first.entry_digest ->
        {:ok, entries}

      _other ->
        failure(:entries, :invalid_readback)
    end
  end

  defp paginated_readback(config, stream_key, [_first, second]) do
    with {:ok, [^second]} <-
           config.backend.receipt_entries(config, stream_key, after_sequence: 1),
         {:ok, [_first_only]} <- config.backend.receipt_entries(config, stream_key, limit: 1) do
      :ok
    else
      _other -> failure(:pagination, :invalid_readback)
    end
  end

  defp archive_objects(config, stream_key) do
    case config.backend.receipt_objects(config, stream_key, []) do
      {:ok, objects} when is_map(objects) and map_size(objects) == 2 -> {:ok, objects}
      _other -> failure(:objects, :invalid_readback)
    end
  end

  defp exact_objects(entries, objects) do
    expected = entries |> Enum.map(& &1.payload_ref) |> MapSet.new()

    if expected == MapSet.new(Map.keys(objects)),
      do: :ok,
      else: failure(:objects, :set_mismatch)
  end

  defp matching_reports(chain, verification, objects) do
    if verification.entry_count == chain.entry_count and
         verification.head_sequence == chain.head_sequence and
         verification.head_entry_digest == chain.head_entry_digest and
         verification.object_count == map_size(objects) do
      :ok
    else
      failure(:verification, :report_mismatch)
    end
  end

  defp fixture(stream_key, revision) do
    Envelope.new!(
      kind: :nondeterminism_sample,
      instance_ref: stream_key,
      canonical_revision: revision,
      correlation_id: "#{stream_key}:receipt-conformance:#{revision}",
      payload_schema_ref: "spectre-ledger.receipt-backend-conformance/1",
      payload: %{sample: revision},
      privacy: :internal,
      recorded_at: 1_800_000_000_000 + revision
    )
  end

  defp failure(phase, code),
    do: {:error, {:ledger_receipt_backend_conformance_failed, phase, code}}
end
