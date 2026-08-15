defmodule Spectre.Ledger.ReceiptSink do
  @moduledoc """
  Durable `Spectre.Receipt.Sink` backed by a Ledger backend.

  The adapter stores canonical envelope bytes under the content address
  required by Spectre 0.3.2, then appends an immutable per-Instance Ledger
  entry. Exact retries are idempotent; a divergent id or payload address fails
  closed. Runtime handles remain in backend options and are never persisted.
  """

  @behaviour Spectre.Receipt.Sink

  alias Spectre.Ledger.Backend.Capabilities
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.ReceiptCodec
  alias Spectre.Ledger.ReceiptWrite
  alias Spectre.Ledger.Telemetry
  alias Spectre.Receipt.Envelope
  alias Spectre.Receipt.Sink

  @impl Spectre.Receipt.Sink
  def append(%Envelope{} = envelope, opts) do
    observe(:append, envelope.id, opts, %{receipt_kind: envelope.kind}, fn ->
      with {:ok, config} <- Config.new(opts),
           {:ok, write} <- ReceiptWrite.new(envelope, opts, config),
           :ok <- Capabilities.validate(config.backend, :receipt_sink) do
        config.backend.append_receipt(config, write)
      end
    end)
  end

  @impl Spectre.Receipt.Sink
  def lookup(id, opts) when is_binary(id) and id != "" do
    observe(:lookup, id, opts, %{}, fn ->
      with {:ok, config} <- Config.new(opts),
           :ok <- Capabilities.validate(config.backend, :receipt_sink),
           result <- config.backend.lookup_receipt(config, id) do
        decode_lookup(result, id, config.max_receipt_bytes)
      end
    end)
  end

  def lookup(_id, _opts), do: {:error, :invalid_ledger_receipt_id}

  @impl Spectre.Receipt.Sink
  def put_payload(%Envelope{} = envelope, opts) do
    observe(:put_payload, envelope.id, opts, %{receipt_kind: envelope.kind}, fn ->
      with {:ok, config} <- Config.new(opts),
           {:ok, write} <- ReceiptWrite.new(envelope, opts, config),
           :ok <- Capabilities.validate(config.backend, :receipt_sink),
           {:ok, ref} <- config.backend.put_receipt_payload(config, write),
           true <- ref == write.payload_ref do
        {:ok, ref}
      else
        false -> {:error, :ledger_receipt_payload_ref_mismatch}
        {:error, _reason} = error -> error
      end
    end)
  end

  @impl Spectre.Receipt.Sink
  def get_payload(ref, opts) when is_binary(ref) and ref != "" do
    observe(:get_payload, ref, opts, %{}, fn ->
      with {:ok, config} <- Config.new(opts),
           :ok <- Capabilities.validate(config.backend, :receipt_sink),
           result <- config.backend.get_receipt_payload(config, ref) do
        decode_payload(result, ref, config.max_receipt_bytes)
      end
    end)
  end

  def get_payload(_ref, _opts), do: {:error, :invalid_ledger_receipt_payload_ref}

  defp decode_lookup(:not_found, _id, _max_bytes), do: :not_found

  defp decode_lookup({:ok, encoded}, id, max_bytes) when is_binary(encoded) do
    case ReceiptCodec.decode(encoded, max_bytes) do
      {:ok, %Envelope{id: ^id} = envelope} -> {:ok, envelope}
      {:ok, %Envelope{}} -> {:error, :ledger_receipt_lookup_id_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp decode_lookup({:error, _reason} = error, _id, _max_bytes), do: error
  defp decode_lookup(_result, _id, _max_bytes), do: {:error, :invalid_ledger_receipt_lookup}

  defp decode_payload(:not_found, _ref, _max_bytes), do: :not_found

  defp decode_payload({:ok, encoded}, ref, max_bytes) when is_binary(encoded) do
    with {:ok, envelope} <- ReceiptCodec.decode(encoded, max_bytes),
         true <- Sink.payload_ref(envelope) == ref do
      {:ok, envelope}
    else
      false -> {:error, :ledger_receipt_payload_ref_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp decode_payload({:error, _reason} = error, _ref, _max_bytes), do: error
  defp decode_payload(_result, _ref, _max_bytes), do: {:error, :invalid_ledger_receipt_payload}

  defp observe(operation, identifier, opts, metadata, callback) do
    started_at = System.monotonic_time()

    try do
      result = callback.()
      emit(operation, identifier, opts, metadata, result, started_at)
      result
    rescue
      exception ->
        emit(operation, identifier, opts, metadata, {:error, exception}, started_at)
        reraise exception, __STACKTRACE__
    catch
      kind, reason ->
        emit(operation, identifier, opts, metadata, {:error, {kind, reason}}, started_at)
        :erlang.raise(kind, reason, __STACKTRACE__)
    end
  end

  defp emit(operation, identifier, opts, metadata, result, started_at) do
    measurements = %{
      count: 1,
      duration_us:
        System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond)
    }

    metadata =
      metadata
      |> Map.merge(%{operation: operation, stream_key: identifier})
      |> put_backend(opts)
      |> outcome_metadata(result)

    Telemetry.emit([:receipt, operation, :stop], measurements, metadata, opts)
  end

  defp put_backend(metadata, opts) when is_list(opts) do
    case Keyword.get(opts, :backend, :memory) do
      backend when is_atom(backend) and not is_nil(backend) ->
        Map.put(metadata, :backend, backend)

      _backend ->
        metadata
    end
  end

  defp put_backend(metadata, _opts), do: metadata

  defp outcome_metadata(metadata, {:ok, status}) when status in [:appended, :idempotent],
    do: Map.merge(metadata, %{outcome: :ok, status: status})

  defp outcome_metadata(metadata, {:ok, _value}), do: Map.put(metadata, :outcome, :ok)
  defp outcome_metadata(metadata, :not_found), do: Map.put(metadata, :outcome, :not_found)

  defp outcome_metadata(metadata, {:error, reason}),
    do: metadata |> Map.put(:outcome, :error) |> Map.put(:reason, reason)

  defp outcome_metadata(metadata, _result), do: Map.put(metadata, :outcome, :invalid)
end
