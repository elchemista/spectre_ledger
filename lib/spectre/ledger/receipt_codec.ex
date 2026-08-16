defmodule Spectre.Ledger.ReceiptCodec do
  @moduledoc false

  alias Spectre.Receipt.Envelope
  alias Spectre.Run.Value, as: RunValue

  @field_pairs Envelope.__struct__()
               |> Map.keys()
               |> List.delete(:__struct__)
               |> Map.new(fn field -> {Atom.to_string(field), field} end)

  @field_names Map.keys(@field_pairs)

  @spec encode(Envelope.t()) :: {:ok, binary()} | {:error, term()}
  def encode(%Envelope{} = envelope) do
    with {:ok, ^envelope} <- Envelope.new(envelope),
         data <- Envelope.to_data(envelope),
         {:ok, encoded} <- canonical_json(data) do
      {:ok, encoded}
    else
      {:ok, _normalized} -> {:error, :noncanonical_ledger_receipt}
      {:error, _reason} -> {:error, :invalid_ledger_receipt}
    end
  rescue
    _exception -> {:error, :invalid_ledger_receipt}
  catch
    _kind, _reason -> {:error, :invalid_ledger_receipt}
  end

  def encode(_envelope), do: {:error, :invalid_ledger_receipt}

  @spec decode(binary(), pos_integer()) :: {:ok, Envelope.t()} | {:error, term()}
  def decode(encoded, max_bytes)
      when is_binary(encoded) and is_integer(max_bytes) and max_bytes > 0 do
    with :ok <- encoded_size(encoded, max_bytes),
         {:ok, ordered} <- decode_json(encoded),
         {:ok, data} <- ordered_to_plain(ordered),
         :ok <- exact_fields(data),
         :ok <- RunValue.prepare(data),
         {:ok, attrs} <- decode_fields(data),
         {:ok, envelope} <- Envelope.new(attrs),
         true <- Envelope.to_data(envelope) == data,
         {:ok, canonical} <- canonical_json(data),
         true <- canonical == encoded do
      {:ok, envelope}
    else
      false -> {:error, :noncanonical_ledger_receipt_encoding}
      {:error, _reason} = error -> error
      _invalid -> {:error, :invalid_ledger_receipt_encoding}
    end
  rescue
    _exception -> {:error, :invalid_ledger_receipt_encoding}
  catch
    _kind, _reason -> {:error, :invalid_ledger_receipt_encoding}
  end

  def decode(_encoded, _max_bytes), do: {:error, :invalid_ledger_receipt_encoding}

  @spec verify(binary(), String.t(), String.t(), pos_integer()) ::
          {:ok, Envelope.t()} | {:error, term()}
  def verify(encoded, receipt_id, envelope_digest, max_bytes) do
    with {:ok, %Envelope{id: ^receipt_id} = envelope} <- decode(encoded, max_bytes),
         true <- Envelope.digest(envelope) == envelope_digest do
      {:ok, envelope}
    else
      {:ok, %Envelope{}} -> {:error, :ledger_receipt_id_mismatch}
      false -> {:error, :ledger_receipt_digest_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp decode_fields(data) do
    Enum.reduce_while(@field_pairs, {:ok, %{}}, fn {encoded_key, field}, {:ok, attrs} ->
      case RunValue.decode(Map.fetch!(data, encoded_key)) do
        {:ok, value} -> {:cont, {:ok, Map.put(attrs, field, value)}}
        {:error, _reason} -> {:halt, {:error, {:invalid_ledger_receipt_field, field}}}
      end
    end)
  end

  defp exact_fields(data) when is_map(data) and not is_struct(data) do
    if map_size(data) == length(@field_names) and
         Enum.all?(@field_names, &Map.has_key?(data, &1)),
       do: :ok,
       else: {:error, :invalid_ledger_receipt_fields}
  end

  defp exact_fields(_data), do: {:error, :invalid_ledger_receipt_fields}

  defp canonical_json(data) do
    case Jason.encode(ordered(data), maps: :strict) do
      {:ok, encoded} -> {:ok, encoded}
      {:error, _reason} -> {:error, :invalid_ledger_receipt_encoding}
    end
  end

  defp ordered(value) when is_map(value) and not is_struct(value) do
    value
    |> Enum.sort_by(fn {key, _item} -> key end)
    |> Enum.map(fn {key, item} -> {key, ordered(item)} end)
    |> Jason.OrderedObject.new()
  end

  defp ordered(value) when is_list(value), do: Enum.map(value, &ordered/1)
  defp ordered(value), do: value

  defp decode_json(encoded) do
    case Jason.decode(encoded, objects: :ordered_objects, strings: :copy) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, _reason} -> {:error, :invalid_ledger_receipt_json}
    end
  end

  defp ordered_to_plain(%Jason.OrderedObject{values: values}) do
    keys = Enum.map(values, &elem(&1, 0))

    if length(keys) == length(Enum.uniq(keys)) do
      Enum.reduce_while(values, {:ok, %{}}, &plain_object_field/2)
    else
      {:error, :duplicate_ledger_receipt_json_key}
    end
  end

  defp ordered_to_plain(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, result} ->
      case ordered_to_plain(value) do
        {:ok, plain} -> {:cont, {:ok, [plain | result]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, result} -> {:ok, Enum.reverse(result)}
      {:error, _reason} = error -> error
    end
  end

  defp ordered_to_plain(value), do: {:ok, value}

  defp plain_object_field({key, value}, {:ok, result}) do
    case ordered_to_plain(value) do
      {:ok, plain} -> {:cont, {:ok, Map.put(result, key, plain)}}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp encoded_size(encoded, max_bytes)
       when byte_size(encoded) > 0 and byte_size(encoded) <= max_bytes,
       do: :ok

  defp encoded_size(_encoded, max_bytes),
    do: {:error, {:ledger_receipt_too_large, max_bytes}}
end
