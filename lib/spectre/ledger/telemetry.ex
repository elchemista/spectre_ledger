defmodule Spectre.Ledger.Telemetry do
  @moduledoc """
  Privacy-safe observational telemetry for Ledger operations.

  Events are emitted below `[:spectre, :ledger, ...]`. Measurements are
  numeric aggregation values. Metadata is reduced to a closed set of safe
  operational fields; raw stream keys, checkpoints, backend options, Repo
  configuration, and error terms are never forwarded.

  The initial event inventory is:

    * `[:spectre, :ledger, :bundle, :export, :stop]`
    * `[:spectre, :ledger, :bundle, :verify, :stop]`
    * `[:spectre, :ledger, :checkpoint, :load, :stop]`
    * `[:spectre, :ledger, :checkpoint, :compare_and_swap, :stop]`
    * `[:spectre, :ledger, :checkpoint, :migrate, :stop]`
    * `[:spectre, :ledger, :receipt, :append, :stop]`
    * `[:spectre, :ledger, :receipt, :lookup, :stop]`
    * `[:spectre, :ledger, :receipt, :put_payload, :stop]`
    * `[:spectre, :ledger, :receipt, :get_payload, :stop]`
    * `[:spectre, :ledger, :doctor, :stop]`

  Backends may use the same boundary for append and load observations.
  Telemetry remains observational because `Spectre.Telemetry` contains handler
  failures and never changes the operation result.

  Passing `telemetry: false` suppresses both the custom handler and the
  standard `:telemetry` sink. Omitting the option, or passing `true`, preserves
  normal emission.
  """

  alias Spectre.Canonical.Value

  @measurement_keys [:byte_count, :count, :duration_us, :entry_count, :object_count]
  @atom_metadata_keys [:operation, :outcome, :receipt_kind, :status]
  @integer_metadata_keys [
    :bundle_version,
    :expected_revision,
    :head_revision,
    :canonical_revision,
    :revision,
    :sequence,
    :schema_version,
    :table_count
  ]

  @doc "Emits one redacted Ledger event through Spectre's telemetry boundary."
  @spec emit(atom() | [atom()], map(), map(), keyword()) :: :ok
  def emit(event, measurements \\ %{}, metadata \\ %{}, opts \\ [])

  def emit(event, measurements, metadata, opts)
      when is_map(measurements) and is_map(metadata) and is_list(opts) do
    if telemetry_disabled?(opts) do
      :ok
    else
      case event_path(event) do
        {:ok, path} ->
          Spectre.Telemetry.emit(
            [:ledger | path],
            safe_measurements(measurements),
            safe_metadata(metadata),
            telemetry_options(opts)
          )

        :error ->
          :ok
      end
    end
  end

  def emit(_event, _measurements, _metadata, _opts), do: :ok

  defp telemetry_disabled?(opts) do
    Keyword.keyword?(opts) and Keyword.get(opts, :telemetry, true) == false
  end

  @doc "Returns a stable digest for an identifier without exposing its value."
  @spec id_digest(term()) :: String.t()
  def id_digest(value) do
    case Value.digest(value) do
      {:ok, digest} -> digest
      {:error, _reason} -> "unavailable"
    end
  end

  @doc "Reduces an arbitrary backend or validation failure to an atom class."
  @spec reason_class(term()) :: atom()
  def reason_class(%{kind: kind}) when is_atom(kind), do: kind

  def reason_class(reason) when is_tuple(reason) and tuple_size(reason) > 0 do
    case elem(reason, 0) do
      kind when is_atom(kind) -> kind
      _other -> :error
    end
  end

  def reason_class(reason) when is_atom(reason), do: reason
  def reason_class(%{__struct__: _module}), do: :exception
  def reason_class(_reason), do: :error

  @spec event_path(term()) :: {:ok, [atom()]} | :error
  defp event_path(event) when is_atom(event) and not is_nil(event), do: {:ok, [event]}

  defp event_path(event) when is_list(event) and event != [] do
    if Enum.all?(event, &(is_atom(&1) and not is_nil(&1))), do: {:ok, event}, else: :error
  end

  defp event_path(_event), do: :error

  @spec safe_measurements(map()) :: map()
  defp safe_measurements(measurements) do
    Map.new(@measurement_keys, fn key -> {key, numeric(Map.get(measurements, key, 0))} end)
  end

  defp numeric(value) when is_number(value), do: value
  defp numeric(_value), do: 0

  @spec safe_metadata(map()) :: map()
  defp safe_metadata(metadata) do
    %{}
    |> copy_atom_metadata(metadata)
    |> copy_integer_metadata(metadata)
    |> put_backend(metadata)
    |> put_stream_id(metadata)
    |> put_reason_class(metadata)
  end

  defp copy_atom_metadata(result, metadata) do
    Enum.reduce(@atom_metadata_keys, result, fn key, current ->
      case Map.get(metadata, key) do
        value when is_atom(value) and not is_nil(value) -> Map.put(current, key, value)
        _other -> current
      end
    end)
  end

  defp copy_integer_metadata(result, metadata) do
    Enum.reduce(@integer_metadata_keys, result, fn key, current ->
      case Map.get(metadata, key) do
        value when is_integer(value) and value >= 0 -> Map.put(current, key, value)
        _other -> current
      end
    end)
  end

  defp put_backend(result, metadata) do
    case Map.get(metadata, :backend) do
      backend when is_atom(backend) and not is_nil(backend) ->
        Map.put(result, :backend, inspect(backend))

      _other ->
        result
    end
  end

  defp put_stream_id(result, metadata) do
    case Map.fetch(metadata, :stream_key) do
      {:ok, stream_key} -> Map.put(result, :stream_id, id_digest(stream_key))
      :error -> result
    end
  end

  defp put_reason_class(result, metadata) do
    cond do
      Map.has_key?(metadata, :reason) ->
        Map.put(result, :reason_class, reason_class(metadata.reason))

      is_atom(Map.get(metadata, :reason_class)) and not is_nil(Map.get(metadata, :reason_class)) ->
        Map.put(result, :reason_class, Map.get(metadata, :reason_class))

      true ->
        result
    end
  end

  defp telemetry_options(opts) do
    if Keyword.keyword?(opts) do
      case Keyword.fetch(opts, :telemetry_handler) do
        {:ok, handler} -> [telemetry_handler: handler]
        :error -> []
      end
    else
      []
    end
  end
end
