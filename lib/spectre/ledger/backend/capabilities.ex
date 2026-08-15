defmodule Spectre.Ledger.Backend.Capabilities do
  @moduledoc false

  @callbacks %{
    checkpoint_store: [load: 2, compare_and_swap: 2],
    checkpoint_archive: [head: 2, entries: 3, objects: 3, put_stream: 4],
    receipt_sink: [
      append_receipt: 2,
      lookup_receipt: 2,
      put_receipt_payload: 2,
      get_receipt_payload: 2
    ],
    receipt_archive: [receipt_entries: 3, receipt_objects: 3]
  }

  @type capability ::
          :checkpoint_store | :checkpoint_archive | :receipt_sink | :receipt_archive

  @spec validate(module(), capability()) :: :ok | {:error, term()}
  def validate(module, capability) when is_atom(module) and is_map_key(@callbacks, capability) do
    cond do
      not Code.ensure_loaded?(module) ->
        {:error, {:ledger_backend_not_loaded, module}}

      missing = missing_callback(module, capability) ->
        {function, arity} = missing
        {:error, {:ledger_backend_callback_missing, module, function, arity}}

      true ->
        :ok
    end
  end

  def validate(_module, _capability), do: {:error, :invalid_ledger_backend_capability}

  @spec complete?(module(), capability()) :: boolean()
  def complete?(module, capability), do: validate(module, capability) == :ok

  @spec callbacks(capability()) :: keyword()
  def callbacks(capability), do: Map.fetch!(@callbacks, capability)

  defp missing_callback(module, capability) do
    Enum.find(callbacks(capability), fn {function, arity} ->
      not function_exported?(module, function, arity)
    end)
  end
end
