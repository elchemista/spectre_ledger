defmodule Spectre.Ledger.ServerTool do
  @moduledoc "Read-only inspection of an installed AgentServer coordination ledger."
  alias Spectre.Ledger.ServerStore
  alias Spectre.Stack.Runtime

  def tools(_config) do
    %{
      ledger: %{
        contract: 1,
        adapter: __MODULE__,
        default: nil,
        operations: %{
          head: %{path: :cognitive, retry: :safe, max_bytes: 16_777_216},
          entries: %{path: :cognitive, retry: :safe, max_bytes: 1_048_576}
        }
      }
    }
  end

  def prepare(:head, [key], _trusted), do: {:ok, %{key: key}}
  def prepare(:entries, [key], _trusted), do: {:ok, %{key: key, limit: 50}}

  def prepare(:entries, [key, limit], _trusted)
      when is_integer(limit) and limit > 0 and limit <= 1000,
      do: {:ok, %{key: key, limit: limit}}

  def prepare(_, _, _), do: {:error, :invalid_ledger_tool_request}

  def invoke(operation, args, context) do
    with {:ok, store} <- store(context) do
      case operation do
        :head -> ServerStore.fetch(store, args.key)
        :entries -> ServerStore.entries(store, args.key, args.limit)
      end
    end
  end

  def project(_operation, result), do: {:ok, result}

  defp store(context) do
    case Keyword.fetch(context.service_context.options, :ledger_server_store) do
      {:ok, store} ->
        {:ok, store}

      :error ->
        with runtime when not is_nil(runtime) <- context.service_context.options[:stack_runtime],
             {:ok, definition} <- Spectre.Definition.fetch(context.service_context.agent),
             {:ok, ref} <-
               Spectre.Stack.resolve(
                 definition.stack,
                 :resource,
                 {:spectre_ledger, :server_store}
               ) do
          Runtime.resolve(runtime, ref)
        else
          nil -> {:error, :ledger_server_store_required}
          {:error, _} = error -> error
        end
    end
  end
end
