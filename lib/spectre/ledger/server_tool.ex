defmodule Spectre.Ledger.ServerTool do
  @moduledoc "Read-only metadata for the current AgentServer checkpoint, fenced by its trusted reference."
  alias Spectre.Server.Checkpoint

  def tools(_config) do
    %{
      ledger: %{
        contract: 1,
        adapter: __MODULE__,
        default: nil,
        operations: %{
          head: %{path: :cognitive, retry: :safe, max_bytes: 65_536},
          entries: %{path: :cognitive, retry: :safe, max_bytes: 1_048_576}
        }
      }
    }
  end

  def prepare(:head, [], _trusted), do: {:ok, %{scope: :current}}
  def prepare(:head, [key], _trusted), do: {:ok, %{key: key}}
  def prepare(:entries, [], _trusted), do: {:ok, %{scope: :current, limit: 50}}
  def prepare(:entries, [key], _trusted), do: {:ok, %{key: key, limit: 50}}

  def prepare(:entries, [key, limit], _trusted)
      when is_integer(limit) and limit > 0 and limit <= 1000,
      do: {:ok, %{key: key, limit: limit}}

  def prepare(_, _, _), do: {:error, :invalid_ledger_tool_request}

  def invoke(operation, args, context) do
    with {:ok, ref} <- current_reference(context, args) do
      case operation do
        :head -> head(ref)
        :entries -> entries(ref, args.limit)
      end
    end
  end

  def project(_operation, result), do: {:ok, result}

  defp current_reference(%{checkpoint_ref: %{adapter: adapter, store: _, key: key} = ref}, args)
       when is_atom(adapter) do
    if Map.get(args, :scope) == :current or Map.fetch(args, :key) == {:ok, key},
      do: {:ok, ref},
      else: {:error, :checkpoint_scope_mismatch}
  end

  defp current_reference(_, _), do: {:error, :server_checkpoint_required}

  defp head(%{adapter: adapter, store: store, key: key}) do
    case adapter.fetch(store, key) do
      {:ok, %{revision: revision, value: value}} ->
        with {:ok, bytes} <- checkpoint_bytes(value) do
          {:ok,
           %{
             revision: revision,
             bytes: byte_size(bytes),
             digest: Base.encode16(:crypto.hash(:sha256, bytes), case: :lower)
           }}
        end

      other ->
        other
    end
  end

  defp entries(%{adapter: adapter, store: store, key: key}, limit) do
    if function_exported?(adapter, :entries, 3),
      do: adapter.entries(store, key, limit),
      else: {:error, :checkpoint_history_unavailable}
  end

  defp checkpoint_bytes(value) when is_binary(value), do: {:ok, value}
  defp checkpoint_bytes(value), do: Checkpoint.encode_value(value)
end
