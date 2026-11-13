defmodule Spectre.Ledger.ServerStoreTest do
  use ExUnit.Case, async: true
  alias Spectre.Ledger.ServerStore

  defmodule Stack do
    use Spectre.Stack
    install(Spectre.Ledger)
  end

  defmodule Agent do
    use Spectre.Agent, stack: Stack
  end

  @tag :tmp_dir
  test "append log survives restart, fences stale writers and never reuses deleted revisions", %{
    tmp_dir: dir
  } do
    path = Path.join(dir, "server.ledger")
    {:ok, store} = ServerStore.start_link(path: path)
    assert :not_found = ServerStore.fetch(store, :conversation)
    assert {:ok, 1} = ServerStore.put_new(store, :conversation, "snapshot-1")
    assert {:error, :exists} = ServerStore.put_new(store, :conversation, "duplicate")
    assert {:ok, 2} = ServerStore.compare_and_swap(store, :conversation, 1, "snapshot-2")
    assert {:error, :conflict} = ServerStore.compare_and_swap(store, :conversation, 1, "stale")
    GenServer.stop(store)

    {:ok, store} = ServerStore.start_link(path: path)
    assert {:ok, %{revision: 2, value: "snapshot-2"}} = ServerStore.fetch(store, :conversation)
    assert :ok = ServerStore.delete(store, :conversation, 2)
    assert :not_found = ServerStore.fetch(store, :conversation)
    assert {:ok, 4} = ServerStore.put_new(store, :conversation, "snapshot-4")
    assert {:ok, entries} = ServerStore.entries(store, :conversation)
    assert Enum.map(entries, & &1.revision) == [1, 2, 3, 4]
    refute Enum.any?(entries, &Map.has_key?(&1, :value))

    assert {ServerStore, ^store, :conversation} =
             Spectre.Ledger.server_checkpoint_store(store, :conversation)

    GenServer.stop(store)
  end

  @tag :tmp_dir
  test "nonportable input and corrupt or truncated records fail without silent recovery", %{
    tmp_dir: dir
  } do
    path = Path.join(dir, "server.ledger")
    {:ok, store} = ServerStore.start_link(path: path)
    assert {:error, {:invalid_store_value, _}} = ServerStore.put_new(store, :key, self())
    assert {:ok, 1} = ServerStore.put_new(store, :key, %{safe: true})
    GenServer.stop(store)
    contents = File.read!(path)
    File.write!(path, binary_part(contents, 0, byte_size(contents) - 1))
    Process.flag(:trap_exit, true)
    assert {:error, :truncated_store_record} = ServerStore.start_link(path: path)
  end

  @tag :tmp_dir
  test "store capacity and duplicate ownership are explicit", %{tmp_dir: dir} do
    path = Path.join(dir, "server.ledger")
    {:ok, store} = ServerStore.start_link(path: path, max_bytes: 64)
    Process.flag(:trap_exit, true)
    assert {:error, :store_path_in_use} = ServerStore.start_link(path: path)

    assert {:error, :store_capacity_exceeded} =
             ServerStore.put_new(store, :key, String.duplicate("x", 80))

    assert :not_found = ServerStore.fetch(store, :key)
    GenServer.stop(store)
  end

  @tag :tmp_dir
  test "symlink and hardlink aliases cannot create another log writer", %{tmp_dir: dir} do
    path = Path.join(dir, "owner.ledger")
    {:ok, store} = ServerStore.start_link(path: path)
    assert {:ok, 1} = ServerStore.put_new(store, :key, :value)
    symlink = Path.join(dir, "alias.ledger")
    assert :ok = File.ln_s(path, symlink)
    Process.flag(:trap_exit, true)
    assert {:error, :invalid_store_file} = ServerStore.start_link(path: symlink)
    hardlink = Path.join(dir, "hardlink.ledger")
    assert :ok = File.ln(path, hardlink)
    assert {:error, :invalid_store_file} = ServerStore.start_link(path: hardlink)
    GenServer.stop(store)
  end

  @tag :tmp_dir
  test "Stack runtime owns the store and inspection resolves its logical resource", %{
    tmp_dir: dir
  } do
    {:ok, runtime} =
      Spectre.Stack.Runtime.start_link(Stack,
        packages: %{spectre_ledger: [server_store: [path: Path.join(dir, "stack.ledger")]]}
      )

    {:ok, ref} = Spectre.Stack.resolve(Stack, :resource, {:spectre_ledger, :server_store})
    assert {:ok, store} = Spectre.Stack.Runtime.resolve(runtime, ref)
    assert {:ok, 1} = ServerStore.put_new(store, :conversation, "checkpoint")
    context = %{service_context: %{agent: Agent, options: [stack_runtime: runtime]}}

    assert {:ok, %{revision: 1, value: "checkpoint"}} =
             Spectre.Ledger.ServerTool.invoke(:head, %{key: :conversation}, context)

    assert {:ok, [%{revision: 1}]} =
             Spectre.Ledger.ServerTool.invoke(:entries, %{key: :conversation, limit: 10}, context)

    Supervisor.stop(runtime)
    refute Process.alive?(store)
  end
end
