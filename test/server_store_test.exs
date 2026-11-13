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
    {:ok, store} = ServerStore.start_link(path: path, max_bytes: 512)
    Process.flag(:trap_exit, true)
    assert {:error, :store_path_in_use} = ServerStore.start_link(path: path)

    assert {:error, :store_capacity_exceeded} =
             ServerStore.put_new(store, :key, String.duplicate("x", 2000))

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
    context = %{checkpoint_ref: %{adapter: ServerStore, store: store, key: :conversation}}

    assert {:ok, %{revision: 1, bytes: 10, digest: _}} =
             Spectre.Ledger.ServerTool.invoke(:head, %{key: :conversation}, context)

    assert {:ok, [%{revision: 1}]} =
             Spectre.Ledger.ServerTool.invoke(:entries, %{key: :conversation, limit: 10}, context)

    Supervisor.stop(runtime)
    refute Process.alive?(store)
  end

  @tag :tmp_dir
  test "metadata inspection cannot select another conversation or return its bytes", %{
    tmp_dir: dir
  } do
    store = start_supervised!({ServerStore, path: Path.join(dir, "scope.ledger")})
    {:ok, 1} = ServerStore.put_new(store, :own, "private own checkpoint")
    {:ok, 1} = ServerStore.put_new(store, :other, "another conversation secret")
    context = %{checkpoint_ref: %{adapter: ServerStore, store: store, key: :own}}
    assert {:ok, result} = Spectre.Ledger.ServerTool.invoke(:head, %{scope: :current}, context)
    assert Map.keys(result) |> Enum.sort() == [:bytes, :digest, :revision]
    refute inspect(result) =~ "private"

    assert {:error, :checkpoint_scope_mismatch} =
             Spectre.Ledger.ServerTool.invoke(:head, %{key: :other}, context)

    assert {:error, :checkpoint_scope_mismatch} =
             Spectre.Ledger.ServerTool.invoke(:entries, %{key: :other, limit: 1}, context)

    assert {:error, :server_checkpoint_required} =
             Spectre.Ledger.ServerTool.invoke(:head, %{scope: :current}, %{})
  end

  @tag :tmp_dir
  test "automatic compaction preserves all live facts and tombstone revisions across a kill", %{
    tmp_dir: dir
  } do
    path = Path.join(dir, "compact.ledger")
    {:ok, store} = ServerStore.start_link(path: path, max_bytes: 12_000)
    {:ok, 1} = ServerStore.put_new(store, {:gam, :fact}, %{admitted: true})
    {:ok, 1} = ServerStore.put_new(store, :deleted, "retired-private-payload")
    assert :ok = ServerStore.delete(store, :deleted, 1)
    assert File.read!(path) =~ "retired-private-payload"
    {:ok, 1} = ServerStore.put_new(store, :current, String.duplicate("a", 1500))

    for expected <- 1..49 do
      assert {:ok, next} =
               ServerStore.compare_and_swap(
                 store,
                 :current,
                 expected,
                 String.duplicate("b", 1500)
               )

      assert next == expected + 1
      assert File.stat!(path).size <= 12_000
    end

    assert :ok = ServerStore.compact(store)
    refute File.read!(path) =~ "retired-private-payload"
    Process.unlink(store)
    monitor = Process.monitor(store)
    Process.exit(store, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^store, :killed}
    {:ok, restored} = ServerStore.start_link(path: path, max_bytes: 12_000)
    assert {:ok, %{revision: 50}} = ServerStore.fetch(restored, :current)
    assert {:ok, %{value: %{admitted: true}}} = ServerStore.fetch(restored, {:gam, :fact})
    assert :not_found = ServerStore.fetch(restored, :deleted)
    assert {:ok, 3} = ServerStore.put_new(restored, :deleted, "new")
    assert {:error, :conflict} = ServerStore.compare_and_swap(restored, :current, 49, "stale")
    GenServer.stop(restored)
  end

  @tag :tmp_dir
  test "capacity rejection keeps the previous acknowledged snapshot and orphan temp files are harmless",
       %{tmp_dir: dir} do
    path = Path.join(dir, "atomic.ledger")
    {:ok, store} = ServerStore.start_link(path: path, max_bytes: 2000)
    assert <<"SPLS2", _::binary>> = File.read!(path)
    assert Bitwise.band(File.stat!(path).mode, 0o777) == 0o600
    {:ok, 1} = ServerStore.put_new(store, :key, "original")

    assert {:error, :store_capacity_exceeded} =
             ServerStore.compare_and_swap(store, :key, 1, String.duplicate("x", 3000))

    assert {:ok, %{revision: 1, value: "original"}} = ServerStore.fetch(store, :key)
    GenServer.stop(store)
    File.write!(path <> ".compact-orphan", "partial unacknowledged temporary file")
    {:ok, restored} = ServerStore.start_link(path: path, max_bytes: 2000)
    assert {:ok, %{revision: 1, value: "original"}} = ServerStore.fetch(restored, :key)
    assert :ok = ServerStore.compact(restored)
    assert Bitwise.band(File.stat!(path).mode, 0o777) == 0o600
    GenServer.stop(restored)
  end

  @tag :tmp_dir
  test "legacy append files upgrade atomically without resetting revisions", %{tmp_dir: dir} do
    path = Path.join(dir, "legacy.ledger")
    record = %{sequence: 1, action: :put, key: :legacy, revision: 1, value: :preserved}
    {:ok, body} = Spectre.Server.Checkpoint.encode_value(record)

    File.write!(
      path,
      <<"SPLS1", byte_size(body)::unsigned-big-32, :crypto.hash(:sha256, body)::binary,
        body::binary>>
    )

    {:ok, store} = ServerStore.start_link(path: path)
    assert {:ok, %{revision: 1, value: :preserved}} = ServerStore.fetch(store, :legacy)
    assert :ok = ServerStore.compact(store)
    assert <<"SPLS2", _::binary>> = File.read!(path)
    assert {:ok, 2} = ServerStore.compare_and_swap(store, :legacy, 1, :updated)
    GenServer.stop(store)
    {:ok, store} = ServerStore.start_link(path: path)
    assert {:ok, %{revision: 2, value: :updated}} = ServerStore.fetch(store, :legacy)
    GenServer.stop(store)
  end
end
