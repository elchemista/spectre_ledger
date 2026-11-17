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
  test "indexed pages survive compaction, restart and fenced deletion", %{tmp_dir: dir} do
    path = Path.join(dir, "pages.ledger")
    {:ok, store} = ServerStore.start_link(path: path)
    assert :not_configured = ServerStore.gam_ledger(store)
    for n <- 1..5, do: assert({:ok, _} = ServerStore.put_new(store, {:inbox, n}, n))
    {:ok, [{_, entry}], cursor} = ServerStore.list(store, {:inbox}, limit: 1)
    assert entry.value == 1
    :ok = ServerStore.compact(store)
    GenServer.stop(store)
    {:ok, restored} = ServerStore.start_link(path: path)
    assert {:ok, rest, nil} = ServerStore.list(restored, {:inbox}, after: cursor)
    assert Enum.map(rest, fn {_, entry} -> entry.value end) == [2, 3, 4, 5]
    assert {:error, :invalid_limit} = ServerStore.entries(restored, {:inbox}, limit: 0)
    GenServer.stop(restored)
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
    {:ok, _} = ServerStore.put_new(store, :other, "another conversation secret")
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
  test "prepared metadata requests remain scoped and reject unbounded history", %{tmp_dir: dir} do
    store = start_supervised!({ServerStore, path: Path.join(dir, "prepared.ledger")})
    value = %{private: "application checkpoint"}
    {:ok, 1} = ServerStore.put_new(store, :own, value)
    context = %{checkpoint_ref: %{adapter: ServerStore, store: store, key: :own}}
    {:ok, bytes} = Spectre.Server.Checkpoint.encode_value(value)

    for args <- [[], [:own]] do
      assert {:ok, request} = Spectre.Ledger.ServerTool.prepare(:head, args, %{})
      assert {:ok, metadata} = Spectre.Ledger.ServerTool.invoke(:head, request, context)
      assert metadata.bytes == byte_size(bytes)
      assert Map.keys(metadata) |> Enum.sort() == [:bytes, :digest, :revision]
      assert {:ok, ^metadata} = Spectre.Ledger.ServerTool.project(:head, metadata)
    end

    for args <- [[], [:own], [:own, 1]] do
      assert {:ok, request} = Spectre.Ledger.ServerTool.prepare(:entries, args, %{})

      assert {:ok, [%{revision: 1}]} =
               Spectre.Ledger.ServerTool.invoke(:entries, request, context)
    end

    assert {:ok, other} = Spectre.Ledger.ServerTool.prepare(:head, [:other], %{})

    assert {:error, :checkpoint_scope_mismatch} =
             Spectre.Ledger.ServerTool.invoke(:head, other, context)

    for {operation, args} <- [{:entries, [:own, 0]}, {:entries, [:own, 1001]}, {:delete, [:own]}] do
      assert {:error, :invalid_ledger_tool_request} =
               Spectre.Ledger.ServerTool.prepare(operation, args, %{})
    end
  end

  @tag :tmp_dir
  test "missing checkpoints and backends without history stay explicit", %{tmp_dir: dir} do
    store = start_supervised!({ServerStore, path: Path.join(dir, "missing.ledger")})
    context = %{checkpoint_ref: %{adapter: ServerStore, store: store, key: :missing}}
    assert :not_found = Spectre.Ledger.ServerTool.invoke(:head, %{scope: :current}, context)

    without_history = put_in(context.checkpoint_ref.adapter, Spectre.Server.Store.Disk)

    assert {:error, :checkpoint_history_unavailable} =
             Spectre.Ledger.ServerTool.invoke(
               :entries,
               %{scope: :current, limit: 1},
               without_history
             )
  end

  @tag :tmp_dir
  test "automatic compaction preserves live facts and revision fences across a kill", %{
    tmp_dir: dir
  } do
    path = Path.join(dir, "compact.ledger")
    {:ok, store} = ServerStore.start_link(path: path, max_bytes: 12_000)
    {:ok, 1} = ServerStore.put_new(store, {:gam, :fact}, %{admitted: true})
    {:ok, deleted_revision} = ServerStore.put_new(store, :deleted, "retired-private-payload")
    assert :ok = ServerStore.delete(store, :deleted, deleted_revision)
    assert File.read!(path) =~ "retired-private-payload"
    {:ok, first_revision} = ServerStore.put_new(store, :current, String.duplicate("a", 1500))

    for expected <- first_revision..(first_revision + 48) do
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
    final_revision = first_revision + 49
    assert {:ok, %{revision: ^final_revision}} = ServerStore.fetch(restored, :current)
    assert {:ok, %{value: %{admitted: true}}} = ServerStore.fetch(restored, {:gam, :fact})
    assert :not_found = ServerStore.fetch(restored, :deleted)
    assert {:ok, recreated_revision} = ServerStore.put_new(restored, :deleted, "new")
    assert recreated_revision > final_revision

    assert {:error, :conflict} =
             ServerStore.compare_and_swap(restored, :current, final_revision - 1, "stale")

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

  @tag :tmp_dir
  test "released journal keys disappear from memory and disk without losing the global ABA fence",
       %{tmp_dir: dir} do
    path = Path.join(dir, "bounded.ledger")
    {:ok, store} = ServerStore.start_link(path: path, max_bytes: 4000)
    {:ok, live_revision} = ServerStore.put_new(store, :live_fact, %{done: true})

    for index <- 1..500 do
      {:ok, revision} = ServerStore.put_new(store, {:journal, index}, :pending)
      assert :ok = ServerStore.delete(store, {:journal, index}, revision)

      if rem(index, 25) == 0 do
        assert :ok = ServerStore.compact(store)
        state = :sys.get_state(store)
        assert map_size(state.entries) == 1
        assert length(state.history) == 1
        assert File.stat!(path).size < 1000
      end
    end

    assert :ok = ServerStore.delete(store, :live_fact, live_revision)
    assert :ok = ServerStore.compact(store)
    state = :sys.get_state(store)
    assert state.entries == %{}
    assert state.history == []
    assert File.stat!(path).size < 512
    watermark = state.sequence
    GenServer.stop(store)
    {:ok, restored} = ServerStore.start_link(path: path, max_bytes: 4000)
    assert {:ok, next_revision} = ServerStore.put_new(restored, {:journal, 1}, :new)
    assert next_revision == watermark + 1
    assert {:error, :conflict} = ServerStore.compare_and_swap(restored, {:journal, 1}, 2, :stale)
    assert {:error, :conflict} = ServerStore.delete(restored, {:journal, 1}, 2)
    assert {:ok, %{value: :new}} = ServerStore.fetch(restored, {:journal, 1})
    GenServer.stop(restored)
  end

  @tag :tmp_dir
  test "SPLS1 and checkpoint-v1 preserve old per-key histories then accept globally fenced recreation",
       %{tmp_dir: dir} do
    records = [
      %{sequence: 1, action: :put, key: :a, revision: 1, value: :a},
      %{sequence: 2, action: :put, key: :b, revision: 1, value: :b},
      %{sequence: 3, action: :delete, key: :a, revision: 2, value: nil}
    ]

    legacy_log = ["SPLS1" | Enum.map(records, &frame/1)]

    checkpoint_v1 = [
      "SPLS2",
      frame(%{checkpoint: 1, sequence: 3, count: 2}),
      frame(%{entry: Enum.at(records, 1)}),
      frame(%{entry: Enum.at(records, 2)})
    ]

    for {name, bytes} <- [{"v1", legacy_log}, {"v2", checkpoint_v1}] do
      path = Path.join(dir, name <> ".ledger")
      File.write!(path, bytes)
      {:ok, store} = ServerStore.start_link(path: path)
      assert {:ok, 4} = ServerStore.put_new(store, :a, :recreated)
      assert {:ok, 5} = ServerStore.put_new(store, :c, :fresh)
      assert {:ok, 2} = ServerStore.compare_and_swap(store, :b, 1, :updated)
      GenServer.stop(store)
      {:ok, store} = ServerStore.start_link(path: path)
      assert {:ok, %{revision: 4, value: :recreated}} = ServerStore.fetch(store, :a)
      assert {:ok, %{revision: 2, value: :updated}} = ServerStore.fetch(store, :b)
      assert :ok = ServerStore.compact(store)
      GenServer.stop(store)

      <<"SPLS2", size::unsigned-big-32, _digest::binary-size(32), header::binary-size(size),
        _::binary>> = File.read!(path)

      assert {:ok, %{checkpoint: 2}} = Spectre.Server.Checkpoint.decode_snapshot(header)
      {:ok, store} = ServerStore.start_link(path: path)
      assert {:error, :conflict} = ServerStore.compare_and_swap(store, :a, 1, :stale)
      assert {:ok, %{revision: 5, value: :fresh}} = ServerStore.fetch(store, :c)
      GenServer.stop(store)
    end
  end

  @tag :tmp_dir
  test "new checkpoint replay rejects ABA fresh revisions and CAS jumps on existing keys", %{
    tmp_dir: dir
  } do
    Process.flag(:trap_exit, true)

    for {name, compact?} <- [{"fresh", true}, {"cas", false}] do
      path = Path.join(dir, name <> ".ledger")
      {:ok, store} = ServerStore.start_link(path: path)
      {:ok, 1} = ServerStore.put_new(store, :key, :original)

      if compact? do
        :ok = ServerStore.delete(store, :key, 1)
        :ok = ServerStore.compact(store)
      else
        {:ok, 2} = ServerStore.put_new(store, :other, :other)
      end

      GenServer.stop(store)
      invalid_revision = if compact?, do: 1, else: 3

      File.write!(
        path,
        frame(%{
          sequence: 3,
          action: :put,
          key: :key,
          revision: invalid_revision,
          value: :invalid
        }),
        [:append]
      )

      assert {:error, :corrupt_store_record} = ServerStore.start_link(path: path)
    end
  end

  @tag :tmp_dir
  test "reads wait for a busy store beyond the old five second timeout", %{tmp_dir: dir} do
    store = start_supervised!({ServerStore, path: Path.join(dir, "busy.ledger")})
    {:ok, 1} = ServerStore.put_new(store, :key, :value)
    :sys.suspend(store)

    try do
      fetch = Task.async(fn -> ServerStore.fetch(store, :key) end)
      entries = Task.async(fn -> ServerStore.entries(store, :key) end)
      Process.sleep(5100)
      assert Task.yield(fetch, 0) == nil
      assert Task.yield(entries, 0) == nil
      :sys.resume(store)
      assert {:ok, %{revision: 1, value: :value}} = Task.await(fetch)
      assert {:ok, [%{revision: 1}]} = Task.await(entries)
    after
      :sys.resume(store)
    end
  end

  defp frame(value) do
    {:ok, body} = Spectre.Server.Checkpoint.encode_value(value)
    <<byte_size(body)::unsigned-big-32, :crypto.hash(:sha256, body)::binary, body::binary>>
  end
end
