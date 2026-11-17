Code.require_file("support/postgres_cluster_test.exs", __DIR__)

defmodule SpectreLedger.ServerStoreTestRepo do
  use Ecto.Repo, otp_app: :spectre_ledger, adapter: Ecto.Adapters.Postgres
end

defmodule SpectreLedger.PostgresServerStoreTest do
  use ExUnit.Case, async: false
  alias Ecto.Adapters.SQL
  alias Spectre.GAM.Canonical.Value
  alias Spectre.GAM.Domain.Projection
  alias Spectre.GAM.Domain.Projection.Rows
  alias Spectre.GAM.Domain.Projection.Store, as: ProjectionStore
  alias Spectre.GAM.Ledger
  alias Spectre.Ledger.ServerStore
  alias Spectre.Ledger.ServerStore.Postgres
  alias Spectre.Server.Checkpoint
  alias Spectre.Server.GAM.Managed
  alias SpectreLedger.ServerStoreTestRepo, as: Repo
  alias SpectreLedger.TestPostgresCluster, as: Cluster

  defmodule MemoryRecovery do
    @behaviour Spectre.GAM.Domain.Recovery.Policy
    def check(_, _), do: :ok

    def recover(incident, _) do
      send(SpectreLedger.PostgresServerStoreTest, {:storage_incident, incident})
      :memory
    end
  end

  defmodule Transport do
    def send_message(message, _) do
      send(SpectreLedger.PostgresServerStoreTest, {:approval_wire, message})
      {:ok, :accepted}
    end
  end

  defmodule Effect do
    def execute_effect(:open, arguments, _) do
      send(SpectreLedger.PostgresServerStoreTest, {:opened, arguments})
      {:ok, %{opened: true}}
    end
  end

  defmodule Effects do
    use Spectre.Stack.Installable, id: :store_approval_test, version: "0.1.0", spectre: "~> 0.4.0"

    def effects(_),
      do: %{
        open: %{
          contract: 1,
          adapter: Effect,
          operation: :open,
          side_effect: :non_idempotent,
          material: :arguments,
          partition: [],
          schema_hash: nil,
          config: %{}
        }
      }
  end

  defmodule Stack do
    use Spectre.Stack
    install(Spectre.Ledger)
    install(Effects)
  end

  defmodule App do
    use Spectre.Server

    def handle_flow({:browser, :open}, input, ctx),
      do: propose(ctx, :open, %{url: input.text}, on_result: :opened)

    def handle_result(:opened, result, ctx) do
      send(SpectreLedger.PostgresServerStoreTest, {:server_settled, self(), result})
      no_response(ctx)
    end

    def handle_result(tag, result, ctx), do: super(tag, result, ctx)

    def handle_approval_inbox(event, ctx) do
      send(SpectreLedger.PostgresServerStoreTest, {:approval_policy, self(), event.event})
      put_private(ctx, :approval_events, Map.get(ctx.private, :approval_events, 0) + 1)
    end
  end

  defmodule Agent do
    use Spectre.Agent, stack: Stack, server: App

    flow :browser do
      on(:open, regex: ~r/.+/)
    end
  end

  defmodule AgentGroup do
    use Supervisor
    def start_link(children), do: Supervisor.start_link(__MODULE__, children)
    def init(children), do: Supervisor.init(children, strategy: :one_for_one)
  end

  setup_all do
    cluster = Cluster.start!(Repo)
    on_exit(fn -> Cluster.stop(cluster) end)
    %{cluster: cluster}
  end

  setup do
    table = "server_store_#{System.unique_integer([:positive])}"
    {:ok, statements} = Postgres.migration_sql(table: table)
    Enum.each(statements, &SQL.query!(Repo, &1, [], log: false))

    {:ok, %{up: statements}} =
      Spectre.GAM.Ledger.Store.Postgres.migration_sql(table_prefix: table <> "_gam")

    Enum.each(statements, &SQL.query!(Repo, &1, [], log: false))
    opts = [backend: :postgres, repo: Repo, table: table, gam_table_prefix: table <> "_gam"]

    {:ok, %{up: projection}} =
      Spectre.Ledger.DomainStore.migration_sql(table_prefix: table <> "_gam")

    Enum.each(projection, &SQL.query!(Repo, &1, [], log: false))
    %{store: start_supervised!({ServerStore, opts}), opts: opts, table: table}
  end

  test "unique insertion, fenced transitions and tombstones survive adapter restart", c do
    key = {:approval, "owner", "id", "peer"}

    results =
      1..24
      |> Task.async_stream(fn _ -> ServerStore.put_new(c.store, key, %{status: :pending}) end,
        max_concurrency: 8
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert [{:ok, first}] = Enum.filter(results, &match?({:ok, _}, &1))
    assert Enum.count(results, &(&1 == {:error, :exists})) == 23

    assert {:ok, second} =
             ServerStore.compare_and_swap(c.store, key, first, %{
               status: :decided,
               signature: "fixed"
             })

    assert {:error, :conflict} =
             ServerStore.compare_and_swap(c.store, key, first, %{status: :other})

    assert {:ok, %{value: %{signature: "fixed"}}} = ServerStore.fetch(c.store, key)
    :ok = stop_supervised(ServerStore)
    restored = start_supervised!({ServerStore, c.opts})
    assert {:ok, %{revision: ^second}} = ServerStore.fetch(restored, key)
    assert :ok = ServerStore.delete(restored, key, second)
    assert :not_found = ServerStore.fetch(restored, key)
    assert {:ok, third} = ServerStore.put_new(restored, key, %{status: :new})
    assert third > second
    assert {:error, :conflict} = ServerStore.compare_and_swap(restored, key, second, %{})
  end

  test "history stays in the database; keyset pages are scoped and byte bounded", c do
    body = String.duplicate("payload", 2_500)
    :erlang.garbage_collect(c.store)
    {:memory, before} = Process.info(c.store, :memory)

    for n <- 1..200 do
      assert {:ok, _} = ServerStore.put_new(c.store, {:approval, "owner", n}, %{body: body})
    end

    assert {:ok, _} = ServerStore.put_new(c.store, {:approval, "other", 1}, %{body: "foreign"})
    :erlang.garbage_collect(c.store)
    {:memory, after_insert} = Process.info(c.store, :memory)
    assert after_insert < before + 262_144

    assert {:ok, first, cursor} = ServerStore.list(c.store, {:approval, "owner"}, limit: 3)
    assert length(first) == 3

    assert {:ok, next, _} =
             ServerStore.list(c.store, {:approval, "owner"}, after: cursor, limit: 3)

    assert MapSet.disjoint?(MapSet.new(first), MapSet.new(next))

    assert {:error, :invalid_store_page} =
             ServerStore.list(c.store, {:approval, "other"}, after: cursor)

    assert {:error, :store_page_entry_too_large} =
             ServerStore.list(c.store, {:approval, "owner"}, max_bytes: 8)

    assert {:ok, [], nil} = ServerStore.list(c.store, {:missing})

    assert %{rows: [[201]]} =
             SQL.query!(Repo, "SELECT count(*) FROM \"#{c.table}\" WHERE NOT deleted", [],
               log: false
             )
  end

  test "a locked write does not serialize unrelated reads behind the owner mailbox", c do
    key = {:approval, "owner", "id"}
    {:ok, revision} = ServerStore.put_new(c.store, key, :pending)
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(Repo, "SELECT * FROM \"#{c.table}\" FOR UPDATE", [], log: false)
          send(parent, :locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :locked
    writer = Task.async(fn -> ServerStore.compare_and_swap(c.store, key, revision, :decided) end)
    reader = Task.async(fn -> ServerStore.fetch(c.store, key) end)
    assert {:ok, %{value: :pending}} = Task.await(reader, 1_000)
    assert is_map(:sys.get_state(c.store, 500))
    send(holder.pid, :release)
    Task.await(holder)
    assert {:ok, _} = Task.await(writer)
  end

  @tag :capture_log
  test "query timeout preserves owner availability and does not acknowledge an unknown write",
       c do
    store =
      start_supervised!(
        {Postgres, Keyword.merge(Keyword.delete(c.opts, :backend), query_timeout: 100)},
        id: :short
      )

    {:ok, revision} = Postgres.put_new(store, :key, :pending)
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(Repo, "SELECT * FROM \"#{c.table}\" FOR UPDATE", [], log: false)
          send(parent, :locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :locked
    assert {:error, {:ambiguous, _}} = Postgres.compare_and_swap(store, :key, revision, :decided)
    assert {:ok, %{value: :pending}} = Postgres.fetch(store, :key)
    assert Process.alive?(store)
    send(holder.pid, :release)
    Task.await(holder)
  end

  test "worker failure, deadline and shutdown release callers and leave no in-flight references",
       c do
    store =
      start_supervised!(
        {Postgres, Keyword.merge(Keyword.delete(c.opts, :backend), max_workers: 1)},
        id: :bounded
      )

    {:ok, revision} = Postgres.put_new(store, :key, :pending)
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(Repo, "SELECT * FROM \"#{c.table}\" FOR UPDATE", [], log: false)
          send(parent, :locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :locked

    for outcome <- [:crash, :deadline, :shutdown] do
      writer = Task.async(fn -> Postgres.compare_and_swap(store, :key, revision, :decided) end)
      {ref, entry} = await_query(store)
      assert {:error, :store_busy} = Postgres.fetch(store, :key)
      assert :sys.get_status(store)

      case outcome do
        :crash -> Process.exit(entry.task.pid, :kill)
        :deadline -> send(store, {:query_timeout, ref})
        :shutdown -> GenServer.stop(store)
      end

      assert {:error, {:ambiguous, _}} = Task.await(writer)
      refute Process.alive?(entry.task.pid)

      if outcome != :shutdown do
        send(store, {:query_timeout, ref})
        send(store, {:DOWN, ref, :process, entry.task.pid, :normal})
        assert :sys.get_state(store).tasks == %{}
      end
    end

    send(holder.pid, :release)
    Task.await(holder)
  end

  test "abrupt owner death also shuts down its active query task", c do
    store =
      start_supervised!({Postgres, Keyword.delete(c.opts, :backend)},
        id: :crash_owner,
        restart: :temporary
      )

    # Hold an actual database row lock so the supervised query remains in flight.
    {:ok, revision} = Postgres.put_new(store, :key, :pending)
    parent = self()

    holder =
      Task.async(fn ->
        Repo.transaction(fn ->
          SQL.query!(Repo, "SELECT * FROM \"#{c.table}\" FOR UPDATE", [], log: false)
          send(parent, :locked)

          receive do
            :release -> :ok
          end
        end)
      end)

    assert_receive :locked
    Task.start(fn -> Postgres.compare_and_swap(store, :key, revision, :decided) end)
    {_ref, entry} = await_query(store)
    worker = entry.task.pid
    supervisor = :sys.get_state(store).supervisor
    monitor = Process.monitor(worker)
    sup_monitor = Process.monitor(supervisor)
    Process.exit(store, :kill)
    assert_receive {:DOWN, ^monitor, :process, ^worker, _}, 1_000
    assert_receive {:DOWN, ^sup_monitor, :process, ^supervisor, _}, 1_000
    send(holder.pid, :release)
    Task.await(holder)
  end

  test "invalid operations and corrupt SQL values fail explicitly", c do
    assert {:ok, _} = Postgres.migration_sql()
    assert {:ok, [], nil} = Postgres.list(c.store, {:missing})
    assert {:error, :invalid_store_operation} = GenServer.call(c.store, :invented)
    assert {:error, :conflict} = Postgres.delete(c.store, :missing, 1)
    assert {:error, _} = Postgres.put_new(c.store, :invalid, self())
    {:ok, revision} = Postgres.put_new(c.store, {:corrupt}, :value)

    SQL.query!(Repo, "UPDATE \"#{c.table}\" SET value=$1 WHERE revision=$2", [<<0>>, revision],
      log: false
    )

    assert {:error, _} = Postgres.fetch(c.store, {:corrupt})
    assert {:error, _} = Postgres.list(c.store, {:corrupt})
  end

  defp await_query(store, attempts \\ 100)

  defp await_query(store, 0),
    do: flunk("query did not start: #{inspect(:sys.get_state(store).tasks)}")

  defp await_query(store, attempts) do
    case Map.to_list(:sys.get_state(store).tasks) do
      [entry] ->
        entry

      [] ->
        Process.sleep(5)
        await_query(store, attempts - 1)
    end
  end

  test "DDL names and backend configuration are validated" do
    assert {:error, :invalid_postgres_store_names} =
             Postgres.migration_sql(table: "unsafe; DROP TABLE x")

    assert {:error, :invalid_postgres_server_store} = Postgres.start_link(repo: nil)

    assert {:error, _} =
             Spectre.Ledger.DomainStore.migration_sql(table_prefix: "unsafe; DROP TABLE x")

    assert {:error, :domain_projection_prefix_too_long} =
             Spectre.Ledger.DomainStore.migration_sql(table_prefix: String.duplicate("x", 47))
  end

  test "Domain projection updates roll back atomically and reject stale or uncommitted revisions",
       c do
    alias Spectre.GAM.Ledger
    domain = "projection-atomic"
    assert {:ok, %{ledger: ledger, projection: projection}} = ServerStore.gam_domain(c.store)

    assert {:ok, 1} =
             Ledger.Store.append(ledger, domain, "one", [%{"fixture" => 1}], 0, recorded_at: 1)

    assert {:ok, first} = Ledger.Store.head(ledger, domain)
    assert {:ok, handle} = ProjectionStore.open(projection, domain)
    zero = %{revision: 0, head_digest: Ledger.Entry.genesis_digest()}

    # Fail after one SQL chunk was written; the whole transaction must roll back.
    invalid = for n <- 1..100, do: {:put, {:acts, n}, %{n: n}}
    invalid = invalid ++ [{:put, {:acts, :invalid}, self()}]
    assert {:error, _} = ProjectionStore.commit(projection, handle, zero, first, invalid)
    assert {:ok, [], nil} = ProjectionStore.page(projection, handle, zero, :acts)
    assert {:error, :domain_projection_rebuilding} = ProjectionStore.head(projection, domain)

    assert :ok =
             ProjectionStore.commit(projection, handle, zero, first, [
               {:put, {:acts, "a"}, %{n: 1}}
             ])

    assert {:error, :stale_domain_projection} =
             ProjectionStore.fetch(projection, handle, %{first | revision: 1.0}, [{:acts, "a"}])

    assert {:error, :stale_domain_projection} =
             ProjectionStore.commit(projection, handle, zero, first, [])

    assert {:error, :projection_target_not_committed} =
             ProjectionStore.commit(projection, handle, first, %{first | revision: 2}, [])

    assert {:ok, rows} =
             ProjectionStore.fetch(projection, handle, first, [{:acts, "a"}, {:acts, "missing"}])

    assert map_size(rows) == 1
    assert {:ok, %{n: 1}} = Rows.decode(rows[{:acts, "a"}])

    assert {:ok, :ok} =
             Repo.transaction(fn ->
               SQL.query!(Repo, "SELECT 1", [], log: false)

               assert {:ok, ^rows} =
                        ProjectionStore.fetch(projection, handle, first, [{:acts, "a"}])

               :ok
             end)

    assert {:ok, 2} =
             Ledger.Store.append(ledger, domain, "two", [%{"fixture" => 2}], 1, recorded_at: 2)

    assert {:ok, second} = Ledger.Store.head(ledger, domain)

    assert :ok =
             ProjectionStore.commit(projection, handle, first, second, [{:delete, {:acts, "a"}}])

    assert {:ok, empty} = ProjectionStore.fetch(projection, handle, second, [{:acts, "a"}])
    assert empty == %{}

    assert {:error, :stale_domain_projection} =
             ProjectionStore.page(projection, handle, first, :acts)

    assert {:error, :invalid_projection_collection} =
             ProjectionStore.page(projection, handle, second, :unknown)

    assert {:error, :invalid_projection_page} =
             ProjectionStore.page(projection, handle, second, :acts, repo: :other)
  end

  test "a projection reader keeps a consistent snapshot without blocking the next commit", c do
    alias Spectre.GAM.Ledger
    domain = "projection-snapshot"
    {:ok, %{ledger: ledger, projection: projection}} = ServerStore.gam_domain(c.store)
    {:ok, 1} = Ledger.Store.append(ledger, domain, "one", [%{"fixture" => 1}], 0, recorded_at: 1)
    {:ok, first} = Ledger.Store.head(ledger, domain)
    {:ok, 2} = Ledger.Store.append(ledger, domain, "two", [%{"fixture" => 2}], 1, recorded_at: 2)
    {:ok, second} = Ledger.Store.head(ledger, domain)
    {:ok, handle} = ProjectionStore.open(projection, domain)
    zero = %{revision: 0, head_digest: Ledger.Entry.genesis_digest()}

    assert :ok =
             ProjectionStore.commit(projection, handle, zero, first, [
               {:put, {:acts, "a"}, %{n: 1}}
             ])

    reader =
      Task.async(fn ->
        receive do
          :read -> ProjectionStore.fetch(projection, handle, first, [{:acts, "a"}])
        end
      end)

    handler = {__MODULE__, :snapshot, make_ref()}
    event = Repo.config()[:telemetry_prefix] ++ [:query]

    :ok =
      :telemetry.attach(handler, event, &__MODULE__.pause_snapshot/4, %{
        reader: reader.pid,
        observer: self(),
        domain: domain
      })

    on_exit(fn -> :telemetry.detach(handler) end)
    send(reader.pid, :read)
    assert_receive :snapshot_head_read, 5_000

    assert :ok =
             ProjectionStore.commit(projection, handle, first, second, [
               {:put, {:acts, "a"}, %{n: 2}}
             ])

    send(reader.pid, :continue_snapshot)
    assert {:ok, rows} = Task.await(reader)
    assert {:ok, %{n: 1}} = Rows.decode(rows[{:acts, "a"}])
    assert {:ok, current} = ProjectionStore.fetch(projection, handle, second, [{:acts, "a"}])
    assert {:ok, %{n: 2}} = Rows.decode(current[{:acts, "a"}])
  end

  def pause_snapshot(_, _, metadata, %{reader: reader, observer: observer, domain: domain}) do
    if self() == reader and metadata.params == [domain] and
         String.starts_with?(metadata.query, "SELECT generation,revision,head_digest") do
      send(observer, :snapshot_head_read)

      receive do
        :continue_snapshot -> :ok
      after
        5_000 -> raise "snapshot reader not resumed"
      end
    end
  end

  @tag :tmp_dir
  @tag :capture_log
  test "a real database outage reports the policy hook and preserves the pending request for retry",
       c do
    Process.register(self(), __MODULE__)
    {_, private} = :crypto.generate_key(:eddsa, :ed25519)
    principal = %{kind: :human, attributes: %{"subject" => "owner"}}

    approvals = [
      address: "spectre://owner",
      transport: {Transport, []},
      signer: [principal: principal, private_key: private, mandate_ref: "owner-consent"]
    ]

    {:ok, normalized} = Spectre.Server.GAM.Managed.Approval.normalize(approvals: approvals)
    signer = normalized[:approvals].signer

    owner =
      start_supervised!(
        {Spectre.Server,
         id: :outage_owner,
         agent: Agent,
         consumer: self(),
         identity: %{subject: "outage", conversation: "test"},
         gam: [storage: :ledger, store: {ServerStore, c.store}, approvals: approvals]}
      )

    assert {:ok, _} = Spectre.Server.approval_inbox(owner)

    authority_opts = [
      agent: Agent,
      runtime: self(),
      consumer: self(),
      identity: %{subject: "separate-authority-outage"},
      storage: :ledger,
      store: {ServerStore, c.store}
    ]

    authority =
      start_supervised!({Spectre.Server.GAM.Managed, authority_opts}, restart: :temporary)

    managed = :sys.get_state(authority)
    monitor = Process.monitor(authority)
    projection = Spectre.GAM.Domain.Sequencer.projection(managed.domain)

    request =
      Map.new(
        ~w(presentation_ref show_ref material_digest candidate_binding_ref data_digest scope_ref rendered_payload),
        &{&1, "fixture"}
      )
      |> Map.merge(%{
        "type" => "approval.request",
        "id" => "outage-request",
        "from" => "spectre://customer",
        "to" => "spectre://owner",
        "principal_ref" => signer.principal.ref,
        "mandate_ref" => signer.mandate_ref,
        "revision" => 1,
        "expires_at" => System.system_time(:millisecond) + 60_000
      })

    {_, 0} =
      System.cmd("pg_ctl", ["-D", c.cluster.data, "-m", "fast", "-w", "stop"],
        stderr_to_stdout: true
      )

    try do
      assert {:error, {:approval_store, _}} = Spectre.Server.receive_approval(owner, request)

      assert_receive {Spectre.Server, ^owner,
                      {:approval_inbox,
                       %{event: :storage_error, operation: :receive, request: ^request}}},
                     5_000

      assert_receive {:approval_policy, ^owner, :storage_error}, 5_000
      assert is_map(Spectre.Server.status(owner))
      refute_receive {Spectre.Server, ^owner, {:approval_request, _}}

      # The working-set read fails before any canonical append is attempted.
      # No authority is reconstructed from missing DB rows. The live owner can
      # retry after recovery, and its head remains available without DB I/O.
      assert {:error, :domain_projection_storage_error} =
               Spectre.Server.GAM.Managed.revoke(authority)

      assert {:ok, %{revision: revision}} = GenServer.call(managed.domain, :head, 500)
      assert revision == projection.revision
      refute_receive {:DOWN, ^monitor, :process, ^authority, _}

      assert is_map(Spectre.Server.status(owner))
    after
      {_, 0} =
        System.cmd(
          "pg_ctl",
          [
            "-D",
            c.cluster.data,
            "-l",
            Path.join(c.cluster.root, "postgres.log"),
            "-o",
            "-F -k #{c.cluster.socket} -p #{c.cluster.port} -h ''",
            "-w",
            "start"
          ],
          stderr_to_stdout: true
        )

      :ok = SQL.disconnect_all(Repo, 0)
      await_database_pool(24, 100)
    end

    assert :ok = Spectre.Server.receive_approval(owner, request)
    assert_receive {Spectre.Server, ^owner, {:approval_request, ^request}}
    assert {:ok, %{entries: [_]}} = Spectre.Server.approval_inbox(owner)
    assert :ok = Spectre.Server.receive_approval(owner, request)
    assert {:ok, %{entries: [_]}} = Spectre.Server.approval_inbox(owner)

    assert :ok = Spectre.Server.GAM.Managed.close(authority)
    assert_receive {:DOWN, ^monitor, :process, ^authority, :normal}

    recovered =
      start_supervised!({Spectre.Server.GAM.Managed, authority_opts}, id: :recovered_authority)

    recovered_state = :sys.get_state(recovered)
    restored = Spectre.GAM.Domain.Sequencer.projection(recovered_state.domain)
    assert restored.catalog.genesis == projection.catalog.genesis
    assert restored.revision == projection.revision
    assert restored.revocations == projection.revocations
    assert {:ok, revoked} = Spectre.Server.GAM.Managed.revoke(recovered)
    assert revoked.primary.act.class == "mandate.revoke"
    assert {:ok, head} = Spectre.GAM.Ledger.Store.head(managed.store, managed.refs.domain)
    assert head.revision > projection.revision
  end

  defp await_database_pool(0, _), do: :ok
  defp await_database_pool(_, 0), do: flunk("database pool did not reconnect")

  defp await_database_pool(reads, attempts) do
    case SQL.query(Repo, "SELECT 1", [], log: false, timeout: 500) do
      {:ok, _} ->
        await_database_pool(reads - 1, attempts - 1)

      {:error, _} ->
        Process.sleep(50)
        await_database_pool(reads, attempts - 1)
    end
  end

  @tag :tmp_dir
  test "two independent Domains persist their GAM history and inbox in PostgreSQL and recover exact decisions",
       c do
    Process.register(self(), __MODULE__)

    runtime =
      start_supervised!(
        {Spectre.Stack.Runtime, stack: Stack, packages: [spectre_ledger: [server_store: c.opts]]}
      )

    {:ok, resource} = Spectre.Stack.resolve(Stack, :resource, {:spectre_ledger, :server_store})
    {:ok, ledger} = Spectre.Stack.Runtime.resolve(runtime, resource)
    {public, private} = :crypto.generate_key(:eddsa, :ed25519)
    principal = %{kind: :human, attributes: %{"subject" => "owner"}}

    customer_approvals = [
      address: "spectre://customer",
      transport: {Transport, []},
      approvers: %{
        "spectre://owner" => [
          principal: principal,
          public_key: public,
          mandate_ref: "owner-consent"
        ]
      }
    ]

    owner_approvals = [
      address: "spectre://owner",
      transport: {Transport, []},
      maintenance_interval: 100,
      signer: [principal: principal, private_key: private, mandate_ref: "owner-consent"]
    ]

    owner_opts = [
      id: :owner,
      agent: Agent,
      stack_runtime: runtime,
      consumer: self(),
      identity: %{subject: "owner", conversation: "owner-chat"},
      gam: [storage: :ledger, approvals: owner_approvals]
    ]

    owner = start_supervised!({Spectre.Server, owner_opts})

    client_opts = [
      id: :customer,
      agent: Agent,
      stack_runtime: runtime,
      consumer: self(),
      identity: %{subject: "customer", conversation: "customer-chat"},
      gam: [
        storage: :ledger,
        approvals: customer_approvals,
        operations: %{open: [confirm: true, approver: {:external, "spectre://owner"}]}
      ]
    ]

    client = start_supervised!({Spectre.Server, client_opts})

    {:ok, client_view} = Spectre.Server.gam_view(client)
    {:ok, owner_view} = Spectre.Server.gam_view(owner)
    refute client_view.opening.domain_ref == owner_view.opening.domain_ref

    for server <- [client, owner] do
      managed = :sys.get_state(server).managed_owner
      assert :sys.get_state(managed).journal == {ServerStore, ledger}
      assert {Spectre.GAM.Ledger.Store.Postgres, _} = :sys.get_state(managed).store
      assert :sys.get_state(managed).ledger == nil
    end

    refute File.exists?(Path.join(c.tmp_dir, "owner/commands.store"))
    {:ok, _} = Spectre.Server.input(client, "https://fixture.test")
    assert_receive {:approval_wire, %{"type" => "approval.request"} = request}, 5_000
    assert :ok = Spectre.Server.receive_approval(owner, request)
    assert_receive {Spectre.Server, ^owner, {:approval_request, ^request}}
    :ok = Spectre.Server.approval_presented(owner, request["id"])
    assert_receive {:approval_wire, %{"type" => "approval.presented"} = shown}
    :ok = Spectre.Server.receive_approval(client, shown)
    assert_receive {Spectre.Server, ^client, {:pending, _, _}}, 5_000
    :ok = Spectre.Server.approval_decide(owner, request["id"], :confirm)
    assert_receive {:approval_wire, %{"type" => "approval.decision"} = decision}
    assert_receive {:approval_policy, ^owner, :decided}
    assert_receive {:approval_policy, ^owner, :tick}, 1_000
    :ok = stop_supervised(:owner)
    assert Process.alive?(ledger)
    restored = start_supervised!({Spectre.Server, owner_opts})
    assert_receive {Spectre.Server, ^restored, {:approval_inbox, %{event: :recovered}}}, 5_000
    assert {:ok, %{decision: ^decision}} = Spectre.Server.approval(restored, request["id"])
    assert {:ok, %{entries: [_], next_cursor: nil}} = Spectre.Server.approval_inbox(restored)
    :ok = Spectre.Server.approval_decide(restored, request["id"], :confirm)
    assert_receive {:approval_wire, ^decision}
    acquired = Spectre.Server.receive_approval(client, decision)
    assert acquired == :ok or match?({:ok, _}, acquired)
    assert_receive {:opened, %{url: "https://fixture.test"}}, 5_000
    refute_receive {:opened, _}
    :ok = stop_supervised(:customer)

    assert %{rows: [[2]]} =
             SQL.query!(Repo, "SELECT count(*) FROM \"#{c.table}_gam_heads\"", [], log: false)

    domain = client_view.opening.domain_ref
    assert {:ok, store} = ServerStore.gam_ledger(ledger)
    assert {:ok, snapshot} = Spectre.GAM.Ledger.Store.load(store, domain, [])
    assert {:ok, _} = Spectre.GAM.Ledger.verify_snapshot(snapshot)
    assert {:ok, projection} = Spectre.GAM.Domain.Projection.replay(snapshot)
    assert map_size(projection.acts) > 0
    assert map_size(projection.outcomes) > 0
    assert projection.revision == snapshot.revision

    assert {:ok, %{projection: persisted}} = ServerStore.gam_domain(ledger)
    assert {:ok, %{handle: handle, cursor: cursor}} = ProjectionStore.head(persisted, domain)
    assert cursor.revision == projection.revision
    [act_ref | _] = Map.keys(projection.acts)

    assert {:ok, rows} =
             ProjectionStore.fetch(persisted, handle, cursor, [
               {:acts, act_ref},
               {:domain, :metadata}
             ])

    assert {:ok, act} = Rows.decode(rows[{:acts, act_ref}])
    assert act == projection.acts[act_ref]
    assert {:ok, metadata} = Rows.decode(rows[{:domain, :metadata}])
    assert metadata.catalog.genesis == projection.catalog.genesis
    assert {:ok, page, next} = ProjectionStore.page(persisted, handle, cursor, :acts, limit: 1)
    assert length(page) == 1

    assert {:ok, [_], _} =
             ProjectionStore.page(persisted, handle, cursor, :acts, limit: 1, after: next)

    assert {:error, :projection_page_entry_too_large} =
             ProjectionStore.page(persisted, handle, cursor, :acts, max_bytes: 1)

    assert {:error, :stale_projection_page} =
             ProjectionStore.page(persisted, handle, cursor, :evidence, after: next)

    # Corrupt only the disposable read model. The restart must derive the
    # original Act from canonical history rather than accepting this row.
    SQL.query!(
      Repo,
      "UPDATE \"#{c.table}_gam_projection_rows\" SET value=$1 WHERE domain_ref=$2 AND collection='acts'",
      [<<0>>, domain],
      log: false
    )

    assert {:ok, damaged} = ProjectionStore.fetch(persisted, handle, cursor, [{:acts, act_ref}])
    assert {:error, _} = Rows.decode(damaged[{:acts, act_ref}])

    recovered_client = start_supervised!({Spectre.Server, client_opts})
    managed = :sys.get_state(:sys.get_state(recovered_client).managed_owner)
    recovered_projection = Spectre.GAM.Domain.Sequencer.projection(managed.domain)
    assert recovered_projection.catalog.genesis == projection.catalog.genesis
    assert recovered_projection.acts == projection.acts
    assert recovered_projection.outcomes == projection.outcomes
    assert recovered_projection.consumed_nonces == projection.consumed_nonces
    assert recovered_projection.revision >= projection.revision

    assert {:error, :stale_domain_projection} =
             ProjectionStore.fetch(persisted, handle, cursor, [{:acts, act_ref}])

    assert {:ok, %{handle: new_handle, cursor: new_cursor}} =
             ProjectionStore.head(persisted, domain)

    assert {:ok, rows} =
             ProjectionStore.fetch(persisted, new_handle, new_cursor, [{:acts, act_ref}])

    assert {:ok, ^act} = Rows.decode(rows[{:acts, act_ref}])
    Spectre.Server.receive_approval(recovered_client, decision)
    refute_receive {:opened, _}
  end

  test "missing derived records are reported and rebuilt from the canonical ledger", c do
    authority_opts = [
      agent: Agent,
      runtime: self(),
      consumer: self(),
      identity: %{subject: "derived-record-repair"},
      storage: :ledger,
      store: {ServerStore, c.store}
    ]

    authority = start_supervised!({Spectre.Server.GAM.Managed, authority_opts})
    managed = :sys.get_state(authority)
    assert {:ok, [evidence]} = Spectre.GAM.observe(managed.scope, Spectre.Input.new("past"))
    assert {:ok, head} = Spectre.GAM.Domain.Sequencer.head(managed.domain)
    encoded = Spectre.GAM.Canonical.Value.encode!(evidence.ref)

    SQL.query!(
      Repo,
      "DELETE FROM \"#{c.table}_gam_projection_rows\" WHERE domain_ref=$1 AND collection='evidence' AND key=$2",
      [managed.refs.domain, encoded],
      log: false
    )

    query = {:query, managed.scope, {:selected_view, %{evidence_refs: [evidence.ref]}}}

    assert {:error, {:domain_projection_record_missing, {:evidence, _}}} =
             GenServer.call(managed.domain, query)

    assert {:ok, ^head} = Spectre.GAM.Domain.Sequencer.head(managed.domain)
    assert :ok = Spectre.Server.GAM.Managed.recover_storage(authority, :retry)

    assert_receive {Spectre.Server.Runtime, nil, :gam_storage_recovered,
                    %{domain: domain, previous_domain: domain, checkpoint: nil}},
                   10_000

    assert domain == managed.refs.domain
    restored = :sys.get_state(authority)

    assert {:ok, view} =
             GenServer.call(
               restored.domain,
               {:query, restored.scope, {:selected_view, %{evidence_refs: [evidence.ref]}}}
             )

    assert evidence in view.evidence
    assert {:ok, ^head} = Spectre.GAM.Domain.Sequencer.head(restored.domain)
  end

  for loss <- [:batch, :entire_history] do
    @tag :capture_log
    test "the agent can restart in RAM after #{loss} is deleted", c do
      Process.register(self(), __MODULE__)

      server =
        start_supervised!(
          {Spectre.Server,
           agent: Agent,
           consumer: self(),
           identity: %{subject: "deleted-#{unquote(loss)}"},
           gam: [
             storage: :ledger,
             store: {ServerStore, c.store},
             recovery_policy: MemoryRecovery,
             operations: %{open: [confirm: false]}
           ]}
        )

      previous = :sys.get_state(:sys.get_state(server).managed_owner)

      table =
        if unquote(loss) == :batch, do: c.table <> "_gam_batches", else: c.table <> "_gam_heads"

      Repo.transaction(fn ->
        # Simulate administrative loss. Production append-only protection is
        # intentionally bypassed only inside this isolated test transaction.
        SQL.query!(Repo, "SET LOCAL session_replication_role = 'replica'", [], log: false)

        SQL.query!(
          Repo,
          "DELETE FROM \"#{c.table}_gam_batches\" WHERE domain_ref=$1",
          [previous.refs.domain],
          log: false
        )

        if unquote(loss) == :entire_history,
          do:
            SQL.query!(
              Repo,
              "DELETE FROM \"#{table}\" WHERE domain_ref=$1",
              [previous.refs.domain],
              log: false
            )
      end)

      assert :ok = Spectre.Server.recover_storage(server, :retry)
      assert_receive {:storage_incident, %{phase: :runtime}}, 10_000

      assert_receive {Spectre.Server, ^server,
                      {:storage_recovered, %{previous_domain: old, domain: fresh}}},
                     10_000

      assert old == previous.refs.domain
      refute fresh == old
      assert {:ok, _} = Spectre.Server.input(server, "https://fixture.test/after-loss")
      assert_receive {:opened, %{url: "https://fixture.test/after-loss"}}, 5_000
      assert :sys.get_state(:sys.get_state(server).managed_owner).ephemeral
    end
  end

  @tag :capture_log
  @tag timeout: 60_000
  test "a disconnected PostgreSQL can be replaced by RAM through the host recovery policy", c do
    Process.register(self(), __MODULE__)

    server =
      start_supervised!(
        {Spectre.Server,
         agent: Agent,
         consumer: self(),
         identity: %{subject: "db-to-ram"},
         gam: [
           storage: :ledger,
           store: {ServerStore, c.store},
           recovery_policy: MemoryRecovery,
           operations: %{open: [confirm: false]}
         ]}
      )

    previous = :sys.get_state(:sys.get_state(server).managed_owner)

    {_, 0} =
      System.cmd("pg_ctl", ["-D", c.cluster.data, "-m", "fast", "-w", "stop"],
        stderr_to_stdout: true
      )

    try do
      assert {:ok, _} = Spectre.Server.input(server, "https://fixture.test/offline")
      assert_receive {:storage_incident, %{phase: :runtime}}, 10_000

      assert_receive {Spectre.Server, ^server,
                      {:storage_recovered, %{previous_domain: old, domain: fresh}}},
                     10_000

      assert old == previous.refs.domain
      refute fresh == old
      assert Process.alive?(server)
      assert {:ok, _} = Spectre.Server.input(server, "https://fixture.test/in-ram")
      assert_receive {:opened, %{url: "https://fixture.test/in-ram"}}, 10_000

      assert_receive {:server_settled, ^server, {:governed, %{outcome: %{status: :succeeded}}}},
                     10_000

      current = :sys.get_state(:sys.get_state(server).managed_owner)
      assert current.ephemeral
      assert current.refs.domain == fresh
      assert :sys.get_state(server).identity.subject == "db-to-ram"
      refute Process.alive?(previous.domain)
    after
      {_, 0} =
        System.cmd(
          "pg_ctl",
          [
            "-D",
            c.cluster.data,
            "-l",
            Path.join(c.cluster.root, "postgres.log"),
            "-o",
            "-F -k #{c.cluster.socket} -p #{c.cluster.port} -h ''",
            "-w",
            "start"
          ],
          stderr_to_stdout: true
        )

      SQL.disconnect_all(Repo, 0)
      await_database_pool(24, 100)
    end

    assert :sys.get_state(:sys.get_state(server).managed_owner).ephemeral
  end

  @tag timeout: 300_000
  test "Domain history grows in PostgreSQL while retained memory stays bounded", c do
    alias Spectre.GAM
    alias Spectre.GAM.Domain.Sequencer

    Process.register(self(), __MODULE__)

    authority_opts = [
      agent: Agent,
      runtime: self(),
      consumer: self(),
      identity: %{subject: "memory-profile"},
      storage: :ledger,
      store: {ServerStore, c.store}
    ]

    authority =
      start_supervised!({Spectre.Server.GAM.Managed, authority_opts}, restart: :temporary)

    managed = :sys.get_state(authority)
    total = String.to_integer(System.get_env("SPECTRE_RAM_EVENTS", "256"))
    metrics = :ets.new(:projection_metrics, [:set, :public])
    :ets.insert(metrics, {:records, 0})
    handler = {__MODULE__, self()}

    :ok =
      :telemetry.attach(
        handler,
        [:spectre, :gam, :projection, :load],
        &__MODULE__.projection_metric/4,
        metrics
      )

    on_exit(fn -> :telemetry.detach(handler) end)
    parent = self()
    sampler = Task.async(fn -> sample_memory(managed.domain, parent, %{}) end)

    checkpoints =
      for n <- 1..total, reduce: [] do
        samples ->
          input = Spectre.Input.new("observation #{n}: " <> String.duplicate("x", 2_048))
          assert {:ok, [_]} = GAM.observe(managed.scope, input)

          if n in [16, div(total, 4), total] do
            :erlang.garbage_collect(managed.domain)
            state = :sys.get_state(managed.domain)
            assert state.projection.evidence == %{}
            assert state.projection.acts == %{}
            assert MapSet.size(state.projection.read_index.batches) == 0
            assert {:message_queue_len, 0} = Process.info(managed.domain, :message_queue_len)
            [{n, Process.info(managed.domain, [:memory, :reductions])} | samples]
          else
            samples
          end
      end
      |> Enum.reverse()

    [{_, first} | _] = checkpoints
    {_, last} = List.last(checkpoints)
    assert last[:memory] < first[:memory] + 131_072
    [{:records, selected_records}] = :ets.lookup(metrics, :records)
    assert selected_records < 128
    :telemetry.detach(handler)
    send(sampler.pid, :stop)
    peak = Task.await(sampler)

    assert %{rows: [[^total]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM \"#{c.table}_gam_projection_rows\" WHERE collection='evidence' AND domain_ref=$1",
               [managed.refs.domain],
               log: false
             )

    assert {:ok, head} = Sequencer.head(managed.domain)
    assert :ok = Spectre.Server.GAM.Managed.close(authority)

    recovered =
      start_supervised!({Spectre.Server.GAM.Managed, authority_opts}, id: :memory_recovered)

    domain = :sys.get_state(recovered).domain
    assert {:ok, ^head} = Sequencer.head(domain)
    :erlang.garbage_collect(domain)
    assert :sys.get_state(domain).projection.evidence == %{}

    profile = %{
      events: total,
      samples: checkpoints,
      peak: peak,
      selected_records: selected_records,
      recovered: Process.info(domain, [:memory, :reductions])
    }

    IO.puts("Domain PostgreSQL RAM: " <> inspect(profile))

    if path = System.get_env("SPECTRE_RAM_REPORT"),
      do: File.write!(path, inspect(profile, pretty: true))
  end

  def projection_metric(_event, measurements, %{full: false}, table) do
    [{:records, old}] = :ets.lookup(table, :records)
    if measurements.records > old, do: :ets.insert(table, {:records, measurements.records})
  end

  def projection_metric(_, _, _, _), do: :ok

  @tag timeout: 300_000
  @tag :capture_log
  test "completed AgentServer effects release Domain history from RAM", c do
    Process.register(self(), __MODULE__)

    server =
      start_supervised!(
        {Spectre.Server,
         agent: Agent,
         consumer: self(),
         history_limit: 2,
         max_seen_events: 16,
         identity: %{subject: "effect-memory-profile"},
         gam: [
           storage: :ledger,
           store: {ServerStore, c.store},
           operations: %{open: [confirm: false]}
         ]}
      )

    domain = :sys.get_state(:sys.get_state(server).managed_owner).domain
    total = String.to_integer(System.get_env("SPECTRE_RAM_EFFECTS", "32"))
    parent = self()
    sampler = Task.async(fn -> sample_memory(domain, parent, %{}) end)

    samples =
      for n <- 1..total, reduce: [] do
        samples ->
          url = "https://fixture.test/memory/#{n}"
          assert {:ok, _} = Spectre.Server.input(server, url)
          assert_receive {:opened, %{url: ^url}}, 10_000

          assert_receive {:server_settled, ^server,
                          {:governed, %{outcome: %{status: :succeeded}}}},
                         10_000

          if n in [8, div(total, 2), total] do
            :erlang.garbage_collect(domain)
            :erlang.garbage_collect(server)
            state = :sys.get_state(domain)
            assert state.projection.acts == %{}
            assert state.projection.evidence == %{}
            assert map_size(state.projection.duties) == 0

            [
              {n,
               %{
                 domain: Process.info(domain, [:memory, :reductions, :message_queue_len]),
                 server: Process.info(server, [:memory, :reductions, :message_queue_len]),
                 processes: process_sample()
               }}
              | samples
            ]
          else
            samples
          end
      end
      |> Enum.reverse()

    [{_, first} | _] = samples
    {_, last} = List.last(samples)
    assert last.domain[:memory] < first.domain[:memory] + 131_072
    assert last.processes.count <= first.processes.count + 16
    assert last.processes.ets <= first.processes.ets + 2
    send(sampler.pid, :stop)
    report = %{effects: total, samples: samples, peak: Task.await(sampler)}
    IO.puts("AgentServer PostgreSQL RAM: " <> inspect(report))
    assert last.server[:memory] < first.server[:memory] + 262_144

    if path = System.get_env("SPECTRE_RAM_EFFECT_REPORT"),
      do: File.write!(path, inspect(report, pretty: true))
  end

  defp process_sample do
    processes =
      for pid <- Process.list(),
          info = Process.info(pid, [:memory, :reductions, :message_queue_len]),
          not is_nil(info) do
        Map.new([{:initial_call, :proc_lib.translate_initial_call(pid)} | info])
      end

    %{
      count: length(processes),
      ets: length(:ets.all()),
      memory: Enum.sum(Enum.map(processes, & &1.memory)),
      queued: Enum.sum(Enum.map(processes, & &1.message_queue_len)),
      largest: Enum.take(Enum.sort_by(processes, & &1.memory, :desc), 8)
    }
  end

  defp sample_memory(domain, parent, peak) do
    values = Map.new(:erlang.memory([:total, :processes, :binary, :ets]))

    values =
      case Process.info(domain, :memory) do
        {:memory, memory} -> Map.put(values, :domain, memory)
        nil -> values
      end

    peak = Map.merge(peak, values, fn _, previous, current -> max(previous, current) end)

    receive do
      :stop -> peak
    after
      20 ->
        if Process.alive?(parent), do: sample_memory(domain, parent, peak), else: peak
    end
  end

  test "reset keeps PostgreSQL history and selects an isolated generation across restart", c do
    Process.register(self(), __MODULE__)
    target_opts = reset_options(c.store, :reset_target)
    target = start_supervised!({Spectre.Server, target_opts})
    other = start_supervised!({Spectre.Server, reset_options(c.store, :reset_other)})
    settled_input(target, "https://before-reset.test")
    settled_input(other, "https://other-agent.test")

    original = :sys.get_state(target)
    old_domain = Managed.info(original.managed_owner).domain
    other_state = :sys.get_state(other)
    other_domain = Managed.info(other_state.managed_owner).domain
    other_session = Spectre.Server.status(other).session
    {:ok, %{ledger: ledger}} = ServerStore.gam_domain(c.store)
    {:ok, old_head} = Ledger.Store.head(ledger, old_domain)
    {:ok, other_head} = Ledger.Store.head(ledger, other_domain)
    old_key = {:managed_agent, old_domain}
    {:ok, old_checkpoint} = ServerStore.fetch(c.store, old_key)

    assert :ok = Spectre.Server.reset(target)
    selected_session = Spectre.Server.status(target).session
    selected = :sys.get_state(target)
    selected_domain = Managed.info(selected.managed_owner).domain
    assert selected_domain != old_domain
    assert selected.history == []
    assert selected.opts[:checkpoint] == {ServerStore, c.store, {:managed_agent, selected_domain}}
    assert {:ok, ^old_head} = Ledger.Store.head(ledger, old_domain)
    assert {:ok, ^old_checkpoint} = ServerStore.fetch(c.store, old_key)
    assert {:ok, archived} = Ledger.Store.load(ledger, old_domain, [])
    assert {:ok, _} = Ledger.verify_snapshot(archived)
    assert {:ok, history} = Projection.replay(archived)

    assert Enum.any?(history.acts, fn {_, act} ->
             act.consequence["payload"] == %{url: "https://before-reset.test"}
           end)

    assert Spectre.Server.status(other).session == other_session
    assert :sys.get_state(other).history == other_state.history

    assert Managed.info(:sys.get_state(other).managed_owner).domain ==
             other_domain

    assert {:ok, ^other_head} = Ledger.Store.head(ledger, other_domain)
    settled_input(target, "https://after-reset.test")
    new_key = {:managed_agent, selected_domain}
    assert {:ok, %{value: checkpoint}} = ServerStore.fetch(c.store, new_key)
    assert {:ok, saved} = Checkpoint.decode_snapshot(checkpoint)
    assert [%{user: "https://after-reset.test"}] = saved.history
    pointer_key = {:spectre_server_session, old_domain}

    assert {:ok, %{value: %{generation: generation} = pointer}} =
             ServerStore.fetch(c.store, pointer_key)

    assert selected_domain == old_domain <> ":session:" <> generation
    assert generation == selected_session
    assert {:ok, encoded_pointer_key} = Value.encode(pointer_key)
    assert {:ok, encoded_pointer} = Value.encode(pointer)

    assert %{rows: [[^encoded_pointer, false]]} =
             SQL.query!(
               Repo,
               "SELECT value,deleted FROM \"#{c.table}\" WHERE key=$1",
               [
                 encoded_pointer_key
               ],
               log: false
             )

    assert %{rows: rows} =
             SQL.query!(Repo, "SELECT DISTINCT domain_ref FROM \"#{c.table}_gam_batches\"", [],
               log: false
             )

    assert MapSet.new(rows, &hd/1) == MapSet.new([old_domain, selected_domain, other_domain])
    :ok = stop_supervised(:reset_target)
    restored = start_supervised!({Spectre.Server, target_opts})
    assert Spectre.Server.status(restored).session == selected_session
    restored_state = :sys.get_state(restored)
    assert Managed.info(restored_state.managed_owner).domain == selected_domain
    assert restored_state.history == saved.history
    assert {:ok, ^old_checkpoint} = ServerStore.fetch(c.store, old_key)
    assert {:ok, ^other_head} = Ledger.Store.head(ledger, other_domain)

    assert %{rows: [[3]]} =
             SQL.query!(Repo, "SELECT count(*) FROM \"#{c.table}_gam_projection_heads\"", [],
               log: false
             )
  end

  @tag :capture_log
  test "an explicit RAM reset works with PostgreSQL offline and retains its durable selection",
       c do
    Process.register(self(), __MODULE__)
    opts = reset_options(c.store, :offline_reset)
    server = start_supervised!({Spectre.Server, opts})
    original_domain = Managed.info(:sys.get_state(server).managed_owner).domain
    assert :ok = Spectre.Server.reset(server)
    settled_input(server, "https://durable-before-outage.test")
    durable_session = Spectre.Server.status(server).session
    durable_domain = Managed.info(:sys.get_state(server).managed_owner).domain
    {:ok, %{ledger: ledger}} = ServerStore.gam_domain(c.store)
    {:ok, durable_head} = Ledger.Store.head(ledger, durable_domain)
    pointer_key = {:spectre_server_session, original_domain}
    {:ok, pointer} = ServerStore.fetch(c.store, pointer_key)

    {_, 0} =
      System.cmd("pg_ctl", ["-D", c.cluster.data, "-m", "fast", "-w", "stop"],
        stderr_to_stdout: true
      )

    memory_domain =
      try do
        assert :ok = Spectre.Server.reset(server, storage: :ephemeral, timeout: 10_000)
        assert Spectre.Server.status(server).session != durable_session
        assert :sys.get_state(server).history == []
        settled_input(server, "https://only-in-ram.test")
        domain = Managed.info(:sys.get_state(server).managed_owner).domain
        :ok = stop_supervised(:offline_reset)
        domain
      after
        {_, 0} =
          System.cmd(
            "pg_ctl",
            [
              "-D",
              c.cluster.data,
              "-l",
              Path.join(c.cluster.root, "postgres.log"),
              "-o",
              "-F -k #{c.cluster.socket} -p #{c.cluster.port} -h ''",
              "-w",
              "start"
            ],
            stderr_to_stdout: true
          )

        :ok = SQL.disconnect_all(Repo, 0)
        await_database_pool(24, 100)
      end

    assert {:ok, ^pointer} = ServerStore.fetch(c.store, pointer_key)
    assert {:ok, ^durable_head} = Ledger.Store.head(ledger, durable_domain)
    restored = start_supervised!({Spectre.Server, opts})
    assert Spectre.Server.status(restored).session == durable_session
    restored_state = :sys.get_state(restored)
    assert Managed.info(restored_state.managed_owner).domain == durable_domain
    assert [%{user: "https://durable-before-outage.test"}] = restored_state.history

    assert %{rows: [[0]]} =
             SQL.query!(
               Repo,
               "SELECT count(*) FROM \"#{c.table}_gam_batches\" WHERE domain_ref=$1",
               [memory_domain],
               log: false
             )
  end

  defp reset_options(store, id) do
    [
      id: id,
      agent: Agent,
      consumer: self(),
      identity: %{subject: Atom.to_string(id), conversation: "postgres-reset"},
      gam: [storage: :ledger, store: {ServerStore, store}, operations: %{open: [confirm: false]}]
    ]
  end

  defp settled_input(server, url) do
    assert {:ok, _} = Spectre.Server.input(server, url)
    assert_receive {:opened, %{url: ^url}}, 5_000

    assert_receive {:server_settled, ^server, {:governed, %{outcome: %{status: :succeeded}}}},
                   5_000

    assert %{busy: false, outbox_size: 0} = Spectre.Server.status(server)
  end

  @tag timeout: 120_000
  @tag :capture_log
  test "100 independent AgentServers share one PostgreSQL resource without crossing Domain or checkpoint identity",
       c do
    Process.register(self(), __MODULE__)
    assert Ecto.Adapter.lookup_meta(Repo).opts[:pool_size] == 12
    started_at = System.monotonic_time(:millisecond)
    capacity = Keyword.put(c.opts, :max_workers, 128)

    runtime =
      start_supervised!(
        {Spectre.Stack.Runtime,
         stack: Stack, packages: [spectre_ledger: [server_store: capacity]]}
      )

    {:ok, resource} = Spectre.Stack.resolve(Stack, :resource, {:spectre_ledger, :server_store})
    {:ok, shared_store} = Spectre.Stack.Runtime.resolve(runtime, resource)

    children =
      for id <- 1..100 do
        {Spectre.Server,
         id: {:agent, id},
         agent: Agent,
         stack_runtime: runtime,
         consumer: self(),
         identity: %{subject: "agent-#{id}", conversation: "shared-database"},
         gam: [storage: :ledger, operations: %{open: [confirm: false]}]}
      end

    group = start_supervised!({AgentGroup, children})

    servers =
      group
      |> Supervisor.which_children()
      |> Map.new(fn {{:agent, id}, server, :worker, _} -> {id, server} end)

    assert map_size(servers) == 100

    servers
    |> Task.async_stream(
      fn {id, server} -> Spectre.Server.input(server, "https://fixture.test/agent/#{id}") end,
      max_concurrency: 16,
      timeout: 30_000
    )
    |> Enum.each(fn result -> assert {:ok, {:ok, _}} = result end)

    opened =
      for _ <- 1..100 do
        assert_receive {:opened, %{url: url}}, 30_000
        url
      end

    assert MapSet.new(opened) == MapSet.new(1..100, &"https://fixture.test/agent/#{&1}")

    settled =
      for _ <- 1..100 do
        assert_receive {:server_settled, server, {:governed, %{outcome: %{status: :succeeded}}}},
                       30_000

        server
      end

    assert MapSet.new(settled) == MapSet.new(Map.values(servers))
    assert {:ok, %{ledger: history, projection: persisted}} = ServerStore.gam_domain(shared_store)

    identities =
      for {id, server} <- servers do
        # The synchronous read also crosses the result callback's checkpoint barrier.
        assert %{busy: false, outbox_size: 0} = Spectre.Server.status(server)
        server_state = :sys.get_state(server)
        managed = :sys.get_state(server_state.managed_owner)
        assert managed.journal == {ServerStore, shared_store}
        domain = managed.refs.domain
        {ServerStore, ^shared_store, checkpoint_key} = server_state.opts[:checkpoint]
        assert {:managed_agent, ^domain} = checkpoint_key

        assert {:ok, %{value: checkpoint}} = ServerStore.fetch(shared_store, checkpoint_key)
        assert {:ok, saved} = Spectre.Server.Checkpoint.decode_snapshot(checkpoint)
        assert saved.identity.subject == "agent-#{id}"
        assert [%{user: "https://fixture.test/agent/" <> suffix}] = saved.history
        assert suffix == Integer.to_string(id)

        assert {:ok, snapshot} = Spectre.GAM.Ledger.Store.load(history, domain, [])
        assert {:ok, projection} = Spectre.GAM.Domain.Projection.replay(snapshot)

        operations =
          projection.acts
          |> Map.values()
          |> Enum.filter(&match?(%{url: _}, &1.consequence["payload"]))

        assert [act] = operations
        assert act.consequence["payload"] == %{url: "https://fixture.test/agent/#{id}"}

        assert Enum.any?(projection.outcomes, fn {_, outcome} ->
                 outcome.act_ref == act.ref and outcome.status == :succeeded
               end)

        assert {:ok, %{cursor: cursor}} = ProjectionStore.head(persisted, domain)
        assert cursor.revision == projection.revision
        {domain, checkpoint_key}
      end

    assert identities |> Enum.map(&elem(&1, 0)) |> Enum.uniq() |> length() == 100
    assert identities |> Enum.map(&elem(&1, 1)) |> Enum.uniq() |> length() == 100

    assert %{rows: [[100]]} =
             SQL.query!(Repo, "SELECT count(*) FROM \"#{c.table}_gam_heads\"", [], log: false)

    assert %{rows: [[100]]} =
             SQL.query!(
               Repo,
               "SELECT count(DISTINCT domain_ref) FROM \"#{c.table}_gam_batches\"",
               [],
               log: false
             )

    assert %{rows: [[100]]} =
             SQL.query!(Repo, "SELECT count(*) FROM \"#{c.table}_gam_projection_heads\"", [],
               log: false
             )

    refute_receive {:opened, _}
    elapsed = System.monotonic_time(:millisecond) - started_at

    IO.puts(
      "100-Agent PostgreSQL isolation: 100 unique Domains/checkpoints, pool=12, max_workers=128, elapsed=#{elapsed}ms"
    )
  end
end
