Code.require_file("support/postgres_cluster_test.exs", __DIR__)

defmodule SpectreLedger.PostgresTestRepo do
  use Ecto.Repo,
    otp_app: :spectre_ledger,
    adapter: Ecto.Adapters.Postgres
end

defmodule SpectreLedger.PostgresBackendTest do
  use ExUnit.Case, async: false

  alias Ecto.Adapters.SQL
  alias Mix.Tasks.SpectreLedger.Doctor, as: DoctorTask
  alias Spectre.AgentRef
  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Instance.Canonical
  alias Spectre.Instance.Canonical.Codec
  alias Spectre.Instance.CheckpointStore, as: CoreCheckpointStore
  alias Spectre.Instance.CheckpointStore.Conformance
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Backend.Conformance, as: LedgerConformance
  alias Spectre.Ledger.Backend.Postgres
  alias Spectre.Ledger.Backend.Postgres.Migration
  alias Spectre.Ledger.Backend.Postgres.Names
  alias Spectre.Ledger.Bundle
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.CheckpointStore
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.Entry
  alias Spectre.Ledger.Receipt
  alias Spectre.State
  alias Spectre.Subject
  alias SpectreLedger.PostgresTestRepo, as: Repo
  alias SpectreLedger.TestPostgresCluster, as: Cluster

  @table_prefix "spectre_ledger_test"

  setup_all do
    cluster = Cluster.start!(Repo)
    on_exit(fn -> Cluster.stop(cluster) end)
    assert :ok = migrate(:up, @table_prefix)
    {:ok, cluster: cluster}
  end

  test "migration installs all five tables, doctor is read-only, and down is reversible" do
    suffix = System.unique_integer([:positive, :monotonic])
    prefix = "ledger_migration_#{suffix}"
    config = config!(postgres_opts("doctor", prefix))

    assert {:ok, before} = Postgres.doctor(config)
    refute before.ready?
    assert before.schema_version == nil
    assert Enum.all?(before.tables, fn {_table, present?} -> not present? end)

    assert :ok = migrate(:up, prefix)
    assert {:ok, installed} = Postgres.doctor(config)
    assert installed.ready?
    assert installed.schema_version == 1
    assert Enum.all?(installed.tables, fn {_table, present?} -> present? end)

    assert :ok = migrate(:down, prefix)
    assert {:ok, removed} = Postgres.doctor(config)
    refute removed.ready?
    assert Enum.all?(removed.tables, fn {_table, present?} -> not present? end)
  end

  test "doctor rejects a versioned schema whose table shapes are incomplete" do
    suffix = System.unique_integer([:positive, :monotonic])
    prefix = "ledger_incomplete_#{suffix}"
    config = config!(postgres_opts("doctor-incomplete", prefix))
    assert {:ok, names} = Names.new(table_prefix: prefix)

    statements = [
      """
      CREATE TABLE #{Names.qualified(names, :meta)} (
        "key" text PRIMARY KEY,
        "value" text NOT NULL,
        "updated_at" timestamptz NOT NULL DEFAULT transaction_timestamp()
      )
      """,
      "INSERT INTO #{Names.qualified(names, :meta)} (\"key\", \"value\") VALUES ('schema_version', '1')",
      "CREATE TABLE #{Names.qualified(names, :streams)} (\"placeholder\" integer)",
      "CREATE TABLE #{Names.qualified(names, :blobs)} (\"placeholder\" integer)",
      "CREATE TABLE #{Names.qualified(names, :entries)} (\"placeholder\" integer)",
      "CREATE TABLE #{Names.qualified(names, :aliases)} (\"placeholder\" integer)"
    ]

    Enum.each(statements, fn statement ->
      assert {:ok, _result} = SQL.query(Repo, statement, [], log: false)
    end)

    assert {:ok, diagnostic} = Postgres.doctor(config)
    refute diagnostic.ready?
    assert diagnostic.schema_version == 1
    assert Enum.all?(diagnostic.tables, fn {_table, present?} -> present? end)
    refute Enum.all?(diagnostic.columns, fn {_table, valid?} -> valid? end)

    assert :ok = migrate(:down, prefix)
  end

  test "passes the public core CAS conformance including the initial row race" do
    ref = isolated_ref("conformance")
    opts = postgres_opts("conformance")

    assert {:ok, report} = Conformance.run({CheckpointStore, opts}, ref)
    assert report.concurrent_cas == :single_winner
    assert report.exact_retry == :accepted
    assert report.revision == 3

    assert {:ok, config} = Config.new(opts)
    assert {:ok, entries} = Postgres.entries(config, ref.key, [])
    assert Enum.map(entries, & &1.revision) == [1, 2, 3]
    assert {:ok, %{entry_count: 3, head_revision: 3}} = Chain.verify(entries)

    assert :ok =
             Conformance.read_after_restart(
               {CheckpointStore, opts},
               {CheckpointStore, opts},
               ref
             )
  end

  test "passes the public Ledger backend conformance including bundle portability" do
    ref = isolated_ref("ledger-conformance")
    opts = postgres_opts("ledger-conformance")

    assert {:ok, report} = LedgerConformance.run(opts, ref)
    assert report.contract_version == 1
    assert report.checkpoint_store.concurrent_cas == :single_winner
    assert report.entry_count == 3
    assert report.head_revision == 3
    assert is_binary(report.bundle_checksum)
    assert report.import == :verified
  end

  test "persists coalesced revisions, accepts exact retry, and rejects stale or divergent writes" do
    ref = isolated_ref("coalesced")
    config = config!(postgres_opts("coalesced"))
    first = checkpoint!(ref, 3, "first")
    second = checkpoint!(ref, 8, "second")
    divergent = checkpoint!(ref, 8, "divergent")
    stale = checkpoint!(ref, 9, "stale")

    first_write = prepare!(config, ref, first, 0, 3)
    second_write = prepare!(config, ref, second, 3, 8)

    assert {:ok, %Receipt{status: :appended, revision: 3}} =
             Postgres.compare_and_swap(config, first_write)

    assert {:ok, %Receipt{status: :idempotent, revision: 3}} =
             Postgres.compare_and_swap(config, first_write)

    assert {:ok, %Receipt{status: :appended, revision: 8}} =
             Postgres.compare_and_swap(config, second_write)

    assert {:error, {:conflict, :revision_digest_mismatch}} =
             Postgres.compare_and_swap(config, prepare!(config, ref, divergent, 3, 8))

    assert {:error, {:conflict, :revision_digest_mismatch}} =
             Postgres.compare_and_swap(config, prepare!(config, ref, first, 0, 3))

    assert {:error, {:stale, 8}} =
             Postgres.compare_and_swap(config, prepare!(config, ref, stale, 3, 9))

    assert {:ok, ^second} = Postgres.load(config, ref)
    assert {:ok, %{revision: 8}} = Postgres.head(config, ref.key)
    assert {:ok, entries} = Postgres.entries(config, ref.key, [])
    assert Enum.map(entries, & &1.revision) == [3, 8]
    assert Enum.map(entries, & &1.expected_revision) == [0, 3]
    assert {:ok, %{head_revision: 8}} = Chain.verify(entries)
  end

  test "CAS rechecks an alias after waiting for the legacy stream lock" do
    legacy_ref = isolated_ref("alias-race-legacy")
    stable_ref = isolated_ref("alias-race-stable")
    namespace = "alias-race"
    opts = postgres_opts(namespace)
    config = config!(opts)
    legacy_checkpoint = checkpoint!(legacy_ref, 1, "alias-race-legacy")
    stable_checkpoint = checkpoint!(stable_ref, 1, "alias-race-stable")
    late_legacy_checkpoint = checkpoint!(legacy_ref, 2, "alias-race-late")

    assert :ok = persist(opts, legacy_ref, legacy_checkpoint, 0, 1)
    assert :ok = persist(opts, stable_ref, stable_checkpoint, 0, 1)
    assert {:ok, legacy_entries} = Postgres.entries(config, legacy_ref.key, [])

    parent = self()

    holder =
      Task.async(fn ->
        hold_legacy_lock_and_install_alias(
          parent,
          namespace,
          legacy_ref.key,
          stable_ref.key
        )
      end)

    holder_pid = holder.pid

    assert_receive {:alias_holder_locked, ^holder_pid, blocker_backend_pid}, 5_000

    write = prepare!(config, legacy_ref, late_legacy_checkpoint, 1, 2)
    cas = Task.async(fn -> Postgres.compare_and_swap(config, write) end)

    try do
      assert :ok = wait_until_blocked_by(blocker_backend_pid, cas)
      send(holder_pid, :install_alias)

      assert {:ok, :aliased} = Task.await(holder, 5_000)
      assert {:error, :ledger_stream_is_alias} = Task.await(cas, 5_000)

      assert {:ok, ^legacy_entries} = Postgres.entries(config, legacy_ref.key, [])
      assert {:ok, ^stable_checkpoint} = Postgres.load(config, legacy_ref)
      assert {:ok, ^stable_checkpoint} = Postgres.load(config, stable_ref)
    after
      send(holder_pid, :abort)
      shutdown_if_alive(cas)
      shutdown_if_alive(holder)
    end
  end

  test "atomically aliases a validated legacy stream and makes retries idempotent" do
    legacy_ref = isolated_ref("legacy")
    stable_ref = isolated_ref("stable")
    opts = postgres_opts("migration")
    config = config!(opts)
    store = {CheckpointStore, opts}
    legacy_checkpoint = checkpoint!(legacy_ref, 4, "legacy")
    migrated_checkpoint = checkpoint!(stable_ref, 4, "stable")

    assert :ok = persist(opts, legacy_ref, legacy_checkpoint, 0, 4)

    assert :ok =
             CoreCheckpointStore.migrate_instance_key(
               store,
               legacy_ref,
               stable_ref,
               legacy_checkpoint,
               migrated_checkpoint,
               []
             )

    assert {:ok, ^migrated_checkpoint} = Postgres.load(config, stable_ref)
    assert {:ok, ^migrated_checkpoint} = Postgres.load(config, legacy_ref)
    assert {:ok, [legacy_entry]} = Postgres.entries(config, legacy_ref.key, [])
    assert legacy_entry.kind == :checkpoint
    assert {:ok, ^legacy_entry} = Postgres.head(config, legacy_ref.key)
    assert {:ok, stable_head} = Postgres.head(config, stable_ref.key)
    assert stable_head.kind == :migration
    assert stable_head.revision == 4
    assert is_binary(stable_head.source_entry_digest)

    assert :ok =
             CoreCheckpointStore.migrate_instance_key(
               store,
               legacy_ref,
               stable_ref,
               legacy_checkpoint,
               migrated_checkpoint,
               []
             )

    next_ref = isolated_ref("next-stable")
    next_migrated = checkpoint!(next_ref, 4, "next-stable")

    assert {:error, :ledger_migration_source_has_aliases} =
             CoreCheckpointStore.migrate_instance_key(
               store,
               stable_ref,
               next_ref,
               migrated_checkpoint,
               next_migrated,
               []
             )

    assert :not_found = Postgres.load(config, next_ref)

    next_checkpoint = checkpoint!(stable_ref, 7, "continued")
    assert :ok = persist(opts, stable_ref, next_checkpoint, 4, 7)
    assert {:ok, ^next_checkpoint} = Postgres.load(config, legacy_ref)
  end

  test "migration preserves a valid revision-zero checkpoint" do
    legacy_ref = isolated_ref("legacy-zero")
    stable_ref = isolated_ref("stable-zero")
    opts = postgres_opts("migration-zero")
    config = config!(opts)
    legacy_checkpoint = checkpoint_zero!(legacy_ref)
    stable_checkpoint = checkpoint_zero!(stable_ref)

    assert {:ok, source_write} =
             CheckpointStore.prepare(
               legacy_ref,
               legacy_checkpoint,
               0,
               0,
               [],
               config,
               :migration
             )

    assert {:ok, source_entry} = Entry.new(source_write, nil)

    assert {:ok, :imported} =
             Postgres.put_stream(
               config,
               legacy_ref.key,
               [source_entry],
               %{source_entry.blob_digest => legacy_checkpoint}
             )

    assert :ok =
             CoreCheckpointStore.migrate_instance_key(
               {CheckpointStore, opts},
               legacy_ref,
               stable_ref,
               legacy_checkpoint,
               stable_checkpoint,
               []
             )

    assert {:ok, ^stable_checkpoint} = Postgres.load(config, legacy_ref)
    assert {:ok, %{kind: :migration, revision: 0}} = Postgres.head(config, stable_ref.key)
  end

  test "imports and verifies a complete stream atomically" do
    source_ref = isolated_ref("export")
    source = config!(postgres_opts("export-source"))
    destination = config!(postgres_opts("export-destination"))
    first = checkpoint!(source_ref, 2, "first")
    fourth = checkpoint!(source_ref, 6, "fourth")

    assert {:ok, _receipt} =
             Postgres.compare_and_swap(source, prepare!(source, source_ref, first, 0, 2))

    assert {:ok, _receipt} =
             Postgres.compare_and_swap(source, prepare!(source, source_ref, fourth, 2, 6))

    assert {:ok, entries} = Postgres.entries(source, source_ref.key, [])

    blobs = %{
      Enum.at(entries, 0).blob_digest => first,
      Enum.at(entries, 1).blob_digest => fourth
    }

    assert {:ok, :imported} = Postgres.put_stream(destination, source_ref.key, entries, blobs)
    assert {:ok, :idempotent} = Postgres.put_stream(destination, source_ref.key, entries, blobs)
    assert {:ok, ^fourth} = Postgres.load(destination, source_ref)
    assert {:ok, ^blobs} = Postgres.objects(destination, source_ref.key, [])

    destination_opts = postgres_opts("export-destination")
    assert {:ok, bundle} = Spectre.Ledger.export_bundle(source_ref, destination_opts)
    assert {:ok, %{head_revision: 6}} = Bundle.verify(bundle)

    assert {:error, :ledger_import_blob_set_mismatch} =
             Postgres.put_stream(
               destination,
               source_ref.key,
               entries,
               Map.put(blobs, String.duplicate("f", 64), "unexpected")
             )
  end

  test "namespaces isolate equal stream keys and identifiers reject SQL fragments" do
    ref = isolated_ref("namespace")
    checkpoint = checkpoint!(ref, 1, "namespace")
    left = config!(postgres_opts("left"))
    right = config!(postgres_opts("right"))

    assert {:ok, _receipt} =
             Postgres.compare_and_swap(left, prepare!(left, ref, checkpoint, 0, 1))

    assert :not_found = Postgres.load(right, ref)

    assert {:error, {:invalid_postgres_identifier, :schema}} =
             Config.new(backend: :postgres, repo: Repo, schema: "public; DROP SCHEMA public")
             |> then(fn {:ok, config} -> Postgres.doctor(config) end)

    assert {:error, {:invalid_postgres_identifier, :table_prefix}} =
             Config.new(backend: :postgres, repo: Repo, table_prefix: "ledger-table")
             |> then(fn {:ok, config} -> Postgres.doctor(config) end)

    assert {:error, :invalid_ledger_entries_options} =
             Postgres.entries(left, ref.key, unknown: true)

    assert {:error, :invalid_ledger_entries_options} =
             Postgres.entries(left, ref.key, limit: 1, limit: 2)

    assert {:error, :invalid_ledger_entries_options} =
             Postgres.entries(left, ref.key, [{"not", "keyword"}])

    assert {:error, :invalid_ledger_stream_key} = Postgres.head(left, "")
    assert {:error, :invalid_ledger_entries_options} = Postgres.entries(left, "", [])
    assert {:error, :invalid_ledger_objects_options} = Postgres.objects(left, "", [])
    assert {:error, :invalid_ledger_objects_options} = Postgres.objects(left, ref.key, :invalid)
    assert {:error, :invalid_ledger_objects_options} = Postgres.objects(left, ref.key, limit: 1)
    assert {:error, :invalid_ledger_import} = Postgres.put_stream(left, "", [], %{})
    assert {:error, :invalid_ledger_config} = Postgres.doctor(:invalid)

    missing_repo = config!(backend: :postgres, repo: SpectreLedger.MissingRepo)

    assert {:error, {:ledger_postgres_repo_not_loaded, SpectreLedger.MissingRepo}} =
             Postgres.head(missing_repo, ref.key)

    assert {:error, :invalid_ledger_postgres_repo} =
             Postgres.head(config!(backend: :postgres, repo: nil), ref.key)

    assert {:error, :invalid_ledger_postgres_options} =
             Postgres.head(config!(backend: :postgres, repo: Repo, query_opts: %{}), ref.key)
  end

  test "load fails closed when a durable head or checkpoint object is corrupted" do
    pointer_ref = isolated_ref("corrupt-pointer")
    blob_ref = isolated_ref("corrupt-blob")
    namespace = "corrupt-load"
    config = config!(postgres_opts(namespace))

    pointer_checkpoint = checkpoint!(pointer_ref, 1, "pointer")
    blob_checkpoint = checkpoint!(blob_ref, 1, "blob")

    assert {:ok, _receipt} =
             Postgres.compare_and_swap(
               config,
               prepare!(config, pointer_ref, pointer_checkpoint, 0, 1)
             )

    assert {:ok, _blob_receipt} =
             Postgres.compare_and_swap(
               config,
               prepare!(config, blob_ref, blob_checkpoint, 0, 1)
             )

    assert {:ok, names} = Names.new(table_prefix: @table_prefix)

    assert {:ok, _result} =
             SQL.query(
               Repo,
               """
               UPDATE #{Names.qualified(names, :streams)}
               SET "head_entry_digest" = $3, "head_blob_digest" = $3
               WHERE "namespace" = $1 AND "stream_key" = $2
               """,
               [namespace, pointer_ref.key, String.duplicate("f", 64)],
               log: false
             )

    assert {:error, :invalid_ledger_postgres_checkpoint} = Postgres.load(config, pointer_ref)

    assert {:ok, _result} =
             SQL.query(
               Repo,
               """
               UPDATE #{Names.qualified(names, :blobs)}
               SET "checkpoint" = $3
               WHERE "namespace" = $1 AND "blob_digest" = $2
               """,
               [namespace, blob_digest!(config, blob_ref), "corrupt"],
               log: false
             )

    assert {:error, :invalid_ledger_postgres_checkpoint} = Postgres.load(config, blob_ref)
  end

  test "write transactions reject isolation levels that break alias visibility" do
    ref = isolated_ref("isolation")
    config = config!(postgres_opts("isolation"))
    checkpoint = checkpoint!(ref, 1, "isolation")
    write = prepare!(config, ref, checkpoint, 0, 1)

    result =
      Repo.checkout(fn ->
        assert {:ok, _set} =
                 SQL.query(
                   Repo,
                   "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ",
                   [],
                   log: false
                 )

        try do
          Postgres.compare_and_swap(config, write)
        after
          assert {:ok, _reset} =
                   SQL.query(
                     Repo,
                     "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED",
                     [],
                     log: false
                   )
        end
      end)

    assert result == {:error, :ledger_postgres_requires_read_committed}
    assert :not_found = Postgres.load(config, ref)
  end

  test "Ledger Doctor Mix task resolves a host-owned PostgreSQL Repo" do
    Mix.shell(Mix.Shell.Process)
    Mix.Task.reenable("spectre_ledger.doctor")

    assert :ok =
             DoctorTask.run([
               "--backend",
               "postgres",
               "--repo",
               "SpectreLedger.PostgresTestRepo",
               "--table-prefix",
               @table_prefix,
               "--namespace",
               "doctor-task",
               "--strict"
             ])

    assert_received {:mix_shell, :info, [output]}
    assert IO.iodata_to_binary(output) =~ "Spectre Ledger doctor 0.1.0: ok"
  end

  defp persist(opts, ref, checkpoint, expected, revision) do
    CoreCheckpointStore.persist(
      {CheckpointStore, opts},
      ref,
      checkpoint,
      expected,
      revision,
      []
    )
  end

  defp prepare!(config, ref, checkpoint, expected, revision) do
    assert {:ok, write} =
             CheckpointStore.prepare(ref, checkpoint, expected, revision, [], config)

    write
  end

  defp checkpoint!(ref, revision, marker) do
    assert {:ok, canonical} =
             Canonical.new(%{
               flow: %State{conversation_id: ref.key},
               work: %{},
               vigil: %{},
               directive: %{},
               control: %{},
               correlations: %{instance_key: ref.key},
               events: %{records: [], ids: %{}}
             })

    canonical =
      Enum.reduce(1..revision, canonical, fn step, current ->
        assert {:ok, correlations} = Canonical.fetch(current, :correlations)

        assert {:ok, snapshot} =
                 Canonical.snapshot(current,
                   read: [:correlations],
                   write: [:correlations],
                   id: "postgres-snapshot-#{marker}-#{step}",
                   correlation_id: "postgres-correlation-#{marker}-#{step}"
                 )

        assert {:ok, change} =
                 Canonical.change(
                   snapshot,
                   %{correlations: Map.put(correlations, :postgres_marker, {marker, step})},
                   id: "postgres-change-#{marker}-#{step}",
                   provenance: %{source: :spectre_ledger_postgres_test},
                   metadata: %{marker: marker, step: step}
                 )

        assert {:ok, next, _transition} = Canonical.commit(current, change)
        next
      end)

    assert canonical.revision == revision
    assert {:ok, checkpoint} = Codec.encode_json(canonical)
    assert {:ok, %{revision: ^revision}} = Foundation.verify_instance_checkpoint(checkpoint)
    checkpoint
  end

  defp checkpoint_zero!(ref) do
    assert {:ok, canonical} =
             Canonical.new(%{
               flow: %State{conversation_id: ref.key},
               work: %{},
               vigil: %{},
               directive: %{},
               control: %{},
               correlations: %{instance_key: ref.key},
               events: %{records: [], ids: %{}}
             })

    assert canonical.revision == 0
    assert {:ok, checkpoint} = Codec.encode_json(canonical)
    assert {:ok, %{revision: 0}} = Foundation.verify_instance_checkpoint(checkpoint)
    checkpoint
  end

  defp config!(opts) do
    assert {:ok, config} = Config.new(opts)
    config
  end

  defp blob_digest!(config, ref) do
    assert {:ok, head} = Postgres.head(config, ref.key)
    head.blob_digest
  end

  defp hold_legacy_lock_and_install_alias(parent, namespace, legacy_key, stable_key) do
    assert {:ok, names} = Names.new(table_prefix: @table_prefix)
    streams = Names.qualified(names, :streams)
    aliases = Names.qualified(names, :aliases)

    Repo.transaction(
      fn ->
        assert {:ok, %{rows: [[backend_pid]]}} =
                 SQL.query(Repo, "SELECT pg_backend_pid()", [], log: false)

        assert {:ok, _locked} =
                 SQL.query(
                   Repo,
                   """
                   SELECT "head_revision"
                   FROM #{streams}
                   WHERE "namespace" = $1 AND "stream_key" = $2
                   FOR UPDATE
                   """,
                   [namespace, legacy_key],
                   log: false
                 )

        send(parent, {:alias_holder_locked, self(), backend_pid})

        receive do
          :install_alias ->
            assert {:ok, %{num_rows: 1}} =
                     SQL.query(
                       Repo,
                       """
                       INSERT INTO #{aliases}
                         ("namespace", "alias_stream_key", "target_stream_key")
                       VALUES ($1, $2, $3)
                       """,
                       [namespace, legacy_key, stable_key],
                       log: false
                     )

            :aliased

          :abort ->
            Repo.rollback(:alias_race_aborted)
        after
          10_000 -> Repo.rollback(:alias_race_timeout)
        end
      end,
      log: false
    )
  end

  defp wait_until_blocked_by(blocker_backend_pid, task) do
    deadline = System.monotonic_time(:millisecond) + 5_000
    wait_until_blocked_by(blocker_backend_pid, task, deadline)
  end

  defp wait_until_blocked_by(blocker_backend_pid, task, deadline) do
    case Task.yield(task, 0) do
      {:ok, result} ->
        flunk("CAS completed before it waited for the legacy lock: #{inspect(result)}")

      {:exit, reason} ->
        flunk("CAS exited before it waited for the legacy lock: #{inspect(reason)}")

      nil ->
        assert {:ok, %{rows: [[blocked?]]}} =
                 SQL.query(
                   Repo,
                   """
                   SELECT EXISTS (
                     SELECT 1
                     FROM pg_stat_activity AS activity
                     WHERE $1 = ANY(pg_blocking_pids(activity.pid))
                       AND position($2 in activity.query) > 0
                   )
                   """,
                   [blocker_backend_pid, "#{@table_prefix}_streams"],
                   log: false
                 )

        cond do
          blocked? ->
            :ok

          System.monotonic_time(:millisecond) < deadline ->
            wait_until_blocked_by(blocker_backend_pid, task, deadline)

          true ->
            flunk("CAS did not block behind the legacy stream lock")
        end
    end
  end

  defp shutdown_if_alive(%Task{pid: pid} = task) do
    if Process.alive?(pid), do: Task.shutdown(task, :brutal_kill)
  end

  defp postgres_opts(namespace, table_prefix \\ @table_prefix) do
    [
      backend: :postgres,
      repo: Repo,
      namespace: namespace,
      table_prefix: table_prefix
    ]
  end

  defp isolated_ref(label) do
    id = System.unique_integer([:positive, :monotonic])

    Ref.new(
      AgentRef.from_id("ledger-postgres-#{label}-#{id}"),
      Subject.new("ledger-postgres-subject-#{label}-#{id}")
    )
  end

  defp migrate(direction, table_prefix) do
    sql =
      case direction do
        :up -> Migration.up_sql(table_prefix: table_prefix)
        :down -> Migration.down_sql(table_prefix: table_prefix)
      end

    with {:ok, statements} <- sql, do: execute_migration(statements)
  end

  defp execute_migration(statements) do
    Enum.reduce_while(statements, :ok, fn statement, :ok ->
      case SQL.query(Repo, statement, [], log: false) do
        {:ok, _result} -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end
end
