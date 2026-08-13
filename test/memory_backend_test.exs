defmodule SpectreLedger.MemoryBackendTest do
  use ExUnit.Case, async: true

  alias Spectre.AgentRef
  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Instance.Canonical
  alias Spectre.Instance.Canonical.Codec
  alias Spectre.Instance.CheckpointStore, as: CoreCheckpointStore
  alias Spectre.Instance.CheckpointStore.Conformance
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Backend.Memory
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.CheckpointStore
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.Receipt
  alias Spectre.State
  alias Spectre.Subject

  test "is caller-owned and passes the public Spectre checkpoint conformance" do
    server = start_supervised!(Memory)
    ref = isolated_ref("conformance")
    opts = memory_opts(server, "conformance")

    assert {:registered_name, []} = Process.info(server, :registered_name)

    assert {:ok, report} = Conformance.run({CheckpointStore, opts}, ref)
    assert report.revision == 3
    assert report.concurrent_cas == :single_winner
    assert report.exact_retry == :accepted

    assert {:ok, config} = Config.new(opts)
    assert {:ok, entries} = Memory.entries(config, ref.key, [])

    assert Enum.map(entries, & &1.revision) == [1, 2, 3]
    assert Enum.map(entries, & &1.expected_revision) == [0, 1, 2]
    assert {:ok, %{entry_count: 3, head_revision: 3}} = Chain.verify(entries)

    assert :ok =
             Conformance.read_after_restart(
               {CheckpointStore, opts},
               {CheckpointStore, opts},
               ref
             )
  end

  test "exact retries are idempotent, divergent same-revision writes conflict, and stale writes report the head" do
    server = start_supervised!(Memory)
    ref = isolated_ref("cas")
    opts = memory_opts(server, "cas")
    config = config!(opts)
    first = checkpoint!(ref, 1, "first")
    divergent = checkpoint!(ref, 1, "divergent")
    second = checkpoint!(ref, 2, "second")

    first_write = prepare!(config, ref, first, 0, 1)

    assert {:ok, %Receipt{status: :appended, revision: 1} = receipt} =
             Memory.compare_and_swap(config, first_write)

    assert {:ok, %Receipt{status: :idempotent} = retry} =
             Memory.compare_and_swap(config, first_write)

    assert retry.entry_digest == receipt.entry_digest

    assert {:error, {:conflict, 1}} =
             Memory.compare_and_swap(config, prepare!(config, ref, divergent, 0, 1))

    assert {:error, {:stale, 1}} =
             Memory.compare_and_swap(config, prepare!(config, ref, second, 0, 2))

    assert {:ok, [only]} = Memory.entries(config, ref.key, [])
    assert only.entry_digest == receipt.entry_digest
    assert {:ok, ^first} = Memory.load(config, ref)
  end

  test "accepts checkpoint-manager coalescing gaps and keeps entries ordered" do
    server = start_supervised!(Memory)
    ref = isolated_ref("coalescing")
    config = config!(memory_opts(server, "coalescing"))
    first = checkpoint!(ref, 1, "first")
    coalesced = checkpoint!(ref, 4, "coalesced")

    assert {:ok, %Receipt{status: :appended}} =
             Memory.compare_and_swap(config, prepare!(config, ref, first, 0, 1))

    assert {:ok, %Receipt{status: :appended}} =
             Memory.compare_and_swap(config, prepare!(config, ref, coalesced, 1, 4))

    assert {:ok, entries} = Memory.entries(config, ref.key, [])
    assert Enum.map(entries, &{&1.expected_revision, &1.revision}) == [{0, 1}, {1, 4}]
    assert {:ok, %{head_revision: 4}} = Chain.verify(entries)
    assert {:ok, head} = Memory.head(config, ref.key)
    assert head.revision == 4
    assert {:ok, [^head]} = Memory.entries(config, ref.key, after_revision: 1, limit: 1)
    assert {:ok, ^coalesced} = Memory.load(config, ref)
  end

  test "isolates identical stream keys by namespace" do
    server = start_supervised!(Memory)
    ref = isolated_ref("namespace")
    left_opts = memory_opts(server, "left")
    right_opts = memory_opts(server, "right")
    left = checkpoint!(ref, 1, "left")
    right = checkpoint!(ref, 1, "right")

    assert :ok = persist(left_opts, ref, left, 0, 1)
    assert :ok = persist(right_opts, ref, right, 0, 1)

    assert {:ok, ^left} = CoreCheckpointStore.load({CheckpointStore, left_opts}, ref, [])
    assert {:ok, ^right} = CoreCheckpointStore.load({CheckpointStore, right_opts}, ref, [])

    assert {:ok, left_head} = Memory.head(config!(left_opts), ref.key)
    assert {:ok, right_head} = Memory.head(config!(right_opts), ref.key)
    refute left_head.entry_digest == right_head.entry_digest
  end

  test "migration aliases reads to the stable stream without deleting legacy history" do
    server = start_supervised!(Memory)
    legacy_ref = isolated_ref("legacy")
    stable_ref = isolated_ref("stable")
    opts = memory_opts(server, "migration")
    store = {CheckpointStore, opts}
    config = config!(opts)
    legacy = checkpoint!(legacy_ref, 1, "legacy")
    migrated = checkpoint!(stable_ref, 1, "migrated")

    assert :ok = persist(opts, legacy_ref, legacy, 0, 1)

    assert :ok =
             CoreCheckpointStore.migrate_instance_key(
               store,
               legacy_ref,
               stable_ref,
               legacy,
               migrated,
               []
             )

    assert {:ok, ^migrated} = CoreCheckpointStore.load(store, legacy_ref, [])
    assert {:ok, ^migrated} = CoreCheckpointStore.load(store, stable_ref, [])

    assert {:ok, [legacy_entry]} = Memory.entries(config, legacy_ref.key, [])
    assert legacy_entry.kind == :checkpoint
    assert {:ok, ^legacy_entry} = Memory.head(config, legacy_ref.key)

    assert {:ok, [migration_entry]} = Memory.entries(config, stable_ref.key, [])
    assert migration_entry.kind == :migration
    assert migration_entry.source_entry_digest == legacy_entry.entry_digest

    assert {:error, {:aliased_stream, stable_key}} =
             persist(opts, legacy_ref, checkpoint!(legacy_ref, 2, "late-legacy"), 1, 2)

    assert stable_key == stable_ref.key
    assert {:ok, [^legacy_entry]} = Memory.entries(config, legacy_ref.key, [])

    assert :ok =
             CoreCheckpointStore.migrate_instance_key(
               store,
               legacy_ref,
               stable_ref,
               legacy,
               migrated,
               []
             )
  end

  test "put_stream validates complete content and is idempotent but never overwrites" do
    server = start_supervised!(Memory)
    ref = isolated_ref("import")
    source = config!(memory_opts(server, "source"))
    destination = config!(memory_opts(server, "destination"))
    first = checkpoint!(ref, 1, "first")
    fourth = checkpoint!(ref, 4, "fourth")

    assert {:ok, _receipt} =
             Memory.compare_and_swap(source, prepare!(source, ref, first, 0, 1))

    assert {:ok, _receipt} =
             Memory.compare_and_swap(source, prepare!(source, ref, fourth, 1, 4))

    assert {:ok, entries} = Memory.entries(source, ref.key, [])

    blobs = %{
      Enum.at(entries, 0).blob_digest => first,
      Enum.at(entries, 1).blob_digest => fourth
    }

    assert {:ok, :imported} = Memory.put_stream(destination, ref.key, entries, blobs)
    assert {:ok, :idempotent} = Memory.put_stream(destination, ref.key, entries, blobs)
    assert {:ok, ^fourth} = Memory.load(destination, ref)

    assert {:error, :ledger_import_blob_set_mismatch} =
             Memory.put_stream(
               destination,
               ref.key,
               entries,
               Map.put(blobs, String.duplicate("f", 64), "unexpected")
             )

    assert {:error, {:stream_conflict, stream_key}} =
             Memory.put_stream(destination, ref.key, [hd(entries)], %{
               hd(entries).blob_digest => first
             })

    assert stream_key == ref.key
    assert {:ok, ^fourth} = Memory.load(destination, ref)
  end

  test "requires a live caller-supplied server" do
    ref = isolated_ref("configuration")
    checkpoint = checkpoint!(ref, 1, "configuration")
    config = config!(backend: :memory, namespace: "missing-server")

    assert {:error, :memory_server_required} = Memory.load(config, ref)

    assert {:error, :memory_server_required} =
             Memory.compare_and_swap(config, prepare!(config, ref, checkpoint, 0, 1))
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
                   id: "memory-backend-snapshot-#{marker}-#{step}",
                   correlation_id: "memory-backend-#{marker}-#{step}"
                 )

        assert {:ok, change} =
                 Canonical.change(
                   snapshot,
                   %{correlations: Map.put(correlations, :memory_marker, {marker, step})},
                   id: "memory-backend-change-#{marker}-#{step}",
                   provenance: %{source: :spectre_ledger_memory_test},
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

  defp config!(opts) do
    assert {:ok, config} = Config.new(opts)
    config
  end

  defp memory_opts(server, namespace) do
    [backend: :memory, server: server, namespace: namespace]
  end

  defp isolated_ref(label) do
    id = System.unique_integer([:positive, :monotonic])

    Ref.new(
      AgentRef.from_id("ledger-memory-#{label}-#{id}"),
      Subject.new("ledger-memory-subject-#{label}-#{id}")
    )
  end
end
