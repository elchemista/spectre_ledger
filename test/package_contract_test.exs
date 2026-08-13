defmodule SpectreLedger.PackageContractTest.ErrorBackend do
  @behaviour Spectre.Ledger.Backend

  def load(_config, _ref), do: {:error, :load_unavailable}
  def compare_and_swap(_config, _write), do: {:error, :write_unavailable}
  def head(_config, _stream_key), do: {:error, :head_unavailable}
  def entries(_config, "objects-error", _opts), do: {:ok, []}
  def entries(_config, _stream_key, _opts), do: {:error, :entries_unavailable}
  def objects(_config, _stream_key, _opts), do: {:error, :objects_unavailable}

  def migrate(_config, _legacy_ref, _stable_ref, _checkpoint, _write),
    do: {:error, :migration_unavailable}

  def put_stream(_config, _stream_key, _entries, _objects),
    do: {:error, :import_unavailable}
end

defmodule SpectreLedger.PackageContractTest.HeadOnlyBackend do
  def head(_config, _stream_key), do: :not_found
end

defmodule SpectreLedger.PackageContractTest do
  use ExUnit.Case, async: false

  alias Spectre.AgentRef
  alias Spectre.Instance.CheckpointStore.Conformance, as: CheckpointConformance
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Backend.Memory
  alias Spectre.Ledger.Bundle
  alias Spectre.Ledger.CheckpointStore
  alias Spectre.Ledger.Config
  alias Spectre.Stack.Conformance, as: StackConformance
  alias Spectre.Stack.Installable
  alias Spectre.Subject

  test "package manifest conforms against Spectre 0.3.1" do
    assert {:ok, package} = Installable.verify(Spectre.Ledger)
    assert package.id == :spectre_ledger
    assert package.version == "0.1.0"
    assert package.spectre == "~> 0.3.1"
    assert package.agent_extensions == []
    assert package.operations == []
    assert package.actions == []
    assert package.resources == []

    assert {:ok, report} = StackConformance.run([Spectre.Ledger])
    assert report.core_version == "0.3.1"
    assert report.package_count == 1
  end

  test "checkpoint_store keeps runtime handles outside the package manifest" do
    server = self()
    store = Spectre.Ledger.checkpoint_store(backend: :memory, server: server)

    assert {Spectre.Ledger.CheckpointStore, opts} = store
    assert opts[:server] == server

    assert {:ok, config} = Config.new(opts)
    assert Config.get_backend(config, :server) == server

    assert {:ok, package} = Installable.verify(Spectre.Ledger)
    refute inspect(package) =~ inspect(server)
  end

  test "public facade reads, verifies, exports, and imports a complete stream" do
    server = start_supervised!(Memory)
    ref = isolated_ref("facade")
    source = [backend: :memory, server: server, namespace: "facade.source"]
    destination = [backend: :memory, server: server, namespace: "facade.destination"]

    assert Spectre.Ledger.version() == "0.1.0"
    assert Spectre.Ledger.checkpoint_store() == {CheckpointStore, []}
    assert Spectre.Ledger.checkpoint_store(source) == {CheckpointStore, source}

    assert {:ok, %{revision: 3}} =
             CheckpointConformance.run({CheckpointStore, source}, ref)

    assert {:ok, head} = Spectre.Ledger.head(ref, source)
    assert head.revision == 3
    assert {:ok, ^head} = Spectre.Ledger.head(ref.key, source)

    assert {:ok, entries} = Spectre.Ledger.entries(ref, source)
    assert Enum.map(entries, & &1.revision) == [1, 2, 3]

    assert {:ok, [^head]} =
             Spectre.Ledger.entries(ref.key, source ++ [after_revision: 2, limit: 1])

    assert {:ok, chain} = Spectre.Ledger.verify(ref, source)
    assert chain.entry_count == 3
    assert chain.head_revision == 3
    assert chain.head_entry_digest == head.entry_digest

    assert {:ok, encoded} = Spectre.Ledger.export_bundle(ref, source)
    assert {:ok, decoded} = Bundle.decode(encoded)
    assert decoded.entries == entries

    assert {:error, :memory_server_required} = Spectre.Ledger.head(ref)
    assert {:error, :memory_server_required} = Spectre.Ledger.entries(ref)
    assert {:error, :memory_server_required} = Spectre.Ledger.verify(ref)
    assert {:error, :memory_server_required} = Spectre.Ledger.export_bundle(ref)
    assert {:error, :memory_server_required} = Spectre.Ledger.import_bundle(decoded)

    assert {:ok, :imported, import_report} =
             Spectre.Ledger.import_bundle(decoded, destination)

    assert import_report.entry_count == 3

    assert {:ok, :idempotent, ^import_report} =
             Spectre.Ledger.import_bundle(encoded, destination)

    assert {:ok, destination_head} = Spectre.Ledger.head(ref, destination)
    assert destination_head.entry_digest == head.entry_digest
  end

  test "public facade preserves backend errors and rejects invalid boundaries" do
    error_backend = SpectreLedger.PackageContractTest.ErrorBackend
    incomplete_backend = SpectreLedger.PackageContractTest.HeadOnlyBackend
    missing_backend = SpectreLedger.PackageContractTest.NotLoadedBackend

    assert {:error, :invalid_ledger_stream_key} = Spectre.Ledger.head(nil, backend: error_backend)

    assert {:error, :invalid_ledger_stream_key} =
             Spectre.Ledger.entries("", backend: error_backend)

    assert {:error, :invalid_ledger_stream_key} =
             Spectre.Ledger.verify(:stream, backend: error_backend)

    assert {:error, :invalid_ledger_stream_key} =
             Spectre.Ledger.export_bundle([], backend: error_backend)

    assert {:error, :head_unavailable} = Spectre.Ledger.head("stream", backend: error_backend)

    assert {:error, :entries_unavailable} =
             Spectre.Ledger.entries("stream", backend: error_backend)

    assert {:error, :entries_unavailable} =
             Spectre.Ledger.verify("stream", backend: error_backend)

    assert {:error, :entries_unavailable} =
             Spectre.Ledger.export_bundle("stream", backend: error_backend)

    assert {:error, :objects_unavailable} =
             Spectre.Ledger.export_bundle("objects-error", backend: error_backend)

    assert {:error, :partial_ledger_stream_not_verifiable} =
             Spectre.Ledger.verify("stream", backend: error_backend, after_revision: 1)

    assert {:error, :partial_ledger_stream_not_verifiable} =
             Spectre.Ledger.verify("stream", backend: error_backend, limit: 1)

    assert {:error, :invalid_ledger_options} = Spectre.Ledger.verify("stream", :invalid)

    assert :not_found = Spectre.Ledger.head("stream", backend: incomplete_backend)

    assert {:error, {:ledger_backend_callback_missing, ^incomplete_backend, :entries, 3}} =
             Spectre.Ledger.entries("stream", backend: incomplete_backend)

    assert {:error, {:ledger_backend_callback_missing, ^incomplete_backend, :put_stream, 4}} =
             Spectre.Ledger.import_bundle("{}", backend: incomplete_backend)

    assert {:error, {:ledger_backend_not_loaded, ^missing_backend}} =
             Spectre.Ledger.head("stream", backend: missing_backend)

    assert {:error, {:ledger_backend_not_loaded, ^missing_backend}} =
             Spectre.Ledger.import_bundle("{}", backend: missing_backend)

    assert {:error, :invalid_ledger_bundle} =
             Spectre.Ledger.import_bundle(:invalid, backend: error_backend)

    assert {:error, :invalid_ledger_bundle_json} =
             Spectre.Ledger.import_bundle("not-json", backend: error_backend)

    assert {:error, :invalid_ledger_namespace} =
             Spectre.Ledger.head("stream", namespace: "../../private")
  end

  test "configuration maps named backends, retains opaque handles, and applies defaults" do
    handle = self()

    assert {:ok, memory} = Config.new(server: handle)
    assert memory.backend == Memory
    assert memory.namespace == "default"
    assert memory.max_checkpoint_bytes == 8_000_000
    assert Config.fetch_backend(memory, :server) == {:ok, handle}
    assert Config.fetch_backend(memory, :missing) == :error
    assert Config.get_backend(memory, :server) == handle
    assert Config.get_backend(memory, :missing) == nil
    assert Config.get_backend(memory, :missing, :fallback) == :fallback
    assert Config.new(memory) == {:ok, memory}

    assert {:ok, postgres} = Config.new(backend: :postgres)
    assert postgres.backend == Spectre.Ledger.Backend.Postgres

    assert {:ok, custom} =
             Config.new(
               backend: SpectreLedger.PackageContractTest.ErrorBackend,
               namespace: "tenant:one",
               max_checkpoint_bytes: 1,
               custom_handle: handle
             )

    assert custom.backend == SpectreLedger.PackageContractTest.ErrorBackend
    assert custom.namespace == "tenant:one"
    assert custom.max_checkpoint_bytes == 1
    assert custom.backend_opts == [custom_handle: handle]
  end

  test "configuration rejects unsafe, duplicate, and structurally invalid options" do
    assert {:error, :invalid_ledger_namespace} = Config.new(namespace: "../../outside")
    assert {:error, :invalid_ledger_namespace} = Config.new(namespace: "has space")
    assert {:error, :invalid_ledger_namespace} = Config.new(namespace: "")
    assert {:error, :invalid_ledger_namespace} = Config.new(namespace: String.duplicate("a", 129))
    assert {:error, :invalid_ledger_namespace} = Config.new(namespace: <<255>>)
    assert {:error, :invalid_max_checkpoint_bytes} = Config.new(max_checkpoint_bytes: 0)
    assert {:error, :invalid_max_checkpoint_bytes} = Config.new(max_checkpoint_bytes: "8")
    assert {:error, :invalid_ledger_backend} = Config.new(backend: nil)
    assert {:error, :invalid_ledger_backend} = Config.new(backend: "memory")
    assert {:error, :invalid_ledger_options} = Config.new(namespace: "one", namespace: "two")
    assert {:error, :invalid_ledger_options} = Config.new([:not_a_keyword])
    assert {:error, :invalid_ledger_options} = Config.new(%{})

    valid = %Config{
      backend: Memory,
      backend_opts: [],
      namespace: "valid",
      max_checkpoint_bytes: 1
    }

    assert {:error, :invalid_ledger_backend} = Config.new(%{valid | backend: nil})

    assert {:error, :invalid_ledger_backend_options} =
             Config.new(%{valid | backend_opts: %{server: self()}})

    assert {:error, :invalid_ledger_namespace} = Config.new(%{valid | namespace: "bad/value"})

    assert {:error, :invalid_max_checkpoint_bytes} =
             Config.new(%{valid | max_checkpoint_bytes: -1})
  end

  defp isolated_ref(label) do
    id = System.unique_integer([:positive, :monotonic])

    Ref.new(
      AgentRef.from_id("ledger-package-#{label}-#{id}"),
      Subject.new("ledger-package-subject-#{label}-#{id}")
    )
  end
end
