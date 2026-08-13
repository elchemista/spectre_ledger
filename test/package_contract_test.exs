defmodule SpectreLedger.PackageContractTest do
  use ExUnit.Case, async: true

  alias Spectre.Ledger.Config
  alias Spectre.Stack.Conformance
  alias Spectre.Stack.Installable

  test "package manifest conforms against Spectre 0.3.1" do
    assert {:ok, package} = Installable.verify(Spectre.Ledger)
    assert package.id == :spectre_ledger
    assert package.version == "0.1.0"
    assert package.spectre == "~> 0.3.1"
    assert package.agent_extensions == []
    assert package.operations == []
    assert package.actions == []
    assert package.resources == []

    assert {:ok, report} = Conformance.run([Spectre.Ledger])
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

  test "configuration rejects unsafe namespaces and invalid limits" do
    assert {:error, :invalid_ledger_namespace} = Config.new(namespace: "../../outside")
    assert {:error, :invalid_ledger_namespace} = Config.new(namespace: "has space")
    assert {:error, :invalid_max_checkpoint_bytes} = Config.new(max_checkpoint_bytes: 0)
  end
end
