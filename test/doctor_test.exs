defmodule SpectreLedger.DoctorTest.ReadOnlyBackend do
  @behaviour Spectre.Ledger.Backend

  def load(_config, _ref), do: raise("Doctor must not load")
  def compare_and_swap(_config, _write), do: raise("Doctor must not write")
  def head(_config, _stream_key), do: raise("Doctor must not read a head")
  def entries(_config, _stream_key, _opts), do: raise("Doctor must not list entries")
  def objects(_config, _stream_key, _opts), do: raise("Doctor must not list objects")
  def put_stream(_config, _stream_key, _entries, _objects), do: raise("Doctor must not import")
end

defmodule SpectreLedger.DoctorTest.IncompleteBackend do
  def load(_config, _ref), do: :not_found
end

defmodule SpectreLedger.DoctorTest.PartialReceiptBackend do
  @behaviour Spectre.Ledger.Backend

  def load(_config, _ref), do: :not_found
  def compare_and_swap(_config, _write), do: {:error, :unsupported}
  def head(_config, _stream_key), do: :not_found
  def entries(_config, _stream_key, _opts), do: {:ok, []}
  def objects(_config, _stream_key, _opts), do: {:ok, %{}}
  def put_stream(_config, _stream_key, _entries, _objects), do: {:error, :unsupported}
  def append_receipt(_config, _write), do: {:error, :unsupported}
end

defmodule SpectreLedger.DoctorTest.DiagnosticBackend do
  @behaviour Spectre.Ledger.Backend

  alias Spectre.Ledger.Config

  def load(_config, _ref), do: raise("Doctor must not load")
  def compare_and_swap(_config, _write), do: raise("Doctor must not write")
  def head(_config, _stream_key), do: raise("Doctor must not read a head")
  def entries(_config, _stream_key, _opts), do: raise("Doctor must not list entries")
  def objects(_config, _stream_key, _opts), do: raise("Doctor must not list objects")
  def put_stream(_config, _stream_key, _entries, _objects), do: raise("Doctor must not import")

  def doctor(%Config{} = config) do
    Config.get_backend(config, :diagnostic_result)
  end
end

defmodule SpectreLedger.DoctorTest do
  use ExUnit.Case, async: false

  alias Spectre.Ledger.Backend.Memory
  alias Spectre.Ledger.Doctor
  alias Spectre.Ledger.Doctor.Report

  test "composes Spectre Doctor and warns honestly for volatile Memory" do
    server = start_supervised!(Memory)

    assert {:ok, report} =
             Doctor.run(backend: :memory, server: server, namespace: "doctor-memory")

    assert report.status == :warning
    assert report.core.status == :ok
    assert report.summary.total == report.core.summary.total + 4
    assert report.summary.warnings == 1

    assert %{status: :warning, code: :memory_backend_volatile, details: details} =
             Enum.find(report.checks, &(&1.id == "ledger.backend"))

    assert details.durable == false
    assert details.configured == true
    refute inspect(report) =~ inspect(server)

    assert Report.acceptable?(report)
    refute Report.acceptable?(report, strict: true)

    assert %{
             "status" => "warning",
             "summary" => %{"warnings" => 1},
             "core" => %{"spectre_version" => "0.3.2"}
           } = report |> Report.format(:json) |> Jason.decode!()

    assert Report.format(report, :text) =~ "Spectre Ledger doctor 0.1.0: warning"

    assert %{status: :ok, code: :ledger_bundle_contract_observed} =
             Enum.find(report.checks, &(&1.id == "ledger.bundle"))

    assert %{status: :ok, code: :receipt_backend_callbacks_valid} =
             Enum.find(report.checks, &(&1.id == "ledger.receipts"))
  end

  test "custom backend diagnostics are callback-only and perform no backend I/O" do
    assert {:ok, report} = Doctor.run(backend: SpectreLedger.DoctorTest.ReadOnlyBackend)
    assert report.status == :ok

    assert %{status: :ok, code: :custom_backend_callbacks_valid} =
             Enum.find(report.checks, &(&1.id == "ledger.backend"))

    assert %{status: :skipped, code: :receipt_backend_not_configured} =
             Enum.find(report.checks, &(&1.id == "ledger.receipts"))
  end

  test "reports a missing backend callback without leaking configuration" do
    secret = "postgres://private-user:private-password@private-host/private-db"

    assert {:ok, report} =
             Doctor.run(
               backend: SpectreLedger.DoctorTest.IncompleteBackend,
               secret_configuration: secret
             )

    assert report.status == :error

    assert %{status: :error, code: :ledger_backend_callback_missing} =
             Enum.find(report.checks, &(&1.id == "ledger.backend"))

    refute inspect(report) =~ secret
    refute Report.format(report, :json) =~ secret

    assert {:ok, unavailable} = Doctor.run(backend: SpectreLedger.NotLoadedBackend)

    assert %{status: :error, code: :ledger_backend_not_loaded} =
             Enum.find(unavailable.checks, &(&1.id == "ledger.backend"))
  end

  test "fails closed when a custom backend exposes only part of the receipt capability" do
    assert {:ok, report} =
             Doctor.run(backend: SpectreLedger.DoctorTest.PartialReceiptBackend)

    assert report.status == :error

    assert %{status: :error, code: :receipt_backend_callbacks_incomplete} =
             Enum.find(report.checks, &(&1.id == "ledger.receipts"))
  end

  test "uses an explicitly read-only custom schema diagnostic when provided" do
    current = %{
      schema_version: 1,
      expected_schema_version: 1,
      tables: %{meta: true, streams: :present, entries: :ok, blobs: %{present: true}}
    }

    assert {:ok, report} =
             Doctor.run(
               backend: SpectreLedger.DoctorTest.DiagnosticBackend,
               diagnostic_result: {:ok, current}
             )

    assert %{status: :ok, code: :backend_schema_current, details: %{table_count: 4}} =
             Enum.find(report.checks, &(&1.id == "ledger.backend"))

    incomplete = put_in(current, [:tables, :entries], false)

    assert {:ok, report} =
             Doctor.run(
               backend: SpectreLedger.DoctorTest.DiagnosticBackend,
               diagnostic_result: {:ok, incomplete}
             )

    assert %{status: :error, code: :backend_schema_incomplete} =
             Enum.find(report.checks, &(&1.id == "ledger.backend"))

    missing_version = %{current | schema_version: nil}

    assert {:ok, report} =
             Doctor.run(
               backend: SpectreLedger.DoctorTest.DiagnosticBackend,
               diagnostic_result: {:ok, missing_version}
             )

    assert %{status: :error, code: :backend_schema_incomplete} =
             Enum.find(report.checks, &(&1.id == "ledger.backend"))

    assert {:ok, report} =
             Doctor.run(
               backend: SpectreLedger.DoctorTest.DiagnosticBackend,
               diagnostic_result: {:error, {:connection_failed, "private-host"}}
             )

    assert %{
             status: :error,
             code: :backend_schema_unavailable,
             details: %{reason_class: :connection_failed}
           } = Enum.find(report.checks, &(&1.id == "ledger.backend"))

    refute inspect(report) =~ "private-host"

    assert {:ok, report} =
             Doctor.run(
               backend: SpectreLedger.DoctorTest.DiagnosticBackend,
               diagnostic_result: :invalid
             )

    assert %{status: :error, code: :backend_doctor_result_invalid} =
             Enum.find(report.checks, &(&1.id == "ledger.backend"))
  end

  test "validates options and always classifies telemetry failures" do
    assert Doctor.contract_version() == 1
    assert {:ok, _default_report} = Doctor.run()
    assert {:error, :invalid_ledger_doctor_options} = Doctor.run(:invalid)

    assert {:error, :duplicate_ledger_doctor_options} =
             Doctor.run(backend: :memory, backend: :memory)

    assert {:error, :invalid_ledger_doctor_core_options} = Doctor.run(core: :invalid)

    caller = self()

    assert {:error, :invalid_ledger_namespace} =
             Doctor.run(
               backend: :memory,
               namespace: "../../private",
               telemetry_handler: fn event, measurements, metadata ->
                 send(caller, {:telemetry, event, measurements, metadata})
               end
             )

    assert_received {:telemetry, [:spectre, :ledger, :doctor, :stop], measurements,
                     %{
                       operation: :doctor,
                       outcome: :error,
                       reason_class: :invalid_ledger_namespace
                     }}

    assert Enum.all?(measurements, fn {_key, value} -> is_number(value) end)
  end

  test "text output includes the core checks that contribute to its summary" do
    assert {:ok, report} = Doctor.run(core: [packages: []])
    assert report.status == :error
    assert report.summary.errors > 0

    text = Report.format(report, :text)
    assert text =~ "packages.compatibility"
    assert text =~ "packages_empty"
  end

  test "mix task emits JSON, accepts warnings normally, and strict mode fails" do
    Mix.shell(Mix.Shell.Process)

    Mix.Task.reenable("spectre_ledger.doctor")
    assert :ok = Mix.Tasks.SpectreLedger.Doctor.run(["--format", "json"])
    assert_received {:mix_shell, :info, [json]}
    assert %{"status" => "warning"} = Jason.decode!(IO.iodata_to_binary(json))

    Mix.Task.reenable("spectre_ledger.doctor")

    assert_raise Mix.Error, ~r/\[spectre_ledger_doctor_strict_failed\]/, fn ->
      Mix.Tasks.SpectreLedger.Doctor.run(["--strict"])
    end

    Mix.Task.reenable("spectre_ledger.doctor")

    assert_raise Mix.Error, ~r/\[spectre_ledger_doctor_invalid_backend\]/, fn ->
      Mix.Tasks.SpectreLedger.Doctor.run(["--backend", "unknown"])
    end

    Mix.Task.reenable("spectre_ledger.doctor")

    assert_raise Mix.Error, ~r/\[spectre_ledger_doctor_invalid_format\]/, fn ->
      Mix.Tasks.SpectreLedger.Doctor.run(["--format", "yaml"])
    end

    Mix.Task.reenable("spectre_ledger.doctor")

    assert_raise Mix.Error, ~r/\[spectre_ledger_doctor_invalid_repo\]/, fn ->
      Mix.Tasks.SpectreLedger.Doctor.run(["--repo", "not-a-module"])
    end

    Mix.Task.reenable("spectre_ledger.doctor")

    assert_raise Mix.Error, ~r/\[spectre_ledger_doctor_invalid_arguments\]/, fn ->
      Mix.Tasks.SpectreLedger.Doctor.run(["unexpected"])
    end
  end

  test "Report redacts unsupported detail values and normalizes keys" do
    assert {:ok, report} = Doctor.run(backend: SpectreLedger.DoctorTest.ReadOnlyBackend)

    check = %{
      id: "redaction",
      status: :ok,
      code: :redaction_checked,
      summary: "redaction checked",
      details: %{
        :atom_key => [:ok, self()],
        "string_key" => :value,
        self() => self()
      }
    }

    custom = %{report | checks: [check]}
    data = custom |> Report.format() |> then(fn _text -> Report.to_map(custom) end)

    assert %{
             "atom_key" => ["ok", "<redacted>"],
             "string_key" => "value",
             "redacted_key" => "<redacted>"
           } = hd(data.checks).details
  end
end
