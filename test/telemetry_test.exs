defmodule SpectreLedger.TelemetryTest do
  use ExUnit.Case, async: true

  alias Spectre.Ledger.Telemetry

  test "keeps a closed numeric/redacted event contract" do
    caller = self()

    handler = fn event, measurements, metadata ->
      send(caller, {:telemetry, event, measurements, metadata})
    end

    assert :ok =
             Telemetry.emit(
               :append,
               %{duration_us: 12, count: :invalid, private: "checkpoint"},
               %{
                 operation: :append,
                 outcome: :error,
                 status: :failed,
                 backend: Spectre.Ledger.Backend.Memory,
                 stream_key: "private-stream",
                 revision: 3,
                 expected_revision: -1,
                 reason: {:conflict, "private checkpoint"},
                 private: "credential"
               },
               telemetry_handler: handler
             )

    assert_received {:telemetry, [:spectre, :ledger, :append], measurements, metadata}

    assert measurements.duration_us == 12
    assert measurements.count == 0
    assert Enum.all?(measurements, fn {_key, value} -> is_number(value) end)

    assert metadata.operation == :append
    assert metadata.outcome == :error
    assert metadata.status == :failed
    assert metadata.backend == "Spectre.Ledger.Backend.Memory"
    assert metadata.revision == 3
    assert metadata.reason_class == :conflict
    assert byte_size(metadata.stream_id) == 64
    refute Map.has_key?(metadata, :expected_revision)
    refute inspect(metadata) =~ "private"
  end

  test "invalid events and handlers remain observational" do
    caller = self()

    handler = fn event, measurements, metadata ->
      send(caller, {event, measurements, metadata})
    end

    assert :ok = Telemetry.emit([:bundle, nil], %{}, %{}, telemetry_handler: handler)
    refute_received _message

    assert :ok = Telemetry.emit([], %{}, %{}, telemetry_handler: handler)
    assert :ok = Telemetry.emit(:event, :bad, %{}, telemetry_handler: handler)
    assert :ok = Telemetry.emit(:event, %{}, :bad, telemetry_handler: handler)
    assert :ok = Telemetry.emit(:event, %{}, %{}, [:not_keyword])
    refute_received _message
  end

  test "identifier and reason classifiers never expose arbitrary terms" do
    assert byte_size(Telemetry.id_digest("opaque")) == 64
    assert Telemetry.id_digest(self()) == "unavailable"

    assert Telemetry.reason_class(%{kind: :timeout}) == :timeout
    assert Telemetry.reason_class({:conflict, "private"}) == :conflict
    assert Telemetry.reason_class({"private", :detail}) == :error
    assert Telemetry.reason_class(:closed) == :closed
    assert Telemetry.reason_class(%RuntimeError{message: "private"}) == :exception
    assert Telemetry.reason_class("private") == :error
  end

  test "copies only non-negative integer metadata and accepts a preclassified reason" do
    caller = self()

    assert :ok =
             Telemetry.emit(
               [:backend, :stop],
               %{byte_count: 9, object_count: 1},
               %{
                 bundle_version: 1,
                 head_revision: 2,
                 schema_version: 2,
                 table_count: 8,
                 reason_class: :unavailable
               },
               telemetry_handler: fn event, measurements, metadata ->
                 send(caller, {event, measurements, metadata})
               end
             )

    assert_received {[:spectre, :ledger, :backend, :stop], measurements,
                     %{
                       bundle_version: 1,
                       head_revision: 2,
                       schema_version: 2,
                       table_count: 8,
                       reason_class: :unavailable
                     }}

    assert measurements.byte_count == 9
    assert measurements.object_count == 1
  end
end
