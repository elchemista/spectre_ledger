defmodule SpectreLedger.IncompleteReceiptBackend do
  def load(_config, _ref), do: :not_found
  def compare_and_swap(_config, _write), do: {:error, :unsupported}
end

defmodule SpectreLedger.ReceiptReplyBackend do
  @moduledoc false

  alias Spectre.Ledger.Config

  def append_receipt(config, _write), do: reply(config, :append_result)
  def lookup_receipt(config, _id), do: reply(config, :lookup_result)
  def put_receipt_payload(config, _write), do: reply(config, :put_result)
  def get_receipt_payload(config, _ref), do: reply(config, :get_result)

  defp reply(config, key) do
    case Config.get_backend(config, key) do
      :raise -> raise "receipt backend failure"
      :throw -> throw(:receipt_backend_failure)
      result -> result
    end
  end
end

defmodule SpectreLedger.ReceiptSinkTest do
  use ExUnit.Case, async: true

  alias Spectre.AgentRef
  alias Spectre.Instance.Ref
  alias Spectre.Ledger
  alias Spectre.Ledger.Backend.Capabilities
  alias Spectre.Ledger.Backend.Memory
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.ReceiptBackend.Conformance, as: BackendConformance
  alias Spectre.Ledger.ReceiptChain
  alias Spectre.Ledger.ReceiptCodec
  alias Spectre.Ledger.ReceiptEntry
  alias Spectre.Ledger.ReceiptSink
  alias Spectre.Ledger.ReceiptWrite
  alias Spectre.Receipt.Envelope
  alias Spectre.Receipt.Sink
  alias Spectre.Receipt.Sink.Conformance
  alias Spectre.Subject

  test "passes Spectre receipt-sink conformance" do
    server = start_supervised!(Memory)
    opts = memory_opts(server, unique_namespace("core-conformance"))

    assert {:ok, report} = Conformance.run(Ledger.receipt_sink(opts))
    assert report.append == :verified
    assert report.idempotency == :verified
    assert report.lookup == :verified
    assert report.payload_store == :verified
  end

  test "passes the complete Ledger receipt-backend conformance" do
    server = start_supervised!(Memory)
    opts = memory_opts(server, unique_namespace("backend-conformance"))

    assert {:ok, report} =
             BackendConformance.run(opts, "instance:receipt-backend-conformance")

    assert report.contract_version == 1
    assert report.sink == :verified
    assert report.concurrent_append == :single_winner
    assert report.entry_count == 2
    assert report.payload_count == 2
    assert report.archive == :verified
  end

  test "receipt-backend conformance rejects invalid inputs and incomplete capabilities" do
    assert {:error, {:ledger_receipt_backend_conformance_failed, :options, :invalid}} =
             BackendConformance.run([], :invalid)

    assert {:error, {:ledger_receipt_backend_conformance_failed, :configuration, :invalid}} =
             BackendConformance.run([namespace: "invalid namespace"], "instance:invalid-config")

    assert {:error,
            {:ledger_receipt_backend_conformance_failed, :configuration, :callback_missing}} =
             BackendConformance.run(
               [backend: SpectreLedger.IncompleteReceiptBackend],
               "instance:incomplete-receipts"
             )
  end

  test "stages payloads, appends idempotently, and verifies the complete chain" do
    server = start_supervised!(Memory)
    namespace = unique_namespace("receipt-chain")
    opts = memory_opts(server, namespace)
    stream_key = "instance:receipt-chain"
    first = envelope(stream_key, 1)
    second = envelope(stream_key, 2)
    sink = normalized_sink(opts)

    first_ref = Sink.payload_ref(first)
    assert {:ok, ^first_ref} = Sink.put_payload(sink, first, [])
    assert {:ok, ^first} = Sink.get_payload(sink, first_ref, [])
    assert {:ok, []} = Ledger.receipt_entries(stream_key, opts)

    assert {:ok, :appended} = Sink.append(sink, first, [])
    assert {:ok, :idempotent} = Sink.append(sink, first, [])
    assert {:ok, :appended} = Sink.append(sink, second, [])
    assert {:ok, ^first} = Ledger.receipt(first.id, opts)
    assert :not_found = Ledger.receipt(first.id <> ":missing", opts)

    assert {:ok, [first_entry, second_entry]} = Ledger.receipt_entries(stream_key, opts)
    assert first_entry.sequence == 1
    assert first_entry.previous_entry_digest == nil
    assert second_entry.sequence == 2
    assert second_entry.previous_entry_digest == first_entry.entry_digest
    assert {:ok, %{entry_count: 2}} = ReceiptChain.verify([first_entry, second_entry])

    assert {:ok, [^first, ^second]} = Ledger.receipts(stream_key, opts)
    assert {:ok, [^second]} = Ledger.receipts(stream_key, opts ++ [after_sequence: 1])

    assert {:ok, report} = Ledger.verify_receipts(stream_key, opts)
    assert report.entry_count == 2
    assert report.object_count == 2
    assert report.head_sequence == 2
    assert report.canonical_ordered
    assert report.capture == :nondeterministic_boundaries
    assert report.state_digest_linkage
    refute report.deterministic_replay_claim
    refute report.exactly_once_external_effects

    assert {:error, :partial_ledger_receipt_stream_not_verifiable} =
             Ledger.verify_receipts(stream_key, opts ++ [limit: 1])
  end

  test "records physical append order without claiming canonical order" do
    server = start_supervised!(Memory)
    opts = memory_opts(server, unique_namespace("out-of-order"))
    stream_key = "instance:out-of-order"
    sink = normalized_sink(opts)
    later = envelope(stream_key, 9)
    earlier = envelope(stream_key, 4)

    assert {:ok, :appended} = Sink.append(sink, later, [])
    assert {:ok, :appended} = Sink.append(sink, earlier, [])

    assert {:ok, report} = Ledger.verify_receipts(stream_key, opts)
    refute report.canonical_ordered
    assert {:ok, [^later, ^earlier]} = Ledger.receipts(stream_key, opts)
  end

  test "isolates receipt ids and payload objects by namespace" do
    server = start_supervised!(Memory)
    stream_key = "instance:isolated"
    envelope = envelope(stream_key, 1)
    left = memory_opts(server, unique_namespace("left"))
    right = memory_opts(server, unique_namespace("right"))

    assert {:ok, :appended} = Sink.append(normalized_sink(left), envelope, [])
    assert {:ok, ^envelope} = Ledger.receipt(envelope.id, left)
    assert :not_found = Ledger.receipt(envelope.id, right)
    assert {:ok, []} = Ledger.receipt_entries(stream_key, right)
  end

  test "concurrent exact appends allocate one entry and remain idempotent" do
    server = start_supervised!(Memory)
    opts = memory_opts(server, unique_namespace("concurrent"))
    sink = normalized_sink(opts)
    stream_key = "instance:concurrent"
    envelope = envelope(stream_key, 1)

    results =
      1..16
      |> Task.async_stream(fn _index -> Sink.append(sink, envelope, []) end,
        max_concurrency: 16,
        ordered: false
      )
      |> Enum.map(fn {:ok, result} -> result end)

    assert Enum.count(results, &(&1 == {:ok, :appended})) == 1
    assert Enum.count(results, &(&1 == {:ok, :idempotent})) == 15
    assert {:ok, [_entry]} = Ledger.receipt_entries(stream_key, opts)
  end

  test "codec is canonical, duplicate-key safe, and bounded" do
    envelope = envelope("instance:codec", 1)
    assert {:ok, encoded} = ReceiptCodec.encode(envelope)
    assert {:ok, ^envelope} = ReceiptCodec.decode(encoded, byte_size(encoded))

    duplicate =
      String.replace(
        encoded,
        ~s("attempt_id":),
        ~s("attempt_id":null,"attempt_id":),
        global: false
      )

    assert {:error, :duplicate_ledger_receipt_json_key} =
             ReceiptCodec.decode(duplicate, byte_size(duplicate))

    assert {:error, {:ledger_receipt_too_large, max}} =
             ReceiptCodec.decode(encoded, byte_size(encoded) - 1)

    assert max == byte_size(encoded) - 1

    assert {:error, :invalid_ledger_receipt} = ReceiptCodec.encode(:invalid)
    assert {:error, :invalid_ledger_receipt_encoding} = ReceiptCodec.decode(:invalid, 10)
    assert {:error, :invalid_ledger_receipt_encoding} = ReceiptCodec.decode(encoded, 0)
    assert {:error, :invalid_ledger_receipt_json} = ReceiptCodec.decode("not-json", 100)

    decoded = Jason.decode!(encoded)
    pretty = Jason.encode!(decoded, pretty: true)

    assert {:error, :noncanonical_ledger_receipt_encoding} =
             ReceiptCodec.decode(pretty, byte_size(pretty))

    unknown = decoded |> Map.put("unknown", true) |> Jason.encode!()
    missing = decoded |> Map.delete("attempt_id") |> Jason.encode!()

    assert {:error, :invalid_ledger_receipt_fields} =
             ReceiptCodec.decode(unknown, byte_size(unknown))

    assert {:error, :invalid_ledger_receipt_fields} =
             ReceiptCodec.decode(missing, byte_size(missing))

    digest = Envelope.digest(envelope)

    assert {:error, :ledger_receipt_id_mismatch} =
             ReceiptCodec.verify(encoded, envelope.id <> ":other", digest, byte_size(encoded))

    assert {:error, :ledger_receipt_digest_mismatch} =
             ReceiptCodec.verify(
               encoded,
               envelope.id,
               String.duplicate("f", 64),
               byte_size(encoded)
             )
  end

  test "codec rejects duplicate keys nested through objects and lists" do
    envelope =
      envelope("instance:nested-codec", 1, %{items: [%{sample: 1}], nested: %{sample: 2}})

    assert {:ok, encoded} = ReceiptCodec.encode(envelope)

    nested_duplicate =
      String.replace(
        encoded,
        ~s({"$spectre":"map","entries":[[{"$spectre":"atom","value":"sample"},2]]}),
        ~s({"$spectre":"map","$spectre":"map","entries":[[{"$spectre":"atom","value":"sample"},2]]}),
        global: false
      )

    list_duplicate =
      String.replace(
        encoded,
        ~s({"$spectre":"map","entries":[[{"$spectre":"atom","value":"sample"},1]]}),
        ~s({"$spectre":"map","$spectre":"map","entries":[[{"$spectre":"atom","value":"sample"},1]]}),
        global: false
      )

    assert {:error, :duplicate_ledger_receipt_json_key} =
             ReceiptCodec.decode(nested_duplicate, byte_size(nested_duplicate))

    assert {:error, :duplicate_ledger_receipt_json_key} =
             ReceiptCodec.decode(list_duplicate, byte_size(list_duplicate))
  end

  test "entry round trips and rejects digest or linkage tampering" do
    envelope = envelope("instance:entry", 7)
    config = config!(max_receipt_bytes: 1_000_000)
    assert {:ok, write} = ReceiptWrite.new(envelope, [], config)
    assert :ok = ReceiptWrite.validate(write, config)
    assert {:ok, entry} = ReceiptEntry.new(write, 1, nil)
    assert :ok = ReceiptEntry.verify(entry)
    assert :ok = ReceiptEntry.verify_envelope(entry, envelope)
    assert {:ok, ^entry} = entry |> ReceiptEntry.to_data() |> ReceiptEntry.from_data()

    assert {:error, :ledger_receipt_entry_digest_mismatch} =
             ReceiptEntry.verify(%{entry | sequence: 2})

    assert {:error, :invalid_ledger_receipt_payload_ref} =
             entry
             |> ReceiptEntry.to_data()
             |> Map.put("payload_ref", "receipt-payload:" <> String.duplicate("f", 64))
             |> ReceiptEntry.from_data()
  end

  test "entry schema rejects malformed fields and envelope-link mismatches" do
    envelope = envelope("instance:entry-validation", 7)
    config = config!(max_receipt_bytes: 1_000_000)
    assert {:ok, write} = ReceiptWrite.new(envelope, [], config)
    assert {:ok, entry} = ReceiptEntry.new(write, 1, nil)
    data = ReceiptEntry.to_data(entry)

    assert {:error, {:unsupported_ledger_receipt_entry_version, 2}} =
             ReceiptEntry.from_data(%{"entry_version" => 2})

    assert {:error, :invalid_ledger_receipt_entry} = ReceiptEntry.from_data(%{})
    assert {:error, :invalid_ledger_receipt_entry} = ReceiptEntry.verify(:invalid)
    refute ReceiptEntry.matches_write?(:invalid, write)

    assert {:error, :invalid_ledger_receipt_envelope} =
             ReceiptEntry.verify_envelope(entry, :invalid)

    invalid_fields = [
      {"stream_key", "", {:invalid_ledger_receipt_entry_field, :stream_key}},
      {"sequence", 0, {:invalid_ledger_receipt_entry_field, :sequence}},
      {"receipt_id", "invalid", :invalid_ledger_receipt_id},
      {"kind", "unknown", :invalid_ledger_receipt_kind},
      {"canonical_revision", -1, {:invalid_ledger_receipt_entry_field, :canonical_revision}},
      {"envelope_digest", "invalid", :invalid_ledger_receipt_digest},
      {"payload_ref", "receipt-payload:invalid", :invalid_ledger_receipt_payload_ref},
      {"previous_entry_digest", "invalid", :invalid_ledger_receipt_digest},
      {"recorded_at", -1, {:invalid_ledger_receipt_entry_field, :recorded_at}},
      {"owner_fencing_token", -1, {:invalid_ledger_receipt_entry_field, :owner_fencing_token}}
    ]

    Enum.each(invalid_fields, fn {field, value, reason} ->
      assert {:error, ^reason} = data |> Map.put(field, value) |> ReceiptEntry.from_data()
    end)

    assert {:error, :ledger_receipt_id_mismatch} =
             ReceiptEntry.verify_envelope(%{entry | receipt_id: "receipt:other"}, envelope)

    assert {:error, :ledger_receipt_kind_mismatch} =
             ReceiptEntry.verify_envelope(%{entry | kind: :run_input_admitted}, envelope)

    assert {:error, :ledger_receipt_revision_mismatch} =
             ReceiptEntry.verify_envelope(%{entry | canonical_revision: 8}, envelope)

    assert {:error, :ledger_receipt_digest_mismatch} =
             ReceiptEntry.verify_envelope(
               %{entry | envelope_digest: String.duplicate("f", 64)},
               envelope
             )

    assert {:error, :ledger_receipt_payload_ref_mismatch} =
             ReceiptEntry.verify_envelope(
               %{entry | payload_ref: "receipt-payload:" <> String.duplicate("f", 64)},
               envelope
             )

    assert {:error, :ledger_receipt_recorded_at_mismatch} =
             ReceiptEntry.verify_envelope(%{entry | recorded_at: 0}, envelope)
  end

  test "chain verifier covers empty, malformed, mixed, broken, and duplicate chains" do
    config = config!(max_receipt_bytes: 1_000_000)
    first_envelope = envelope("instance:chain-errors", 1)
    second_envelope = envelope("instance:chain-errors", 2)
    mixed_envelope = envelope("instance:other-chain", 2)
    assert {:ok, first_write} = ReceiptWrite.new(first_envelope, [], config)
    assert {:ok, second_write} = ReceiptWrite.new(second_envelope, [], config)
    assert {:ok, mixed_write} = ReceiptWrite.new(mixed_envelope, [], config)
    assert {:ok, first} = ReceiptEntry.new(first_write, 1, nil)
    assert {:ok, second} = ReceiptEntry.new(second_write, 2, first.entry_digest)
    assert {:ok, bad_start} = ReceiptEntry.new(first_write, 2, nil)
    assert {:ok, mixed} = ReceiptEntry.new(mixed_write, 2, first.entry_digest)
    assert {:ok, broken} = ReceiptEntry.new(second_write, 3, first.entry_digest)
    assert {:ok, duplicate} = ReceiptEntry.new(first_write, 2, first.entry_digest)

    assert {:ok, %{entry_count: 0, canonical_ordered: true}} = ReceiptChain.verify([])
    assert {:error, :invalid_ledger_receipt_chain} = ReceiptChain.verify(:invalid)
    assert {:error, :invalid_ledger_receipt_chain_start} = ReceiptChain.verify([bad_start])
    assert {:error, :invalid_ledger_receipt_chain} = ReceiptChain.verify([first, :invalid])
    assert {:error, :mixed_ledger_receipt_streams} = ReceiptChain.verify([first, mixed])
    assert {:error, :broken_ledger_receipt_chain} = ReceiptChain.verify([first, broken])

    assert {:error, :duplicate_ledger_receipt_entry} =
             ReceiptChain.verify([first, duplicate])

    assert {:ok, %{entry_count: 2}} = ReceiptChain.verify([first, second])
  end

  test "chain canonical-order report treats missing revisions as non-ordering evidence" do
    config = config!(max_receipt_bytes: 1_000_000)
    stream_key = "instance:nil-revisions"
    first_envelope = unrevisioned_envelope(stream_key, "first")
    second_envelope = envelope(stream_key, 2)
    third_envelope = unrevisioned_envelope(stream_key, "third")
    assert {:ok, first_write} = ReceiptWrite.new(first_envelope, [], config)
    assert {:ok, second_write} = ReceiptWrite.new(second_envelope, [], config)
    assert {:ok, third_write} = ReceiptWrite.new(third_envelope, [], config)
    assert {:ok, first} = ReceiptEntry.new(first_write, 1, nil)
    assert {:ok, second} = ReceiptEntry.new(second_write, 2, first.entry_digest)
    assert {:ok, third} = ReceiptEntry.new(third_write, 3, second.entry_digest)

    assert {:ok, %{canonical_ordered: true, head_sequence: 3}} =
             ReceiptChain.verify([first, second, third])
  end

  test "receipt size limits and backend capabilities fail closed" do
    server = start_supervised!(Memory)
    envelope = envelope("instance:bounds", 1, %{text: String.duplicate("x", 256)})

    assert {:error, {:ledger_receipt_too_large, 64}} =
             ReceiptSink.put_payload(
               envelope,
               memory_opts(server, unique_namespace("bounded")) ++ [max_receipt_bytes: 64]
             )

    assert {:error,
            {:ledger_backend_callback_missing, SpectreLedger.IncompleteReceiptBackend,
             :append_receipt, 2}} =
             ReceiptSink.append(envelope,
               backend: SpectreLedger.IncompleteReceiptBackend,
               namespace: unique_namespace("incomplete")
             )
  end

  test "receipt writes validate stream ownership, fencing, and exact bytes" do
    config = config!(max_receipt_bytes: 1_000_000)
    envelope = envelope("instance:write-validation", 1)
    assert {:ok, write} = ReceiptWrite.new(envelope, [owner_fencing_token: 4], config)
    assert write.owner_fencing_token == 4
    assert :ok = ReceiptWrite.validate(write, config)

    assert {:error, :invalid_ledger_receipt_write} =
             ReceiptWrite.validate(%{write | encoded: write.encoded <> " "}, config)

    assert {:error, :invalid_ledger_receipt_write} = ReceiptWrite.validate(:invalid, config)
    assert {:error, :invalid_ledger_receipt_write} = ReceiptWrite.new(:invalid, [], config)

    assert {:error, :invalid_owner_fencing_token} =
             ReceiptWrite.new(envelope, [owner_fencing_token: -1], config)

    assert {:error, {:ledger_receipt_instance_ref_mismatch, "instance:write-validation"}} =
             ReceiptWrite.new(envelope, [instance_ref: "instance:different"], config)

    unscoped = unrevisioned_envelope(nil, "unscoped")
    assert {:ok, scoped} = ReceiptWrite.new(unscoped, [instance_ref: "instance:scoped"], config)
    assert scoped.stream_key == "instance:scoped"

    ref =
      Ref.new(
        AgentRef.from_id("receipt-write-ref"),
        Subject.new("receipt-write-subject")
      )

    assert {:ok, ref_scoped} = ReceiptWrite.new(unscoped, [instance_ref: ref], config)
    assert ref_scoped.stream_key == ref.key

    assert {:error, {:ledger_receipt_instance_ref_mismatch, "instance:write-validation"}} =
             ReceiptWrite.new(envelope, [instance_ref: ref], config)

    assert {:error, :invalid_ledger_receipt_stream_key} =
             ReceiptWrite.new(unscoped, [instance_ref: self()], config)
  end

  test "sink wrapper rejects malformed replies and preserves raised ambiguity" do
    backend = SpectreLedger.ReceiptReplyBackend
    envelope = envelope("instance:sink-errors", 1)

    assert {:error, :invalid_ledger_receipt_id} = ReceiptSink.lookup(:invalid, [])
    assert {:error, :invalid_ledger_receipt_payload_ref} = ReceiptSink.get_payload(:invalid, [])

    assert {:error, :invalid_ledger_receipt_lookup} =
             ReceiptSink.lookup(envelope.id, backend: backend, lookup_result: :invalid)

    assert {:ok, encoded} = ReceiptCodec.encode(envelope)
    different = envelope("instance:sink-errors", 2)
    assert {:ok, different_encoded} = ReceiptCodec.encode(different)

    assert {:error, :ledger_receipt_lookup_id_mismatch} =
             ReceiptSink.lookup(envelope.id,
               backend: backend,
               lookup_result: {:ok, different_encoded}
             )

    assert {:error, :invalid_ledger_receipt_json} =
             ReceiptSink.lookup(envelope.id,
               backend: backend,
               lookup_result: {:ok, "not-json"}
             )

    assert {:error, :invalid_ledger_receipt_payload} =
             ReceiptSink.get_payload(
               Sink.payload_ref(envelope),
               backend: backend,
               get_result: :invalid
             )

    assert {:error, :ledger_receipt_payload_ref_mismatch} =
             ReceiptSink.get_payload("receipt-payload:" <> String.duplicate("f", 64),
               backend: backend,
               get_result: {:ok, encoded}
             )

    assert {:error, :invalid_ledger_receipt_json} =
             ReceiptSink.get_payload(Sink.payload_ref(envelope),
               backend: backend,
               get_result: {:ok, "not-json"}
             )

    assert {:error, :backend_read_failed} =
             ReceiptSink.get_payload(Sink.payload_ref(envelope),
               backend: backend,
               get_result: {:error, :backend_read_failed}
             )

    assert {:error, :ledger_receipt_payload_ref_mismatch} =
             ReceiptSink.put_payload(envelope,
               backend: backend,
               put_result: {:ok, "receipt-payload:wrong"}
             )

    assert_raise RuntimeError, "receipt backend failure", fn ->
      ReceiptSink.append(envelope, backend: backend, append_result: :raise)
    end

    assert catch_throw(ReceiptSink.append(envelope, backend: backend, append_result: :throw)) ==
             :receipt_backend_failure

    assert :invalid = ReceiptSink.append(envelope, backend: backend, append_result: :invalid)

    assert :not_found =
             ReceiptSink.get_payload(
               Sink.payload_ref(envelope),
               backend: backend,
               get_result: :not_found
             )

    assert {:error, :invalid_ledger_options} = ReceiptSink.append(envelope, :invalid)

    assert {:error, :invalid_ledger_backend} =
             ReceiptSink.append(envelope, backend: "invalid")
  end

  test "public receipt facade validates default and malformed calls" do
    assert {:error, :memory_server_required} = Ledger.receipt("receipt:missing")
    assert {:error, :memory_server_required} = Ledger.receipt_payload("receipt-payload:missing")
    assert {:error, :memory_server_required} = Ledger.receipt_entries("instance:missing")
    assert {:error, :memory_server_required} = Ledger.receipts("instance:missing")
    assert {:error, :memory_server_required} = Ledger.verify_receipts("instance:missing")
    assert {:error, :invalid_ledger_receipt_id} = Ledger.receipt(:invalid)
    assert {:error, :invalid_ledger_receipt_payload_ref} = Ledger.receipt_payload(:invalid)

    assert {:error, :invalid_ledger_options} =
             Ledger.verify_receipts("instance:missing", :invalid)
  end

  test "backend capability checks reject missing modules and invalid capability names" do
    missing = SpectreLedger.MissingReceiptBackend

    assert {:error, {:ledger_backend_not_loaded, ^missing}} =
             Capabilities.validate(missing, :receipt_sink)

    assert {:error, :invalid_ledger_backend_capability} =
             Capabilities.validate(Memory, :unknown)

    assert Capabilities.complete?(Memory, :receipt_sink)
    assert length(Capabilities.callbacks(:receipt_archive)) == 2
  end

  test "configuration validates receipt limits independently of checkpoint limits" do
    assert {:ok, config} =
             Config.new(max_checkpoint_bytes: 10, max_receipt_bytes: 20)

    assert config.max_checkpoint_bytes == 10
    assert config.max_receipt_bytes == 20
    assert {:error, :invalid_max_receipt_bytes} = Config.new(max_receipt_bytes: 0)
  end

  test "receipt telemetry exposes only classified metadata and digested identifiers" do
    server = start_supervised!(Memory)
    envelope = envelope("instance:telemetry-private", 1, %{private: "secret-response"})
    caller = self()

    opts =
      memory_opts(server, unique_namespace("telemetry")) ++
        [
          telemetry_handler: fn event, measurements, metadata ->
            send(caller, {:receipt_telemetry, event, measurements, metadata})
          end
        ]

    assert {:ok, :appended} = ReceiptSink.append(envelope, opts)

    assert_receive {:receipt_telemetry, [:spectre, :ledger, :receipt, :append, :stop],
                    %{count: 1, duration_us: duration}, metadata}

    assert duration >= 0
    assert metadata.operation == :append
    assert metadata.outcome == :ok
    assert metadata.status == :appended
    assert metadata.receipt_kind == :nondeterminism_sample
    assert byte_size(metadata.stream_id) == 64
    refute inspect(metadata) =~ envelope.id
    refute inspect(metadata) =~ "secret-response"
  end

  defp envelope(stream_key, canonical_revision, payload \\ nil) do
    Envelope.new!(
      kind: :nondeterminism_sample,
      instance_ref: stream_key,
      canonical_revision: canonical_revision,
      correlation_id: "#{stream_key}:#{canonical_revision}",
      payload_schema_ref: "spectre-ledger.test/sample-1",
      payload: payload || %{sample: canonical_revision},
      privacy: :internal,
      recorded_at: 1_800_000_000_000 + canonical_revision
    )
  end

  defp unrevisioned_envelope(stream_key, suffix) do
    Envelope.new!(
      kind: :nondeterminism_sample,
      instance_ref: stream_key,
      correlation_id: "#{stream_key || "unscoped"}:#{suffix}",
      payload_schema_ref: "spectre-ledger.test/sample-1",
      payload: %{sample: suffix},
      privacy: :internal,
      recorded_at: 1_800_000_100_000
    )
  end

  defp normalized_sink(opts) do
    assert {:ok, sink} = Sink.normalize(Ledger.receipt_sink(opts))
    sink
  end

  defp memory_opts(server, namespace),
    do: [backend: :memory, server: server, namespace: namespace]

  defp config!(opts) do
    assert {:ok, config} = Config.new(opts)
    config
  end

  defp unique_namespace(prefix) do
    "#{prefix}.#{System.unique_integer([:positive, :monotonic])}"
  end
end
