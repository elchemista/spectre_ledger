defmodule SpectreLedger.BundleTest do
  use ExUnit.Case, async: true

  alias Spectre.AgentRef
  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Instance.Canonical
  alias Spectre.Instance.Canonical.Codec
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Backend.Memory
  alias Spectre.Ledger.Bundle
  alias Spectre.Ledger.CheckpointStore
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.Entry
  alias Spectre.State
  alias Spectre.Subject

  test "exports deterministic JSON with honest completeness claims and verifies it" do
    {entries, objects} = fixture(2)

    assert {:ok, first} = Bundle.export(entries, objects)
    assert {:ok, second} = Bundle.export(entries, Map.new(Enum.reverse(Map.to_list(objects))))
    assert first == second

    assert {:ok, decoded} = Bundle.decode(first)
    assert decoded.entries == entries
    assert decoded.objects == objects

    assert decoded.manifest == %{
             "capture" => "persisted_checkpoints",
             "completeness" => "checkpoint_playback",
             "every_revision" => false,
             "deterministic_replay_claim" => false
           }

    assert {:ok, report} = Bundle.verify(first)
    assert {:ok, ^report} = Bundle.verify(decoded)
    assert report.entry_count == 2
    assert report.object_count == 2
    assert report.head_revision == 2
    assert report.capture == :persisted_checkpoints
    assert report.completeness == :checkpoint_playback
    refute report.every_revision
    refute report.deterministic_replay_claim
    refute inspect(report) =~ hd(entries).stream_key

    assert Jason.decode!(first) == Bundle.to_data(decoded)
  end

  test "rejects envelope, checksum, entry, object, checkpoint, and base64 corruption" do
    {entries, objects} = fixture(2)
    {:ok, encoded} = Bundle.export(entries, objects)
    data = Jason.decode!(encoded)

    assert {:error, :ledger_bundle_checksum_mismatch} =
             data
             |> Map.put("checksum", String.duplicate("0", 64))
             |> Jason.encode!()
             |> Bundle.decode()

    assert {:error, :invalid_ledger_bundle_manifest} =
             data
             |> put_in(["manifest", "every_revision"], true)
             |> Jason.encode!()
             |> Bundle.decode()

    assert {:error, {:invalid_ledger_bundle_keys, :bundle}} =
             data
             |> Map.put("extra", true)
             |> resign()
             |> Bundle.decode()

    assert {:error, {:unsupported_ledger_bundle_format, "other/bundle"}} =
             data
             |> Map.put("format", "other/bundle")
             |> resign()
             |> Bundle.decode()

    assert {:error, {:unsupported_ledger_bundle_version, 2}} =
             data
             |> Map.put("bundle_version", 2)
             |> resign()
             |> Bundle.decode()

    [first | rest] = data["entries"]
    corrupt_entry = [Map.put(first, "revision", 99) | rest]

    assert {:error, {:invalid_ledger_bundle_entry, 0, :ledger_entry_digest_mismatch}} =
             data
             |> Map.put("entries", corrupt_entry)
             |> resign()
             |> Bundle.decode()

    [digest | _rest] = Map.keys(data["objects"])

    assert {:error, :invalid_ledger_bundle_base64} =
             data
             |> put_in(["objects", digest], data["objects"][digest] <> "=")
             |> resign()
             |> Bundle.decode()

    [entry | _rest] = entries
    corrupt_objects = Map.put(objects, entry.blob_digest, objects[entry.blob_digest] <> " ")

    assert {:error, {:invalid_ledger_bundle_object, 0, :ledger_bundle_content_mismatch}} =
             resign_bundle(entries, corrupt_objects) |> Bundle.verify()

    missing = Map.delete(objects, entry.blob_digest)

    assert {:error, :ledger_bundle_object_set_mismatch} =
             resign_bundle(entries, missing) |> Bundle.verify()

    assert {:error, :empty_ledger_bundle} =
             data
             |> Map.put("entries", [])
             |> Map.put("objects", %{})
             |> resign()
             |> Bundle.decode()

    assert {:error, :invalid_ledger_bundle_envelope} =
             data
             |> Map.put("entries", %{})
             |> resign()
             |> Bundle.decode()

    assert {:error, :invalid_ledger_bundle_envelope} =
             data
             |> Map.put("objects", [])
             |> resign()
             |> Bundle.decode()

    assert {:error, :invalid_ledger_bundle_envelope} = Bundle.decode("[]")
    assert {:error, :invalid_ledger_bundle} = Bundle.decode(:not_binary)
    assert {:error, :invalid_ledger_bundle} = Bundle.verify(:not_a_bundle)
  end

  test "enforces encoded, collection, object, total-object, and nesting limits" do
    {entries, objects} = fixture(2)
    {:ok, encoded} = Bundle.export(entries, objects)
    largest = objects |> Map.values() |> Enum.map(&byte_size/1) |> Enum.max()
    total = objects |> Map.values() |> Enum.map(&byte_size/1) |> Enum.sum()

    assert {:error, {:ledger_bundle_too_large, 10}} = Bundle.decode(encoded, max_bytes: 10)

    assert {:error, {:ledger_bundle_count_exceeded, :entries, 1}} =
             Bundle.export(entries, objects, max_entries: 1)

    assert {:error, {:ledger_bundle_count_exceeded, :objects, 1}} =
             Bundle.decode(encoded, max_objects: 1)

    assert {:error, {:ledger_bundle_object_too_large, max}} =
             Bundle.verify(encoded, max_object_bytes: largest - 1)

    assert max == largest - 1

    assert {:error, {:ledger_bundle_object_too_large, 1}} =
             Bundle.decode(encoded, max_object_bytes: 1)

    assert {:error, {:ledger_bundle_objects_too_large, max_total}} =
             Bundle.export(entries, objects, max_total_object_bytes: total - 1)

    assert max_total == total - 1

    assert {:error, {:ledger_bundle_depth_exceeded, 2}} =
             Bundle.decode(encoded, max_depth: 2)
  end

  test "rejects invalid options and duplicate JSON keys without creating atoms" do
    {entries, objects} = fixture(1)

    assert {:error, {:invalid_ledger_bundle_limit, :max_entries}} =
             Bundle.export(entries, objects, max_entries: 0)

    assert {:error, :unknown_ledger_bundle_options} =
             Bundle.export(entries, objects, unknown: true)

    assert {:error, :duplicate_ledger_bundle_options} =
             Bundle.export(entries, objects, max_entries: 1, max_entries: 2)

    assert {:error, :duplicate_ledger_bundle_json_key} =
             Bundle.decode(~s({"format":"spectre/ledger-bundle","format":"duplicate"}))

    assert {:error, :invalid_ledger_bundle_options} = Bundle.decode("{}", :invalid)
    assert {:error, :invalid_ledger_bundle_content} = Bundle.export(:entries, :objects)

    assert {:error, :invalid_ledger_bundle_object} =
             Bundle.export(entries, %{hd(entries).blob_digest => :not_binary})

    assert {:error, :invalid_ledger_bundle_object_digest} =
             objects
             |> then(fn object_map ->
               data = %{
                 "format" => "spectre/ledger-bundle",
                 "bundle_version" => 1,
                 "manifest" => Bundle.manifest(),
                 "entries" => Enum.map(entries, &Entry.to_data/1),
                 "objects" => %{"not-a-digest" => Base.encode64(hd(Map.values(object_map)))}
               }

               resign(data)
             end)
             |> Bundle.decode()

    assert {:error, :invalid_ledger_bundle_object} =
             %{
               "format" => "spectre/ledger-bundle",
               "bundle_version" => 1,
               "manifest" => Bundle.manifest(),
               "entries" => Enum.map(entries, &Entry.to_data/1),
               "objects" => %{hd(entries).blob_digest => 123}
             }
             |> resign()
             |> Bundle.decode()

    assert {:error, {:invalid_ledger_bundle_keys, :bundle}} =
             Bundle.decode(~s({"\u0065scaped": {"value": "quote: \\\""}}))

    before_count = :erlang.system_info(:atom_count)
    random_key = "never_atom_#{System.unique_integer([:positive])}"
    assert {:error, _reason} = Bundle.decode(~s({"#{random_key}":true}))
    assert :erlang.system_info(:atom_count) == before_count
  end

  test "telemetry exposes numeric measurements and classified errors but no raw data" do
    {entries, objects} = fixture(1)
    caller = self()

    handler = fn event, measurements, metadata ->
      send(caller, {:telemetry, event, measurements, metadata})
    end

    assert {:ok, _bundle} = Bundle.export(entries, objects, telemetry_handler: handler)

    assert_received {:telemetry, [:spectre, :ledger, :bundle, :export, :stop], measurements,
                     %{operation: :export, outcome: :ok}}

    assert Enum.all?(measurements, fn {_key, value} -> is_number(value) end)
    refute inspect(measurements) =~ hd(entries).stream_key
    refute inspect(measurements) =~ hd(Map.values(objects))

    assert {:error, _reason} =
             Bundle.verify("private checkpoint text", telemetry_handler: handler)

    assert_received {:telemetry, [:spectre, :ledger, :bundle, :verify, :stop], _measurements,
                     %{operation: :verify, outcome: :error, reason_class: _class} = metadata}

    refute inspect(metadata) =~ "private checkpoint text"
  end

  test "does not call Spectre.Run.restore while verifying checkpoints" do
    {entries, objects} = fixture(1)
    {:ok, bundle} = Bundle.export(entries, objects)

    :erlang.trace_pattern({Spectre.Run, :restore, 1}, true, [:local])
    :erlang.trace(self(), true, [:call])

    try do
      assert {:ok, _report} = Bundle.verify(bundle)
      refute_received {:trace, _pid, :call, {Spectre.Run, :restore, _args}}
    after
      :erlang.trace(self(), false, [:call])
      :erlang.trace_pattern({Spectre.Run, :restore, 1}, false, [:local])
    end
  end

  test "rejects a content-addressed object that is not a Spectre checkpoint" do
    ref = instance_ref()
    checkpoint = "not a Spectre checkpoint"
    blob_digest = sha256(checkpoint)

    write = %Spectre.Ledger.Write{
      ref: ref,
      checkpoint: checkpoint,
      expected_revision: 0,
      revision: 1,
      checkpoint_digest: sha256("semantic-placeholder"),
      blob_digest: blob_digest,
      byte_size: byte_size(checkpoint),
      owner_fencing_token: nil,
      source_entry_digest: nil,
      kind: :checkpoint
    }

    assert {:ok, entry} = Entry.new(write, nil)

    assert {:error, {:invalid_ledger_bundle_object, 0, :invalid_spectre_checkpoint}} =
             resign_bundle([entry], %{blob_digest => checkpoint}) |> Bundle.verify()
  end

  test "rejects a valid checkpoint signed into a different Ledger stream" do
    source_ref = instance_ref()
    destination_ref = instance_ref()
    checkpoint = checkpoint!(source_ref, 1)
    assert {:ok, report} = Foundation.verify_instance_checkpoint(checkpoint)
    blob_digest = sha256(checkpoint)

    write = %Spectre.Ledger.Write{
      ref: destination_ref,
      checkpoint: checkpoint,
      expected_revision: 0,
      revision: 1,
      checkpoint_digest: report.digest,
      blob_digest: blob_digest,
      byte_size: byte_size(checkpoint),
      owner_fencing_token: nil,
      source_entry_digest: nil,
      kind: :checkpoint
    }

    assert {:ok, entry} = Entry.new(write, nil)

    assert {:error, {:invalid_ledger_bundle_object, 0, :invalid_spectre_checkpoint}} =
             resign_bundle([entry], %{blob_digest => checkpoint}) |> Bundle.verify()

    server = start_supervised!(Memory)

    assert {:ok, config} =
             Config.new(backend: :memory, server: server, namespace: "cross-stream")

    assert {:error, {:invalid_ledger_import_checkpoint, 1}} =
             Memory.put_stream(
               config,
               destination_ref.key,
               [entry],
               %{blob_digest => checkpoint}
             )
  end

  defp fixture(count) do
    ref = instance_ref()

    Enum.reduce(1..count, {[], %{}, nil, 0}, fn revision,
                                                {entries, objects, previous, expected} ->
      checkpoint = checkpoint!(ref, revision)
      config = config!()

      assert {:ok, write} =
               CheckpointStore.prepare(ref, checkpoint, expected, revision, [], config)

      assert {:ok, entry} = Entry.new(write, previous)

      {
        entries ++ [entry],
        Map.put(objects, entry.blob_digest, checkpoint),
        entry.entry_digest,
        revision
      }
    end)
    |> then(fn {entries, objects, _previous, _expected} -> {entries, objects} end)
  end

  defp checkpoint!(ref, revision) do
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
                   id: "bundle-snapshot-#{revision}-#{step}",
                   correlation_id: "bundle-correlation-#{revision}-#{step}"
                 )

        assert {:ok, change} =
                 Canonical.change(
                   snapshot,
                   %{correlations: Map.put(correlations, :bundle_marker, {revision, step})},
                   id: "bundle-change-#{revision}-#{step}",
                   provenance: %{source: :spectre_ledger_bundle_test},
                   metadata: %{revision: revision, step: step}
                 )

        assert {:ok, next, _transition} = Canonical.commit(current, change)
        next
      end)

    assert {:ok, checkpoint} = Codec.encode_json(canonical)
    assert {:ok, %{revision: ^revision}} = Foundation.verify_instance_checkpoint(checkpoint)
    checkpoint
  end

  defp config! do
    assert {:ok, config} = Config.new(backend: SpectreLedger.BundleTest.UnusedBackend)
    config
  end

  defp instance_ref do
    id = System.unique_integer([:positive, :monotonic])
    Ref.new(AgentRef.from_id("ledger-bundle-#{id}"), Subject.new("bundle-subject-#{id}"))
  end

  defp resign_bundle(entries, objects) do
    data = %{
      "format" => "spectre/ledger-bundle",
      "bundle_version" => 1,
      "manifest" => Bundle.manifest(),
      "entries" => Enum.map(entries, &Entry.to_data/1),
      "objects" => Map.new(objects, fn {digest, bytes} -> {digest, Base.encode64(bytes)} end)
    }

    resign(data)
  end

  defp resign(data) do
    unsigned = Map.delete(data, "checksum")
    checksum = unsigned |> canonical_json() |> sha256()
    data |> Map.put("checksum", checksum) |> canonical_json()
  end

  defp canonical_json(data), do: data |> ordered() |> Jason.encode!(maps: :strict)

  defp ordered(value) when is_map(value) do
    value
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.map(fn {key, item} -> {key, ordered(item)} end)
    |> Jason.OrderedObject.new()
  end

  defp ordered(value) when is_list(value), do: Enum.map(value, &ordered/1)
  defp ordered(value), do: value

  defp sha256(bytes),
    do: bytes |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)

  defmodule UnusedBackend do
  end
end
