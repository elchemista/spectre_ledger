defmodule SpectreLedger.EntryChainTest do
  use ExUnit.Case, async: true

  alias Spectre.AgentRef
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.Entry
  alias Spectre.Ledger.Write
  alias Spectre.Subject

  test "entries have deterministic portable identities and round-trip" do
    {:ok, entry} = Entry.new(write(0, 3, "a", "b"), nil)

    assert entry.entry_version == 1
    assert entry.expected_revision == 0
    assert entry.revision == 3
    assert {:ok, ^entry} = entry |> Entry.to_data() |> Entry.from_data()
    assert :ok = Entry.verify(entry)

    {:ok, reissued} = Entry.new(%{write(0, 3, "a", "b") | owner_fencing_token: 99}, nil)
    assert reissued.entry_digest == entry.entry_digest
    refute reissued.owner_fencing_token == entry.owner_fencing_token

    assert {:error, :ledger_entry_digest_mismatch} =
             entry
             |> Entry.to_data()
             |> Map.put("revision", 4)
             |> Entry.from_data()
  end

  test "chain accepts coalesced revisions and rejects broken links" do
    {:ok, first} = Entry.new(write(0, 3, "a", "b"), nil)
    {:ok, second} = Entry.new(write(3, 8, "c", "d"), first.entry_digest)

    assert {:ok, report} = Chain.verify([first, second])
    assert report.entry_count == 2
    assert report.head_revision == 8
    assert report.head_entry_digest == second.entry_digest

    {:ok, wrong_link} = Entry.new(write(3, 8, "c", "d"), String.duplicate("f", 64))

    assert {:error, :ledger_chain_previous_digest_mismatch} =
             Chain.verify([first, wrong_link])

    {:ok, wrong_expected} = Entry.new(write(2, 8, "c", "d"), first.entry_digest)

    assert {:error, :ledger_chain_revision_gap} = Chain.verify([first, wrong_expected])
  end

  test "chain is bound to exactly one stream" do
    {:ok, first} = Entry.new(write(0, 1, "a", "b"), nil)

    other_write =
      write(1, 2, "c", "d")
      |> Map.put(:ref, instance_ref("other"))

    {:ok, second} = Entry.new(other_write, first.entry_digest)

    assert {:error, :ledger_chain_stream_mismatch} = Chain.verify([first, second])
  end

  defp write(expected, revision, checkpoint_seed, blob_seed) do
    %Write{
      ref: instance_ref("subject"),
      checkpoint: "checkpoint",
      expected_revision: expected,
      revision: revision,
      checkpoint_digest: digest(checkpoint_seed),
      blob_digest: digest(blob_seed),
      byte_size: 10,
      owner_fencing_token: 7,
      source_entry_digest: nil,
      kind: :checkpoint
    }
  end

  defp instance_ref(subject) do
    Ref.new(AgentRef.from_id("ledger-entry-test"), Subject.new(subject))
  end

  defp digest(seed) do
    seed
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
