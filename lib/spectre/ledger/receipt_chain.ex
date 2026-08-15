defmodule Spectre.Ledger.ReceiptChain do
  @moduledoc """
  Verification for one physical boundary-receipt append chain.

  Receipt delivery can be concurrent, especially in observational mode.
  Verification therefore proves append sequence, object linkage and entry
  integrity without asserting that canonical revisions arrived in order.
  """

  alias Spectre.Ledger.ReceiptEntry

  @doc "Verifies a complete receipt-entry chain in ascending sequence order."
  @spec verify([ReceiptEntry.t()]) :: {:ok, map()} | {:error, term()}
  def verify([]) do
    {:ok,
     %{
       entry_count: 0,
       stream_key: nil,
       head_sequence: 0,
       head_entry_digest: nil,
       canonical_ordered: true,
       kinds: %{}
     }}
  end

  def verify([%ReceiptEntry{} = first | rest]) do
    with :ok <- ReceiptEntry.verify(first),
         :ok <- first_entry(first),
         {:ok, state} <- reduce(rest, initial(first)) do
      {:ok, report(state)}
    end
  end

  def verify(_entries), do: {:error, :invalid_ledger_receipt_chain}

  defp first_entry(%ReceiptEntry{sequence: 1, previous_entry_digest: nil}), do: :ok
  defp first_entry(_entry), do: {:error, :invalid_ledger_receipt_chain_start}

  defp initial(first) do
    %{
      previous: first,
      count: 1,
      receipt_ids: MapSet.new([first.receipt_id]),
      entry_digests: MapSet.new([first.entry_digest]),
      canonical_ordered: true,
      last_canonical_revision: first.canonical_revision,
      kinds: %{first.kind => 1}
    }
  end

  defp reduce([], state), do: {:ok, state}

  defp reduce([%ReceiptEntry{} = entry | rest], state) do
    with :ok <- ReceiptEntry.verify(entry),
         :ok <- same_stream(state.previous, entry),
         :ok <- next_link(state.previous, entry),
         :ok <- unique_entry(state, entry) do
      reduce(rest, advance(state, entry))
    end
  end

  defp reduce(_entries, _state), do: {:error, :invalid_ledger_receipt_chain}

  defp same_stream(%ReceiptEntry{stream_key: key}, %ReceiptEntry{stream_key: key}), do: :ok
  defp same_stream(_previous, _entry), do: {:error, :mixed_ledger_receipt_streams}

  defp next_link(previous, entry) do
    if entry.sequence == previous.sequence + 1 and
         entry.previous_entry_digest == previous.entry_digest,
       do: :ok,
       else: {:error, :broken_ledger_receipt_chain}
  end

  defp unique_entry(state, entry) do
    if MapSet.member?(state.receipt_ids, entry.receipt_id) or
         MapSet.member?(state.entry_digests, entry.entry_digest),
       do: {:error, :duplicate_ledger_receipt_entry},
       else: :ok
  end

  defp advance(state, entry) do
    %{
      state
      | previous: entry,
        count: state.count + 1,
        receipt_ids: MapSet.put(state.receipt_ids, entry.receipt_id),
        entry_digests: MapSet.put(state.entry_digests, entry.entry_digest),
        canonical_ordered:
          state.canonical_ordered and
            canonical_ordered?(state.last_canonical_revision, entry.canonical_revision),
        last_canonical_revision: entry.canonical_revision || state.last_canonical_revision,
        kinds: Map.update(state.kinds, entry.kind, 1, &(&1 + 1))
    }
  end

  defp canonical_ordered?(nil, _current), do: true
  defp canonical_ordered?(_previous, nil), do: true
  defp canonical_ordered?(previous, current), do: current >= previous

  defp report(state) do
    %{
      entry_count: state.count,
      stream_key: state.previous.stream_key,
      head_sequence: state.previous.sequence,
      head_entry_digest: state.previous.entry_digest,
      canonical_ordered: state.canonical_ordered,
      kinds: state.kinds
    }
  end
end
