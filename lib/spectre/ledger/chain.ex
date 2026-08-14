defmodule Spectre.Ledger.Chain do
  @moduledoc "Verifies the append-only ordering and digest links of one Ledger stream."

  alias Spectre.Ledger.Entry

  @doc "Verifies a complete stream beginning at expected revision zero."
  @spec verify([Entry.t()]) :: {:ok, map()} | {:error, term()}
  def verify(entries) when is_list(entries) do
    with :ok <- require_entries(entries),
         {:ok, state} <- Enum.reduce_while(entries, {:ok, initial()}, &verify_next/2) do
      {:ok,
       %{
         stream_key: state.stream_key,
         entry_count: state.count,
         head_revision: state.revision,
         head_entry_digest: state.digest
       }}
    end
  end

  def verify(_entries), do: {:error, :invalid_ledger_chain}

  defp initial,
    do: %{stream_key: nil, revision: 0, digest: nil, count: 0}

  defp require_entries([]), do: {:error, :empty_ledger_chain}
  defp require_entries(_entries), do: :ok

  defp verify_next(%Entry{} = entry, {:ok, state}) do
    with :ok <- Entry.verify(entry),
         :ok <- same_stream(state.stream_key, entry.stream_key),
         :ok <- expected_revision(state.revision, entry.expected_revision),
         :ok <- previous_digest(state.digest, entry.previous_entry_digest) do
      {:cont,
       {:ok,
        %{
          stream_key: entry.stream_key,
          revision: entry.revision,
          digest: entry.entry_digest,
          count: state.count + 1
        }}}
    else
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp verify_next(_entry, _state), do: {:halt, {:error, :invalid_ledger_chain_entry}}

  defp same_stream(nil, _stream_key), do: :ok
  defp same_stream(stream_key, stream_key), do: :ok
  defp same_stream(_expected, _actual), do: {:error, :ledger_chain_stream_mismatch}

  defp expected_revision(revision, revision), do: :ok

  defp expected_revision(_expected, _actual),
    do: {:error, :ledger_chain_revision_gap}

  defp previous_digest(digest, digest), do: :ok

  defp previous_digest(_expected, _actual),
    do: {:error, :ledger_chain_previous_digest_mismatch}
end
