defmodule SpectreLedger.TestEtsLedgerBackend do
  @moduledoc false

  use GenServer

  @behaviour Spectre.Ledger.Backend

  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.Entry
  alias Spectre.Ledger.Receipt
  alias Spectre.Ledger.Write

  @default_timeout 5_000

  def start_link(opts \\ [])

  def start_link([]), do: GenServer.start_link(__MODULE__, :ok)
  def start_link(_opts), do: {:error, :invalid_test_ets_ledger_backend_options}

  def snapshot(server) when is_pid(server), do: GenServer.call(server, :snapshot)

  @impl GenServer
  def init(:ok) do
    table = :ets.new(__MODULE__, [:set, :protected, read_concurrency: true])
    {:ok, %{table: table, sequence: 0}}
  end

  @impl Spectre.Ledger.Backend
  def load(%Config{} = config, %Ref{key: stream_key}) do
    call(config, {:load, config.namespace, stream_key})
  end

  @impl Spectre.Ledger.Backend
  def compare_and_swap(%Config{} = config, %Write{} = write) do
    call(config, {:compare_and_swap, config.namespace, write})
  end

  @impl Spectre.Ledger.Backend
  def head(%Config{} = config, stream_key) when is_binary(stream_key) and stream_key != "" do
    call(config, {:head, config.namespace, stream_key})
  end

  def head(%Config{}, _stream_key), do: {:error, :invalid_ledger_stream_key}

  @impl Spectre.Ledger.Backend
  def entries(%Config{} = config, stream_key, opts)
      when is_binary(stream_key) and stream_key != "" and is_list(opts) do
    with {:ok, query} <- entry_query(opts) do
      call(config, {:entries, config.namespace, stream_key, query})
    end
  end

  def entries(%Config{}, _stream_key, _opts), do: {:error, :invalid_ledger_entry_query}

  @impl Spectre.Ledger.Backend
  def objects(%Config{} = config, stream_key, [])
      when is_binary(stream_key) and stream_key != "" do
    call(config, {:objects, config.namespace, stream_key})
  end

  def objects(%Config{}, _stream_key, _opts), do: {:error, :invalid_ledger_object_query}

  @impl Spectre.Ledger.Backend
  def put_stream(%Config{} = config, stream_key, entries, objects) do
    with :ok <- validate_import(config, stream_key, entries, objects) do
      call(config, {:put_stream, config.namespace, stream_key, entries, objects})
    end
  end

  @impl GenServer
  def handle_call({:load, namespace, stream_key}, _from, state) do
    reply =
      case lookup(state.table, head_key(namespace, stream_key)) do
        {:ok, entry} -> lookup_blob(state.table, namespace, entry.blob_digest)
        :not_found -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call({:head, namespace, stream_key}, _from, state) do
    reply =
      case lookup(state.table, head_key(namespace, stream_key)) do
        {:ok, entry} -> {:ok, entry}
        :not_found -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call({:entries, namespace, stream_key, query}, _from, state) do
    entries =
      state.table
      |> stream_entries(namespace, stream_key)
      |> select_entries(query)

    {:reply, {:ok, entries}, state}
  end

  def handle_call({:objects, namespace, stream_key}, _from, state) do
    objects =
      state.table
      |> stream_entries(namespace, stream_key)
      |> Map.new(fn entry ->
        {:ok, checkpoint} = lookup_blob(state.table, namespace, entry.blob_digest)
        {entry.blob_digest, checkpoint}
      end)

    {:reply, {:ok, objects}, state}
  end

  def handle_call({:compare_and_swap, namespace, write}, _from, state) do
    {reply, next_state} =
      case append(state.table, namespace, write) do
        {:ok, receipt} ->
          event = write_event(namespace, write.ref.key, receipt.status, receipt.revision)
          {{:ok, receipt}, record(state, event)}

        {:error, reason} = error ->
          event = write_event(namespace, write.ref.key, :rejected, write.revision, reason)
          {error, record(state, event)}
      end

    {:reply, reply, next_state}
  end

  def handle_call({:put_stream, namespace, stream_key, entries, objects}, _from, state) do
    current_entries = stream_entries(state.table, namespace, stream_key)

    {reply, next_state} =
      cond do
        current_entries == [] ->
          insert_stream(state.table, namespace, entries, objects)
          event = write_event(namespace, stream_key, :imported, List.last(entries).revision)
          {{:ok, :imported}, record(state, event)}

        current_entries == entries and
            stream_objects(state.table, namespace, current_entries) == objects ->
          event = write_event(namespace, stream_key, :idempotent, List.last(entries).revision)
          {{:ok, :idempotent}, record(state, event)}

        true ->
          reason = {:stream_conflict, stream_key}

          event =
            write_event(namespace, stream_key, :rejected, List.last(entries).revision, reason)

          {{:error, reason}, record(state, event)}
      end

    {:reply, reply, next_state}
  end

  def handle_call(:snapshot, _from, state) do
    streams =
      state.table
      |> all_stream_keys()
      |> Map.new(fn {namespace, stream_key} ->
        entries = stream_entries(state.table, namespace, stream_key)

        {{namespace, stream_key},
         %{
           entries: entries,
           objects: stream_objects(state.table, namespace, entries),
           head: List.last(entries)
         }}
      end)

    writes =
      for {{:write, sequence}, event} <- :ets.tab2list(state.table) do
        {sequence, event}
      end
      |> Enum.sort_by(&elem(&1, 0))
      |> Enum.map(&elem(&1, 1))

    snapshot = %{
      storage: :ets,
      table_size: :ets.info(state.table, :size),
      streams: streams,
      writes: writes
    }

    {:reply, snapshot, state}
  end

  defp append(table, namespace, %Write{} = write) do
    case lookup(table, head_key(namespace, write.ref.key)) do
      :not_found -> append_initial(table, namespace, write)
      {:ok, head} -> append_after(table, namespace, head, write)
    end
  end

  defp append_initial(table, namespace, %Write{expected_revision: 0} = write) do
    with {:ok, entry} <- Entry.new(write, nil) do
      insert_entry(table, namespace, entry, write.checkpoint)
      {:ok, receipt(entry, :appended)}
    end
  end

  defp append_initial(_table, _namespace, %Write{}), do: {:error, {:stale, 0}}

  defp append_after(table, namespace, head, %Write{} = write) do
    cond do
      write.revision == head.revision and exact_retry?(table, namespace, head, write) ->
        {:ok, receipt(head, :idempotent)}

      write.revision == head.revision ->
        {:error, {:conflict, head.revision}}

      write.expected_revision != head.revision ->
        {:error, {:stale, head.revision}}

      true ->
        with {:ok, entry} <- Entry.new(write, head.entry_digest) do
          insert_entry(table, namespace, entry, write.checkpoint)
          {:ok, receipt(entry, :appended)}
        end
    end
  end

  defp exact_retry?(table, namespace, head, %Write{} = write) do
    with {:ok, candidate} <- Entry.new(write, head.previous_entry_digest),
         {:ok, checkpoint} <- lookup_blob(table, namespace, head.blob_digest) do
      candidate.entry_digest == head.entry_digest and checkpoint == write.checkpoint
    else
      _other -> false
    end
  end

  defp insert_entry(table, namespace, entry, checkpoint) do
    :ets.insert(table, [
      {entry_key(namespace, entry.stream_key, entry.revision), entry},
      {blob_key(namespace, entry.blob_digest), checkpoint},
      {head_key(namespace, entry.stream_key), entry}
    ])

    :ok
  end

  defp insert_stream(table, namespace, entries, objects) do
    Enum.each(entries, fn entry ->
      :ets.insert(table, {entry_key(namespace, entry.stream_key, entry.revision), entry})
    end)

    Enum.each(objects, fn {digest, checkpoint} ->
      :ets.insert(table, {blob_key(namespace, digest), checkpoint})
    end)

    :ets.insert(table, {head_key(namespace, hd(entries).stream_key), List.last(entries)})
    :ok
  end

  defp validate_import(config, stream_key, entries, objects) do
    with true <- is_binary(stream_key) and stream_key != "",
         true <- is_list(entries) and entries != [],
         true <- is_map(objects),
         {:ok, %{stream_key: ^stream_key}} <- Chain.verify(entries),
         true <- MapSet.new(Map.keys(objects)) == MapSet.new(Enum.map(entries, & &1.blob_digest)),
         :ok <- validate_objects(config, entries, objects) do
      :ok
    else
      false -> {:error, :invalid_imported_stream}
      {:ok, _report} -> {:error, :imported_stream_key_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp validate_objects(config, entries, objects) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      checkpoint = Map.get(objects, entry.blob_digest)

      result =
        with true <- is_binary(checkpoint),
             true <- byte_size(checkpoint) in 1..config.max_checkpoint_bytes,
             true <- sha256(checkpoint) == entry.blob_digest,
             {:ok, report} <- Foundation.verify_instance_checkpoint(checkpoint, entry.stream_key),
             true <- report.digest == entry.checkpoint_digest,
             true <- report.revision == entry.revision do
          :ok
        else
          _invalid -> {:error, {:invalid_ledger_import_checkpoint, entry.revision}}
        end

      case result do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp call(config, message) do
    with {:ok, server, timeout} <- server(config) do
      GenServer.call(server, message, timeout)
    end
  end

  defp server(config) do
    timeout = Config.get_backend(config, :timeout, @default_timeout)

    with {:ok, server} <- Config.fetch_backend(config, :server),
         true <- is_pid(server) and Process.alive?(server),
         true <- timeout == :infinity or (is_integer(timeout) and timeout > 0) do
      {:ok, server, timeout}
    else
      :error -> {:error, :test_ets_ledger_server_required}
      false -> {:error, :invalid_test_ets_ledger_server}
    end
  end

  defp entry_query(opts) do
    if Keyword.keyword?(opts) do
      after_revision = Keyword.get(opts, :after_revision, -1)
      limit = Keyword.get(opts, :limit, :infinity)

      if is_integer(after_revision) and after_revision >= -1 and
           (limit == :infinity or (is_integer(limit) and limit > 0)) do
        {:ok, %{after_revision: after_revision, limit: limit}}
      else
        {:error, :invalid_ledger_entry_query}
      end
    else
      {:error, :invalid_ledger_entry_query}
    end
  end

  defp select_entries(entries, %{after_revision: after_revision, limit: limit}) do
    selected = Enum.drop_while(entries, &(&1.revision <= after_revision))
    if limit == :infinity, do: selected, else: Enum.take(selected, limit)
  end

  defp stream_entries(table, namespace, stream_key) do
    for(
      {{:entry, ^namespace, ^stream_key, _revision}, entry} <- :ets.tab2list(table),
      do: entry
    )
    |> Enum.sort_by(& &1.revision)
  end

  defp stream_objects(table, namespace, entries) do
    Map.new(entries, fn entry ->
      {:ok, checkpoint} = lookup_blob(table, namespace, entry.blob_digest)
      {entry.blob_digest, checkpoint}
    end)
  end

  defp all_stream_keys(table) do
    table
    |> :ets.tab2list()
    |> Enum.flat_map(fn
      {{:head, namespace, stream_key}, _entry} -> [{namespace, stream_key}]
      _record -> []
    end)
    |> Enum.sort()
  end

  defp lookup(table, key) do
    case :ets.lookup(table, key) do
      [{^key, value}] -> {:ok, value}
      [] -> :not_found
    end
  end

  defp lookup_blob(table, namespace, digest) do
    case lookup(table, blob_key(namespace, digest)) do
      {:ok, checkpoint} -> {:ok, checkpoint}
      :not_found -> {:error, :ledger_checkpoint_blob_not_found}
    end
  end

  defp record(state, event) do
    sequence = state.sequence + 1
    :ets.insert(state.table, {{:write, sequence}, event})
    %{state | sequence: sequence}
  end

  defp write_event(namespace, stream_key, status, revision, reason \\ nil) do
    %{
      namespace: namespace,
      stream_key: stream_key,
      status: status,
      revision: revision,
      reason: reason
    }
  end

  defp receipt(entry, status) do
    %Receipt{
      stream_key: entry.stream_key,
      revision: entry.revision,
      checkpoint_digest: entry.checkpoint_digest,
      entry_digest: entry.entry_digest,
      status: status
    }
  end

  defp head_key(namespace, stream_key), do: {:head, namespace, stream_key}
  defp entry_key(namespace, stream_key, revision), do: {:entry, namespace, stream_key, revision}
  defp blob_key(namespace, digest), do: {:blob, namespace, digest}

  defp sha256(checkpoint) do
    checkpoint
    |> then(&:crypto.hash(:sha256, &1))
    |> Base.encode16(case: :lower)
  end
end
