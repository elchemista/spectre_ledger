defmodule Spectre.Ledger.ServerStore do
  @moduledoc """
  Durable local coordination log for Spectre 0.4 AgentServer checkpoints.

  Acknowledged mutations have been synced to disk. At capacity, the store
  atomically compacts the latest value of every key, including the pending
  mutation. `compact/1` requests the same operation explicitly. A persistent global
  sequence protects revision fences, so deleted keys disappear from memory and
  disk during compaction. All live keys (including GAM journal records) survive.
  Capacity must still accommodate the live data; compaction cannot shrink that.

  Compaction replaces append history with current entry metadata. This is a
  coordination store, not an immutable audit archive. `delete/3` appends a
  tombstone; old payload bytes remain until compaction. Compaction removes them
  from the active file, but does not promise forensic erasure from filesystem
  snapshots, backups or storage media.

  Initialization and compaction use an exclusive temporary file, file sync,
  atomic rename and directory sync. Replay rejects corrupt or truncated tails:
  it cannot determine whether a damaged frame was previously acknowledged, so
  recovery requires operator inspection rather than silent data loss. This is
  a single-host owner, not a distributed filesystem lock.
  """
  use GenServer
  @behaviour Spectre.Server.Store
  alias Spectre.Server.Checkpoint
  alias Spectre.Server.Store.Page

  @magic "SPLS2"
  @legacy_magic "SPLS1"
  @default_max_bytes 256 * 1_024 * 1_024

  def start_link(opts) do
    if Keyword.get(opts, :backend) == :postgres,
      do: Spectre.Ledger.ServerStore.Postgres.start_link(Keyword.delete(opts, :backend)),
      else: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  end

  @impl Spectre.Server.Store
  def fetch(store, key), do: GenServer.call(store, {:fetch, key}, :infinity)
  @impl Spectre.Server.Store
  def put_new(store, key, value), do: GenServer.call(store, {:put_new, key, value}, :infinity)
  @impl Spectre.Server.Store
  def compare_and_swap(store, key, revision, value),
    do: GenServer.call(store, {:cas, key, revision, value}, :infinity)

  @impl Spectre.Server.Store
  def delete(store, key, revision), do: GenServer.call(store, {:delete, key, revision}, :infinity)

  @impl Spectre.Server.Store
  def list(store, prefix, opts \\ []), do: GenServer.call(store, {:list, prefix, opts}, :infinity)

  @impl Spectre.Server.Store
  def gam_ledger(store), do: GenServer.call(store, :gam_ledger)

  @impl true
  def gam_domain(store), do: GenServer.call(store, :gam_domain)

  @doc "Atomically removes superseded payloads while preserving live keys and revision fences."
  def compact(store), do: GenServer.call(store, :compact, :infinity)

  @doc "Returns bounded current append metadata; compaction retains only the latest entry per live key."
  def entries(store, key, limit \\ 50),
    do: GenServer.call(store, {:entries, key, limit}, :infinity)

  @impl true
  def init(opts) do
    with {:ok, path, max_bytes} <- options(opts),
         :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, stat} <- File.stat(Path.dirname(path)),
         lock <-
           {__MODULE__, node(), stat.major_device, stat.minor_device, stat.inode,
            Path.basename(path)},
         :yes <- :global.register_name(lock, self()),
         {:ok, state} <- load(path, max_bytes),
         :ok <- initialize(path, state, max_bytes),
         {:ok, file} <- :file.open(String.to_charlist(path), [:append, :binary, :raw]),
         {:ok, %{size: size}} <- File.stat(path) do
      {:ok, Map.merge(state, %{file: file, path: path, max_bytes: max_bytes, bytes: size})}
    else
      :no -> {:stop, :store_path_in_use}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call(:gam_ledger, _from, state), do: {:reply, :not_configured, state}
  def handle_call(:gam_domain, _from, state), do: {:reply, :not_configured, state}

  def handle_call({:fetch, key}, _from, state) do
    result =
      case Map.get(state.entries, key) do
        %{deleted?: false, revision: revision, value: value} ->
          {:ok, %{revision: revision, value: value}}

        _ ->
          :not_found
      end

    {:reply, result, state}
  end

  def handle_call({:list, prefix, opts}, _from, state) do
    fetch = fn key ->
      entry = Map.fetch!(state.entries, key)
      Map.take(entry, [:revision, :value])
    end

    {:reply, Page.read(state.index, fetch, prefix, opts), state}
  end

  def handle_call({:put_new, key, value}, _from, state) do
    case Map.get(state.entries, key) do
      %{deleted?: false} -> {:reply, {:error, :exists}, state}
      previous -> commit(state, :put, key, max(revision(previous), state.sequence) + 1, value)
    end
  end

  def handle_call({:cas, key, expected, value}, _from, state),
    do: compare_and_commit(state, :put, key, expected, value)

  def handle_call({:delete, key, expected}, _from, state),
    do: compare_and_commit(state, :delete, key, expected, nil)

  def handle_call(:compact, _from, state), do: compact_reply(state, state, :ok)

  def handle_call({:entries, key, limit}, _from, state)
      when is_integer(limit) and limit > 0 and limit <= 1000 do
    entries = state.history |> Enum.filter(&(&1.key == key)) |> Enum.take(limit) |> Enum.reverse()
    {:reply, {:ok, entries}, state}
  end

  def handle_call({:entries, _key, _limit}, _from, state),
    do: {:reply, {:error, :invalid_limit}, state}

  @impl true
  def terminate(_reason, state), do: :file.close(state.file)

  defp compare_and_commit(state, action, key, expected, value) do
    case Map.get(state.entries, key) do
      %{deleted?: false, revision: ^expected} -> commit(state, action, key, expected + 1, value)
      _ -> {:reply, {:error, :conflict}, state}
    end
  end

  defp commit(state, action, key, revision, value) do
    record = %{
      sequence: state.sequence + 1,
      action: action,
      key: key,
      revision: revision,
      value: value
    }

    case encode_frame(record) do
      {:ok, frame, body} ->
        updated = apply_record(state, record, body)
        reply = if action == :delete, do: :ok, else: {:ok, revision}
        persist_mutation(state, updated, frame, reply)

      {:error, reason} ->
        {:reply, {:error, {:invalid_store_value, reason}}, state}
    end
  end

  defp persist_mutation(state, updated, frame, reply) do
    if state.bytes + byte_size(frame) <= state.max_bytes do
      case persist(state.file, frame) do
        :ok -> {:reply, reply, %{updated | bytes: state.bytes + byte_size(frame)}}
        {:error, reason} -> uncertain(state, reason)
      end
    else
      compact_reply(state, updated, reply)
    end
  end

  defp compact_reply(previous, updated, reply) do
    updated = %{
      updated
      | entries: Map.reject(updated.entries, fn {_key, entry} -> entry.deleted? end),
        fresh_revision: :global
    }

    with {:ok, data, size, history} <- compact_data(updated, previous.max_bytes),
         :ok <- atomic_replace(previous.path, data),
         {:ok, file} <- :file.open(String.to_charlist(previous.path), [:append, :binary, :raw]) do
      :file.close(previous.file)
      {:reply, reply, %{updated | file: file, bytes: size, history: history}}
    else
      {:error, :store_capacity_exceeded} -> {:reply, {:error, :store_capacity_exceeded}, previous}
      {:error, reason} -> uncertain(previous, reason)
    end
  end

  defp uncertain(state, reason),
    do:
      {:stop, {:store_write_uncertain, reason}, {:error, {:store_write_uncertain, reason}}, state}

  defp compact_data(state, max_bytes) do
    header = %{checkpoint: 2, sequence: state.sequence, count: map_size(state.entries)}

    with {:ok, frame, _} <- encode_frame(header) do
      initial = {:ok, [frame, @magic], byte_size(@magic) + byte_size(frame), []}

      state.entries
      |> Enum.sort_by(fn {_key, entry} -> entry.sequence end)
      |> Enum.reduce_while(initial, &compact_entry(&1, &2, max_bytes))
      |> finish_compaction(max_bytes)
    end
  end

  defp compact_entry({key, entry}, {:ok, frames, size, history}, max_bytes) do
    record = entry_record(key, entry)

    with {:ok, frame, _} <- encode_frame(%{entry: record}),
         {:ok, body} <- Checkpoint.encode_value(record),
         true <- size + byte_size(frame) <= max_bytes do
      {:cont,
       {:ok, [frame | frames], size + byte_size(frame), [metadata(record, body) | history]}}
    else
      false -> {:halt, {:error, :store_capacity_exceeded}}
      {:error, _} = error -> {:halt, error}
    end
  end

  defp finish_compaction({:ok, frames, size, history}, max_bytes) when size <= max_bytes,
    do: {:ok, Enum.reverse(frames), size, history}

  defp finish_compaction({:ok, _, _, _}, _), do: {:error, :store_capacity_exceeded}
  defp finish_compaction({:error, _} = error, _), do: error

  defp entry_record(key, entry),
    do: %{
      sequence: entry.sequence,
      action: if(entry.deleted?, do: :delete, else: :put),
      key: key,
      revision: entry.revision,
      value: entry.value
    }

  defp initialize(path, %{bytes: 0} = state, max_bytes) do
    with {:ok, data, _, _} <- compact_data(state, max_bytes), do: atomic_replace(path, data)
  end

  defp initialize(_path, _state, _max_bytes), do: :ok

  defp atomic_replace(path, data) do
    temporary =
      path <> ".compact-" <> Base.url_encode64(:crypto.strong_rand_bytes(12), padding: false)

    try do
      with :ok <- write_temporary(temporary, data),
           :ok <- File.rename(temporary, path),
           do: sync_directory(Path.dirname(path))
    after
      File.rm(temporary)
    end
  end

  defp write_temporary(path, data) do
    with_file(path, [:write, :binary, :raw, :exclusive], fn file ->
      with :ok <- File.chmod(path, 0o600), do: persist(file, data)
    end)
  end

  defp sync_directory(path), do: with_file(path, [:read, :raw, :directory], &:file.sync/1)

  defp with_file(path, modes, fun) do
    with {:ok, file} <- :file.open(String.to_charlist(path), modes) do
      try do
        fun.(file)
      after
        :file.close(file)
      end
    end
  end

  defp persist(file, data) do
    with :ok <- :file.write(file, data), do: :file.sync(file)
  end

  defp encode_frame(record) do
    with {:ok, body} <- Checkpoint.encode_value(record) do
      {:ok,
       <<byte_size(body)::unsigned-big-32, :crypto.hash(:sha256, body)::binary, body::binary>>,
       body}
    end
  end

  defp options(opts) do
    path = Keyword.get(opts, :path)
    max_bytes = Keyword.get(opts, :max_bytes, @default_max_bytes)

    if is_binary(path) and path != "" and is_integer(max_bytes) and max_bytes > byte_size(@magic),
      do: {:ok, Path.expand(path), max_bytes},
      else: {:error, :invalid_store_options}
  end

  defp load(path, max_bytes) do
    case File.lstat(path) do
      {:error, :enoent} ->
        {:ok, empty_state()}

      {:ok, %{type: type, links: links}} when type != :regular or links != 1 ->
        {:error, :invalid_store_file}

      {:ok, %{size: size}} when size > max_bytes ->
        {:error, :store_capacity_exceeded}

      {:ok, _} ->
        read_log(path)

      {:error, reason} ->
        {:error, {:store_read_failed, reason}}
    end
  end

  defp read_log(path) do
    with {:ok, contents} <- File.read(path),
         {:ok, loaded} <- decode_log(contents),
         do: {:ok, %{loaded | bytes: byte_size(contents)}}
  end

  defp decode_log(<<@legacy_magic, rest::binary>>), do: replay(rest, empty_state(:compatible))

  defp decode_log(<<@magic, rest::binary>>) do
    with {:ok, %{checkpoint: version, sequence: sequence, count: count} = header, _, tail} <-
           read_frame(rest),
         true <-
           version in [1, 2] and map_size(header) == 3 and is_integer(sequence) and sequence >= 0 and
             is_integer(count) and count >= 0 and count <= sequence,
         {:ok, state, tail} <- restore_entries(tail, snapshot_state(version, sequence), count) do
      replay(tail, state)
    else
      false -> {:error, :corrupt_store_record}
      {:error, _} = error -> error
      _ -> {:error, :corrupt_store_record}
    end
  end

  defp decode_log(_), do: {:error, :invalid_store_header}

  defp restore_entries(tail, state, 0) do
    if state.fresh_revision == :global or latest_sequence(state.history) == state.sequence,
      do: {:ok, state, tail},
      else: {:error, :corrupt_store_record}
  end

  defp restore_entries(bytes, state, remaining) do
    with {:ok, %{entry: record} = wrapper, _, tail} <- read_frame(bytes),
         true <- map_size(wrapper) == 1,
         :ok <- valid_snapshot_record(record, state),
         {:ok, body} <- Checkpoint.encode_value(record) do
      next = apply_record(state, record, body)
      restore_entries(tail, %{next | sequence: state.sequence}, remaining - 1)
    else
      {:error, _} = error -> error
      _ -> {:error, :corrupt_store_record}
    end
  end

  defp replay(<<>>, state), do: {:ok, state}

  defp replay(bytes, state) do
    with {:ok, record, body, rest} <- read_frame(bytes),
         :ok <- valid_record(record, state),
         do: replay(rest, apply_record(state, record, body))
  end

  defp read_frame(
         <<size::unsigned-big-32, digest::binary-size(32), body::binary-size(size), rest::binary>>
       ) do
    with true <- :crypto.hash(:sha256, body) == digest,
         {:ok, record} <- Checkpoint.decode_snapshot(body) do
      {:ok, record, body, rest}
    else
      _ -> {:error, :corrupt_store_record}
    end
  end

  defp read_frame(_), do: {:error, :truncated_store_record}

  defp valid_snapshot_record(
         %{sequence: sequence, action: action, key: key, revision: version, value: value} = record,
         state
       )
       when action in [:put, :delete] and map_size(record) == 5 and is_integer(sequence) and
              is_integer(version) do
    if valid_snapshot_position?(sequence, version, state) and
         not Map.has_key?(state.entries, key) and (action != :delete or is_nil(value)),
       do: :ok,
       else: {:error, :corrupt_store_record}
  end

  defp valid_snapshot_record(_, _), do: {:error, :corrupt_store_record}

  defp valid_snapshot_position?(sequence, version, state) do
    version > 0 and version <= sequence and sequence > latest_sequence(state.history) and
      sequence <= state.sequence
  end

  defp latest_sequence([%{sequence: sequence} | _]), do: sequence
  defp latest_sequence([]), do: 0

  defp valid_record(
         %{sequence: sequence, action: action, key: key, revision: version, value: value} = record,
         state
       )
       when action in [:put, :delete] and map_size(record) == 5 and is_integer(sequence) and
              is_integer(version) do
    previous = Map.get(state.entries, key)
    valid_delete = action != :delete or (match?(%{deleted?: false}, previous) and is_nil(value))

    if sequence == state.sequence + 1 and valid_revision?(action, version, previous, state) and
         valid_delete,
       do: :ok,
       else: {:error, :corrupt_store_record}
  end

  defp valid_record(_, _), do: {:error, :corrupt_store_record}

  # SPLS1 and checkpoint-v1 files may contain historical per-key fresh revisions.
  # Newly compacted files fence every fresh/recreated key with the global sequence.
  # Existing live keys always retain the strict CAS successor rule.
  defp valid_revision?(:put, version, previous, state)
       when is_nil(previous) or previous.deleted? do
    global = max(revision(previous), state.sequence) + 1

    version == global or
      (state.fresh_revision == :compatible and version == revision(previous) + 1)
  end

  defp valid_revision?(_action, version, previous, _state), do: version == revision(previous) + 1

  defp snapshot_state(version, sequence) do
    mode = if version == 1, do: :compatible, else: :global
    %{empty_state(mode) | sequence: sequence}
  end

  defp apply_record(state, record, body) do
    entry = %{
      revision: record.revision,
      value: record.value,
      deleted?: record.action == :delete,
      sequence: record.sequence
    }

    %{
      state
      | sequence: record.sequence,
        entries: Map.put(state.entries, record.key, entry),
        index:
          if(entry.deleted?,
            do: Page.delete(state.index, record.key),
            else: Page.put(state.index, record.key)
          ),
        history: [metadata(record, body) | state.history]
    }
  end

  defp metadata(record, body),
    do:
      record
      |> Map.delete(:value)
      |> Map.put(:digest, Base.encode16(:crypto.hash(:sha256, body), case: :lower))

  defp empty_state(mode \\ :global),
    do: %{
      sequence: 0,
      entries: %{},
      history: [],
      bytes: 0,
      fresh_revision: mode,
      index: Page.index([])
    }

  defp revision(nil), do: 0
  defp revision(%{revision: revision}), do: revision
end
