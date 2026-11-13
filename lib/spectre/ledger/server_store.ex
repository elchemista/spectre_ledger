defmodule Spectre.Ledger.ServerStore do
  @moduledoc """
  Append-only coordination store for Spectre 0.4 AgentServer checkpoints.

  Every mutation is a bounded canonical record with a checksum. An acknowledged
  write has been synced to disk. Replay verifies ordering and revisions; a torn
  or corrupt tail is reported, never silently discarded. This is a local,
  single-owner log, not the GAM fact ledger or the legacy Instance archive.

  Configure `checkpoint: {Spectre.Ledger.ServerStore, store, conversation_key}`.
  Keep the owner under supervision. Revisions remain monotonic after deletion.
  Capacity is bounded; an operator must rotate/migrate a full log explicitly.
  """
  use GenServer
  @behaviour Spectre.Server.Store
  alias Spectre.Server.Checkpoint

  @magic "SPLS1"
  @default_max_bytes 256 * 1_024 * 1_024

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, Keyword.take(opts, [:name]))
  @impl Spectre.Server.Store
  def fetch(store, key), do: GenServer.call(store, {:fetch, key})
  @impl Spectre.Server.Store
  def put_new(store, key, value), do: GenServer.call(store, {:put_new, key, value}, :infinity)
  @impl Spectre.Server.Store
  def compare_and_swap(store, key, revision, value),
    do: GenServer.call(store, {:cas, key, revision, value}, :infinity)

  @impl Spectre.Server.Store
  def delete(store, key, revision), do: GenServer.call(store, {:delete, key, revision}, :infinity)

  @doc "Returns bounded append metadata without exposing checkpoint payloads."
  def entries(store, key, limit \\ 50), do: GenServer.call(store, {:entries, key, limit})

  @impl true
  def init(opts) do
    path = opts |> Keyword.fetch!(:path) |> Path.expand()
    max_bytes = Keyword.get(opts, :max_bytes, @default_max_bytes)

    with true <- is_integer(max_bytes) and max_bytes > byte_size(@magic),
         :ok <- File.mkdir_p(Path.dirname(path)),
         {:ok, stat} <- File.stat(Path.dirname(path)),
         lock <-
           {__MODULE__, node(), stat.major_device, stat.minor_device, stat.inode,
            Path.basename(path)},
         :yes <- :global.register_name(lock, self()),
         {:ok, state} <- load(path, max_bytes),
         {:ok, file} <- :file.open(String.to_charlist(path), [:append, :binary, :raw]),
         :ok <- initialize(file, state.bytes, path) do
      {:ok,
       Map.merge(state, %{
         file: file,
         path: path,
         max_bytes: max_bytes,
         lock: lock,
         bytes: max(state.bytes, byte_size(@magic))
       })}
    else
      false -> {:stop, :invalid_store_capacity}
      :no -> {:stop, :store_path_in_use}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
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

  def handle_call({:put_new, key, value}, _from, state) do
    case Map.get(state.entries, key) do
      %{deleted?: false} -> {:reply, {:error, :exists}, state}
      previous -> commit(state, :put, key, revision(previous) + 1, value)
    end
  end

  def handle_call({:cas, key, expected, value}, _from, state) do
    case Map.get(state.entries, key) do
      %{deleted?: false, revision: ^expected} -> commit(state, :put, key, expected + 1, value)
      _ -> {:reply, {:error, :conflict}, state}
    end
  end

  def handle_call({:delete, key, expected}, _from, state) do
    case Map.get(state.entries, key) do
      %{deleted?: false, revision: ^expected} -> commit(state, :delete, key, expected + 1, nil)
      _ -> {:reply, {:error, :conflict}, state}
    end
  end

  def handle_call({:entries, key, limit}, _from, state)
      when is_integer(limit) and limit > 0 and limit <= 1000 do
    entries = state.history |> Enum.filter(&(&1.key == key)) |> Enum.take(limit) |> Enum.reverse()
    {:reply, {:ok, entries}, state}
  end

  def handle_call({:entries, _key, _limit}, _from, state),
    do: {:reply, {:error, :invalid_limit}, state}

  @impl true
  def terminate(_reason, state), do: :file.close(state.file)

  defp commit(state, action, key, revision, value) do
    record = %{
      sequence: state.sequence + 1,
      action: action,
      key: key,
      revision: revision,
      value: value
    }

    with {:ok, body} <- Checkpoint.encode_value(record),
         frame <-
           <<byte_size(body)::unsigned-big-32, :crypto.hash(:sha256, body)::binary, body::binary>>,
         true <- state.bytes + byte_size(frame) <= state.max_bytes do
      persist_record(state, action, revision, record, body, frame)
    else
      false -> {:reply, {:error, :store_capacity_exceeded}, state}
      {:error, reason} -> {:reply, {:error, {:invalid_store_value, reason}}, state}
    end
  end

  defp persist_record(state, action, revision, record, body, frame) do
    case persist(state.file, frame) do
      :ok ->
        updated = apply_record(state, record, body)
        reply = if action == :delete, do: :ok, else: {:ok, revision}
        {:reply, reply, %{updated | bytes: state.bytes + byte_size(frame)}}

      {:error, reason} ->
        {:stop, {:store_write_uncertain, reason}, {:error, {:store_write_uncertain, reason}},
         state}
    end
  end

  defp persist(file, data) do
    with :ok <- :file.write(file, data), do: :file.sync(file)
  end

  defp initialize(file, 0, path) do
    with :ok <- persist(file, @magic),
         {:ok, directory} <-
           :file.open(String.to_charlist(Path.dirname(path)), [:read, :raw, :directory]) do
      result = :file.sync(directory)
      :file.close(directory)
      result
    end
  end

  defp initialize(_file, _bytes, _path), do: :ok

  defp load(path, max_bytes) do
    state = %{sequence: 0, entries: %{}, history: [], bytes: 0}

    case File.lstat(path) do
      {:error, :enoent} ->
        {:ok, state}

      {:ok, %{type: type, links: links}} when type != :regular or links != 1 ->
        {:error, :invalid_store_file}

      {:ok, %{size: size}} when size > max_bytes ->
        {:error, :store_capacity_exceeded}

      {:ok, _} ->
        with {:ok, contents} <- File.read(path),
             <<@magic, rest::binary>> <- contents,
             {:ok, loaded} <- replay(rest, state) do
          {:ok, %{loaded | bytes: byte_size(contents)}}
        else
          {:error, _} = error -> error
          _ -> {:error, :invalid_store_header}
        end

      {:error, reason} ->
        {:error, {:store_read_failed, reason}}
    end
  end

  defp replay(<<>>, state), do: {:ok, state}

  defp replay(
         <<size::unsigned-big-32, digest::binary-size(32), body::binary-size(size),
           rest::binary>>,
         state
       ) do
    with true <- :crypto.hash(:sha256, body) == digest,
         {:ok, record} <- Checkpoint.decode_snapshot(body),
         :ok <- valid_record(record, state) do
      replay(rest, apply_record(state, record, body))
    else
      _ -> {:error, :corrupt_store_record}
    end
  end

  defp replay(_tail, _state), do: {:error, :truncated_store_record}

  defp valid_record(
         %{sequence: sequence, action: action, key: key, revision: version, value: value} = record,
         state
       )
       when action in [:put, :delete] and map_size(record) == 5 do
    previous = Map.get(state.entries, key)
    valid_delete = action != :delete or (match?(%{deleted?: false}, previous) and is_nil(value))

    if sequence == state.sequence + 1 and version == revision(previous) + 1 and valid_delete,
      do: :ok,
      else: {:error, :invalid_store_revision}
  end

  defp valid_record(_record, _state), do: {:error, :invalid_store_record}

  defp apply_record(state, record, body) do
    entry = %{revision: record.revision, value: record.value, deleted?: record.action == :delete}

    metadata =
      record
      |> Map.delete(:value)
      |> Map.put(:digest, Base.encode16(:crypto.hash(:sha256, body), case: :lower))

    %{
      state
      | sequence: record.sequence,
        entries: Map.put(state.entries, record.key, entry),
        history: [metadata | state.history]
    }
  end

  defp revision(nil), do: 0
  defp revision(entry), do: entry.revision
end
