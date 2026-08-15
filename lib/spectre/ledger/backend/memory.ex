defmodule Spectre.Ledger.Backend.Memory do
  @moduledoc """
  Caller-owned, in-memory Ledger backend.

  The server is deliberately not registered. Callers start it under their own
  supervision tree and pass its pid as `:server` in the Ledger backend options.
  One server may host multiple isolated Ledger namespaces.

  This backend is intended for tests and ephemeral runtimes. Its CAS and
  migration operations are atomic within the owning `GenServer`, but its state
  is not durable across a server restart.
  """

  use GenServer

  @behaviour Spectre.Ledger.Backend

  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.Entry
  alias Spectre.Ledger.Receipt
  alias Spectre.Ledger.ReceiptCodec
  alias Spectre.Ledger.ReceiptEntry
  alias Spectre.Ledger.ReceiptWrite
  alias Spectre.Ledger.Write
  alias Spectre.Receipt.Envelope
  alias Spectre.Receipt.Sink

  @default_timeout 5_000

  @type stream :: %{
          entries: [Entry.t()],
          blobs: %{required(String.t()) => binary()},
          checkpoint: binary()
        }

  @type namespace :: %{
          streams: %{optional(String.t()) => stream()},
          aliases: %{optional(String.t()) => String.t()},
          receipt_streams: %{optional(String.t()) => [ReceiptEntry.t()]},
          receipt_index: %{optional(String.t()) => ReceiptEntry.t()},
          receipt_payloads: %{
            optional(String.t()) => %{
              envelope_digest: String.t(),
              encoded: binary(),
              byte_size: pos_integer()
            }
          }
        }

  @type state :: %{optional(String.t()) => namespace()}

  @doc "Starts an unregistered Memory backend owned by the caller."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ [])

  def start_link(opts) when is_list(opts) and opts == [],
    do: GenServer.start_link(__MODULE__, %{})

  def start_link(opts) when is_list(opts) do
    if Keyword.keyword?(opts),
      do: {:error, {:invalid_memory_backend_options, Keyword.keys(opts)}},
      else: {:error, :invalid_memory_backend_options}
  end

  def start_link(_opts), do: {:error, :invalid_memory_backend_options}

  @impl GenServer
  def init(state), do: {:ok, state}

  @impl Spectre.Ledger.Backend
  def load(%Config{} = config, %Ref{key: stream_key}) do
    with :ok <- valid_stream_key(stream_key),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(server, {:load, config.namespace, stream_key}, timeout)
    end
  end

  @impl Spectre.Ledger.Backend
  def compare_and_swap(%Config{} = config, %Write{} = write) do
    with :ok <- validate_write(write, config),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(server, {:compare_and_swap, config.namespace, write}, timeout)
    end
  end

  @impl Spectre.Ledger.Backend
  def head(%Config{} = config, stream_key) do
    with :ok <- valid_stream_key(stream_key),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(server, {:head, config.namespace, stream_key}, timeout)
    end
  end

  @impl Spectre.Ledger.Backend
  def entries(%Config{} = config, stream_key, opts) do
    with :ok <- valid_stream_key(stream_key),
         {:ok, query} <- entry_query(opts),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(server, {:entries, config.namespace, stream_key, query}, timeout)
    end
  end

  @impl Spectre.Ledger.Backend
  def objects(%Config{} = config, stream_key, opts) do
    with :ok <- valid_stream_key(stream_key),
         :ok <- no_object_options(opts),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(server, {:objects, config.namespace, stream_key}, timeout)
    end
  end

  @impl Spectre.Ledger.Backend
  def migrate(
        %Config{} = config,
        %Ref{} = legacy_ref,
        %Ref{} = stable_ref,
        legacy_checkpoint,
        %Write{} = write
      ) do
    with :ok <- valid_migration_refs(legacy_ref, stable_ref, write),
         :ok <- validate_write(write, config),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(
        server,
        {:migrate, config.namespace, legacy_ref.key, stable_ref.key, legacy_checkpoint, write},
        timeout
      )
    end
  end

  @impl Spectre.Ledger.Backend
  def put_stream(%Config{} = config, stream_key, entries, blobs) do
    with {:ok, stream} <- imported_stream(config, stream_key, entries, blobs),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(server, {:put_stream, config.namespace, stream_key, stream}, timeout)
    end
  end

  @impl Spectre.Ledger.Backend
  def append_receipt(%Config{} = config, %ReceiptWrite{} = write) do
    with :ok <- ReceiptWrite.validate(write, config),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(server, {:append_receipt, config.namespace, write}, timeout)
    end
  end

  @impl Spectre.Ledger.Backend
  def lookup_receipt(%Config{} = config, id) when is_binary(id) and id != "" do
    with {:ok, server, timeout} <- server(config) do
      GenServer.call(
        server,
        {:lookup_receipt, config.namespace, id, config.max_receipt_bytes},
        timeout
      )
    end
  end

  def lookup_receipt(%Config{}, _id), do: {:error, :invalid_ledger_receipt_id}

  @impl Spectre.Ledger.Backend
  def put_receipt_payload(%Config{} = config, %ReceiptWrite{} = write) do
    with :ok <- ReceiptWrite.validate(write, config),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(server, {:put_receipt_payload, config.namespace, write}, timeout)
    end
  end

  @impl Spectre.Ledger.Backend
  def get_receipt_payload(%Config{} = config, ref) when is_binary(ref) and ref != "" do
    with {:ok, server, timeout} <- server(config) do
      GenServer.call(
        server,
        {:get_receipt_payload, config.namespace, ref, config.max_receipt_bytes},
        timeout
      )
    end
  end

  def get_receipt_payload(%Config{}, _ref),
    do: {:error, :invalid_ledger_receipt_payload_ref}

  @impl Spectre.Ledger.Backend
  def receipt_entries(%Config{} = config, stream_key, opts) do
    with :ok <- valid_stream_key(stream_key),
         {:ok, query} <- receipt_entry_query(opts),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(server, {:receipt_entries, config.namespace, stream_key, query}, timeout)
    end
  end

  @impl Spectre.Ledger.Backend
  def receipt_objects(%Config{} = config, stream_key, opts) do
    with :ok <- valid_stream_key(stream_key),
         :ok <- no_object_options(opts),
         {:ok, server, timeout} <- server(config) do
      GenServer.call(
        server,
        {:receipt_objects, config.namespace, stream_key, config.max_receipt_bytes},
        timeout
      )
    end
  end

  @impl GenServer
  def handle_call({:load, namespace, stream_key}, _from, state) do
    space = namespace(state, namespace)

    reply =
      with {:ok, target_key} <- resolve_alias(space.aliases, stream_key),
           {:ok, stream} <- fetch_stream(space.streams, target_key) do
        {:ok, stream.checkpoint}
      else
        :not_found -> :not_found
        {:error, _reason} = error -> error
      end

    {:reply, reply, state}
  end

  def handle_call({:head, namespace, stream_key}, _from, state) do
    space = namespace(state, namespace)

    reply =
      case Map.fetch(space.streams, stream_key) do
        {:ok, %{entries: entries}} -> {:ok, List.last(entries)}
        :error -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call({:entries, namespace, stream_key, query}, _from, state) do
    space = namespace(state, namespace)

    entries =
      case Map.fetch(space.streams, stream_key) do
        {:ok, %{entries: entries}} -> select_entries(entries, query)
        :error -> []
      end

    {:reply, {:ok, entries}, state}
  end

  def handle_call({:objects, namespace, stream_key}, _from, state) do
    space = namespace(state, namespace)

    objects =
      case Map.fetch(space.streams, stream_key) do
        {:ok, %{blobs: blobs}} -> blobs
        :error -> %{}
      end

    {:reply, {:ok, objects}, state}
  end

  def handle_call({:compare_and_swap, namespace, write}, _from, state) do
    space = namespace(state, namespace)

    case append(space, write) do
      {:ok, receipt, next_space} ->
        {:reply, {:ok, receipt}, Map.put(state, namespace, next_space)}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call(
        {:migrate, namespace, legacy_key, stable_key, legacy_checkpoint, write},
        _from,
        state
      ) do
    space = namespace(state, namespace)

    case alias_migration(space, legacy_key, stable_key, legacy_checkpoint, write) do
      {:ok, status, next_space} ->
        {:reply, {:ok, status}, Map.put(state, namespace, next_space)}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:put_stream, namespace, stream_key, stream}, _from, state) do
    space = namespace(state, namespace)

    result =
      cond do
        Map.has_key?(space.aliases, stream_key) ->
          {:error, {:aliased_stream, Map.fetch!(space.aliases, stream_key)}}

        not Map.has_key?(space.streams, stream_key) ->
          {:ok, :imported, put_in(space, [:streams, stream_key], stream)}

        Map.fetch!(space.streams, stream_key) == stream ->
          {:ok, :idempotent, space}

        true ->
          {:error, {:stream_conflict, stream_key}}
      end

    case result do
      {:ok, status, next_space} ->
        {:reply, {:ok, status}, Map.put(state, namespace, next_space)}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:append_receipt, namespace, write}, _from, state) do
    space = namespace(state, namespace)

    case append_boundary_receipt(space, write) do
      {:ok, status, next_space} ->
        {:reply, {:ok, status}, Map.put(state, namespace, next_space)}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:lookup_receipt, namespace, id, max_bytes}, _from, state) do
    space = namespace(state, namespace)

    reply =
      case Map.fetch(space.receipt_index, id) do
        {:ok, entry} -> receipt_object(space, entry, max_bytes)
        :error -> :not_found
      end

    {:reply, reply, state}
  end

  def handle_call({:put_receipt_payload, namespace, write}, _from, state) do
    space = namespace(state, namespace)

    case put_receipt_object(space, write) do
      {:ok, next_space} ->
        {:reply, {:ok, write.payload_ref}, Map.put(state, namespace, next_space)}

      {:error, _reason} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:get_receipt_payload, namespace, ref, max_bytes}, _from, state) do
    space = namespace(state, namespace)
    {:reply, payload_object(space, ref, max_bytes), state}
  end

  def handle_call({:receipt_entries, namespace, stream_key, query}, _from, state) do
    entries =
      state
      |> namespace(namespace)
      |> Map.fetch!(:receipt_streams)
      |> Map.get(stream_key, [])
      |> select_receipt_entries(query)

    {:reply, {:ok, entries}, state}
  end

  def handle_call({:receipt_objects, namespace, stream_key, max_bytes}, _from, state) do
    space = namespace(state, namespace)
    entries = Map.get(space.receipt_streams, stream_key, [])
    {:reply, receipt_objects_for_entries(space, entries, max_bytes), state}
  end

  @spec append_boundary_receipt(namespace(), ReceiptWrite.t()) ::
          {:ok, :appended | :idempotent, namespace()} | {:error, term()}
  defp append_boundary_receipt(space, %ReceiptWrite{} = write) do
    with {:ok, space} <- put_receipt_object(space, write) do
      append_or_replay_boundary_receipt(space, write)
    end
  end

  defp append_or_replay_boundary_receipt(space, write) do
    case Map.fetch(space.receipt_index, write.receipt_id) do
      {:ok, entry} ->
        if ReceiptEntry.matches_write?(entry, write),
          do: {:ok, :idempotent, space},
          else: {:error, :ledger_receipt_id_conflict}

      :error ->
        append_new_boundary_receipt(space, write)
    end
  end

  defp append_new_boundary_receipt(space, write) do
    entries = Map.get(space.receipt_streams, write.stream_key, [])
    previous = List.last(entries)
    sequence = if previous, do: previous.sequence + 1, else: 1
    previous_digest = if previous, do: previous.entry_digest, else: nil

    with {:ok, entry} <- ReceiptEntry.new(write, sequence, previous_digest) do
      next_space = %{
        space
        | receipt_streams: Map.put(space.receipt_streams, write.stream_key, entries ++ [entry]),
          receipt_index: Map.put(space.receipt_index, write.receipt_id, entry)
      }

      {:ok, :appended, next_space}
    end
  end

  defp put_receipt_object(space, %ReceiptWrite{} = write) do
    object = %{
      envelope_digest: write.envelope_digest,
      encoded: write.encoded,
      byte_size: write.byte_size
    }

    case Map.fetch(space.receipt_payloads, write.payload_ref) do
      :error ->
        {:ok,
         %{space | receipt_payloads: Map.put(space.receipt_payloads, write.payload_ref, object)}}

      {:ok, ^object} ->
        {:ok, space}

      {:ok, _different} ->
        {:error, :ledger_receipt_payload_conflict}
    end
  end

  defp receipt_object(space, %ReceiptEntry{} = entry, max_bytes) do
    with :ok <- ReceiptEntry.verify(entry),
         {:ok, encoded} <- payload_object(space, entry.payload_ref, max_bytes),
         {:ok, envelope} <-
           ReceiptCodec.verify(encoded, entry.receipt_id, entry.envelope_digest, max_bytes),
         :ok <- ReceiptEntry.verify_envelope(entry, envelope) do
      {:ok, encoded}
    end
  end

  defp payload_object(space, ref, max_bytes) do
    case Map.fetch(space.receipt_payloads, ref) do
      {:ok, %{envelope_digest: digest, encoded: encoded, byte_size: byte_size}}
      when is_binary(encoded) and byte_size == byte_size(encoded) and byte_size <= max_bytes ->
        with {:ok, envelope} <- ReceiptCodec.decode(encoded, max_bytes),
             true <- Sink.payload_ref(envelope) == ref,
             true <- Envelope.digest(envelope) == digest do
          {:ok, encoded}
        else
          false -> {:error, :invalid_ledger_receipt_payload}
          {:error, _reason} = error -> error
        end

      {:ok, _invalid} ->
        {:error, :invalid_ledger_receipt_payload}

      :error ->
        :not_found
    end
  end

  defp receipt_objects_for_entries(space, entries, max_bytes) do
    Enum.reduce_while(entries, {:ok, %{}}, fn entry, {:ok, objects} ->
      case receipt_object(space, entry, max_bytes) do
        {:ok, encoded} ->
          {:cont, {:ok, Map.put(objects, entry.payload_ref, encoded)}}

        :not_found ->
          {:halt, {:error, :ledger_receipt_payload_not_found}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
  end

  @spec append(namespace(), Write.t()) ::
          {:ok, Receipt.t(), namespace()} | {:error, term()}
  defp append(space, %Write{} = write) do
    stream_key = write.ref.key

    case Map.fetch(space.aliases, stream_key) do
      {:ok, target_key} ->
        {:error, {:aliased_stream, target_key}}

      :error ->
        append_to_stream(space, stream_key, Map.get(space.streams, stream_key), write)
    end
  end

  @spec append_to_stream(namespace(), String.t(), stream() | nil, Write.t()) ::
          {:ok, Receipt.t(), namespace()} | {:error, term()}
  defp append_to_stream(space, stream_key, nil, %Write{expected_revision: 0} = write) do
    with {:ok, entry} <- Entry.new(write, nil) do
      stream = new_stream(entry, write.checkpoint)
      next_space = put_in(space, [:streams, stream_key], stream)
      {:ok, receipt(entry, :appended), next_space}
    end
  end

  defp append_to_stream(_space, _stream_key, nil, %Write{}),
    do: {:error, {:stale, 0}}

  defp append_to_stream(space, stream_key, stream, %Write{} = write) do
    head = List.last(stream.entries)

    cond do
      write.revision == head.revision and exact_retry?(stream, write) ->
        {:ok, receipt(head, :idempotent), space}

      write.revision == head.revision ->
        {:error, {:conflict, head.revision}}

      write.expected_revision != head.revision ->
        {:error, {:stale, head.revision}}

      true ->
        with {:ok, entry} <- Entry.new(write, head.entry_digest) do
          next_stream = append_entry(stream, entry, write.checkpoint)
          next_space = put_in(space, [:streams, stream_key], next_stream)
          {:ok, receipt(entry, :appended), next_space}
        end
    end
  end

  @spec exact_retry?(stream(), Write.t()) :: boolean()
  defp exact_retry?(stream, %Write{} = write) do
    previous_digest =
      case Enum.reverse(stream.entries) do
        [_head, previous | _rest] -> previous.entry_digest
        [_head] -> nil
      end

    case Entry.new(write, previous_digest) do
      {:ok, candidate} ->
        candidate.entry_digest == List.last(stream.entries).entry_digest and
          stream.checkpoint == write.checkpoint

      {:error, _reason} ->
        false
    end
  end

  @spec alias_migration(namespace(), String.t(), String.t(), binary() | map(), Write.t()) ::
          {:ok, :aliased, namespace()} | {:error, term()}
  defp alias_migration(space, legacy_key, stable_key, legacy_checkpoint, write) do
    with :ok <- available_alias(space, legacy_key, stable_key),
         {:ok, _source} <- migration_source(space, legacy_key, legacy_checkpoint, write),
         {:ok, target} <- migration_target(space, stable_key, write) do
      next_space =
        space
        |> put_in([:streams, stable_key], target)
        |> put_in([:aliases, legacy_key], stable_key)

      {:ok, :aliased, next_space}
    end
  end

  @spec available_alias(namespace(), String.t(), String.t()) :: :ok | {:error, term()}
  defp available_alias(space, legacy_key, stable_key) do
    cond do
      Map.get(space.aliases, legacy_key) == stable_key ->
        :ok

      Enum.any?(space.aliases, fn {_alias_key, target_key} -> target_key == legacy_key end) ->
        {:error, :ledger_migration_source_has_aliases}

      Map.has_key?(space.aliases, legacy_key) ->
        {:error, {:alias_conflict, legacy_key}}

      Map.has_key?(space.aliases, stable_key) ->
        {:error, {:migration_target_is_alias, stable_key}}

      true ->
        :ok
    end
  end

  @spec migration_source(namespace(), String.t(), binary() | map(), Write.t()) ::
          {:ok, stream()} | {:error, term()}
  defp migration_source(space, legacy_key, legacy_checkpoint, write) do
    with {:ok, source} <- fetch_stream(space.streams, legacy_key),
         true <- source.checkpoint == legacy_checkpoint,
         true <- List.last(source.entries).entry_digest == write.source_entry_digest do
      {:ok, source}
    else
      :not_found -> {:error, :migration_source_not_found}
      false -> {:error, :migration_source_mismatch}
    end
  end

  @spec migration_target(namespace(), String.t(), Write.t()) ::
          {:ok, stream()} | {:error, term()}
  defp migration_target(space, stable_key, write) do
    with {:ok, entry} <- Entry.new(write, nil) do
      candidate = new_stream(entry, write.checkpoint)

      case Map.fetch(space.streams, stable_key) do
        :error ->
          {:ok, candidate}

        {:ok, current} ->
          verify_migration_target(current, entry, write)
      end
    end
  end

  @spec verify_migration_target(stream(), Entry.t(), Write.t()) ::
          {:ok, stream()} | {:error, term()}
  defp verify_migration_target(current, entry, write) do
    if List.last(current.entries).entry_digest == entry.entry_digest and
         current.checkpoint == write.checkpoint do
      {:ok, current}
    else
      {:error, {:migration_target_conflict, List.last(current.entries).revision}}
    end
  end

  @spec imported_stream(Config.t(), term(), term(), term()) ::
          {:ok, stream()} | {:error, term()}
  defp imported_stream(config, stream_key, entries, blobs) do
    with :ok <- valid_stream_key(stream_key),
         true <- is_list(entries) and entries != [],
         true <- is_map(blobs),
         {:ok, %{stream_key: ^stream_key}} <- Chain.verify(entries),
         :ok <- exact_blob_set(entries, blobs),
         :ok <- verify_blobs(entries, blobs, config.max_checkpoint_bytes) do
      {:ok,
       %{
         entries: entries,
         blobs: blobs,
         checkpoint: Map.fetch!(blobs, List.last(entries).blob_digest)
       }}
    else
      false -> {:error, :invalid_imported_stream}
      {:ok, _report} -> {:error, :imported_stream_key_mismatch}
      {:error, _reason} = error -> error
    end
  end

  @spec exact_blob_set([Entry.t()], map()) :: :ok | {:error, term()}
  defp exact_blob_set(entries, blobs) do
    required = entries |> Enum.map(& &1.blob_digest) |> MapSet.new()
    supplied = blobs |> Map.keys() |> MapSet.new()

    if required == supplied and MapSet.size(required) == length(entries),
      do: :ok,
      else: {:error, :ledger_import_blob_set_mismatch}
  end

  @spec verify_blobs([Entry.t()], map(), pos_integer()) :: :ok | {:error, term()}
  defp verify_blobs(entries, blobs, max_bytes) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      blob = Map.fetch!(blobs, entry.blob_digest)

      result =
        with true <- is_binary(blob),
             true <- byte_size(blob) in 1..max_bytes,
             true <- sha256(blob) == entry.blob_digest,
             {:ok, report} <-
               Foundation.verify_instance_checkpoint(blob, entry.stream_key),
             true <- report.digest == entry.checkpoint_digest,
             true <- report.revision == entry.revision do
          :ok
        else
          false -> {:error, {:invalid_ledger_import_blob, entry.revision}}
          {:error, _reason} -> {:error, {:invalid_ledger_import_checkpoint, entry.revision}}
        end

      case result do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec validate_write(Write.t(), Config.t()) :: :ok | {:error, term()}
  defp validate_write(%Write{} = write, config) do
    with :ok <- valid_stream_key(write.ref.key),
         true <- is_binary(write.checkpoint),
         true <- write.byte_size == byte_size(write.checkpoint),
         true <- write.byte_size in 1..config.max_checkpoint_bytes,
         true <- sha256(write.checkpoint) == write.blob_digest,
         {:ok, report} <- Foundation.verify_instance_checkpoint(write.checkpoint, write.ref),
         true <- report.digest == write.checkpoint_digest,
         true <- report.revision == write.revision do
      :ok
    else
      false -> {:error, :invalid_ledger_checkpoint_write}
      {:error, _reason} -> {:error, :invalid_ledger_checkpoint_write}
    end
  end

  @spec valid_migration_refs(Ref.t(), Ref.t(), Write.t()) :: :ok | {:error, term()}
  defp valid_migration_refs(legacy_ref, stable_ref, write) do
    cond do
      legacy_ref.key == stable_ref.key -> {:error, :identical_migration_streams}
      write.ref.key != stable_ref.key -> {:error, :migration_target_mismatch}
      write.kind != :migration -> {:error, :invalid_migration_write}
      write.expected_revision != 0 -> {:error, :invalid_migration_expected_revision}
      true -> :ok
    end
  end

  @spec server(Config.t()) :: {:ok, pid(), timeout()} | {:error, term()}
  defp server(config) do
    with {:ok, server} <- Config.fetch_backend(config, :server),
         true <- is_pid(server) and Process.alive?(server),
         {:ok, timeout} <- call_timeout(Config.get_backend(config, :timeout, @default_timeout)) do
      {:ok, server, timeout}
    else
      :error -> {:error, :memory_server_required}
      false -> {:error, :invalid_memory_server}
      {:error, _reason} = error -> error
    end
  end

  @spec call_timeout(term()) :: {:ok, timeout()} | {:error, term()}
  defp call_timeout(:infinity), do: {:ok, :infinity}
  defp call_timeout(value) when is_integer(value) and value > 0, do: {:ok, value}
  defp call_timeout(_value), do: {:error, :invalid_memory_timeout}

  @spec entry_query(term()) :: {:ok, map()} | {:error, term()}
  defp entry_query(opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         {:ok, after_revision} <- after_revision(Keyword.get(opts, :after_revision, -1)),
         {:ok, limit} <- entry_limit(Keyword.get(opts, :limit, :infinity)) do
      {:ok, %{after_revision: after_revision, limit: limit}}
    else
      false -> {:error, :invalid_ledger_entry_query}
      {:error, _reason} = error -> error
    end
  end

  defp entry_query(_opts), do: {:error, :invalid_ledger_entry_query}

  defp after_revision(value) when is_integer(value) and value >= -1, do: {:ok, value}
  defp after_revision(_value), do: {:error, :invalid_ledger_after_revision}

  defp entry_limit(:infinity), do: {:ok, :infinity}
  defp entry_limit(value) when is_integer(value) and value > 0, do: {:ok, value}
  defp entry_limit(_value), do: {:error, :invalid_ledger_entry_limit}

  @spec receipt_entry_query(term()) :: {:ok, map()} | {:error, term()}
  defp receipt_entry_query(opts) when is_list(opts) do
    with true <- Keyword.keyword?(opts),
         true <- length(Keyword.keys(opts)) == length(Enum.uniq(Keyword.keys(opts))),
         true <- Enum.all?(Keyword.keys(opts), &(&1 in [:after_sequence, :limit])),
         {:ok, after_sequence} <-
           after_sequence(Keyword.get(opts, :after_sequence, 0)),
         {:ok, limit} <- entry_limit(Keyword.get(opts, :limit, :infinity)) do
      {:ok, %{after_sequence: after_sequence, limit: limit}}
    else
      false -> {:error, :invalid_ledger_receipt_entry_query}
      {:error, _reason} = error -> error
    end
  end

  defp receipt_entry_query(_opts), do: {:error, :invalid_ledger_receipt_entry_query}

  defp after_sequence(value) when is_integer(value) and value >= 0, do: {:ok, value}
  defp after_sequence(_value), do: {:error, :invalid_ledger_receipt_after_sequence}

  defp no_object_options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) and opts == [],
      do: :ok,
      else: {:error, :invalid_ledger_object_query}
  end

  defp no_object_options(_opts), do: {:error, :invalid_ledger_object_query}

  @spec select_entries([Entry.t()], map()) :: [Entry.t()]
  defp select_entries(entries, %{after_revision: after_revision, limit: limit}) do
    selected = Enum.drop_while(entries, &(&1.revision <= after_revision))
    if limit == :infinity, do: selected, else: Enum.take(selected, limit)
  end

  @spec select_receipt_entries([ReceiptEntry.t()], map()) :: [ReceiptEntry.t()]
  defp select_receipt_entries(entries, %{after_sequence: after_sequence, limit: limit}) do
    selected = Enum.drop_while(entries, &(&1.sequence <= after_sequence))
    if limit == :infinity, do: selected, else: Enum.take(selected, limit)
  end

  @spec resolve_alias(map(), String.t()) :: {:ok, String.t()} | {:error, term()}
  defp resolve_alias(aliases, stream_key), do: resolve_alias(aliases, stream_key, %{})

  defp resolve_alias(aliases, stream_key, seen) do
    cond do
      Map.has_key?(seen, stream_key) ->
        {:error, :ledger_alias_cycle}

      target = Map.get(aliases, stream_key) ->
        resolve_alias(aliases, target, Map.put(seen, stream_key, true))

      true ->
        {:ok, stream_key}
    end
  end

  @spec fetch_stream(map(), String.t()) :: {:ok, stream()} | :not_found
  defp fetch_stream(streams, stream_key) do
    case Map.fetch(streams, stream_key) do
      {:ok, stream} -> {:ok, stream}
      :error -> :not_found
    end
  end

  @spec namespace(state(), String.t()) :: namespace()
  defp namespace(state, namespace) do
    Map.get(state, namespace, %{
      streams: %{},
      aliases: %{},
      receipt_streams: %{},
      receipt_index: %{},
      receipt_payloads: %{}
    })
  end

  @spec new_stream(Entry.t(), binary()) :: stream()
  defp new_stream(entry, checkpoint) do
    %{entries: [entry], blobs: %{entry.blob_digest => checkpoint}, checkpoint: checkpoint}
  end

  @spec append_entry(stream(), Entry.t(), binary()) :: stream()
  defp append_entry(stream, entry, checkpoint) do
    %{
      entries: stream.entries ++ [entry],
      blobs: Map.put(stream.blobs, entry.blob_digest, checkpoint),
      checkpoint: checkpoint
    }
  end

  @spec receipt(Entry.t(), :appended | :idempotent) :: Receipt.t()
  defp receipt(entry, status) do
    %Receipt{
      stream_key: entry.stream_key,
      revision: entry.revision,
      checkpoint_digest: entry.checkpoint_digest,
      entry_digest: entry.entry_digest,
      status: status
    }
  end

  @spec valid_stream_key(term()) :: :ok | {:error, term()}
  defp valid_stream_key(value) when is_binary(value) and value != "", do: :ok
  defp valid_stream_key(_value), do: {:error, :invalid_ledger_stream_key}

  @spec sha256(binary()) :: String.t()
  defp sha256(bytes),
    do: bytes |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
end
