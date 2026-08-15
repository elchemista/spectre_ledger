defmodule Spectre.Ledger.Backend.Postgres do
  @moduledoc """
  PostgreSQL backend for Spectre Ledger checkpoints and boundary receipts.

  The adapter uses a host-owned Ecto Repo and parameterized SQL. One locked
  stream row serializes each checkpoint or receipt head advancement; immutable
  objects and entries are inserted in the same database transaction. The two
  chains have independent heads. Ledger never starts, supervises, or configures
  the host Repo.

  Runtime options include:

    * `:repo` - required running Ecto Repo module;
    * `:schema` - validated PostgreSQL schema, default `"public"`;
    * `:table_prefix` - validated table prefix, default `"spectre_ledger"`;
    * `:query_opts` and `:transaction_opts` - optional Ecto SQL options.

  Write transactions fail closed unless PostgreSQL reports `READ COMMITTED`.
  This keeps alias migration, checkpoint CAS, and receipt idempotency within one
  visibility model; hosts must not override Ledger transactions to
  repeatable-read or serializable.

  Install the SQL schema with `mix spectre_ledger.gen.migration` before use.
  """

  @behaviour Spectre.Ledger.Backend

  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Backend.Postgres.Migration
  alias Spectre.Ledger.Backend.Postgres.Names
  alias Spectre.Ledger.Backend.Postgres.Runtime
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

  @entry_columns """
  "stream_key", "kind", "expected_revision", "revision",
  "checkpoint_digest", "blob_digest", "previous_entry_digest",
  "source_entry_digest", "owner_fencing_token", "entry_digest"
  """

  @qualified_entry_columns """
  e."stream_key", e."kind", e."expected_revision", e."revision",
  e."checkpoint_digest", e."blob_digest", e."previous_entry_digest",
  e."source_entry_digest", e."owner_fencing_token", e."entry_digest"
  """

  @receipt_entry_columns """
  "stream_key", "sequence", "receipt_id", "kind", "canonical_revision",
  "envelope_digest", "payload_ref", "previous_entry_digest", "recorded_at",
  "owner_fencing_token", "entry_digest"
  """

  @qualified_receipt_entry_columns """
  r."stream_key", r."sequence", r."receipt_id", r."kind", r."canonical_revision",
  r."envelope_digest", r."payload_ref", r."previous_entry_digest", r."recorded_at",
  r."owner_fencing_token", r."entry_digest"
  """

  @expected_columns %{
    aliases: ~w(namespace alias_stream_key target_stream_key inserted_at),
    blobs: ~w(namespace blob_digest checkpoint_digest checkpoint byte_size inserted_at),
    entries:
      ~w(namespace stream_key revision expected_revision kind checkpoint_digest blob_digest previous_entry_digest source_entry_digest owner_fencing_token entry_digest inserted_at),
    meta: ~w(key value updated_at),
    receipt_entries:
      ~w(namespace stream_key sequence receipt_id kind canonical_revision envelope_digest payload_ref previous_entry_digest recorded_at owner_fencing_token entry_digest inserted_at),
    receipt_payloads: ~w(namespace payload_ref envelope_digest envelope byte_size inserted_at),
    receipt_streams:
      ~w(namespace stream_key head_sequence head_entry_digest inserted_at updated_at),
    streams:
      ~w(namespace stream_key head_revision head_entry_digest head_blob_digest inserted_at updated_at)
  }

  @impl Spectre.Ledger.Backend
  def load(%Config{} = config, %Ref{key: stream_key}) do
    with {:ok, runtime} <- Runtime.from_config(config),
         {:ok, stream_key} <- resolve_key(runtime, config.namespace, stream_key),
         {:ok, result} <-
           Runtime.read(
             runtime,
             """
             SELECT #{@qualified_entry_columns}, b."checkpoint",
                    b."checkpoint_digest", b."byte_size"
             FROM #{Names.qualified(runtime.names, :streams)} AS s
             LEFT JOIN #{Names.qualified(runtime.names, :entries)} AS e
               ON e."namespace" = s."namespace"
              AND e."stream_key" = s."stream_key"
              AND e."revision" = s."head_revision"
              AND e."entry_digest" = s."head_entry_digest"
             LEFT JOIN #{Names.qualified(runtime.names, :blobs)} AS b
               ON b."namespace" = s."namespace"
              AND b."blob_digest" = s."head_blob_digest"
             WHERE s."namespace" = $1 AND s."stream_key" = $2
             """,
             [config.namespace, stream_key]
           ) do
      case result.rows do
        [row] -> decode_loaded_checkpoint(row, stream_key)
        [] -> :not_found
        _rows -> {:error, :invalid_ledger_postgres_load_result}
      end
    end
  end

  @impl Spectre.Ledger.Backend
  def compare_and_swap(%Config{} = config, %Write{} = write) do
    with :ok <- validate_write(write, config),
         {:ok, runtime} <- Runtime.from_config(config) do
      Runtime.transaction(runtime, fn -> compare_and_swap!(runtime, config.namespace, write) end)
    end
  end

  @impl Spectre.Ledger.Backend
  def head(%Config{} = config, stream_key) when is_binary(stream_key) and stream_key != "" do
    with {:ok, runtime} <- Runtime.from_config(config),
         {:ok, result} <-
           Runtime.read(
             runtime,
             """
             SELECT #{@entry_columns}
             FROM #{Names.qualified(runtime.names, :entries)} AS e
             WHERE e."namespace" = $1 AND e."stream_key" = $2
             ORDER BY e."revision" DESC
             LIMIT 1
             """,
             [config.namespace, stream_key]
           ) do
      case result.rows do
        [row] -> decode_entry(row)
        [] -> :not_found
        _rows -> {:error, :invalid_ledger_postgres_head_result}
      end
    end
  end

  def head(%Config{}, _stream_key), do: {:error, :invalid_ledger_stream_key}

  @impl Spectre.Ledger.Backend
  def entries(%Config{} = config, stream_key, opts)
      when is_binary(stream_key) and stream_key != "" and is_list(opts) do
    with {:ok, runtime} <- Runtime.from_config(config),
         {:ok, after_revision, limit} <- list_options(opts),
         {:ok, result} <-
           Runtime.read(
             runtime,
             entries_sql(runtime, limit),
             entries_params(config.namespace, stream_key, after_revision, limit)
           ) do
      decode_entries(result.rows)
    end
  end

  def entries(%Config{}, _stream_key, _opts), do: {:error, :invalid_ledger_entries_options}

  @doc "Returns the immutable checkpoint objects referenced by one stream."
  @spec objects(Config.t(), String.t(), keyword()) ::
          {:ok, %{optional(String.t()) => binary()}} | {:error, term()}
  @impl Spectre.Ledger.Backend
  def objects(config, stream_key, opts \\ [])

  def objects(%Config{} = config, stream_key, opts)
      when is_binary(stream_key) and stream_key != "" and is_list(opts) do
    with true <- Keyword.keyword?(opts) and opts == [],
         {:ok, runtime} <- Runtime.from_config(config),
         {:ok, result} <-
           Runtime.read(
             runtime,
             """
             SELECT DISTINCT b."blob_digest", b."checkpoint"
             FROM #{Names.qualified(runtime.names, :entries)} AS e
             JOIN #{Names.qualified(runtime.names, :blobs)} AS b
               ON b."namespace" = e."namespace"
              AND b."blob_digest" = e."blob_digest"
             WHERE e."namespace" = $1 AND e."stream_key" = $2
             ORDER BY b."blob_digest" ASC
             """,
             [config.namespace, stream_key]
           ) do
      {:ok, Map.new(result.rows, fn [digest, checkpoint] -> {digest, checkpoint} end)}
    else
      false -> {:error, :invalid_ledger_objects_options}
      {:error, _reason} = error -> error
    end
  end

  def objects(%Config{}, _stream_key, _opts), do: {:error, :invalid_ledger_objects_options}

  @impl Spectre.Ledger.Backend
  def migrate(
        %Config{} = config,
        %Ref{key: legacy_key},
        %Ref{key: stable_key},
        legacy_checkpoint,
        %Write{ref: %Ref{key: stable_key}} = write
      )
      when legacy_key != stable_key and
             (is_binary(legacy_checkpoint) or is_map(legacy_checkpoint)) do
    with :ok <- validate_migration_write(write, config),
         {:ok, runtime} <- Runtime.from_config(config) do
      Runtime.transaction(runtime, fn ->
        migrate_locked!(
          runtime,
          config.namespace,
          legacy_key,
          stable_key,
          legacy_checkpoint,
          write
        )
      end)
    end
  end

  def migrate(%Config{}, _legacy_ref, _stable_ref, _legacy_checkpoint, _write),
    do: {:error, :invalid_ledger_migration}

  @impl Spectre.Ledger.Backend
  def put_stream(%Config{} = config, stream_key, entries, blobs)
      when is_binary(stream_key) and stream_key != "" and is_list(entries) and is_map(blobs) do
    with {:ok, runtime} <- Runtime.from_config(config),
         {:ok, chain} <- Chain.verify(entries),
         :ok <- import_stream_key(chain.stream_key, stream_key),
         :ok <- validate_import(config, entries, blobs) do
      Runtime.transaction(runtime, fn ->
        put_stream_locked!(runtime, config.namespace, stream_key, chain, entries, blobs)
      end)
    end
  end

  def put_stream(%Config{}, _stream_key, _entries, _blobs),
    do: {:error, :invalid_ledger_import}

  @impl Spectre.Ledger.Backend
  def append_receipt(%Config{} = config, %ReceiptWrite{} = write) do
    with :ok <- ReceiptWrite.validate(write, config),
         {:ok, runtime} <- Runtime.from_config(config) do
      Runtime.transaction(runtime, fn ->
        append_receipt!(runtime, config.namespace, write, config.max_receipt_bytes)
      end)
    end
  end

  @impl Spectre.Ledger.Backend
  def lookup_receipt(%Config{} = config, receipt_id)
      when is_binary(receipt_id) and receipt_id != "" do
    with {:ok, runtime} <- Runtime.from_config(config),
         {:ok, result} <-
           Runtime.read(
             runtime,
             """
             SELECT #{@qualified_receipt_entry_columns},
                    p."envelope_digest", p."envelope", p."byte_size"
             FROM #{Names.qualified(runtime.names, :receipt_entries)} AS r
             JOIN #{Names.qualified(runtime.names, :receipt_payloads)} AS p
               ON p."namespace" = r."namespace"
              AND p."payload_ref" = r."payload_ref"
             WHERE r."namespace" = $1 AND r."receipt_id" = $2
             """,
             [config.namespace, receipt_id]
           ) do
      decode_receipt_lookup(result.rows, receipt_id, config.max_receipt_bytes)
    end
  end

  def lookup_receipt(%Config{}, _receipt_id), do: {:error, :invalid_ledger_receipt_id}

  @impl Spectre.Ledger.Backend
  def put_receipt_payload(%Config{} = config, %ReceiptWrite{} = write) do
    with :ok <- ReceiptWrite.validate(write, config),
         {:ok, runtime} <- Runtime.from_config(config) do
      Runtime.transaction(runtime, fn ->
        insert_receipt_payload!(runtime, config.namespace, write, config.max_receipt_bytes)
        write.payload_ref
      end)
    end
  end

  @impl Spectre.Ledger.Backend
  def get_receipt_payload(%Config{} = config, payload_ref)
      when is_binary(payload_ref) and payload_ref != "" do
    with {:ok, runtime} <- Runtime.from_config(config),
         {:ok, result} <-
           Runtime.read(
             runtime,
             """
             SELECT "envelope_digest", "envelope", "byte_size"
             FROM #{Names.qualified(runtime.names, :receipt_payloads)}
             WHERE "namespace" = $1 AND "payload_ref" = $2
             """,
             [config.namespace, payload_ref]
           ) do
      decode_receipt_payload(result.rows, payload_ref, config.max_receipt_bytes)
    end
  end

  def get_receipt_payload(%Config{}, _payload_ref),
    do: {:error, :invalid_ledger_receipt_payload_ref}

  @impl Spectre.Ledger.Backend
  def receipt_entries(%Config{} = config, stream_key, opts)
      when is_binary(stream_key) and stream_key != "" and is_list(opts) do
    with {:ok, runtime} <- Runtime.from_config(config),
         {:ok, after_sequence, limit} <- receipt_list_options(opts),
         {:ok, result} <-
           Runtime.read(
             runtime,
             receipt_entries_sql(runtime, limit),
             receipt_entries_params(config.namespace, stream_key, after_sequence, limit)
           ) do
      decode_receipt_entries(result.rows)
    end
  end

  def receipt_entries(%Config{}, _stream_key, _opts),
    do: {:error, :invalid_ledger_receipt_entries_options}

  @impl Spectre.Ledger.Backend
  def receipt_objects(%Config{} = config, stream_key, opts)
      when is_binary(stream_key) and stream_key != "" and is_list(opts) do
    with true <- Keyword.keyword?(opts) and opts == [],
         {:ok, runtime} <- Runtime.from_config(config),
         {:ok, result} <-
           Runtime.read(
             runtime,
             """
             SELECT DISTINCT p."payload_ref", p."envelope_digest",
                             p."envelope", p."byte_size"
             FROM #{Names.qualified(runtime.names, :receipt_entries)} AS r
             JOIN #{Names.qualified(runtime.names, :receipt_payloads)} AS p
               ON p."namespace" = r."namespace"
              AND p."payload_ref" = r."payload_ref"
             WHERE r."namespace" = $1 AND r."stream_key" = $2
             ORDER BY p."payload_ref" ASC
             """,
             [config.namespace, stream_key]
           ),
         {:ok, objects} <- decode_receipt_objects(result.rows, config.max_receipt_bytes) do
      {:ok, objects}
    else
      false -> {:error, :invalid_ledger_receipt_objects_options}
      {:error, _reason} = error -> error
    end
  end

  def receipt_objects(%Config{}, _stream_key, _opts),
    do: {:error, :invalid_ledger_receipt_objects_options}

  @doc "Performs a read-only check of the configured Ledger SQL schema."
  @spec doctor(Config.t()) :: {:ok, map()} | {:error, term()}
  def doctor(%Config{} = config) do
    with {:ok, runtime} <- Runtime.from_config(config),
         {:ok, table_result} <-
           Runtime.read(
             runtime,
             """
             SELECT "table_name"
             FROM information_schema.tables
             WHERE "table_schema" = $1 AND "table_name" = ANY($2::text[])
             """,
             [runtime.names.schema, Map.values(runtime.names.tables)]
           ),
         tables = doctor_tables(runtime.names, table_result.rows),
         {:ok, column_result} <-
           Runtime.read(
             runtime,
             """
             SELECT "table_name", "column_name"
             FROM information_schema.columns
             WHERE "table_schema" = $1 AND "table_name" = ANY($2::text[])
             """,
             [runtime.names.schema, Map.values(runtime.names.tables)]
           ),
         columns = doctor_columns(runtime.names, column_result.rows),
         {:ok, schema_version} <- doctor_schema_version(runtime, tables.meta) do
      {:ok,
       %{
         ready?:
           Enum.all?(tables, fn {_name, present?} -> present? end) and
             Enum.all?(columns, fn {_name, valid?} -> valid? end) and
             schema_version == Migration.schema_version(),
         schema: runtime.names.schema,
         table_prefix: runtime.names.table_prefix,
         tables: tables,
         columns: columns,
         schema_version: schema_version,
         expected_schema_version: Migration.schema_version()
       }}
    end
  end

  def doctor(_config), do: {:error, :invalid_ledger_config}

  defp append_receipt!(runtime, namespace, write, max_bytes) do
    insert_receipt_payload!(runtime, namespace, write, max_bytes)
    state = ensure_and_lock_receipt_stream!(runtime, namespace, write.stream_key)

    case existing_receipt_entry!(runtime, namespace, write.receipt_id) do
      %ReceiptEntry{} = existing ->
        replay_receipt_or_conflict!(runtime, namespace, existing, write, max_bytes)

      nil ->
        append_new_receipt!(runtime, namespace, state, write, max_bytes)
    end
  end

  defp replay_receipt_or_conflict!(runtime, namespace, existing, write, max_bytes) do
    if ReceiptEntry.matches_write?(existing, write) do
      verify_receipt_payload!(
        runtime,
        namespace,
        write.payload_ref,
        write.envelope_digest,
        max_bytes
      )

      :idempotent
    else
      Runtime.rollback(runtime, :ledger_receipt_id_conflict)
    end
  end

  defp append_new_receipt!(runtime, namespace, state, write, max_bytes) do
    {:ok, entry} = ReceiptEntry.new(write, state.sequence + 1, state.entry_digest)

    case insert_receipt_entry!(runtime, namespace, entry) do
      :inserted ->
        advance_receipt_head!(runtime, namespace, state, entry)
        :appended

      :conflict ->
        case existing_receipt_entry!(runtime, namespace, write.receipt_id) do
          %ReceiptEntry{} = existing ->
            replay_receipt_or_conflict!(runtime, namespace, existing, write, max_bytes)

          nil ->
            Runtime.rollback(runtime, :ledger_receipt_entry_conflict)
        end
    end
  end

  defp insert_receipt_payload!(runtime, namespace, write, max_bytes) do
    Runtime.query!(
      runtime,
      """
      INSERT INTO #{Names.qualified(runtime.names, :receipt_payloads)}
        ("namespace", "payload_ref", "envelope_digest", "envelope", "byte_size")
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT ("namespace", "payload_ref") DO NOTHING
      """,
      [namespace, write.payload_ref, write.envelope_digest, write.encoded, write.byte_size]
    )

    verify_receipt_payload!(
      runtime,
      namespace,
      write.payload_ref,
      write.envelope_digest,
      max_bytes
    )
  end

  defp verify_receipt_payload!(runtime, namespace, payload_ref, envelope_digest, max_bytes) do
    result =
      Runtime.query!(
        runtime,
        """
        SELECT "envelope_digest", "envelope", "byte_size"
        FROM #{Names.qualified(runtime.names, :receipt_payloads)}
        WHERE "namespace" = $1 AND "payload_ref" = $2
        """,
        [namespace, payload_ref]
      )

    case result.rows do
      [[^envelope_digest, _encoded, _byte_size]] ->
        case decode_receipt_payload(result.rows, payload_ref, max_bytes) do
          {:ok, _encoded} -> :ok
          {:error, reason} -> Runtime.rollback(runtime, reason)
        end

      [[_different_digest, _encoded, _byte_size]] ->
        Runtime.rollback(runtime, :ledger_receipt_payload_digest_collision)

      [] ->
        Runtime.rollback(runtime, :ledger_receipt_payload_not_found)

      _rows ->
        Runtime.rollback(runtime, :invalid_ledger_postgres_receipt_payload_result)
    end
  end

  defp ensure_and_lock_receipt_stream!(runtime, namespace, stream_key) do
    Runtime.query!(
      runtime,
      """
      INSERT INTO #{Names.qualified(runtime.names, :receipt_streams)}
        ("namespace", "stream_key")
      VALUES ($1, $2)
      ON CONFLICT ("namespace", "stream_key") DO NOTHING
      """,
      [namespace, stream_key]
    )

    result =
      Runtime.query!(
        runtime,
        """
        SELECT "head_sequence", "head_entry_digest"
        FROM #{Names.qualified(runtime.names, :receipt_streams)}
        WHERE "namespace" = $1 AND "stream_key" = $2
        FOR UPDATE
        """,
        [namespace, stream_key]
      )

    case result.rows do
      [[sequence, entry_digest]] -> %{sequence: sequence, entry_digest: entry_digest}
      [] -> Runtime.rollback(runtime, :ledger_receipt_stream_not_found)
      _rows -> Runtime.rollback(runtime, :invalid_ledger_postgres_receipt_stream)
    end
  end

  defp existing_receipt_entry!(runtime, namespace, receipt_id) do
    result =
      Runtime.query!(
        runtime,
        """
        SELECT #{@receipt_entry_columns}
        FROM #{Names.qualified(runtime.names, :receipt_entries)} AS r
        WHERE r."namespace" = $1 AND r."receipt_id" = $2
        """,
        [namespace, receipt_id]
      )

    case result.rows do
      [row] -> decode_receipt_entry!(runtime, row)
      [] -> nil
      _rows -> Runtime.rollback(runtime, :invalid_ledger_postgres_receipt_entry)
    end
  end

  defp insert_receipt_entry!(runtime, namespace, entry) do
    result =
      Runtime.query!(
        runtime,
        """
        INSERT INTO #{Names.qualified(runtime.names, :receipt_entries)}
          ("namespace", "stream_key", "sequence", "receipt_id", "kind",
           "canonical_revision", "envelope_digest", "payload_ref",
           "previous_entry_digest", "recorded_at", "owner_fencing_token", "entry_digest")
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
        ON CONFLICT DO NOTHING
        """,
        [
          namespace,
          entry.stream_key,
          entry.sequence,
          entry.receipt_id,
          Atom.to_string(entry.kind),
          entry.canonical_revision,
          entry.envelope_digest,
          entry.payload_ref,
          entry.previous_entry_digest,
          entry.recorded_at,
          entry.owner_fencing_token,
          entry.entry_digest
        ]
      )

    case result.num_rows do
      1 -> :inserted
      0 -> :conflict
      _count -> Runtime.rollback(runtime, :invalid_ledger_postgres_receipt_insert)
    end
  end

  defp advance_receipt_head!(runtime, namespace, state, entry) do
    result =
      Runtime.query!(
        runtime,
        """
        UPDATE #{Names.qualified(runtime.names, :receipt_streams)}
        SET "head_sequence" = $3, "head_entry_digest" = $4,
            "updated_at" = transaction_timestamp()
        WHERE "namespace" = $1 AND "stream_key" = $2
          AND "head_sequence" = $5
          AND "head_entry_digest" IS NOT DISTINCT FROM $6
        """,
        [
          namespace,
          entry.stream_key,
          entry.sequence,
          entry.entry_digest,
          state.sequence,
          state.entry_digest
        ]
      )

    if result.num_rows != 1,
      do: Runtime.rollback(runtime, :ledger_receipt_head_conflict)
  end

  defp compare_and_swap!(runtime, namespace, write) do
    state = ensure_and_lock_stream!(runtime, namespace, write.ref.key)
    reject_alias!(runtime, namespace, write.ref.key)

    case existing_entry!(runtime, namespace, write.ref.key, write.revision) do
      %Entry{} = existing -> replay_or_conflict!(runtime, namespace, state, existing, write)
      nil -> append_checkpoint!(runtime, namespace, state, write)
    end
  end

  defp replay_or_conflict!(runtime, namespace, state, existing, write) do
    if state.revision == write.revision and exact_write?(existing, write) do
      verify_blob!(runtime, namespace, write.blob_digest, write)
      receipt(existing, :idempotent)
    else
      Runtime.rollback(runtime, {:conflict, :revision_digest_mismatch})
    end
  end

  defp migrate_locked!(runtime, namespace, legacy_key, stable_key, checkpoint, write) do
    ensure_streams!(runtime, namespace, [legacy_key, stable_key])
    lock_streams!(runtime, namespace, [legacy_key, stable_key])

    legacy_state = stream_state!(runtime, namespace, legacy_key)
    stable_state = stream_state!(runtime, namespace, stable_key)
    reject_alias_target!(runtime, namespace, legacy_key)

    migrate_alias!(
      runtime,
      namespace,
      legacy_key,
      stable_key,
      legacy_state,
      stable_state,
      checkpoint,
      write
    )
  end

  defp migrate_alias!(
         runtime,
         namespace,
         legacy_key,
         stable_key,
         legacy_state,
         stable_state,
         checkpoint,
         write
       ) do
    case alias_target!(runtime, namespace, legacy_key) do
      ^stable_key ->
        verify_legacy_source!(
          runtime,
          namespace,
          legacy_state,
          checkpoint,
          write.source_entry_digest
        )

        ensure_migration_target!(runtime, namespace, stable_state, stable_key, write)
        :aliased

      nil ->
        reject_alias!(runtime, namespace, stable_key)

        verify_legacy_source!(
          runtime,
          namespace,
          legacy_state,
          checkpoint,
          write.source_entry_digest
        )

        ensure_or_append_migration!(runtime, namespace, stable_state, write)
        insert_alias!(runtime, namespace, legacy_key, stable_key)
        :aliased

      _other_target ->
        Runtime.rollback(runtime, :ledger_alias_conflict)
    end
  end

  defp put_stream_locked!(runtime, namespace, stream_key, chain, entries, blobs) do
    state = ensure_and_lock_stream!(runtime, namespace, stream_key)
    reject_alias!(runtime, namespace, stream_key)

    case state do
      %{entry_digest: nil, revision: 0} ->
        import_entries!(runtime, namespace, entries, blobs)
        set_import_head!(runtime, namespace, stream_key, List.last(entries))
        :imported

      %{entry_digest: digest, revision: revision}
      when digest == chain.head_entry_digest and revision == chain.head_revision ->
        verify_imported_entries!(runtime, namespace, stream_key, entries)
        :idempotent

      %{revision: revision} ->
        Runtime.rollback(runtime, {:ledger_import_conflict, revision})
    end
  end

  defp append_checkpoint!(runtime, namespace, state, write) do
    if state.revision != write.expected_revision do
      Runtime.rollback(runtime, {:stale, state.revision})
    end

    {:ok, entry} = Entry.new(write, state.entry_digest)
    insert_blob!(runtime, namespace, entry, write.checkpoint)
    insert_entry!(runtime, namespace, entry)
    advance_head!(runtime, namespace, state, entry)
    receipt(entry, :appended)
  end

  defp ensure_or_append_migration!(runtime, namespace, state, write) do
    case existing_entry!(runtime, namespace, write.ref.key, write.revision) do
      %Entry{} = existing ->
        if current_entry?(state, existing) and exact_write?(existing, write) do
          verify_blob!(runtime, namespace, write.blob_digest, write)
        else
          Runtime.rollback(runtime, {:ledger_migration_target_conflict, existing.revision})
        end

      nil when state.revision == 0 and is_nil(state.entry_digest) ->
        {:ok, entry} = Entry.new(write, nil)
        insert_blob!(runtime, namespace, entry, write.checkpoint)
        insert_entry!(runtime, namespace, entry)
        advance_head!(runtime, namespace, state, entry)

      nil ->
        Runtime.rollback(runtime, {:ledger_migration_target_conflict, state.revision})
    end
  end

  defp ensure_migration_target!(runtime, namespace, state, stable_key, write) do
    case existing_entry!(runtime, namespace, stable_key, write.revision) do
      %Entry{} = entry ->
        if current_entry?(state, entry) and exact_write?(entry, write) do
          verify_blob!(runtime, namespace, write.blob_digest, write)
        else
          Runtime.rollback(runtime, {:ledger_migration_target_conflict, entry.revision})
        end

      nil ->
        Runtime.rollback(runtime, {:ledger_migration_target_conflict, state.revision})
    end
  end

  defp verify_legacy_source!(runtime, namespace, state, legacy_checkpoint, source_digest) do
    cond do
      is_nil(state.entry_digest) ->
        Runtime.rollback(runtime, :ledger_migration_source_not_found)

      state.entry_digest != source_digest ->
        Runtime.rollback(runtime, :ledger_migration_source_changed)

      true ->
        checkpoint = checkpoint_for_blob!(runtime, namespace, state.blob_digest)

        unless same_checkpoint?(checkpoint, legacy_checkpoint) do
          Runtime.rollback(runtime, :ledger_migration_source_mismatch)
        end
    end
  end

  defp same_checkpoint?(checkpoint, checkpoint) when is_binary(checkpoint), do: true

  defp same_checkpoint?(checkpoint, legacy_checkpoint) when is_map(legacy_checkpoint) do
    with {:ok, current} <- Foundation.verify_instance_checkpoint(checkpoint),
         {:ok, legacy} <- Foundation.verify_instance_checkpoint(legacy_checkpoint) do
      current.digest == legacy.digest
    else
      _error -> false
    end
  end

  defp same_checkpoint?(_checkpoint, _legacy_checkpoint), do: false

  defp import_stream_key(stream_key, stream_key), do: :ok
  defp import_stream_key(_actual, _expected), do: {:error, :ledger_import_stream_mismatch}

  defp validate_import(config, entries, blobs) do
    required = entries |> Enum.map(& &1.blob_digest) |> MapSet.new()
    supplied = blobs |> Map.keys() |> MapSet.new()

    with true <- required == supplied and MapSet.size(required) == length(entries),
         :ok <- validate_blob_map(config, blobs) do
      validate_import_entries(entries, blobs)
    else
      false -> {:error, :ledger_import_blob_set_mismatch}
      {:error, _reason} = error -> error
    end
  end

  defp validate_import_entries(entries, blobs) do
    Enum.reduce_while(entries, :ok, fn entry, :ok ->
      case validate_import_entry(entry, blobs) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp validate_import_entry(entry, blobs) do
    case Map.fetch(blobs, entry.blob_digest) do
      {:ok, checkpoint} -> validate_import_checkpoint(entry, checkpoint)
      :error -> {:error, {:ledger_import_blob_missing, entry.blob_digest}}
    end
  end

  defp validate_blob_map(config, blobs) do
    Enum.reduce_while(blobs, :ok, fn
      {digest, checkpoint}, :ok when is_binary(digest) and is_binary(checkpoint) ->
        cond do
          byte_size(checkpoint) == 0 ->
            {:halt, {:error, :empty_ledger_checkpoint}}

          byte_size(checkpoint) > config.max_checkpoint_bytes ->
            {:halt,
             {:error,
              {:ledger_checkpoint_too_large, byte_size(checkpoint), config.max_checkpoint_bytes}}}

          sha256(checkpoint) != digest ->
            {:halt, {:error, {:ledger_import_blob_digest_mismatch, digest}}}

          true ->
            {:cont, :ok}
        end

      _invalid, :ok ->
        {:halt, {:error, :invalid_ledger_import_blob}}
    end)
  end

  defp validate_import_checkpoint(%Entry{} = entry, checkpoint) do
    with {:ok, report} <- Foundation.verify_instance_checkpoint(checkpoint, entry.stream_key),
         true <- report.digest == entry.checkpoint_digest,
         true <- report.revision == entry.revision do
      :ok
    else
      false -> {:error, {:ledger_import_checkpoint_digest_mismatch, entry.blob_digest}}
      {:error, _reason} -> {:error, {:invalid_ledger_import_checkpoint, entry.blob_digest}}
    end
  end

  defp import_entries!(runtime, namespace, entries, blobs) do
    Enum.each(entries, fn entry ->
      checkpoint = Map.fetch!(blobs, entry.blob_digest)
      insert_blob!(runtime, namespace, entry, checkpoint)
      insert_entry!(runtime, namespace, entry)
    end)
  end

  defp verify_imported_entries!(runtime, namespace, stream_key, entries) do
    stored = all_entries!(runtime, namespace, stream_key)

    unless Enum.map(stored, & &1.entry_digest) == Enum.map(entries, & &1.entry_digest) do
      Runtime.rollback(runtime, :ledger_import_content_conflict)
    end
  end

  defp set_import_head!(runtime, namespace, stream_key, %Entry{} = head) do
    result =
      Runtime.query!(
        runtime,
        """
        UPDATE #{Names.qualified(runtime.names, :streams)}
        SET "head_revision" = $3, "head_entry_digest" = $4,
            "head_blob_digest" = $5, "updated_at" = transaction_timestamp()
        WHERE "namespace" = $1 AND "stream_key" = $2
          AND "head_revision" = 0 AND "head_entry_digest" IS NULL
        """,
        [namespace, stream_key, head.revision, head.entry_digest, head.blob_digest]
      )

    if result.num_rows != 1, do: Runtime.rollback(runtime, :ledger_import_head_conflict)
  end

  defp ensure_and_lock_stream!(runtime, namespace, stream_key) do
    ensure_streams!(runtime, namespace, [stream_key])
    lock_streams!(runtime, namespace, [stream_key])
    stream_state!(runtime, namespace, stream_key)
  end

  defp ensure_streams!(runtime, namespace, stream_keys) do
    Enum.each(Enum.uniq(stream_keys), fn stream_key ->
      Runtime.query!(
        runtime,
        """
        INSERT INTO #{Names.qualified(runtime.names, :streams)}
          ("namespace", "stream_key")
        VALUES ($1, $2)
        ON CONFLICT ("namespace", "stream_key") DO NOTHING
        """,
        [namespace, stream_key]
      )
    end)
  end

  defp lock_streams!(runtime, namespace, stream_keys) do
    stream_keys
    |> Enum.uniq()
    |> Enum.sort()
    |> Enum.each(fn stream_key ->
      Runtime.query!(
        runtime,
        """
        SELECT "head_revision"
        FROM #{Names.qualified(runtime.names, :streams)}
        WHERE "namespace" = $1 AND "stream_key" = $2
        FOR UPDATE
        """,
        [namespace, stream_key]
      )
    end)
  end

  defp stream_state!(runtime, namespace, stream_key) do
    result =
      Runtime.query!(
        runtime,
        """
        SELECT "head_revision", "head_entry_digest", "head_blob_digest"
        FROM #{Names.qualified(runtime.names, :streams)}
        WHERE "namespace" = $1 AND "stream_key" = $2
        """,
        [namespace, stream_key]
      )

    case result.rows do
      [[revision, entry_digest, blob_digest]] ->
        %{revision: revision, entry_digest: entry_digest, blob_digest: blob_digest}

      [] ->
        Runtime.rollback(runtime, :ledger_stream_not_found)

      _rows ->
        Runtime.rollback(runtime, :invalid_ledger_postgres_stream)
    end
  end

  defp existing_entry!(runtime, namespace, stream_key, revision) do
    result =
      Runtime.query!(
        runtime,
        """
        SELECT #{@entry_columns}
        FROM #{Names.qualified(runtime.names, :entries)} AS e
        WHERE e."namespace" = $1 AND e."stream_key" = $2 AND e."revision" = $3
        """,
        [namespace, stream_key, revision]
      )

    case result.rows do
      [row] -> decode_entry!(runtime, row)
      [] -> nil
      _rows -> Runtime.rollback(runtime, :invalid_ledger_postgres_entry)
    end
  end

  defp all_entries!(runtime, namespace, stream_key) do
    result =
      Runtime.query!(
        runtime,
        """
        SELECT #{@entry_columns}
        FROM #{Names.qualified(runtime.names, :entries)} AS e
        WHERE e."namespace" = $1 AND e."stream_key" = $2
        ORDER BY e."revision" ASC
        """,
        [namespace, stream_key]
      )

    Enum.map(result.rows, &decode_entry!(runtime, &1))
  end

  defp insert_blob!(runtime, namespace, %Entry{} = entry, checkpoint) do
    Runtime.query!(
      runtime,
      """
      INSERT INTO #{Names.qualified(runtime.names, :blobs)}
        ("namespace", "blob_digest", "checkpoint_digest", "checkpoint", "byte_size")
      VALUES ($1, $2, $3, $4, $5)
      ON CONFLICT ("namespace", "blob_digest") DO NOTHING
      """,
      [namespace, entry.blob_digest, entry.checkpoint_digest, checkpoint, byte_size(checkpoint)]
    )

    verify_blob!(runtime, namespace, entry.blob_digest, %{
      checkpoint: checkpoint,
      checkpoint_digest: entry.checkpoint_digest,
      byte_size: byte_size(checkpoint)
    })
  end

  defp verify_blob!(runtime, namespace, blob_digest, expected) do
    result =
      Runtime.query!(
        runtime,
        """
        SELECT "checkpoint_digest", "checkpoint", "byte_size"
        FROM #{Names.qualified(runtime.names, :blobs)}
        WHERE "namespace" = $1 AND "blob_digest" = $2
        """,
        [namespace, blob_digest]
      )

    case result.rows do
      [[checkpoint_digest, checkpoint, byte_size]]
      when checkpoint_digest == expected.checkpoint_digest and
             checkpoint == expected.checkpoint and byte_size == expected.byte_size ->
        :ok

      [_row] ->
        Runtime.rollback(runtime, :ledger_blob_digest_collision)

      [] ->
        Runtime.rollback(runtime, :ledger_blob_not_found)
    end
  end

  defp checkpoint_for_blob!(runtime, namespace, blob_digest) do
    result =
      Runtime.query!(
        runtime,
        """
        SELECT "checkpoint"
        FROM #{Names.qualified(runtime.names, :blobs)}
        WHERE "namespace" = $1 AND "blob_digest" = $2
        """,
        [namespace, blob_digest]
      )

    case result.rows do
      [[checkpoint]] -> checkpoint
      [] -> Runtime.rollback(runtime, :ledger_blob_not_found)
    end
  end

  defp insert_entry!(runtime, namespace, %Entry{} = entry) do
    result =
      Runtime.query!(
        runtime,
        """
        INSERT INTO #{Names.qualified(runtime.names, :entries)}
          ("namespace", "stream_key", "revision", "expected_revision", "kind",
           "checkpoint_digest", "blob_digest", "previous_entry_digest",
           "source_entry_digest", "owner_fencing_token", "entry_digest")
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
        """,
        [
          namespace,
          entry.stream_key,
          entry.revision,
          entry.expected_revision,
          Atom.to_string(entry.kind),
          entry.checkpoint_digest,
          entry.blob_digest,
          entry.previous_entry_digest,
          entry.source_entry_digest,
          entry.owner_fencing_token,
          entry.entry_digest
        ]
      )

    if result.num_rows != 1, do: Runtime.rollback(runtime, :ledger_entry_not_inserted)
  end

  defp advance_head!(runtime, namespace, state, %Entry{} = entry) do
    result =
      Runtime.query!(
        runtime,
        """
        UPDATE #{Names.qualified(runtime.names, :streams)}
        SET "head_revision" = $3, "head_entry_digest" = $4,
            "head_blob_digest" = $5, "updated_at" = transaction_timestamp()
        WHERE "namespace" = $1 AND "stream_key" = $2
          AND "head_revision" = $6
          AND "head_entry_digest" IS NOT DISTINCT FROM $7
        """,
        [
          namespace,
          entry.stream_key,
          entry.revision,
          entry.entry_digest,
          entry.blob_digest,
          state.revision,
          state.entry_digest
        ]
      )

    if result.num_rows != 1, do: Runtime.rollback(runtime, :ledger_head_conflict)
  end

  defp insert_alias!(runtime, namespace, alias_key, target_key) do
    Runtime.query!(
      runtime,
      """
      INSERT INTO #{Names.qualified(runtime.names, :aliases)}
        ("namespace", "alias_stream_key", "target_stream_key")
      VALUES ($1, $2, $3)
      ON CONFLICT ("namespace", "alias_stream_key") DO NOTHING
      """,
      [namespace, alias_key, target_key]
    )

    case alias_target!(runtime, namespace, alias_key) do
      ^target_key -> :ok
      _other -> Runtime.rollback(runtime, :ledger_alias_conflict)
    end
  end

  defp reject_alias!(runtime, namespace, stream_key) do
    case alias_target!(runtime, namespace, stream_key) do
      nil -> :ok
      _target -> Runtime.rollback(runtime, :ledger_stream_is_alias)
    end
  end

  defp reject_alias_target!(runtime, namespace, stream_key) do
    result =
      Runtime.query!(
        runtime,
        """
        SELECT 1
        FROM #{Names.qualified(runtime.names, :aliases)}
        WHERE "namespace" = $1 AND "target_stream_key" = $2
        LIMIT 1
        """,
        [namespace, stream_key]
      )

    case result.rows do
      [] -> :ok
      [[1]] -> Runtime.rollback(runtime, :ledger_migration_source_has_aliases)
      _rows -> Runtime.rollback(runtime, :invalid_ledger_alias)
    end
  end

  defp alias_target!(runtime, namespace, stream_key) do
    result =
      Runtime.query!(
        runtime,
        """
        SELECT "target_stream_key"
        FROM #{Names.qualified(runtime.names, :aliases)}
        WHERE "namespace" = $1 AND "alias_stream_key" = $2
        """,
        [namespace, stream_key]
      )

    case result.rows do
      [[target]] -> target
      [] -> nil
      _rows -> Runtime.rollback(runtime, :invalid_ledger_alias)
    end
  end

  defp resolve_key(runtime, namespace, stream_key) do
    with {:ok, result} <-
           Runtime.read(
             runtime,
             """
             SELECT "target_stream_key"
             FROM #{Names.qualified(runtime.names, :aliases)}
             WHERE "namespace" = $1 AND "alias_stream_key" = $2
             """,
             [namespace, stream_key]
           ) do
      case result.rows do
        [] -> {:ok, stream_key}
        [[target]] -> reject_alias_chain(runtime, namespace, target)
        _rows -> {:error, :invalid_ledger_alias}
      end
    end
  end

  defp reject_alias_chain(runtime, namespace, target) do
    with {:ok, result} <-
           Runtime.read(
             runtime,
             """
             SELECT 1
             FROM #{Names.qualified(runtime.names, :aliases)}
             WHERE "namespace" = $1 AND "alias_stream_key" = $2
             """,
             [namespace, target]
           ) do
      case result.rows do
        [] -> {:ok, target}
        _rows -> {:error, :ledger_alias_chain_not_supported}
      end
    end
  end

  defp exact_write?(%Entry{} = entry, %Write{} = write) do
    entry.stream_key == write.ref.key and entry.kind == write.kind and
      entry.expected_revision == write.expected_revision and entry.revision == write.revision and
      entry.checkpoint_digest == write.checkpoint_digest and
      entry.blob_digest == write.blob_digest and
      entry.source_entry_digest == write.source_entry_digest
  end

  defp current_entry?(state, entry) do
    state.revision == entry.revision and state.entry_digest == entry.entry_digest and
      state.blob_digest == entry.blob_digest
  end

  defp validate_write(%Write{} = write, %Config{} = config) do
    with true <- is_binary(write.ref.key) and write.ref.key != "",
         true <- is_binary(write.checkpoint),
         true <- write.byte_size == byte_size(write.checkpoint),
         true <- write.byte_size in 1..config.max_checkpoint_bytes,
         true <- sha256(write.checkpoint) == write.blob_digest,
         {:ok, report} <- Foundation.verify_instance_checkpoint(write.checkpoint, write.ref),
         true <- report.digest == write.checkpoint_digest,
         true <- report.revision == write.revision do
      :ok
    else
      _invalid -> {:error, :invalid_ledger_checkpoint_write}
    end
  end

  defp validate_migration_write(
         %Write{kind: :migration, expected_revision: 0, source_entry_digest: source_digest} =
           write,
         %Config{} = config
       )
       when is_binary(source_digest),
       do: validate_write(write, config)

  defp validate_migration_write(%Write{}, %Config{}),
    do: {:error, :invalid_ledger_migration_write}

  defp receipt(%Entry{} = entry, status) do
    %Receipt{
      stream_key: entry.stream_key,
      revision: entry.revision,
      checkpoint_digest: entry.checkpoint_digest,
      entry_digest: entry.entry_digest,
      status: status
    }
  end

  defp decode_entries(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, entries} ->
      case decode_entry(row) do
        {:ok, entry} -> {:cont, {:ok, [entry | entries]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_receipt_entries(rows) do
    Enum.reduce_while(rows, {:ok, []}, fn row, {:ok, entries} ->
      case decode_receipt_entry(row) do
        {:ok, entry} -> {:cont, {:ok, [entry | entries]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} -> {:ok, Enum.reverse(entries)}
      {:error, _reason} = error -> error
    end
  end

  defp decode_receipt_entry!(runtime, row) do
    case decode_receipt_entry(row) do
      {:ok, entry} -> entry
      {:error, reason} -> Runtime.rollback(runtime, reason)
    end
  end

  defp decode_receipt_entry([
         stream_key,
         sequence,
         receipt_id,
         kind,
         canonical_revision,
         envelope_digest,
         payload_ref,
         previous_entry_digest,
         recorded_at,
         owner_fencing_token,
         entry_digest
       ]) do
    ReceiptEntry.from_data(%{
      "format" => "spectre/ledger-receipt-entry",
      "entry_version" => ReceiptEntry.version(),
      "stream_key" => stream_key,
      "sequence" => sequence,
      "receipt_id" => receipt_id,
      "kind" => kind,
      "canonical_revision" => canonical_revision,
      "envelope_digest" => envelope_digest,
      "payload_ref" => payload_ref,
      "previous_entry_digest" => previous_entry_digest,
      "recorded_at" => recorded_at,
      "owner_fencing_token" => owner_fencing_token,
      "entry_digest" => entry_digest
    })
  end

  defp decode_receipt_entry(_row), do: {:error, :invalid_ledger_postgres_receipt_entry}

  defp decode_receipt_lookup(rows, receipt_id, max_bytes) do
    case rows do
      [row] when is_list(row) and length(row) == 14 ->
        {entry_data, [envelope_digest, encoded, byte_size]} = Enum.split(row, 11)

        with {:ok, %ReceiptEntry{receipt_id: ^receipt_id} = entry} <-
               decode_receipt_entry(entry_data),
             {:ok, ^encoded} <-
               decode_receipt_payload(
                 [[envelope_digest, encoded, byte_size]],
                 entry.payload_ref,
                 max_bytes
               ),
             {:ok, envelope} <-
               ReceiptCodec.verify(encoded, entry.receipt_id, entry.envelope_digest, max_bytes),
             :ok <- ReceiptEntry.verify_envelope(entry, envelope) do
          {:ok, encoded}
        else
          {:ok, %ReceiptEntry{}} -> {:error, :ledger_receipt_lookup_id_mismatch}
          {:error, _reason} = error -> error
          _invalid -> {:error, :invalid_ledger_postgres_receipt}
        end

      [] ->
        :not_found

      _rows ->
        {:error, :invalid_ledger_postgres_receipt_lookup_result}
    end
  end

  defp decode_receipt_payload(rows, payload_ref, max_bytes) do
    case rows do
      [[envelope_digest, encoded, byte_size]] ->
        with true <- is_binary(encoded) and byte_size(encoded) == byte_size,
             {:ok, envelope} <- ReceiptCodec.decode(encoded, max_bytes),
             true <- Envelope.digest(envelope) == envelope_digest,
             true <- Sink.payload_ref(envelope) == payload_ref do
          {:ok, encoded}
        else
          false -> {:error, :invalid_ledger_postgres_receipt_payload}
          {:error, _reason} = error -> error
        end

      [] ->
        :not_found

      _rows ->
        {:error, :invalid_ledger_postgres_receipt_payload_result}
    end
  end

  defp decode_receipt_objects(rows, max_bytes) do
    Enum.reduce_while(rows, {:ok, %{}}, fn
      [payload_ref, envelope_digest, encoded, byte_size], {:ok, objects} ->
        case decode_receipt_payload(
               [[envelope_digest, encoded, byte_size]],
               payload_ref,
               max_bytes
             ) do
          {:ok, ^encoded} -> {:cont, {:ok, Map.put(objects, payload_ref, encoded)}}
          {:error, _reason} = error -> {:halt, error}
          :not_found -> {:halt, {:error, :ledger_receipt_payload_not_found}}
        end

      _row, {:ok, _objects} ->
        {:halt, {:error, :invalid_ledger_postgres_receipt_object}}
    end)
  end

  defp decode_entry!(runtime, row) do
    case decode_entry(row) do
      {:ok, entry} -> entry
      {:error, reason} -> Runtime.rollback(runtime, reason)
    end
  end

  defp decode_entry([
         stream_key,
         kind,
         expected_revision,
         revision,
         checkpoint_digest,
         blob_digest,
         previous_entry_digest,
         source_entry_digest,
         owner_fencing_token,
         entry_digest
       ]) do
    Entry.from_data(%{
      "format" => "spectre/ledger-entry",
      "entry_version" => Entry.version(),
      "stream_key" => stream_key,
      "kind" => kind,
      "expected_revision" => expected_revision,
      "revision" => revision,
      "checkpoint_digest" => checkpoint_digest,
      "blob_digest" => blob_digest,
      "previous_entry_digest" => previous_entry_digest,
      "source_entry_digest" => source_entry_digest,
      "owner_fencing_token" => owner_fencing_token,
      "entry_digest" => entry_digest
    })
  end

  defp decode_entry(_row), do: {:error, :invalid_ledger_postgres_entry}

  defp decode_loaded_checkpoint(row, stream_key) when is_list(row) and length(row) == 13 do
    {entry_data, [checkpoint, blob_checkpoint_digest, byte_size]} = Enum.split(row, 10)

    with {:ok, %Entry{stream_key: ^stream_key} = entry} <- decode_entry(entry_data),
         true <- is_binary(checkpoint) and byte_size(checkpoint) == byte_size,
         true <- sha256(checkpoint) == entry.blob_digest,
         true <- blob_checkpoint_digest == entry.checkpoint_digest,
         {:ok, report} <- Foundation.verify_instance_checkpoint(checkpoint, stream_key),
         true <- report.digest == entry.checkpoint_digest,
         true <- report.revision == entry.revision do
      {:ok, checkpoint}
    else
      _invalid -> {:error, :invalid_ledger_postgres_checkpoint}
    end
  end

  defp decode_loaded_checkpoint(_row, _stream_key),
    do: {:error, :invalid_ledger_postgres_load_result}

  defp entries_sql(runtime, nil) do
    """
    SELECT #{@entry_columns}
    FROM #{Names.qualified(runtime.names, :entries)} AS e
    WHERE e."namespace" = $1 AND e."stream_key" = $2 AND e."revision" > $3
    ORDER BY e."revision" ASC
    """
  end

  defp entries_sql(runtime, _limit) do
    entries_sql(runtime, nil) <> " LIMIT $4"
  end

  defp entries_params(namespace, stream_key, after_revision, nil),
    do: [namespace, stream_key, after_revision]

  defp entries_params(namespace, stream_key, after_revision, limit),
    do: [namespace, stream_key, after_revision, limit]

  defp receipt_entries_sql(runtime, nil) do
    """
    SELECT #{@receipt_entry_columns}
    FROM #{Names.qualified(runtime.names, :receipt_entries)} AS r
    WHERE r."namespace" = $1 AND r."stream_key" = $2 AND r."sequence" > $3
    ORDER BY r."sequence" ASC
    """
  end

  defp receipt_entries_sql(runtime, _limit) do
    receipt_entries_sql(runtime, nil) <> " LIMIT $4"
  end

  defp receipt_entries_params(namespace, stream_key, after_sequence, nil),
    do: [namespace, stream_key, after_sequence]

  defp receipt_entries_params(namespace, stream_key, after_sequence, limit),
    do: [namespace, stream_key, after_sequence, limit]

  defp receipt_list_options(opts) do
    if Keyword.keyword?(opts),
      do: keyword_receipt_list_options(opts),
      else: {:error, :invalid_ledger_receipt_entries_options}
  end

  defp keyword_receipt_list_options(opts) do
    after_sequence = Keyword.get(opts, :after_sequence, 0)
    limit = Keyword.get(opts, :limit)
    keys = Keyword.keys(opts)

    cond do
      length(keys) != length(Enum.uniq(keys)) or
          Enum.any?(keys, &(&1 not in [:after_sequence, :limit])) ->
        {:error, :invalid_ledger_receipt_entries_options}

      not (is_integer(after_sequence) and after_sequence >= 0) ->
        {:error, :invalid_ledger_receipt_after_sequence}

      invalid_limit?(limit) ->
        {:error, :invalid_ledger_receipt_entry_limit}

      true ->
        {:ok, after_sequence, limit}
    end
  end

  defp list_options(opts) do
    if Keyword.keyword?(opts),
      do: keyword_list_options(opts),
      else: {:error, :invalid_ledger_entries_options}
  end

  defp keyword_list_options(opts) do
    after_revision = Keyword.get(opts, :after_revision, -1)
    limit = Keyword.get(opts, :limit)
    keys = Keyword.keys(opts)

    cond do
      invalid_list_keys?(keys) ->
        {:error, :invalid_ledger_entries_options}

      invalid_after_revision?(after_revision) ->
        {:error, :invalid_ledger_after_revision}

      invalid_limit?(limit) ->
        {:error, :invalid_ledger_entry_limit}

      true ->
        {:ok, after_revision, limit}
    end
  end

  defp invalid_list_keys?(keys) do
    length(keys) != length(Enum.uniq(keys)) or
      Enum.any?(keys, &(&1 not in [:after_revision, :limit]))
  end

  defp invalid_after_revision?(revision),
    do: not (is_integer(revision) and revision >= -1)

  defp invalid_limit?(limit),
    do: not (is_nil(limit) or valid_limit?(limit))

  defp valid_limit?(limit), do: is_integer(limit) and limit > 0 and limit <= 1_000_000

  defp doctor_tables(names, rows) do
    present = MapSet.new(rows, fn [table] -> table end)

    Map.new(names.tables, fn {name, table} -> {name, MapSet.member?(present, table)} end)
  end

  defp doctor_columns(names, rows) do
    actual =
      Enum.group_by(rows, fn [table, _column] -> table end, fn [_table, column] -> column end)

    Map.new(names.tables, fn {name, table} ->
      {name,
       MapSet.new(Map.get(actual, table, [])) == MapSet.new(Map.fetch!(@expected_columns, name))}
    end)
  end

  defp doctor_schema_version(_runtime, false), do: {:ok, nil}

  defp doctor_schema_version(runtime, true) do
    with {:ok, result} <-
           Runtime.read(
             runtime,
             "SELECT \"value\" FROM #{Names.qualified(runtime.names, :meta)} WHERE \"key\" = $1",
             ["schema_version"]
           ) do
      decode_schema_version(result.rows)
    end
  end

  defp decode_schema_version([[value]]) when is_binary(value) do
    case Integer.parse(value) do
      {version, ""} -> {:ok, version}
      _invalid -> {:ok, :invalid}
    end
  end

  defp decode_schema_version([]), do: {:ok, nil}
  defp decode_schema_version(_rows), do: {:error, :invalid_ledger_postgres_meta}

  defp sha256(bytes),
    do: bytes |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
end
