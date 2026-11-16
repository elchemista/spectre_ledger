defmodule Spectre.Ledger.DomainStore do
  @moduledoc """
  PostgreSQL read model for the Domain behind Managed GAM.

  Uses the host Repo and the canonical ledger's namespace. The two projection
  tables contain disposable current records, not another event journal. A new
  Domain owner fences the previous generation and rebuilds only these derived
  tables. Canonical history is never removed. All writes are transactional;
  reads require the exact generation/revision in one repeatable-read snapshot.
  Inside an existing host transaction they retain its isolation and shared head lock.
  """
  @behaviour Spectre.GAM.Domain.Projection.Store
  alias Ecto.Adapters.SQL
  alias Spectre.GAM.Canonical.Value
  alias Spectre.GAM.Domain.Projection.Rows
  alias Spectre.GAM.Domain.Projection.IndexRows
  alias Spectre.GAM.Ledger.Entry

  @impl true
  def open(domain, opts) when is_binary(domain) and byte_size(domain) > 0 do
    opts = Keyword.put(opts, :timeout, Keyword.get(opts, :rebuild_timeout, 60_000))

    transaction(opts, fn config ->
      generation = Base.url_encode64(:crypto.strong_rand_bytes(24), padding: false)

      query!(config, "SELECT pg_advisory_xact_lock(hashtextextended($1, 0))", [
        config.heads <> domain
      ])

      query!(
        config,
        "INSERT INTO #{config.heads} (domain_ref,generation,revision,head_digest) VALUES ($1,$2,0,$3) ON CONFLICT (domain_ref) DO UPDATE SET generation=EXCLUDED.generation,revision=0,head_digest=EXCLUDED.head_digest",
        [domain, generation, Entry.genesis_digest()]
      )

      query!(config, "DELETE FROM #{config.rows} WHERE domain_ref=$1", [domain])
      {:ok, %{domain: domain, generation: generation}}
    end)
  end

  def open(_, _), do: {:error, :invalid_domain_ref}

  @impl true
  def head(domain, opts) do
    transaction(opts, fn config ->
      case query!(
             config,
             "SELECT generation,revision,head_digest FROM #{config.heads} WHERE domain_ref=$1",
             [domain]
           ).rows do
        [[_, 0, _]] ->
          {:error, :domain_projection_rebuilding}

        [[generation, revision, digest]] ->
          {:ok,
           %{
             handle: %{domain: domain, generation: generation},
             cursor: %{revision: revision, head_digest: digest}
           }}

        [] ->
          :not_found
      end
    end)
  end

  @impl true
  def commit(handle, before, next, changes, opts) do
    transaction(opts, fn config ->
      lock!(config, handle, before, "UPDATE")

      unless is_integer(next.revision) and next.revision > before.revision and
               Spectre.GAM.Portable.sha256_digest?(next.head_digest),
             do: abort(config, :invalid_projection_advance)

      # Confirm the target exists in the same canonical Domain history. This is
      # not a substitute for semantic validation by the caller's pure Fold.
      case query!(
             config,
             "SELECT 1 FROM (SELECT last_revision,head_digest FROM #{config.batches} WHERE domain_ref=$1 AND first_revision <= $2 ORDER BY first_revision DESC LIMIT 1) batch WHERE last_revision=$2 AND head_digest=$3",
             [handle.domain, next.revision, next.head_digest]
           ).rows do
        [[1]] -> :ok
        _ -> abort(config, :projection_target_not_committed)
      end

      changes |> Stream.chunk_every(100) |> Enum.each(&write_chunk!(config, handle.domain, &1))

      query!(
        config,
        "UPDATE #{config.heads} SET revision=$2,head_digest=$3 WHERE domain_ref=$1",
        [handle.domain, next.revision, next.head_digest]
      )

      :ok
    end)
  end

  @impl true
  def fetch(handle, cursor, keys, opts) when is_list(keys) and length(keys) <= 100 do
    read_transaction(opts, fn config, mode ->
      lock!(config, handle, cursor, mode)

      {collections, identities} =
        keys
        |> Enum.uniq()
        |> Enum.map(fn {collection, key} ->
          {collection!(config, collection), Value.encode!(key)}
        end)
        |> Enum.unzip()

      max_bytes = Keyword.get(opts, :max_bytes, 2_097_152)

      unless is_integer(max_bytes) and max_bytes in 1..16_777_216,
        do: abort(config, :invalid_projection_page)

      rows =
        query!(
          config,
          "SELECT r.collection,CASE WHEN sum(octet_length(r.key)+octet_length(r.value)) OVER () <= $4 THEN r.key END,CASE WHEN sum(octet_length(r.key)+octet_length(r.value)) OVER () <= $4 THEN r.value END FROM #{config.rows} r JOIN unnest($2::text[],$3::bytea[]) q(collection,key) ON r.collection=q.collection AND r.key=q.key WHERE r.domain_ref=$1",
          [handle.domain, collections, identities, max_bytes]
        ).rows

      if Enum.any?(rows, &missing_value?/1),
        do: abort(config, :projection_page_entry_too_large)

      {:ok,
       Map.new(rows, fn [collection, key, value] ->
         {{String.to_existing_atom(collection), decode_key!(config, key)}, value}
       end)}
    end)
  end

  def fetch(_, _, _, _), do: {:error, :invalid_projection_keys}

  defp missing_value?([_, _, bytes]), do: is_nil(bytes)

  @impl true
  def select(handle, cursor, collection, selection, opts),
    do: page(handle, cursor, collection, Keyword.merge(opts, selection))

  @impl true
  def page(handle, cursor, collection, opts) do
    read_transaction(opts, fn config, mode ->
      lock!(config, handle, cursor, mode)
      name = collection!(config, collection)
      {limit, max_bytes} = page_budget!(config, opts)
      selection = Keyword.take(opts, [:group, :from, :through, :nearest])
      {group, first, last, nearest} = selection!(config, selection)
      after_key = page_key!(config, handle, cursor, {collection, selection}, opts[:after])
      order = if nearest, do: "position,key", else: "key"
      count = if nearest, do: 1, else: limit + 1

      sql =
        "WITH candidates AS (SELECT key,value FROM #{config.rows} WHERE domain_ref=$1 AND collection=$2 AND ($3::bytea IS NULL OR key > $3) AND ($7::bytea IS NULL OR lookup=$7) AND ($8::bigint IS NULL OR position >= $8) AND ($9::bigint IS NULL OR position <= $9) ORDER BY #{order} LIMIT $4), budget AS (SELECT key,value,sum(octet_length(key)+octet_length(value)) OVER (ORDER BY key) used,row_number() OVER (ORDER BY key) position FROM candidates) SELECT CASE WHEN used <= $5 AND position <= $6 THEN key END,CASE WHEN used <= $5 AND position <= $6 THEN value END FROM budget ORDER BY key"

      found =
        query!(config, sql, [
          handle.domain,
          name,
          after_key,
          count,
          max_bytes,
          limit,
          group,
          first,
          last
        ]).rows

      case found do
        [[_, nil] | _] -> abort(config, :projection_page_entry_too_large)
        _ -> :ok
      end

      page = found |> Enum.take(limit) |> Enum.take_while(fn [_, value] -> not is_nil(value) end)

      next =
        if length(page) < length(found),
          do: {handle, cursor, {collection, selection}, page |> List.last() |> hd()},
          else: nil

      {:ok,
       Enum.map(page, fn [key, value] -> {{collection, decode_key!(config, key)}, value} end),
       next}
    end)
  end

  defp page_budget!(config, opts) do
    limit = Keyword.get(opts, :limit, 50)
    bytes = Keyword.get(opts, :max_bytes, 2_097_152)

    unless is_integer(limit) and limit in 1..100 and is_integer(bytes) and bytes in 1..16_777_216,
      do: abort(config, :invalid_projection_page)

    {limit, bytes}
  end

  defp selection!(config, opts) do
    first = opts[:from]
    last = opts[:through]
    nearest = Keyword.get(opts, :nearest, false)

    unless valid_position?(first) and valid_position?(last) and is_boolean(nearest),
      do: abort(config, :invalid_projection_page)

    group = if Keyword.has_key?(opts, :group), do: Value.encode!(opts[:group])
    {group, first, last, nearest}
  end

  defp valid_position?(nil), do: true
  defp valid_position?(value), do: is_integer(value) and value >= 0

  defp page_key!(_, _, _, _, nil), do: nil

  defp page_key!(_, handle, cursor, collection, {handle, cursor, collection, key})
       when is_binary(key), do: key

  defp page_key!(config, _, _, _, _), do: abort(config, :stale_projection_page)

  defp write_chunk!(config, domain, changes) do
    {puts, deletes} = Enum.split_with(changes, &match?({:put, _, _}, &1))

    rows = Enum.map(puts, &encode_put!(config, &1))

    if rows != [] do
      query!(
        config,
        "INSERT INTO #{config.rows} (domain_ref,collection,key,value,lookup,position) SELECT $1,* FROM unnest($2::text[],$3::bytea[],$4::bytea[],$5::bytea[],$6::bigint[]) ON CONFLICT (domain_ref,collection,key) DO UPDATE SET value=EXCLUDED.value,lookup=EXCLUDED.lookup,position=EXCLUDED.position",
        [
          domain,
          Enum.map(rows, &elem(&1, 0)),
          Enum.map(rows, &elem(&1, 1)),
          Enum.map(rows, &elem(&1, 2)),
          Enum.map(rows, &elem(&1, 3)),
          Enum.map(rows, &elem(&1, 4))
        ]
      )
    end

    Enum.each(deletes, fn {:delete, {collection, key}} ->
      query!(
        config,
        "DELETE FROM #{config.rows} WHERE domain_ref=$1 AND collection=$2 AND key=$3",
        [domain, collection!(config, collection), Value.encode!(key)]
      )
    end)
  end

  defp encode_put!(config, {:put, {collection, key}, value}) do
    with {:ok, bytes} <- Rows.encode(value), {:ok, identity} <- Value.encode(key) do
      group =
        if collection == :record_refs, do: elem(key, 0), else: IndexRows.group(collection, key)

      lookup = if is_nil(group), do: nil, else: Value.encode!(group)

      {collection!(config, collection), identity, bytes, lookup,
       IndexRows.position(collection, key)}
    else
      {:error, reason} -> abort(config, reason)
    end
  end

  defp lock!(config, handle, cursor, mode) do
    lock = if mode, do: " FOR #{mode}", else: ""

    result =
      query!(
        config,
        "SELECT generation,revision,head_digest FROM #{config.heads} WHERE domain_ref=$1#{lock}",
        [handle.domain]
      )

    expected = [handle.generation, cursor.revision, cursor.head_digest]
    unless result.rows === [expected], do: abort(config, :stale_domain_projection)
  end

  defp collection!(config, collection) do
    if collection in Rows.collections(),
      do: Atom.to_string(collection),
      else: abort(config, :invalid_projection_collection)
  end

  defp decode_key!(config, bytes) do
    case Value.decode(bytes) do
      {:ok, key} -> key
      {:error, reason} -> abort(config, reason)
    end
  end

  @doc "DDL for the existing Ledger namespace; run through an application migration."
  def migration_sql(opts \\ []) do
    with {:ok, config} <- names(opts) do
      {:ok,
       %{
         up: [
           "CREATE TABLE #{config.heads} (domain_ref text PRIMARY KEY,generation text NOT NULL,revision bigint NOT NULL CHECK (revision >= 0),head_digest text NOT NULL)",
           "CREATE TABLE #{config.rows} (domain_ref text NOT NULL REFERENCES #{config.heads}(domain_ref),collection text NOT NULL,key bytea NOT NULL,value bytea NOT NULL,lookup bytea,position bigint,PRIMARY KEY(domain_ref,collection,key))",
           "CREATE INDEX #{config.lookup_index} ON #{config.rows} (domain_ref,collection,lookup,key)",
           "CREATE INDEX #{config.position_index} ON #{config.rows} (domain_ref,collection,position,key)"
         ],
         down: ["DROP TABLE #{config.rows}", "DROP TABLE #{config.heads}"]
       }}
    end
  end

  @doc "Adds indexed selections to a previously created, disposable Domain projection."
  def upgrade_sql(opts \\ []) do
    with {:ok, config} <- names(opts) do
      {:ok,
       [
         "ALTER TABLE #{config.rows} ADD COLUMN IF NOT EXISTS lookup bytea, ADD COLUMN IF NOT EXISTS position bigint",
         "CREATE INDEX IF NOT EXISTS #{config.lookup_index} ON #{config.rows} (domain_ref,collection,lookup,key)",
         "CREATE INDEX IF NOT EXISTS #{config.position_index} ON #{config.rows} (domain_ref,collection,position,key)"
       ]}
    end
  end

  defp names(opts) do
    with {:ok, ns} <-
           Spectre.GAM.Ledger.Store.Postgres.namespace(
             Keyword.take(opts, [:schema, :table_prefix])
           ),
         true <- byte_size(ns.table_prefix) <= 46 do
      prefix = "\"#{ns.schema}\".\"#{ns.table_prefix}"

      {:ok,
       %{
         heads: prefix <> "_projection_heads\"",
         rows: prefix <> "_projection_rows\"",
         lookup_index: "\"#{ns.table_prefix}_p_lookup\"",
         position_index: "\"#{ns.table_prefix}_p_position\"",
         batches: prefix <> "_batches\""
       }}
    else
      false -> {:error, :domain_projection_prefix_too_long}
      error -> error
    end
  end

  defp transaction(opts, fun) do
    with {:ok, names} <- names(opts) do
      config =
        Map.merge(names, %{
          repo: Keyword.fetch!(opts, :repo),
          timeout: Keyword.get(opts, :timeout, 4_000)
        })

      config.repo.transaction(fn -> fun.(config) end, timeout: config.timeout, log: false)
    end
    |> case do
      {:ok, result} -> result
      {:error, reason} -> {:error, reason}
    end
  rescue
    _ -> {:error, :domain_projection_storage_error}
  catch
    :exit, _ -> {:error, :domain_projection_storage_error}
  end

  defp read_transaction(opts, fun) do
    repo = Keyword.fetch!(opts, :repo)
    nested? = not function_exported?(repo, :in_transaction?, 0) or repo.in_transaction?()

    transaction(opts, fn config ->
      if nested? do
        # Respect an application's existing transaction and isolation level.
        fun.(config, "SHARE")
      else
        # Head and rows share one MVCC snapshot. Unlike tuple locks on every
        # lookup, these read-only transactions do not generate lock WAL writes.
        query!(config, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", [])
        fun.(config, nil)
      end
    end)
  end

  defp query!(config, sql, params),
    do: apply(SQL, :query!, [config.repo, sql, params, [timeout: config.timeout, log: false]])

  defp abort(config, reason), do: config.repo.rollback(reason)
end
