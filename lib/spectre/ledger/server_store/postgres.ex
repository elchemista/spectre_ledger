defmodule Spectre.Ledger.ServerStore.Postgres do
  @moduledoc """
  Coordination Store on a host-owned Ecto Repo, without a cache of stored rows.

  Each operation runs in a monitored task. The owner retains configuration and
  in-flight references only; keyset pages bound both row count and payload bytes.
  Requests and terminal decisions remain in PostgreSQL until the host explicitly
  deletes them. Delete preserves a revision tombstone against stale writers.

  Configure `ServerStore` with `backend: :postgres, repo: MyApp.Repo`, optionally
  `schema:` and `table:`. Install `migration_sql/1` in the application's migration
  first. No Repo or schema is created automatically. `query_timeout` defaults to
  4 seconds and `max_workers` to 16; these are I/O budgets, not inbox quotas.

  Managed `storage: :ledger` obtains canonical history and the Domain read model
  through `gam_domain/1`. `gam_ledger/1` remains available for history-only callers.
  Both share this Repo/schema and `query_timeout`; `gam_table_prefix`
  defaults to `"spectre_gam"`. Install `Spectre.GAM.Ledger.Store.Postgres.migration_sql/1`
  and `Spectre.Ledger.DomainStore.migration_sql/1` separately. Canonical GAM history
  is append-only; derived Domain records are also persisted with revision fencing.
  The live Domain retains authority roots and active work; archived records stay
  in PostgreSQL. Existing disk Domains are not migrated automatically.
  """
  use GenServer
  @behaviour Spectre.Server.Store
  alias Spectre.GAM.Canonical.Value
  alias Spectre.Ledger.ServerStore
  alias Spectre.Server.Store.Page

  @sql :"Elixir.Ecto.Adapters.SQL"
  @identifier ~r/\A[a-z_][a-z0-9_]{0,47}\z/

  defguardp valid_timeout?(value)
            when is_integer(value) and value > 0 and value < 4_294_966_295

  def start_link(opts) do
    with {:ok, config} <- configuration(opts),
         do: GenServer.start_link(__MODULE__, config, Keyword.take(opts, [:name]))
  end

  @impl true
  defdelegate fetch(store, key), to: ServerStore
  @impl true
  defdelegate put_new(store, key, value), to: ServerStore
  @impl true
  defdelegate compare_and_swap(store, key, revision, value), to: ServerStore

  @impl true
  defdelegate delete(store, key, revision), to: ServerStore
  @impl true
  defdelegate list(store, prefix, opts \\ []), to: ServerStore

  @impl true
  defdelegate gam_ledger(store), to: ServerStore

  @impl true
  defdelegate gam_domain(store), to: ServerStore

  @doc "DDL for an application-owned migration; the adapter never executes it at startup."
  def migration_sql(opts \\ []) do
    with {:ok, names} <- names(opts) do
      {:ok,
       [
         "CREATE SEQUENCE #{names.sequence}",
         "CREATE TABLE #{names.table} (sort_key bytea PRIMARY KEY, key bytea NOT NULL, revision bigint NOT NULL DEFAULT nextval('#{names.sequence}'), value bytea, deleted boolean NOT NULL DEFAULT false, inserted_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp())"
       ]}
    end
  end

  @impl true
  def init(config) do
    {:ok, supervisor} = Task.Supervisor.start_link()
    {:ok, Map.put(config, :supervisor, supervisor)}
  end

  defp configuration(opts) do
    allowed = [
      :name,
      :repo,
      :schema,
      :table,
      :query_timeout,
      :max_workers,
      :gam_table_prefix,
      :rebuild_timeout
    ]

    with true <- Keyword.keyword?(opts) and Keyword.keys(opts) -- allowed == [],
         {:ok, names} <- names(opts),
         {:ok, namespace} <-
           Spectre.GAM.Ledger.Store.Postgres.namespace(
             schema: Keyword.get(opts, :schema, "public"),
             table_prefix: Keyword.get(opts, :gam_table_prefix, "spectre_gam")
           ),
         true <- byte_size(namespace.table_prefix) <= 46,
         repo when is_atom(repo) and not is_nil(repo) <- opts[:repo],
         true <- Code.ensure_loaded?(repo) and function_exported?(repo, :transaction, 2),
         true <- Code.ensure_loaded?(@sql) and function_exported?(@sql, :query, 4),
         timeout when valid_timeout?(timeout) <- Keyword.get(opts, :query_timeout, 4_000),
         rebuild_timeout when valid_timeout?(rebuild_timeout) <-
           Keyword.get(opts, :rebuild_timeout, 60_000),
         workers when is_integer(workers) and workers > 0 <- Keyword.get(opts, :max_workers, 16) do
      ledger_opts = [
        repo: repo,
        schema: namespace.schema,
        table_prefix: namespace.table_prefix,
        query_opts: [timeout: timeout, log: false],
        transaction_opts: [timeout: timeout, log: false]
      ]

      {:ok,
       Map.merge(names, %{
         repo: repo,
         timeout: timeout,
         max_workers: workers,
         tasks: %{},
         gam_ledger: {Spectre.GAM.Ledger.Store.Postgres, ledger_opts},
         gam_projection:
           {Spectre.Ledger.DomainStore,
            [
              repo: repo,
              schema: namespace.schema,
              table_prefix: namespace.table_prefix,
              timeout: timeout,
              rebuild_timeout: rebuild_timeout
            ]}
       })}
    else
      _ -> {:error, :invalid_postgres_server_store}
    end
  end

  @impl true
  def handle_call(:gam_ledger, _from, state), do: {:reply, {:ok, state.gam_ledger}, state}

  def handle_call(:gam_domain, _from, state),
    do: {:reply, {:ok, %{ledger: state.gam_ledger, projection: state.gam_projection}}, state}

  def handle_call(_operation, _from, %{tasks: tasks, max_workers: limit} = state)
      when map_size(tasks) >= limit,
      do: {:reply, {:error, :store_busy}, state}

  def handle_call(operation, from, state) do
    config = Map.drop(state, [:tasks, :max_workers, :supervisor])

    task = Task.Supervisor.async_nolink(state.supervisor, fn -> execute(config, operation) end)
    timer = Process.send_after(self(), {:query_timeout, task.ref}, state.timeout + 1_000)
    entry = %{task: task, timer: timer, from: from}
    {:noreply, put_in(state, [:tasks, task.ref], entry)}
  end

  @impl true
  def handle_info({ref, result}, state) when is_reference(ref), do: complete(state, ref, result)

  def handle_info({:DOWN, ref, :process, _, _}, state),
    do: complete(state, ref, {:error, {:ambiguous, :store_query_failed}})

  def handle_info({:query_timeout, ref}, state) do
    if entry = state.tasks[ref], do: Process.exit(entry.task.pid, :kill)
    complete(state, ref, {:error, {:ambiguous, :store_query_timeout}})
  end

  defp complete(state, ref, result) do
    case Map.pop(state.tasks, ref) do
      {nil, _} ->
        {:noreply, state}

      {entry, tasks} ->
        Process.cancel_timer(entry.timer)
        Process.demonitor(ref, [:flush])
        GenServer.reply(entry.from, result)
        {:noreply, %{state | tasks: tasks}}
    end
  end

  @impl true
  def terminate(_, state) do
    Enum.each(state.tasks, fn {_, entry} ->
      Process.cancel_timer(entry.timer)
      Process.exit(entry.task.pid, :kill)
      GenServer.reply(entry.from, {:error, {:ambiguous, :store_closed}})
    end)

    if Process.alive?(state.supervisor), do: Supervisor.stop(state.supervisor)
  end

  @impl true
  def format_status(status),
    do: %{status | state: %{active_queries: map_size(status.state.tasks)}}

  defp execute(config, operation) do
    do_execute(config, operation)
  rescue
    _ -> {:error, {:ambiguous, :store_query_failed}}
  catch
    _, _ -> {:error, {:ambiguous, :store_query_failed}}
  end

  defp do_execute(config, {:fetch, key}) do
    with {:ok, result} <-
           query(
             config,
             "SELECT revision, value FROM #{config.table} WHERE sort_key=$1 AND NOT deleted",
             [Page.key(key)]
           ) do
      case result.rows do
        [] -> :not_found
        [[revision, bytes]] -> decode(revision, bytes)
      end
    end
  end

  defp do_execute(config, {:put_new, key, value}) do
    with {:ok, bytes} <- Value.encode(value), {:ok, encoded_key} <- Value.encode(key) do
      sql =
        "INSERT INTO #{config.table} AS current (sort_key,key,value) VALUES ($1,$2,$3) ON CONFLICT (sort_key) DO UPDATE SET key=EXCLUDED.key, value=EXCLUDED.value, deleted=false, revision=nextval('#{config.sequence}'), inserted_at=clock_timestamp(), updated_at=clock_timestamp() WHERE current.deleted RETURNING revision"

      mutation(config, sql, [Page.key(key), encoded_key, bytes], :exists)
    end
  end

  defp do_execute(config, {:cas, key, expected, value})
       when is_integer(expected) and expected > 0 do
    with {:ok, bytes} <- Value.encode(value) do
      sql =
        "UPDATE #{config.table} SET value=$3, revision=nextval('#{config.sequence}'), updated_at=clock_timestamp() WHERE sort_key=$1 AND revision=$2 AND NOT deleted RETURNING revision"

      mutation(config, sql, [Page.key(key), expected, bytes], :conflict)
    end
  end

  defp do_execute(config, {:delete, key, expected}) when is_integer(expected) and expected > 0 do
    sql =
      "UPDATE #{config.table} SET value=NULL, deleted=true, revision=nextval('#{config.sequence}'), updated_at=clock_timestamp() WHERE sort_key=$1 AND revision=$2 AND NOT deleted RETURNING revision"

    case mutation(config, sql, [Page.key(key), expected], :conflict) do
      {:ok, _} -> :ok
      other -> other
    end
  end

  defp do_execute(config, {:list, prefix, opts}) do
    with {:ok, page} <- Page.options(prefix, opts) do
      # One PostgreSQL statement/snapshot computes payload budgets before values
      # cross the socket. The extra row is metadata only when the page is full.
      sql =
        "WITH candidates AS (SELECT sort_key,key,revision,value,octet_length(key)+octet_length(value)+64 AS bytes FROM #{config.table} WHERE NOT deleted AND sort_key >= $1 AND sort_key < $2 AND ($3::bytea IS NULL OR sort_key > $3) ORDER BY sort_key LIMIT $4), budgeted AS (SELECT *,sum(bytes) OVER (ORDER BY sort_key) AS used,row_number() OVER (ORDER BY sort_key) AS row FROM candidates) SELECT sort_key,CASE WHEN used <= $5 AND row <= $6 THEN key END,revision,CASE WHEN used <= $5 AND row <= $6 THEN value END FROM budgeted ORDER BY sort_key"

      with {:ok, result} <-
             query(config, sql, [
               page.prefix,
               Page.upper_bound(page.prefix),
               page.after,
               page.limit + 1,
               page.max_bytes,
               page.limit
             ]) do
        decode_page(result.rows, page)
      end
    end
  end

  defp do_execute(_, _), do: {:error, :invalid_store_operation}

  defp mutation(config, sql, params, conflict) do
    with {:ok, result} <- durable_query(config, sql, params) do
      case result.rows do
        [[revision]] -> {:ok, revision}
        [] -> {:error, conflict}
      end
    end
  end

  defp durable_query(config, sql, params) do
    config.repo.transaction(
      fn ->
        with {:ok, _} <- query(config, "SET LOCAL synchronous_commit=on", []),
             {:ok, result} <- query(config, sql, params) do
          result
        else
          {:error, reason} -> config.repo.rollback(reason)
        end
      end,
      log: false,
      timeout: config.timeout
    )
    |> case do
      {:ok, result} -> {:ok, result}
      {:error, reason} -> {:error, reason}
    end
  end

  defp query(config, sql, params) do
    case apply(@sql, :query, [config.repo, sql, params, [log: false, timeout: config.timeout]]) do
      {:ok, result} -> {:ok, result}
      {:error, _} -> {:error, {:ambiguous, :store_query_failed}}
    end
  end

  defp decode(revision, bytes) do
    with {:ok, value} <- Value.decode(bytes), do: {:ok, %{revision: revision, value: value}}
  end

  defp decode_page([], _page), do: {:ok, [], nil}
  defp decode_page([[_, nil, _, _] | _], _page), do: {:error, :store_page_entry_too_large}

  defp decode_page(rows, page) do
    {selected, remaining} = Enum.split_while(rows, fn [_, key, _, _] -> not is_nil(key) end)

    Enum.reduce_while(selected, {:ok, []}, fn [_, key, revision, bytes], {:ok, entries} ->
      with {:ok, key} <- Value.decode(key), {:ok, entry} <- decode(revision, bytes) do
        {:cont, {:ok, [{key, entry} | entries]}}
      else
        error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, entries} ->
        cursor = if remaining != [], do: Page.cursor(page, selected |> List.last() |> hd())
        {:ok, Enum.reverse(entries), cursor}

      error ->
        error
    end
  end

  defp names(opts) do
    schema = Keyword.get(opts, :schema, "public")
    table = Keyword.get(opts, :table, "spectre_server_store")

    if is_binary(schema) and is_binary(table) and Regex.match?(@identifier, schema) and
         Regex.match?(@identifier, table) do
      {:ok, %{table: ~s("#{schema}"."#{table}"), sequence: ~s("#{schema}"."#{table}_revision")}}
    else
      {:error, :invalid_postgres_store_names}
    end
  end
end
