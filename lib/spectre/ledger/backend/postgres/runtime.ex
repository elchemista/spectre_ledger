defmodule Spectre.Ledger.Backend.Postgres.Runtime do
  @moduledoc false

  alias Ecto.Adapters.SQL
  alias Spectre.Ledger.Backend.Postgres.Names
  alias Spectre.Ledger.Config

  @rollback_tag {__MODULE__, :rollback}

  @enforce_keys [:repo, :names, :query_opts, :transaction_opts]
  defstruct [:repo, :names, :query_opts, :transaction_opts]

  @type t :: %__MODULE__{
          repo: module(),
          names: Names.t(),
          query_opts: keyword(),
          transaction_opts: keyword()
        }

  @spec from_config(Config.t()) :: {:ok, t()} | {:error, term()}
  def from_config(%Config{} = config) do
    repo = Config.get_backend(config, :repo)
    query_opts = Config.get_backend(config, :query_opts, log: false)
    transaction_opts = Config.get_backend(config, :transaction_opts, log: false)

    with :ok <- validate_repo(repo),
         :ok <- validate_options(:query_opts, query_opts),
         :ok <- validate_options(:transaction_opts, transaction_opts),
         {:ok, names} <- Names.from_config(config) do
      {:ok,
       %__MODULE__{
         repo: repo,
         names: names,
         query_opts: query_opts,
         transaction_opts: transaction_opts
       }}
    end
  end

  @spec read(t(), iodata(), list()) :: {:ok, SQL.query_result()} | {:error, term()}
  def read(%__MODULE__{} = runtime, sql, params \\ []) do
    SQL.query(runtime.repo, sql, params, runtime.query_opts)
    |> case do
      {:ok, result} -> {:ok, result}
      {:error, exception} -> {:error, {:ledger_postgres_query, error_class(exception)}}
    end
  rescue
    exception -> {:error, {:ledger_postgres_query, error_class(exception)}}
  catch
    kind, _reason -> {:error, {:ledger_postgres_query, kind}}
  end

  @spec query!(t(), iodata(), list()) :: SQL.query_result() | no_return()
  def query!(%__MODULE__{} = runtime, sql, params \\ []) do
    case SQL.query(runtime.repo, sql, params, runtime.query_opts) do
      {:ok, result} -> result
      {:error, exception} -> rollback(runtime, {:ledger_postgres_query, error_class(exception)})
    end
  end

  @spec transaction(t(), (-> term())) :: {:ok, term()} | {:error, term()}
  def transaction(%__MODULE__{} = runtime, fun) when is_function(fun, 0) do
    guarded = fn ->
      require_read_committed!(runtime)
      fun.()
    end

    runtime.repo.transaction(guarded, runtime.transaction_opts)
    |> case do
      {:ok, result} ->
        {:ok, result}

      {:error, {@rollback_tag, reason}} ->
        {:error, reason}

      {:error, reason} ->
        {:error, {:ambiguous, {:ledger_postgres_transaction, error_class(reason)}}}
    end
  rescue
    exception ->
      {:error, {:ambiguous, {:ledger_postgres_transaction, error_class(exception)}}}
  catch
    kind, _reason -> {:error, {:ambiguous, {:ledger_postgres_transaction, kind}}}
  end

  @spec rollback(t(), term()) :: no_return()
  def rollback(%__MODULE__{repo: repo}, reason), do: repo.rollback({@rollback_tag, reason})

  defp validate_repo(repo) when is_atom(repo) and not is_nil(repo) do
    cond do
      not Code.ensure_loaded?(repo) -> {:error, {:ledger_postgres_repo_not_loaded, repo}}
      not function_exported?(repo, :transaction, 2) -> {:error, :invalid_ledger_postgres_repo}
      not function_exported?(repo, :rollback, 1) -> {:error, :invalid_ledger_postgres_repo}
      true -> :ok
    end
  end

  defp validate_repo(_repo), do: {:error, :invalid_ledger_postgres_repo}

  defp validate_options(_kind, value) when is_list(value) do
    if Keyword.keyword?(value), do: :ok, else: {:error, :invalid_ledger_postgres_options}
  end

  defp validate_options(_kind, _value), do: {:error, :invalid_ledger_postgres_options}

  defp require_read_committed!(runtime) do
    case query!(runtime, "SHOW transaction_isolation", []) do
      %{rows: [["read committed"]]} -> :ok
      %{rows: [[_other]]} -> rollback(runtime, :ledger_postgres_requires_read_committed)
      _result -> rollback(runtime, :invalid_ledger_postgres_isolation_result)
    end
  end

  defp error_class(%{__struct__: module}) when is_atom(module), do: module
  defp error_class(reason) when is_atom(reason), do: reason
  defp error_class({kind, _detail}) when is_atom(kind), do: kind
  defp error_class(_reason), do: :unknown
end
