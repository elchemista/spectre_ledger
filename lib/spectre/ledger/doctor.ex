defmodule Spectre.Ledger.Doctor do
  @moduledoc """
  Read-only diagnostics for Spectre Ledger and its configured backend.

  Ledger Doctor composes `Spectre.Doctor` rather than duplicating its runtime,
  Foundation, Agent, Stack, and checkpoint-store checks. It never starts a
  Memory server, Repo, or application and never writes or migrates storage.
  PostgreSQL diagnostics are delegated only to the Ledger PostgreSQL backend's
  explicitly read-only schema probe. A custom backend may expose the same
  read-only `doctor/1` result shape; Doctor never calls its storage callbacks.
  """

  alias Spectre.Ledger.Backend.Memory
  alias Spectre.Ledger.Backend.Postgres
  alias Spectre.Ledger.Bundle
  alias Spectre.Ledger.CheckpointStore
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.Doctor.Report
  alias Spectre.Ledger.Telemetry
  alias Spectre.Stack.Installable

  @contract_version 1
  @reserved_options [:core]
  @required_callbacks [
    load: 2,
    compare_and_swap: 2,
    head: 2,
    entries: 3,
    objects: 3,
    put_stream: 4
  ]

  @doc "Runs composed core, package, bundle, and backend diagnostics."
  @spec run(keyword()) :: {:ok, Report.t()} | {:error, term()}
  def run(opts \\ []) do
    started_at = System.monotonic_time()

    result =
      with :ok <- options(opts),
           {:ok, core_opts, ledger_opts} <- split_options(opts),
           {:ok, config} <- Config.new(ledger_opts),
           {:ok, core} <- core_report(core_opts, ledger_opts) do
        checks = [
          safe("ledger.package", &package_check/0),
          safe("ledger.bundle", &bundle_check/0),
          safe("ledger.backend", fn -> backend_check(config) end)
        ]

        {:ok, report(core, checks)}
      end

    metadata =
      case result do
        {:ok, report} -> %{operation: :doctor, outcome: report.status}
        {:error, reason} -> %{operation: :doctor, outcome: :error, reason: reason}
      end

    Telemetry.emit(
      [:doctor, :stop],
      %{
        duration_us:
          System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond),
        count: 1
      },
      metadata,
      if(is_list(opts), do: opts, else: [])
    )

    result
  end

  @doc "Returns the stable Ledger Doctor report contract version."
  @spec contract_version() :: 1
  def contract_version, do: @contract_version

  defp options(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      keys = Keyword.keys(opts)

      cond do
        length(keys) != length(Enum.uniq(keys)) ->
          {:error, :duplicate_ledger_doctor_options}

        Enum.count(keys, &(&1 in @reserved_options)) > 1 ->
          {:error, :duplicate_ledger_doctor_options}

        true ->
          :ok
      end
    else
      {:error, :invalid_ledger_doctor_options}
    end
  end

  defp options(_opts), do: {:error, :invalid_ledger_doctor_options}

  defp split_options(opts) do
    core_opts = Keyword.get(opts, :core, [])

    if is_list(core_opts) and Keyword.keyword?(core_opts) do
      {:ok, core_opts, Keyword.drop(opts, @reserved_options)}
    else
      {:error, :invalid_ledger_doctor_core_options}
    end
  end

  defp core_report(core_opts, ledger_opts) do
    checkpoint_store = {CheckpointStore, ledger_opts}
    Spectre.Doctor.run(Keyword.put(core_opts, :checkpoint_store, checkpoint_store))
  end

  defp package_check do
    case Installable.verify(Spectre.Ledger) do
      {:ok, package} ->
        check(:ok, "ledger.package", :ledger_package_valid, %{
          version: package.version,
          contract: package.contract
        })

      _error ->
        check(:error, "ledger.package", :ledger_package_invalid)
    end
  end

  defp bundle_check do
    claims = Bundle.manifest()

    check(:ok, "ledger.bundle", :ledger_bundle_contract_observed, %{
      bundle_version: Bundle.version(),
      completeness: Map.fetch!(claims, "completeness"),
      every_revision: Map.fetch!(claims, "every_revision"),
      deterministic_replay_claim: Map.fetch!(claims, "deterministic_replay_claim")
    })
  end

  defp backend_check(%Config{} = config) do
    case callbacks(config.backend) do
      :ok -> backend_diagnostic(config)
      {:error, code} -> check(:error, "ledger.backend", code)
    end
  end

  defp backend_diagnostic(%Config{backend: Memory} = config) do
    configured? =
      case Config.fetch_backend(config, :server) do
        {:ok, server} when is_pid(server) -> Process.alive?(server)
        _other -> false
      end

    code =
      if configured?, do: :memory_backend_volatile, else: :memory_backend_volatile_unconfigured

    check(:warning, "ledger.backend", code, %{
      backend: inspect(Memory),
      durable: false,
      configured: configured?
    })
  end

  defp backend_diagnostic(%Config{backend: Postgres} = config) do
    storage_diagnostic(
      Postgres,
      Postgres.doctor(config),
      :postgres_schema_current,
      :postgres_schema_incomplete,
      :postgres_schema_unavailable,
      :postgres_doctor_result_invalid
    )
  end

  defp backend_diagnostic(%Config{backend: backend} = config) do
    if function_exported?(backend, :doctor, 1) do
      storage_diagnostic(
        backend,
        backend.doctor(config),
        :backend_schema_current,
        :backend_schema_incomplete,
        :backend_schema_unavailable,
        :backend_doctor_result_invalid
      )
    else
      check(:ok, "ledger.backend", :custom_backend_callbacks_valid, %{
        backend: inspect(backend),
        persistence: "backend_defined"
      })
    end
  end

  defp storage_diagnostic(
         backend,
         {:ok,
          %{
            schema_version: schema_version,
            expected_schema_version: expected_schema_version,
            tables: tables
          } = diagnostic},
         current_code,
         incomplete_code,
         _unavailable_code,
         _invalid_code
       )
       when is_integer(expected_schema_version) and is_map(tables) do
    structurally_ready? = Map.get(diagnostic, :ready?, true)

    if structurally_ready? and is_integer(schema_version) and
         schema_version == expected_schema_version and tables_ready?(tables) do
      check(:ok, "ledger.backend", current_code, %{
        backend: inspect(backend),
        schema_version: schema_version,
        table_count: map_size(tables)
      })
    else
      check(:error, "ledger.backend", incomplete_code, %{
        backend: inspect(backend),
        schema_version: schema_version,
        expected_schema_version: expected_schema_version,
        table_count: map_size(tables)
      })
    end
  end

  defp storage_diagnostic(
         backend,
         {:error, reason},
         _current_code,
         _incomplete_code,
         unavailable_code,
         _invalid_code
       ) do
    check(:error, "ledger.backend", unavailable_code, %{
      backend: inspect(backend),
      reason_class: Telemetry.reason_class(reason)
    })
  end

  defp storage_diagnostic(
         backend,
         _result,
         _current_code,
         _incomplete_code,
         _unavailable_code,
         invalid_code
       ),
       do: check(:error, "ledger.backend", invalid_code, %{backend: inspect(backend)})

  defp tables_ready?(tables) do
    Enum.all?(tables, fn
      {_table, value} when value in [true, :ok, :present] -> true
      {_table, %{present: true}} -> true
      _entry -> false
    end)
  end

  defp callbacks(backend) do
    if Code.ensure_loaded?(backend),
      do: loaded_callbacks(backend),
      else: {:error, :ledger_backend_not_loaded}
  end

  defp loaded_callbacks(backend) do
    case Enum.find(@required_callbacks, fn {function, arity} ->
           not function_exported?(backend, function, arity)
         end) do
      nil -> :ok
      _missing -> {:error, :ledger_backend_callback_missing}
    end
  end

  defp safe(id, callback) do
    case callback.() do
      %{id: ^id, status: status, code: code, summary: summary, details: details} = result
      when status in [:ok, :warning, :error, :skipped] and is_atom(code) and
             is_binary(summary) and is_map(details) ->
        result

      _invalid ->
        check(:error, id, :ledger_doctor_check_invalid)
    end
  rescue
    _exception -> check(:error, id, :ledger_doctor_check_exception)
  catch
    _kind, _reason -> check(:error, id, :ledger_doctor_check_failure)
  end

  defp check(status, id, code, details \\ %{}) do
    summary = code |> code_name() |> String.replace("_", " ")
    %{id: id, status: status, code: code, summary: summary, details: details}
  end

  defp code_name(code) when is_atom(code), do: Atom.to_string(code)

  defp report(core, checks) do
    ledger_counts = Enum.frequencies_by(checks, & &1.status)

    summary = %{
      total: core.summary.total + length(checks),
      passed: core.summary.passed + (ledger_counts[:ok] || 0),
      warnings: core.summary.warnings + (ledger_counts[:warning] || 0),
      errors: core.summary.errors + (ledger_counts[:error] || 0),
      skipped: core.summary.skipped + (ledger_counts[:skipped] || 0)
    }

    status =
      cond do
        summary.errors > 0 -> :error
        summary.warnings > 0 -> :warning
        true -> :ok
      end

    %Report{
      contract_version: @contract_version,
      ledger_version: Spectre.Ledger.version(),
      status: status,
      core: core,
      checks: checks,
      summary: summary
    }
  end
end
