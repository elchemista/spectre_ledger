defmodule Mix.Tasks.SpectreLedger.Doctor do
  @moduledoc "Runs read-only Spectre Ledger diagnostics in text or JSON form."

  use Mix.Task

  alias Spectre.Ledger.Doctor
  alias Spectre.Ledger.Doctor.Report

  @shortdoc "Check Spectre Ledger contracts and storage"
  @switches [
    backend: :string,
    format: :string,
    strict: :boolean,
    repo: :string,
    namespace: :string,
    schema: :string,
    table_prefix: :string
  ]

  @impl Mix.Task
  @doc false
  @spec run([String.t()]) :: :ok | no_return()
  def run(argv) do
    Mix.Task.run("app.config")
    {opts, args, invalid} = OptionParser.parse(argv, strict: @switches)
    if args != [] or invalid != [], do: fail("invalid_arguments", "invalid arguments")

    format = format!(opts[:format] || "text")
    doctor_opts = doctor_options!(opts)

    report =
      case Doctor.run(doctor_opts) do
        {:ok, report} -> report
        {:error, _reason} -> fail("invalid_options", "invalid doctor options")
      end

    Mix.shell().info(Report.format(report, format))
    enforce!(report, opts[:strict] == true)
    :ok
  end

  defp doctor_options!(opts) do
    backend = backend!(opts[:backend] || "memory")

    [backend: backend]
    |> put_option(:repo, repo!(opts[:repo]))
    |> put_option(:namespace, opts[:namespace])
    |> put_option(:schema, opts[:schema])
    |> put_option(:table_prefix, opts[:table_prefix])
  end

  defp backend!("memory"), do: :memory
  defp backend!("postgres"), do: :postgres
  defp backend!(_backend), do: fail("invalid_backend", "expected --backend memory or postgres")

  defp format!("text"), do: :text
  defp format!("json"), do: :json
  defp format!(_format), do: fail("invalid_format", "expected --format text or json")

  defp repo!(nil), do: nil

  defp repo!(name) do
    parts =
      name
      |> String.trim()
      |> String.trim_leading("Elixir.")
      |> String.split(".", trim: true)

    if parts != [] and Enum.all?(parts, &Regex.match?(~r/^[A-Z][A-Za-z0-9_]*$/, &1)) do
      module = Module.safe_concat(parts)

      if Code.ensure_loaded?(module),
        do: module,
        else: fail("invalid_repo", "Repo module is unavailable")
    else
      fail("invalid_repo", "Repo module is unavailable")
    end
  rescue
    _exception -> fail("invalid_repo", "Repo module is unavailable")
  end

  defp put_option(opts, _key, nil), do: opts
  defp put_option(opts, key, value), do: Keyword.put(opts, key, value)

  defp enforce!(report, strict?) do
    unless Report.acceptable?(report, strict: strict?) do
      code = if report.summary.errors == 0 and strict?, do: "strict_failed", else: "failed"
      fail(code, "checks did not pass")
    end
  end

  @spec fail(String.t(), String.t()) :: no_return()
  defp fail(code, message), do: Mix.raise("[spectre_ledger_doctor_#{code}] #{message}")
end
