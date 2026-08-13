defmodule SpectreLedger.MixProject do
  use Mix.Project

  @version "0.1.0"
  @source_url "https://github.com/elchemista/spectre_ledger"

  def project do
    [
      app: :spectre_ledger,
      name: "Spectre Ledger",
      version: @version,
      elixir: "~> 1.19",
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      test_coverage: [summary: [threshold: 90]],
      test_ignore_filters: [&String.starts_with?(&1, "test/support/")],
      description: "Append-only durable checkpoint ledger for Spectre.",
      package: package(),
      docs: docs(),
      dialyzer: [plt_add_apps: [:mix, :ecto_sql]],
      source_url: @source_url,
      homepage_url: @source_url
    ]
  end

  def application do
    [extra_applications: [:logger, :crypto]]
  end

  defp deps do
    [
      spectre_dep(),
      {:jason, "~> 1.4"},
      {:ecto_sql, "~> 3.14"},
      {:postgrex, "~> 0.22.4"},
      {:ex_doc, "~> 0.40.3", only: :dev, runtime: false},
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev, :test], runtime: false}
    ]
  end

  defp spectre_dep do
    case System.get_env("SPECTRE_PATH") do
      path when is_binary(path) and path != "" ->
        {:spectre, path: Path.expand(path, __DIR__), override: true}

      _other ->
        {:spectre, "~> 0.3.1"}
    end
  end

  defp package do
    [
      name: "spectre_ledger",
      maintainers: ["elchemista"],
      files: ~w(lib priv docs mix.exs .formatter.exs README.md CHANGELOG.md SECURITY.md LICENSE),
      licenses: ["Apache-2.0"],
      links: %{
        "Documentation" => "https://hexdocs.pm/spectre_ledger/#{@version}",
        "GitHub" => @source_url,
        "Changelog" => "#{@source_url}/blob/#{@version}/CHANGELOG.md"
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      source_ref: @version,
      extras: [
        "README.md",
        "CHANGELOG.md",
        "SECURITY.md",
        "docs/ARCHITECTURE.md",
        "docs/OPERATIONS.md",
        "docs/PUBLIC_API.md"
      ]
    ]
  end
end
