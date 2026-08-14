defmodule Mix.Tasks.SpectreLedger.Gen.Migration do
  @moduledoc """
  Generates an Ecto migration for the PostgreSQL Ledger backend.

      mix spectre_ledger.gen.migration
      mix spectre_ledger.gen.migration --path priv/my_repo/migrations
      mix spectre_ledger.gen.migration --schema audit --table-prefix spectre_ledger

  The generated file embeds validated, quoted SQL and therefore does not call
  Ledger while the host runs migrations. It never overwrites a file unless
  `--force` is explicitly supplied.

  ## Options

    * `--path` - destination directory, default `priv/repo/migrations`;
    * `--schema` - PostgreSQL schema, default `public`;
    * `--table-prefix` - Ledger table prefix, default `spectre_ledger`;
    * `--dry-run` - report the destination without writing;
    * `--force` - allow replacement of an existing regular file.
  """

  use Mix.Task

  alias Spectre.Ledger.Backend.Postgres.Migration
  alias Spectre.Ledger.Backend.Postgres.Names

  @shortdoc "Generate the PostgreSQL Ledger migration"
  @switches [
    path: :string,
    schema: :string,
    table_prefix: :string,
    timestamp: :string,
    dry_run: :boolean,
    force: :boolean
  ]

  @impl Mix.Task
  @doc false
  def run(argv) do
    Mix.Task.run("app.config")
    {opts, args, invalid} = OptionParser.parse(argv, strict: @switches)

    if args != [] or invalid != [] do
      Mix.raise("expected: mix spectre_ledger.gen.migration [options]")
    end

    naming_opts =
      [schema: Keyword.get(opts, :schema, "public")]
      |> Keyword.put(:table_prefix, Keyword.get(opts, :table_prefix, "spectre_ledger"))

    with {:ok, _names} <- Names.new(naming_opts),
         {:ok, up_sql} <- Migration.up_sql(naming_opts),
         {:ok, down_sql} <- Migration.down_sql(naming_opts) do
      generate(opts, up_sql, down_sql)
    else
      {:error, reason} -> Mix.raise("invalid PostgreSQL naming: #{inspect(reason)}")
    end
  end

  defp generate(opts, up_sql, down_sql) do
    timestamp = timestamp!(Keyword.get(opts, :timestamp))
    directory = Keyword.get(opts, :path, "priv/repo/migrations")
    destination = Path.join(directory, "#{timestamp}_create_spectre_ledger.exs")
    force? = Keyword.get(opts, :force, false)
    dry_run? = Keyword.get(opts, :dry_run, false)

    ensure_safe_destination!(destination, force?)
    announce(destination, dry_run?)
    maybe_write_migration(destination, timestamp, up_sql, down_sql, force?, dry_run?)
  end

  defp announce(destination, dry_run?) do
    action = if dry_run?, do: "would create", else: "create"
    Mix.shell().info("#{action} #{destination}")
  end

  defp maybe_write_migration(_destination, _timestamp, _up_sql, _down_sql, _force?, true),
    do: :ok

  defp maybe_write_migration(destination, timestamp, up_sql, down_sql, force?, false) do
    template =
      Application.app_dir(
        :spectre_ledger,
        "priv/templates/spectre_ledger.gen.migration/migration.exs.eex"
      )

    unless File.regular?(template),
      do: Mix.raise("Spectre Ledger migration template is missing")

    Mix.Generator.copy_template(
      template,
      destination,
      [
        migration_module: "CreateSpectreLedger#{timestamp}",
        up_sql: up_sql,
        down_sql: down_sql
      ],
      force: force?,
      quiet: true,
      format_elixir: true
    )

    :ok
  end

  defp timestamp!(nil) do
    DateTime.utc_now()
    |> Calendar.strftime("%Y%m%d%H%M%S")
  end

  defp timestamp!(timestamp) when is_binary(timestamp) do
    if Regex.match?(~r/\A[0-9]{14}\z/, timestamp),
      do: timestamp,
      else: Mix.raise("--timestamp must contain exactly 14 digits")
  end

  defp ensure_safe_destination!(destination, force?) do
    inspect_parents!(Path.dirname(destination))

    case File.lstat(destination) do
      {:error, :enoent} ->
        :ok

      {:ok, %File.Stat{type: :regular}} when force? ->
        :ok

      {:ok, %File.Stat{type: :regular}} ->
        Mix.raise("refusing to overwrite #{destination}; pass --force to replace it")

      {:ok, %File.Stat{type: type}} ->
        Mix.raise("refusing to replace #{type} at #{destination}")

      {:error, reason} ->
        Mix.raise("cannot inspect #{destination}: #{inspect(reason)}")
    end
  end

  defp inspect_parents!(directory) do
    root = File.cwd!() |> Path.expand()
    expanded = Path.expand(directory, root)

    _parent =
      expanded
      |> relative_segments(root)
      |> Enum.reduce(root, fn segment, parent ->
        path = Path.join(parent, segment)

        case File.lstat(path) do
          {:ok, %File.Stat{type: :directory}} ->
            :ok

          {:ok, %File.Stat{type: :symlink}} ->
            Mix.raise("migration parent is a symlink: #{path}")

          {:ok, %File.Stat{type: type}} ->
            Mix.raise("migration parent is #{type}: #{path}")

          {:error, :enoent} ->
            :ok

          {:error, reason} ->
            Mix.raise("cannot inspect migration parent: #{inspect(reason)}")
        end

        path
      end)

    :ok
  end

  defp relative_segments(path, root) do
    relative = Path.relative_to(path, root)
    segments = Path.split(relative)

    cond do
      relative == "." ->
        []

      Path.type(relative) == :absolute ->
        Mix.raise("migration path must stay inside the current project")

      match?([".." | _rest], segments) ->
        Mix.raise("migration path must stay inside the current project")

      true ->
        segments
    end
  end
end
