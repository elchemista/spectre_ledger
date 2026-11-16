ExUnit.start()

# Let the core candidate include the real PostgreSQL integration run in its
# coverage report. Normal package tests and coverage remain independent.
if directory = System.get_env("SPECTRE_CORE_COVERAGE_DIR") do
  unless Code.ensure_loaded?(:cover), do: raise("use --cover with SPECTRE_CORE_COVERAGE_DIR")

  case :cover.start() do
    {:ok, _} -> :ok
    {:error, {:already_started, _}} -> :ok
  end

  modules = Application.spec(:spectre, :modules)

  Enum.each(modules, fn module ->
    {:ok, ^module} = :cover.compile_beam(:code.which(module))
  end)

  ExUnit.after_suite(fn _ ->
    File.mkdir_p!(directory)
    :ok = :cover.export(String.to_charlist(Path.join(directory, "postgres.coverdata")))
  end)
end
