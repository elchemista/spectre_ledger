defmodule SpectreLedger.PublicApiManifestTest do
  use ExUnit.Case, async: true

  @manifest_path Path.expand("../docs/PUBLIC_API.md", __DIR__)
  @callable_line ~r/^  - (functions|macros|callbacks): (.+)$/u
  @callable ~r/`([^`\/]+)\/(\d+)`/u
  @module_line ~r/^- `([^`]+)`$/u

  test "normative manifest matches the compiled public BEAM surface" do
    manifest = File.read!(@manifest_path)

    assert manifest =~ "# Spectre Ledger public API — #{Spectre.Ledger.version()}"
    assert manifest =~ "Spectre `~> 0.3.2`"
    assert Mix.Project.config()[:version] == Spectre.Ledger.version()

    {module_names, callables} = parse_manifest(manifest)

    assert module_names != []
    assert callables != []
    assert Enum.uniq(module_names) == module_names
    assert Enum.uniq(callables) == callables

    compiled_modules = application_modules()

    Enum.each(module_names, fn module_name ->
      assert Map.has_key?(compiled_modules, module_name),
             "#{module_name} is declared public but absent from the compiled application"
    end)

    Enum.each(callables, fn {module_name, kind, name, arity} ->
      module = Map.fetch!(compiled_modules, module_name)

      assert {name, arity} in compiled_callables(module, kind),
             "#{module_name}.#{name}/#{arity} is declared as #{kind} but absent from the compiled contract"
    end)
  end

  defp application_modules do
    assert {:ok, modules} = :application.get_key(:spectre_ledger, :modules)

    # `function_exported?/3` never loads a module, so the compiled contract has
    # to be read from modules the code server already holds. Test files load in
    # parallel with the async run, so nothing guarantees another case loaded
    # them first.
    Enum.each(modules, &Code.ensure_loaded!/1)

    Map.new(modules, &{inspect(&1), &1})
  end

  defp parse_manifest(manifest) do
    manifest
    |> String.split("\n")
    |> Enum.reduce({[], [], nil}, &parse_line/2)
    |> then(fn {modules, callables, _current} ->
      {Enum.reverse(modules), Enum.reverse(callables)}
    end)
  end

  defp parse_line(line, {modules, callables, current}) do
    case Regex.run(@module_line, line) do
      [_, module_name] ->
        {[module_name | modules], callables, module_name}

      nil ->
        parse_callable_line(line, modules, callables, current)
    end
  end

  defp parse_callable_line(line, modules, callables, current) do
    case Regex.run(@callable_line, line) do
      [_, kind, entries] when is_binary(current) ->
        parsed = parse_callables!(current, callable_kind(kind), entries)
        {modules, Enum.reverse(parsed, callables), current}

      nil ->
        {modules, callables, current}

      _without_module ->
        raise "public API callable row appears before a module: #{inspect(line)}"
    end
  end

  defp parse_callables!(module_name, kind, entries) do
    parsed =
      Enum.map(Regex.scan(@callable, entries), fn [encoded, name, arity] ->
        {{module_name, kind, name, String.to_integer(arity)}, encoded}
      end)

    residue =
      Enum.reduce(parsed, entries, fn {_callable, encoded}, value ->
        String.replace(value, encoded, "", global: false)
      end)
      |> String.replace(",", "")
      |> String.trim()

    if parsed == [] or residue != "" do
      raise "invalid public API callable row for #{module_name}: #{inspect(entries)}"
    end

    Enum.map(parsed, &elem(&1, 0))
  end

  defp callable_kind("functions"), do: :function
  defp callable_kind("macros"), do: :macro
  defp callable_kind("callbacks"), do: :callback

  defp compiled_callables(module, :function),
    do:
      Enum.map(module.__info__(:functions), fn {name, arity} -> {Atom.to_string(name), arity} end)

  defp compiled_callables(module, :macro),
    do: Enum.map(module.__info__(:macros), fn {name, arity} -> {Atom.to_string(name), arity} end)

  defp compiled_callables(module, :callback) do
    if function_exported?(module, :behaviour_info, 1) do
      Enum.map(module.behaviour_info(:callbacks), fn {name, arity} ->
        {Atom.to_string(name), arity}
      end)
    else
      []
    end
  end
end
