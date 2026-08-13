defmodule SpectreLedger.HexReleaseContractTest do
  use ExUnit.Case, async: false

  @root Path.expand("..", __DIR__)
  @version "0.1.0"
  @spectre_requirement "~> 0.3.1"
  @documentation_files ["README.md", "CHANGELOG.md", "SECURITY.md"] ++
                         Path.wildcard("docs/*.md", match_dot: true)

  test "Mix and Hex metadata describe the 0.1.0 release for Spectre 0.3.1" do
    config = Mix.Project.config()
    package = Keyword.fetch!(config, :package)

    assert config[:version] == @version
    assert config[:homepage_url] == "https://github.com/elchemista/spectre_ledger"
    assert config[:description] == "Append-only durable checkpoint ledger for Spectre."
    assert package[:name] == "spectre_ledger"
    assert package[:maintainers] == ["elchemista"]
    assert package[:licenses] == ["Apache-2.0"]

    assert package[:links] == %{
             "Documentation" => "https://hexdocs.pm/spectre_ledger/#{@version}",
             "GitHub" => "https://github.com/elchemista/spectre_ledger",
             "Changelog" =>
               "https://github.com/elchemista/spectre_ledger/blob/#{@version}/CHANGELOG.md"
           }

    assert {:ex_doc, "~> 0.40.3", ex_doc_opts} =
             Enum.find(config[:deps], &match?({:ex_doc, _, _}, &1))

    assert ex_doc_opts[:only] == :dev
    assert ex_doc_opts[:runtime] == false
  end

  test "all Markdown guides are in ExDoc and every local link resolves" do
    extras = Mix.Project.config() |> Keyword.fetch!(:docs) |> Keyword.fetch!(:extras)

    assert MapSet.subset?(
             MapSet.new(Path.wildcard("docs/*.md", match_dot: true)),
             MapSet.new(extras)
           )

    assert "docs/PUBLIC_API.md" in extras

    for relative_file <- @documentation_files,
        target <- markdown_targets(relative_file),
        local_target?(target) do
      assert_local_target!(relative_file, target)
    end
  end

  @tag timeout: 30_000
  test "Hex unpack contains the runtime contract and a remote Spectre requirement" do
    unpack_dir =
      Path.join(
        System.tmp_dir!(),
        "spectre-ledger-hex-#{System.unique_integer([:positive, :monotonic])}"
      )

    build_dir = Path.join(unpack_dir, ".mix-build")
    on_exit(fn -> File.rm_rf!(unpack_dir) end)

    {output, status} =
      System.cmd(
        System.find_executable("mix"),
        ["hex.build", "--unpack", "--output", unpack_dir],
        cd: @root,
        env: [
          {"SPECTRE_PATH", nil},
          {"MIX_ENV", "prod"},
          {"MIX_BUILD_PATH", build_dir}
        ],
        stderr_to_stdout: true
      )

    assert status == 0, output
    assert output =~ "Building spectre_ledger #{@version}"

    metadata_path = Path.join(unpack_dir, "hex_metadata.config")
    assert {:ok, metadata} = :file.consult(String.to_charlist(metadata_path))
    assert metadata_value(metadata, "name") == "spectre_ledger"
    assert metadata_value(metadata, "version") == @version

    requirements =
      metadata
      |> metadata_value("requirements")
      |> Map.new(fn requirement ->
        {metadata_value(requirement, "name"), metadata_value(requirement, "requirement")}
      end)

    assert requirements == %{
             "ecto_sql" => "~> 3.14",
             "jason" => "~> 1.4",
             "postgrex" => "~> 0.22.4",
             "spectre" => @spectre_requirement
           }

    files = metadata_value(metadata, "files")
    assert "docs/PUBLIC_API.md" in files
    assert "lib/spectre/ledger/backend/conformance.ex" in files
    assert "priv/templates/spectre_ledger.gen.migration/migration.exs.eex" in files
    refute Enum.any?(files, &String.starts_with?(&1, "test"))
    refute "mix.lock" in files
  end

  test "local Spectre override is development-only and stays on 0.3.1" do
    assert File.read!(Path.join(@root, "mix.exs")) =~
             ~s({:spectre, "#{@spectre_requirement}"})

    assert File.read!(Path.join(@root, "README.md")) =~
             ~s({:spectre, "#{@spectre_requirement}"})

    case System.get_env("SPECTRE_PATH") do
      path when is_binary(path) and path != "" ->
        assert {:spectre, opts} =
                 Enum.find(Mix.Project.config()[:deps], &match?({:spectre, _}, &1))

        assert opts[:override] == true
        assert opts[:path] == Path.expand(path, @root)
        assert Spectre.version() == "0.3.1"

      _other ->
        assert {:spectre, @spectre_requirement} =
                 Enum.find(Mix.Project.config()[:deps], &match?({:spectre, _}, &1))
    end
  end

  defp metadata_value(properties, key) do
    properties
    |> Enum.find_value(fn
      {encoded_key, value} when is_binary(encoded_key) ->
        if encoded_key == key, do: decode_metadata(value)

      _other ->
        nil
    end)
  end

  defp decode_metadata(value) when is_binary(value), do: value
  defp decode_metadata(value) when is_list(value), do: Enum.map(value, &decode_metadata/1)
  defp decode_metadata({key, value}), do: {decode_metadata(key), decode_metadata(value)}
  defp decode_metadata(value), do: value

  defp markdown_targets(relative_file) do
    relative_file
    |> then(&Path.join(@root, &1))
    |> File.read!()
    |> then(&Regex.scan(~r/\[[^\]]+\]\(([^)]+)\)/u, &1, capture: :all_but_first))
    |> List.flatten()
  end

  defp local_target?(target) do
    not String.starts_with?(target, ["#", "http://", "https://", "mailto:"])
  end

  defp assert_local_target!(relative_file, target) do
    path =
      target
      |> String.split("#", parts: 2)
      |> hd()
      |> then(&Path.expand(&1, Path.dirname(Path.join(@root, relative_file))))

    assert File.exists?(path),
           "#{relative_file} links to missing local documentation target #{inspect(target)}"
  end
end
