defmodule SpectreLedger.PostgresGeneratorTest do
  use ExUnit.Case, async: false

  alias Mix.Tasks.SpectreLedger.Gen.Migration, as: Generator
  alias Spectre.Ledger.Backend.Postgres.Migration
  alias Spectre.Ledger.Backend.Postgres.Names

  test "generator embeds validated SQL without a runtime Ledger dependency" do
    root = temporary_directory!("generator")
    on_exit(fn -> File.rm_rf!(root) end)

    File.cd!(root, fn ->
      Mix.Task.reenable("spectre_ledger.gen.migration")

      assert :ok =
               Generator.run([
                 "--timestamp",
                 "20260813010203",
                 "--schema",
                 "audit",
                 "--table-prefix",
                 "spectre_history"
               ])

      path = "priv/repo/migrations/20260813010203_create_spectre_ledger.exs"
      assert File.regular?(path)
      source = File.read!(path)
      assert source =~ "defmodule CreateSpectreLedger20260813010203"
      assert source =~ ~s(\\\"audit\\\".\\\"spectre_history_entries\\\")
      assert source =~ ~s(\\\"audit\\\".\\\"spectre_history_receipt_entries\\\")
      assert source =~ "inference_attempt_terminal"
      refute source =~ "Spectre.Ledger.Backend"
      assert {:ok, _ast} = Code.string_to_quoted(source)

      Mix.Task.reenable("spectre_ledger.gen.migration")

      assert_raise Mix.Error, ~r/refusing to overwrite/, fn ->
        Generator.run(["--timestamp", "20260813010203"])
      end
    end)
  end

  test "dry-run writes nothing and unsafe identifiers are rejected" do
    root = temporary_directory!("dry-run")
    on_exit(fn -> File.rm_rf!(root) end)

    File.cd!(root, fn ->
      Mix.Task.reenable("spectre_ledger.gen.migration")
      assert :ok = Generator.run(["--timestamp", "20260813010204", "--dry-run"])
      refute File.exists?("priv")

      Mix.Task.reenable("spectre_ledger.gen.migration")

      assert_raise Mix.Error, ~r/invalid PostgreSQL naming/, fn ->
        Generator.run(["--schema", "public; select pg_sleep(10)"])
      end

      Mix.Task.reenable("spectre_ledger.gen.migration")

      assert_raise Mix.Error, ~r/must stay inside the current project/, fn ->
        Generator.run(["--path", System.tmp_dir!(), "--dry-run"])
      end

      Mix.Task.reenable("spectre_ledger.gen.migration")

      assert_raise Mix.Error, ~r/must stay inside the current project/, fn ->
        Generator.run(["--path", "../outside", "--dry-run"])
      end
    end)
  end

  test "migration SQL options are closed and unambiguous" do
    assert {:ok, _default_up} = Migration.up_sql()
    assert {:error, :invalid_postgres_naming_options} = Migration.up_sql(unknown: true)

    assert {:error, :invalid_postgres_naming_options} =
             Migration.up_sql(schema: "public", schema: "audit")

    assert {:error, :invalid_postgres_naming_options} = Migration.down_sql([:not_keyword])

    assert {:error, :invalid_postgres_naming_options} =
             Names.new(:invalid)

    assert {:error, {:invalid_postgres_identifier, :schema}} =
             Names.new(schema: String.duplicate("a", 64))
  end

  test "generator rejects invalid invocations and overwrites only with force" do
    root = temporary_directory!("errors")
    on_exit(fn -> File.rm_rf!(root) end)

    File.cd!(root, fn ->
      Mix.Task.reenable("spectre_ledger.gen.migration")

      assert_raise Mix.Error, ~r/expected: mix spectre_ledger.gen.migration/, fn ->
        Generator.run(["unexpected"])
      end

      Mix.Task.reenable("spectre_ledger.gen.migration")

      assert_raise Mix.Error, ~r/exactly 14 digits/, fn ->
        Generator.run(["--timestamp", "2026"])
      end

      path = "priv/repo/migrations/20260813010205_create_spectre_ledger.exs"
      File.mkdir_p!(Path.dirname(path))
      File.write!(path, "old")
      Mix.Task.reenable("spectre_ledger.gen.migration")

      assert :ok = Generator.run(["--timestamp", "20260813010205", "--force"])
      refute File.read!(path) == "old"

      directory_path = "priv/repo/migrations/20260813010206_create_spectre_ledger.exs"
      File.mkdir_p!(directory_path)
      Mix.Task.reenable("spectre_ledger.gen.migration")

      assert_raise Mix.Error, ~r/refusing to replace directory/, fn ->
        Generator.run(["--timestamp", "20260813010206", "--force"])
      end
    end)
  end

  defp temporary_directory!(label) do
    path =
      Path.join(
        System.tmp_dir!(),
        "spectre-ledger-postgres-#{label}-#{System.unique_integer([:positive, :monotonic])}"
      )

    File.mkdir_p!(path)
    path
  end
end
