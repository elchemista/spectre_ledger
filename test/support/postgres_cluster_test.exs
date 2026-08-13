defmodule SpectreLedger.TestPostgresCluster do
  @moduledoc false

  @enforce_keys [:root, :data, :socket, :port, :repo, :repo_pid, :repo_supervisor]
  defstruct [:root, :data, :socket, :port, :repo, :repo_pid, :repo_supervisor]

  def start!(repo) do
    unique = System.unique_integer([:positive, :monotonic])
    root = temporary_directory!()
    data = Path.join(root, "data")
    socket = Path.join(root, "socket")
    port = 40_000 + rem(unique, 20_000)

    File.mkdir_p!(data)
    File.mkdir_p!(socket)

    initialize!(repo, root, data, socket, port)
  end

  defp initialize!(repo, root, data, socket, port) do
    run!("initdb", ["-D", data, "--encoding=UTF8", "--no-locale", "--auth=trust"])

    run!("pg_ctl", [
      "-D",
      data,
      "-l",
      Path.join(root, "postgres.log"),
      "-o",
      "-F -k #{socket} -p #{port} -h ''",
      "-w",
      "start"
    ])

    repo_options = [
      database: "postgres",
      username: System.get_env("USER") || "dev",
      socket_dir: socket,
      port: port,
      pool_size: 12,
      queue_target: 5_000,
      queue_interval: 5_000,
      show_sensitive_data_on_connection_error: false
    ]

    start_repo!(repo, repo_options, root, data, socket, port)
  rescue
    exception ->
      stop_postgres(data)
      File.rm_rf!(root)
      reraise exception, __STACKTRACE__
  end

  defp start_repo!(repo, options, root, data, socket, port) do
    {repo_supervisor, repo_pid} = start_supervised_process!(repo, options)

    %__MODULE__{
      root: root,
      data: data,
      socket: socket,
      port: port,
      repo: repo,
      repo_pid: repo_pid,
      repo_supervisor: repo_supervisor
    }
  rescue
    exception ->
      stop_postgres(data)
      File.rm_rf!(root)
      reraise exception, __STACKTRACE__
  end

  defp start_supervised_process!(repo, options) do
    case Supervisor.start_link([{repo, options}], strategy: :one_for_one) do
      {:ok, supervisor} ->
        case Process.whereis(repo) do
          repo_pid when is_pid(repo_pid) -> {supervisor, repo_pid}
          nil -> raise "test Ecto Repo did not register"
        end

      {:error, reason} ->
        raise "could not start Repo supervisor: #{inspect(reason)}"
    end
  end

  def stop(%__MODULE__{} = cluster) do
    if Process.alive?(cluster.repo_supervisor) do
      Supervisor.stop(cluster.repo_supervisor, :normal, 15_000)
    end

    stop_postgres(cluster.data)
    File.rm_rf!(cluster.root)
    :ok
  end

  defp stop_postgres(data) when is_binary(data) do
    if File.regular?(Path.join(data, "postmaster.pid")) do
      System.cmd("pg_ctl", ["-D", data, "-m", "fast", "-w", "stop"], stderr_to_stdout: true)
    end

    :ok
  end

  defp stop_postgres(_data), do: :ok

  defp run!(executable, arguments) do
    case System.cmd(executable, arguments, stderr_to_stdout: true) do
      {_output, 0} -> :ok
      {output, status} -> raise "#{executable} failed (#{status}): #{output}"
    end
  end

  defp temporary_directory! do
    template = Path.join(System.tmp_dir!(), "spectre-ledger-postgres-XXXXXXXX")

    case System.cmd("mktemp", ["-d", template], stderr_to_stdout: true) do
      {path, 0} -> String.trim(path)
      {output, status} -> raise "mktemp failed (#{status}): #{output}"
    end
  end
end

defmodule SpectreLedger.PostgresClusterSupportTest do
  use ExUnit.Case, async: true

  test "support helper is loaded for PostgreSQL integration tests" do
    assert function_exported?(SpectreLedger.TestPostgresCluster, :start!, 1)
    assert function_exported?(SpectreLedger.TestPostgresCluster, :stop, 1)
  end
end
