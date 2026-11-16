# Run from spectre_ledger: SPECTRE_PATH=../spectre MIX_ENV=test mix run bench/otp_profile.exs
# Isolated local PostgreSQL fixture; no production database or external service.
for path <- Path.wildcard(Path.join(to_string(:code.lib_dir()), "tools-*/ebin")),
    do: :code.add_patha(String.to_charlist(path))

ExUnit.start(autorun: false)
Code.require_file("../test/support/postgres_cluster_test.exs", __DIR__)
Logger.configure(level: :warning)

defmodule SpectreOTPProfile.Repo do
  use Ecto.Repo, otp_app: :spectre_ledger, adapter: Ecto.Adapters.Postgres
end

defmodule SpectreOTPProfile.Transport do
  def send_message(message, _) do
    send(SpectreOTPProfile, {:wire, message})
    {:ok, :accepted}
  end
end

defmodule SpectreOTPProfile.FilePayloads do
  @behaviour Spectre.Payload.Store

  # Isolated benchmark adapter: real file I/O, with no in-memory payload cache.
  def put(bytes, _context, opts) do
    reference = Integer.to_string(System.unique_integer([:positive, :monotonic]))

    with :ok <- File.write(Path.join(opts[:root], reference), bytes, [:binary, :sync]),
         do: {:ok, reference}
  end

  def fetch(reference, _context, opts), do: File.read(Path.join(opts[:root], reference))
end

defmodule SpectreOTPProfile.Effect do
  def execute_effect(:open, _, _) do
    send(SpectreOTPProfile, :executed)
    {:ok, %{opened: true}}
  end
end

defmodule SpectreOTPProfile.Effects do
  use Spectre.Stack.Installable, id: :profile_effects, version: "0.1.0", spectre: "~> 0.4.0"

  def effects(_),
    do: %{
      open: %{
        contract: 1,
        adapter: SpectreOTPProfile.Effect,
        operation: :open,
        side_effect: :non_idempotent,
        material: :arguments,
        partition: [],
        schema_hash: nil,
        config: %{}
      }
    }
end

defmodule SpectreOTPProfile.Stack do
  use Spectre.Stack
  install(Spectre.Ledger)
  install(SpectreOTPProfile.Effects)
end

defmodule SpectreOTPProfile.App do
  use Spectre.Server

  def handle_flow({:browse, :open}, input, ctx),
    do: propose(ctx, :open, %{url: input.text}, on_result: :opened)

  def handle_result(:opened, _, ctx), do: no_response(ctx)
  def handle_result(tag, result, ctx), do: super(tag, result, ctx)

  def handle_approval_inbox(_, ctx),
    do: put_private(ctx, :approval_events, Map.get(ctx.private, :approval_events, 0) + 1)
end

defmodule SpectreOTPProfile.Skill do
  use Spectre.Skill, id: :browser, server: SpectreOTPProfile.App

  flow :browse do
    on(:open, regex: ~r/.+/)
  end
end

defmodule SpectreOTPProfile.Agent do
  use Spectre.Agent, stack: SpectreOTPProfile.Stack, server: SpectreOTPProfile.App
  skill(SpectreOTPProfile.Skill, as: :browser, governed: [:open])
end

defmodule SpectreOTPProfile do
  alias Spectre.Ledger.ServerStore.Postgres
  alias SpectreOTPProfile.{Agent, Repo, Stack, Transport}

  @fields [
    :memory,
    :reductions,
    :heap_size,
    :total_heap_size,
    :stack_size,
    :message_queue_len,
    :current_function,
    :status,
    :registered_name,
    :links,
    :monitors,
    :binary
  ]

  def run do
    Process.flag(:trap_exit, true)
    Process.register(self(), __MODULE__)

    root =
      Path.join(System.tmp_dir!(), "spectre-otp-profile-#{System.unique_integer([:positive])}")

    File.mkdir_p!(root)
    output = System.get_env("PROFILE_OUTPUT") || Path.join(root, "results")
    File.mkdir_p!(output)
    cluster = SpectreLedger.TestPostgresCluster.start!(Repo)

    try do
      perform(root, output)
    after
      SpectreLedger.TestPostgresCluster.stop(cluster)
      File.rm_rf!(Path.join(root, "customer"))
      File.rm_rf!(Path.join(root, "owner"))
    end
  end

  defp perform(root, output) do
    if file_payloads?(), do: File.mkdir_p!(Path.join(root, "payloads"))
    {:ok, statements} = Postgres.migration_sql()
    Enum.each(statements, &Ecto.Adapters.SQL.query!(Repo, &1, [], log: false))

    {:ok, %{up: statements}} =
      Spectre.GAM.Ledger.Store.Postgres.migration_sql(table_prefix: "spectre_gam")

    Enum.each(statements, &Ecto.Adapters.SQL.query!(Repo, &1, [], log: false))

    {:ok, %{up: projection}} =
      Spectre.Ledger.DomainStore.migration_sql(table_prefix: "spectre_gam")

    Enum.each(projection, &Ecto.Adapters.SQL.query!(Repo, &1, [], log: false))
    baseline = sample("baseline")

    {:ok, runtime} =
      Spectre.Stack.Runtime.start_link(Stack,
        packages: [spectre_ledger: [server_store: [backend: :postgres, repo: Repo]]]
      )

    {public, private} = :crypto.generate_key(:eddsa, :ed25519)
    principal = %{kind: :human, attributes: %{"subject" => "profile-owner"}}

    customer = [
      address: "spectre://customer",
      transport: {Transport, []},
      approvers: %{
        "spectre://owner" => [principal: principal, public_key: public, mandate_ref: "consent"]
      }
    ]

    owner = [
      address: "spectre://owner",
      transport: {Transport, []},
      maintenance_interval: 200,
      signer: [principal: principal, private_key: private, mandate_ref: "consent"]
    ]

    {:ok, personal} = start_server(root, runtime, "owner", owner, %{})

    {:ok, client} =
      start_server(root, runtime, "customer", customer, %{
        open: [confirm: true, approver: {:external, "spectre://owner"}]
      })

    labels = labels([personal, client], runtime)
    warm = cycle(client, personal, 0)
    drain()
    started = System.monotonic_time(:millisecond)
    batches = String.to_integer(System.get_env("PROFILE_BATCHES") || "10")
    size = String.to_integer(System.get_env("PROFILE_BATCH_SIZE") || "100")
    samples = [sample("warm", labels)]
    sampler = start_sampler()
    profiling = System.get_env("PROFILE_EPROF") == "true"

    if profiling do
      :eprof.start()
      :eprof.log(String.to_charlist(Path.join(output, "eprof.txt")))
      :profiling = :eprof.start_profiling(profile_roots(labels))
    end

    samples =
      Enum.reduce(1..batches, samples, fn batch, acc ->
        for n <- 1..size do
          # Authenticated host input fixture: immutable presentation with a new request identity.
          # Growing the approver's history does not execute the customer's operation.
          id = "profile-#{batch}-#{n}"

          request = %{
            warm
            | "id" => id,
              "expires_at" => System.system_time(:millisecond) + 600_000
          }

          :ok = Spectre.Server.receive_approval(personal, request)
          :ok = Spectre.Server.approval_presented(personal, %{id: id, from: request["from"]})

          :ok =
            Spectre.Server.approval_decide(personal, %{id: id, from: request["from"]}, :reject)

          receive do
            {:wire, %{"type" => "approval.presented"}} -> :ok
          after
            5_000 -> raise "reply missing"
          end

          receive do
            {:wire, %{"type" => "approval.decision"}} -> :ok
          after
            5_000 -> raise "decision missing"
          end

          drain()
        end

        # Actual independent-Domain/Skill execution alongside the growing history.
        cycle(client, personal, batch)
        {:ok, %{entries: page}} = Spectre.Server.approval_inbox(personal, limit: 10)
        true = length(page) == 10
        drain()
        [sample("batch_#{batch}", labels) | acc]
      end)

    if profiling do
      :eprof.stop_profiling()
      :eprof.analyze(:total, sort: :time)
      :eprof.analyze(:procs, sort: :time)
      :eprof.stop()
    end

    idle_ms = String.to_integer(System.get_env("PROFILE_IDLE_MS") || "2000")
    Process.sleep(idle_ms)
    drain()
    idle1 = sample("idle_1", labels)
    Process.sleep(idle_ms)
    drain()
    idle2 = sample("idle_2", labels)
    send(sampler, {:finish, self()})

    peaks =
      receive do
        {:peaks, result} -> result
      after
        5_000 -> raise "sampler missing"
      end

    health = health([personal, client])
    task_cleanup = task_cleanup(runtime)

    {:ok, %{rows: [[records]]}} =
      Ecto.Adapters.SQL.query(
        Repo,
        "SELECT count(*) FROM spectre_server_store WHERE NOT deleted",
        [],
        log: false
      )

    storage = storage_metrics(root)
    elapsed = System.monotonic_time(:millisecond) - started
    for server <- [personal, client], do: GenServer.stop(server, :normal, 30_000)
    Supervisor.stop(runtime)
    Process.sleep(200)
    drain()
    teardown = sample("teardown")

    result = %{
      otp: System.otp_release(),
      elixir: System.version(),
      schedulers: System.schedulers_online(),
      gam_storage:
        if(System.get_env("PROFILE_GAM_STORAGE") == "disk", do: "disk", else: "ledger"),
      payload_store: if(file_payloads?(), do: "file", else: "inline"),
      storage: storage,
      batches: batches,
      inbox_per_batch: size,
      elapsed_ms: elapsed,
      records: records,
      health: health,
      task_cleanup: task_cleanup,
      peaks: peaks,
      samples: [baseline] ++ Enum.reverse(samples) ++ [idle1, idle2, teardown]
    }

    File.write!(Path.join(output, "processes.json"), Jason.encode!(result, pretty: true))
    File.write!(Path.join(output, "report.md"), report(result))
    IO.puts("Profile: #{output}; #{records} live rows; #{elapsed} ms; health=#{inspect(health)}")
  end

  defp start_server(root, runtime, name, approvals, operations) do
    options = [
      agent: Agent,
      stack_runtime: runtime,
      consumer: self(),
      identity: %{subject: name, conversation: "profile"},
      gam: [
        storage: profile_storage(root, name),
        approvals: approvals,
        operations: operations
      ]
    ]

    options =
      if file_payloads?(),
        do:
          Keyword.put(options, :payload_store, {
            SpectreOTPProfile.FilePayloads,
            root: Path.join(root, "payloads")
          }),
        else: options

    Spectre.Server.start_link(options)
  end

  defp file_payloads?(), do: System.get_env("PROFILE_PAYLOAD_STORE") == "file"

  defp storage_metrics(root) do
    queries = [
      coordination:
        "SELECT coalesce(sum(octet_length(value)),0) FROM spectre_server_store WHERE NOT deleted",
      canonical: "SELECT coalesce(sum(octet_length(encoded_entries)),0) FROM spectre_gam_batches",
      projection: "SELECT coalesce(sum(octet_length(value)),0) FROM spectre_gam_projection_rows"
    ]

    database =
      Map.new(queries, fn {kind, query} ->
        %{rows: [[bytes]]} = Ecto.Adapters.SQL.query!(Repo, query, [], log: false)
        {kind, bytes}
      end)

    files = Path.wildcard(Path.join([root, "payloads", "*"]))

    %{
      database_payload_bytes: database,
      external_files: length(files),
      external_bytes: Enum.reduce(files, 0, fn file, total -> total + File.stat!(file).size end)
    }
  end

  defp profile_storage(root, name) do
    if System.get_env("PROFILE_GAM_STORAGE") == "disk",
      do: {:disk, Path.join(root, name)},
      else: :ledger
  end

  defp cycle(client, owner, n) do
    {:ok, _} = Spectre.Server.input(client, "https://fixture.test/#{n}", skill: :browser)

    request =
      receive do
        {:wire, %{"type" => "approval.request"} = r} -> r
      after
        10_000 -> raise "no request"
      end

    :ok = Spectre.Server.receive_approval(owner, request)
    :ok = Spectre.Server.approval_presented(owner, request["id"])

    shown =
      receive do
        {:wire, %{"type" => "approval.presented"} = r} -> r
      after
        5_000 -> raise "no show"
      end

    :ok = Spectre.Server.receive_approval(client, shown)
    :ok = Spectre.Server.approval_decide(owner, request["id"], :confirm)

    decision =
      receive do
        {:wire, %{"type" => "approval.decision"} = r} -> r
      after
        5_000 -> raise "no decision"
      end

    result = Spectre.Server.receive_approval(client, decision)
    true = result == :ok or match?({:ok, _}, result)

    receive do
      :executed -> :ok
    after
      10_000 -> raise "effect missing"
    end

    request
  end

  defp labels(servers, runtime) do
    {:ok, store} = coordination_owner(runtime)
    store_state = :sys.get_state(store)

    base = %{
      runtime => "stack",
      store => "ledger.coordination",
      store_state.supervisor => "ledger.query_supervisor",
      Process.whereis(Repo) => "repo",
      Process.whereis(Spectre.Server.TaskSupervisor) => "server.task_supervisor"
    }

    Enum.reduce(servers, base, fn server, acc ->
      state = :sys.get_state(server)
      managed = :sys.get_state(state.managed_owner)
      gate = :sys.get_state(managed.gate)
      name = to_string(state.identity.subject)

      acc
      |> Map.put(server, name <> ".server")
      |> Map.put(state.managed_owner, name <> ".managed")
      |> Map.put(managed.gate, name <> ".gate")
      |> Map.put(gate.tasks, name <> ".gate_task_supervisor")
      |> Map.put(managed.domain, name <> ".domain")
      |> skill_labels(state.skill_supervisor, name)
    end)
  end

  defp skill_labels(labels, nil, _name), do: labels

  defp skill_labels(labels, supervisor, name) do
    supervisor
    |> Supervisor.which_children()
    |> Enum.reduce(Map.put(labels, supervisor, name <> ".skills"), fn
      {id, pid, _, _}, acc when is_pid(pid) ->
        Map.put(acc, pid, name <> ".skill:" <> to_string(id))

      _, acc ->
        acc
    end)
  end

  defp coordination_owner(runtime) do
    with {:ok, reference} <-
           Spectre.Stack.resolve(Stack, :resource, {:spectre_ledger, :server_store}),
         do: Spectre.Stack.Runtime.resolve(runtime, reference)
  end

  defp task_cleanup(runtime) do
    {:ok, store} = coordination_owner(runtime)
    store_state = :sys.get_state(store)

    %{
      store_queries: map_size(store_state.tasks),
      store_query_children: length(Task.Supervisor.children(store_state.supervisor)),
      shared_server_children: length(Task.Supervisor.children(Spectre.Server.TaskSupervisor))
    }
  end

  defp health(servers) do
    Enum.map(servers, fn server ->
      managed = :sys.get_state(:sys.get_state(server).managed_owner)
      gate = :sys.get_state(managed.gate)
      %{server: inspect(server), io: map_size(gate.io), deliveries: map_size(gate.deliveries)}
    end)
  end

  defp sample(label, labels \\ %{}) do
    # Full GC is intrusive: only use this developer tool in an isolated VM.
    processes = Process.list() -- [self()]
    Enum.each(processes, &:erlang.garbage_collect/1)

    rows =
      Enum.flat_map(processes, fn pid ->
        case Process.info(pid, @fields) do
          nil -> []
          info -> [row(pid, info, labels)]
        end
      end)

    %{
      phase: label,
      processes: rows,
      count: length(rows),
      memory: Enum.sum(Enum.map(rows, & &1.memory)),
      vm_memory: :erlang.memory(:total),
      vm_breakdown: Map.new(:erlang.memory()),
      ports: length(Port.list()),
      ets: ets_inventory()
    }
  end

  defp ets_inventory do
    Enum.flat_map(:ets.all(), fn table ->
      case :ets.info(table) do
        :undefined ->
          []

        info ->
          [
            %{
              id: inspect(table),
              name: inspect(info[:name]),
              owner: inspect(info[:owner]),
              size: info[:size],
              bytes: info[:memory] * :erlang.system_info(:wordsize)
            }
          ]
      end
    end)
  end

  defp row(pid, info, labels) do
    initial =
      case Process.info(pid, :dictionary) do
        {:dictionary, items} -> Keyword.get(items, :"$initial_call")
        _ -> nil
      end

    initial =
      initial ||
        case Process.info(pid, :initial_call) do
          {:initial_call, value} -> value
          _ -> :exited
        end

    info
    |> Map.new()
    |> Map.put(:pid, inspect(pid))
    |> Map.put(:role, labels[pid] || inspect(initial))
    |> Map.update!(:binary, fn bins -> Enum.sum(Enum.map(bins, &elem(&1, 1))) end)
    |> Map.update!(:links, &length/1)
    |> Map.update!(:monitors, &length/1)
    |> Map.update!(:current_function, &inspect/1)
    |> Map.update!(:registered_name, &inspect/1)
  end

  defp profile_roots(labels) do
    owners =
      Enum.filter(Process.list(), fn pid ->
        case Process.info(pid, :dictionary) do
          {:dictionary, items} ->
            case Keyword.get(items, :"$initial_call") do
              {:supervisor, Task.Supervisor, _} ->
                true

              {module, _, _} ->
                module in [
                  Spectre.Server.Runtime,
                  Spectre.GAM.Domain.Sequencer,
                  Spectre.Ledger.ServerStore.Postgres,
                  DBConnection.Connection
                ]

              _ ->
                false
            end

          _ ->
            false
        end
      end)

    Enum.uniq(Map.keys(labels) ++ owners ++ [Process.whereis(Spectre.Server.TaskSupervisor)])
  end

  defp start_sampler do
    spawn_link(fn -> poll(%{max_processes: 0, max_mailbox: 0, max_process_memory: 0}) end)
  end

  defp poll(peaks) do
    pids = Process.list() -- [self()]

    infos =
      Enum.flat_map(pids, fn pid ->
        case Process.info(pid, [:memory, :message_queue_len]) do
          nil -> []
          i -> [i]
        end
      end)

    peaks = %{
      max_processes: max(peaks.max_processes, length(pids)),
      max_mailbox:
        max(peaks.max_mailbox, Enum.max(Enum.map(infos, & &1[:message_queue_len]), fn -> 0 end)),
      max_process_memory:
        max(peaks.max_process_memory, Enum.max(Enum.map(infos, & &1[:memory]), fn -> 0 end))
    }

    receive do
      {:finish, caller} -> send(caller, {:peaks, peaks})
    after
      50 -> poll(peaks)
    end
  end

  defp drain do
    receive do
      _ -> drain()
    after
      0 -> :ok
    end
  end

  defp report(result) do
    lines =
      Enum.map(result.samples, fn s ->
        "| #{s.phase} | #{s.count} | #{s.memory} | #{s.vm_memory} |"
      end)

    "# Isolated OTP profile\n\nOTP #{result.otp}, Elixir #{result.elixir}, #{result.schedulers} schedulers. " <>
      "GAM storage: #{result.gam_storage}; payload store: #{result.payload_store}. #{result.batches * result.inbox_per_batch} synthetic inbox requests plus #{result.batches + 1} real two-Domain Skill approvals; " <>
      "#{result.elapsed_ms} ms. All process samples follow full GC. PostgreSQL rows retained: #{result.records}.\n\n" <>
      "| Phase | Processes | Process bytes | VM bytes |\n| --- | ---: | ---: | ---: |\n" <>
      Enum.join(lines, "\n") <>
      "\n\nPeaks (50 ms sampling): #{inspect(result.peaks)}.\n\n" <>
      "Gate cleanup before teardown: #{inspect(result.health)}.\n\n" <>
      "Query and shared task cleanup before teardown: #{inspect(result.task_cleanup)}.\n\n" <>
      "Stored encoded payload bytes (not total database, index or filesystem allocation): #{inspect(result.storage)}.\n\n" <>
      "Raw JSON contains every live process: reductions, memory, heaps, mailbox, current function, links, monitors and binary bytes; no mailbox or retained value contents. " <>
      "This finite probe does not prove the absence of every leak or production durability.\n"
  end
end

SpectreOTPProfile.run()
