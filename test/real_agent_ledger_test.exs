Code.require_file("support/ets_ledger_backend.ex", __DIR__)

defmodule SpectreLedger.RealAgentLedgerTest.Renderer do
  @moduledoc false

  def render(prompt, input, _context), do: "#{prompt}:#{input.text}"
end

defmodule SpectreLedger.RealAgentLedgerTest.Actions do
  @moduledoc false

  def publish_report(%{title: title} = arguments, context) do
    send(Keyword.fetch!(context.opts, :test_pid), {:report_published, arguments})
    {:ok, %{published: title, status: "delivered"}}
  end
end

defmodule SpectreLedger.RealAgentLedgerTest.Operations do
  @moduledoc false

  def assemble_report(%{title: title}, context) do
    send(Keyword.fetch!(context.opts, :test_pid), {:report_assembled, title})

    {:ok,
     %{
       title: title,
       sections: ["policy", "effect", "work"],
       status: "ready"
     }}
  end
end

defmodule SpectreLedger.RealAgentLedgerTest.ReportWork do
  @moduledoc false

  use Spectre.Work,
    id: :ledger_report_work,
    version: 1,
    input: :map,
    state: :map,
    artifact_policy: %{publish_results: true, publish_artifacts: true}

  uses_operation(:assemble_ledger_report)

  @impl true
  def init(%{title: title}, _context) do
    {:ok, %{title: title, phase: :queued, report: nil}}
  end

  @impl true
  def next(%{phase: :queued, title: title}, _context) do
    run(:assemble_ledger_report, %{title: title}, phase: :assembling)
  end

  def next(%{phase: :done}, _context), do: complete(:report_ready)

  @impl true
  def apply_result(state, %{operation: :assemble_ledger_report}, result, _context) do
    {:ok, %{state | phase: :done, report: result.value}, phase: :assembled}
  end

  @impl true
  def complete(%{phase: :done, report: report}, _context), do: complete(report)
  def complete(_state, _context), do: :continue
end

defmodule SpectreLedger.RealAgentLedgerTest.Agent do
  @moduledoc false

  use Spectre.Agent, id: :spectre_ledger_real_agent_test

  alias SpectreLedger.RealAgentLedgerTest.Actions
  alias SpectreLedger.RealAgentLedgerTest.Operations
  alias SpectreLedger.RealAgentLedgerTest.Renderer
  alias SpectreLedger.RealAgentLedgerTest.ReportWork

  actions Actions do
    protect(:publish_report, with: :confirm_publish)
  end

  policy :confirm_publish do
    request(:publish_confirmation)
    accept(:approved, regex: ~r/^yes$/i)
    reject(:rejected, regex: ~r/^no$/i)
    otherwise(ask: :publish_confirmation_retry)
    attempts(3, then: :cancel_pending)
  end

  operation(:assemble_ledger_report, {Operations, :assemble_report},
    input: :map,
    output: :map,
    side_effect: :none
  )

  router(via: [:regex], semantic_cache?: false, classification_log?: false)

  flow :ledger_demo do
    on :PUBLISH_REPORT, regex: ~r/^publish report$/i do
      action(:publish_report,
        args: %{title: "Ledger audit"},
        reply: :publish_confirmation,
        renderer: {Renderer, :render}
      )
    end

    on :BUILD_REPORT, regex: ~r/^build report$/i do
      work(ReportWork,
        input: %{title: "Ledger history"},
        origin: :chat,
        reply_text: "report work started"
      )
    end
  end
end

defmodule SpectreLedger.RealAgentLedgerTest do
  use ExUnit.Case, async: false

  alias Spectre.AgentRef
  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Instance
  alias Spectre.Instance.Canonical
  alias Spectre.Instance.Canonical.Codec
  alias Spectre.Instance.Ref
  alias Spectre.Ledger
  alias Spectre.Ledger.Backend.Conformance, as: BackendConformance
  alias Spectre.Ledger.Bundle
  alias Spectre.Operation.View
  alias Spectre.Run.Ref, as: RunRef
  alias Spectre.Subject
  alias Spectre.Turn
  alias SpectreLedger.TestEtsLedgerBackend, as: EtsBackend

  @agent SpectreLedger.RealAgentLedgerTest.Agent

  test "the ETS test backend satisfies the complete Ledger backend contract" do
    server = start_supervised!(EtsBackend)
    subject = unique_subject("ets-conformance")
    ref = Ref.new(AgentRef.new(@agent), subject)
    opts = ledger_opts(server, unique_namespace("ets_conformance"))

    assert {:ok, report} = BackendConformance.run(opts, ref)
    assert report.contract_version == 1
    assert report.checkpoint_store.concurrent_cas == :single_winner
    assert report.entry_count == 3
    assert report.import == :verified

    snapshot = EtsBackend.snapshot(server)
    assert snapshot.storage == :ets
    assert snapshot.table_size > 0
    assert map_size(snapshot.streams) == 2
    assert Enum.any?(snapshot.writes, &(&1.status == :appended))
    assert Enum.any?(snapshot.writes, &(&1.status == :rejected))
    assert Enum.any?(snapshot.writes, &(&1.status == :imported))
    assert Enum.any?(snapshot.writes, &(&1.status == :idempotent))
  end

  test "a registered real Agent persists policy, Effect, Work, export, import, and restore" do
    server = start_supervised!(EtsBackend)
    subject = unique_subject("real-agent")
    source_namespace = unique_namespace("real_agent")
    imported_namespace = source_namespace <> ".imported"
    source_opts = ledger_opts(server, source_namespace)
    source_store = Ledger.checkpoint_store(source_opts)

    assert {:ok, instance} =
             Spectre.summon(
               agent: @agent,
               subject: subject,
               checkpoint_store: source_store,
               checkpoint_mode: :manual,
               idle: false,
               opts: [test_pid: self()]
             )

    on_exit(fn ->
      if Process.alive?(instance), do: GenServer.stop(instance, :normal)
    end)

    assert {:ok, ^instance} = Spectre.lookup_instance(@agent, subject)
    ref = Instance.ref(instance)

    assert {:ok, %Turn{observable: {:needs, _request}} = staged_turn} =
             Spectre.turn(instance, "publish report")

    assert {:awaiting, %Spectre.Awaitable{name: :confirm_publish, status: :open}, _result} =
             staged_turn.decision

    staged = flush_current!(instance)

    assert {:ok, %Turn{observable: {:awaiting, %RunRef{} = invocation_ref}} = approved_turn} =
             Spectre.turn(instance, "yes")

    assert {:needs, %Spectre.Effect{name: :publish_report, status: :approved}, _result} =
             approved_turn.decision

    approved = flush_current!(instance)

    assert {:ok, %Turn{observable: {:reply, _reply, _reply_ref}} = completed_turn} =
             Spectre.resume(
               instance,
               invocation_ref,
               {:execute, invocation_ref},
               test_pid: self()
             )

    assert {:completed, %Spectre.Effect{name: :publish_report, status: :completed}, _result} =
             completed_turn.decision

    assert_receive {:report_published, %{title: "Ledger audit"}}, 1_000
    assert_eventually(fn -> run_complete?(instance, invocation_ref.run_id) end)
    completed = flush_current!(instance)

    assert {:ok,
            %Turn{
              decision: {:reply, work_result},
              observable: {:reply, "report work started", work_turn_ref}
            }} = Spectre.turn(instance, "build report")

    work_ref = work_result.metadata.operation_ref
    assert_receive {:report_assembled, "Ledger history"}, 1_000

    assert {:ok, %View{status: :terminal, terminal_category: :completed}} =
             eventually_loop(instance, work_ref)

    assert_eventually(fn -> run_complete?(instance, work_turn_ref.run_id) end)
    work_completed = flush_current!(instance)

    persisted = [staged, approved, completed, work_completed]
    persisted_revisions = Enum.map(persisted, & &1.revision)
    assert persisted_revisions == Enum.sort(persisted_revisions)
    assert length(Enum.uniq(persisted_revisions)) == 4

    assert {:ok, entries} = Ledger.entries(ref, source_opts)
    assert Enum.map(entries, & &1.revision) == persisted_revisions

    assert {:ok, %{entry_count: 4, head_revision: head_revision}} =
             Ledger.verify(ref, source_opts)

    assert head_revision == work_completed.revision

    assert {:ok, encoded_bundle} = Ledger.export_bundle(ref, source_opts)

    assert {:ok,
            %{
              entry_count: 4,
              object_count: 4,
              head_revision: ^head_revision,
              every_revision: false,
              deterministic_replay_claim: false
            } = verification} = Bundle.verify(encoded_bundle)

    assert {:ok, decoded_bundle} = Bundle.decode(encoded_bundle)
    analysis = analyze(decoded_bundle)

    assert Enum.map(analysis, & &1.revision) == persisted_revisions
    assert Enum.any?(analysis, &(:waiting_policy in &1.effect_statuses))
    assert Enum.any?(analysis, &(:approved in &1.effect_statuses))
    assert Enum.any?(analysis, &(:completed in &1.effect_statuses))

    final_analysis = List.last(analysis)
    assert final_analysis.run_count >= 2
    assert :started in final_analysis.operation_event_types
    assert :completed in final_analysis.operation_event_types

    assert Enum.any?(final_analysis.work, fn loop ->
             loop.controller_id == :ledger_report_work and loop.status == :terminal and
               loop.result.status == "ready"
           end)

    imported_opts = ledger_opts(server, imported_namespace)

    assert {:ok, :imported, import_report} =
             Ledger.import_bundle(encoded_bundle, imported_opts)

    assert import_report.checksum == verification.checksum

    assert {:ok, :idempotent, _report} =
             Ledger.import_bundle(encoded_bundle, imported_opts)

    snapshot = EtsBackend.snapshot(server)
    assert snapshot.storage == :ets
    assert Map.has_key?(snapshot.streams, {source_namespace, ref.key})
    assert Map.has_key?(snapshot.streams, {imported_namespace, ref.key})

    assert :ok = GenServer.stop(instance, :normal)

    imported_store = Ledger.checkpoint_store(imported_opts)

    assert {:ok, restored} =
             Spectre.summon(
               agent: @agent,
               subject: subject,
               checkpoint_store: imported_store,
               checkpoint_mode: :manual,
               idle: false,
               opts: [test_pid: self()]
             )

    on_exit(fn ->
      if Process.alive?(restored), do: GenServer.stop(restored, :normal)
    end)

    assert {:ok, ^restored} = Spectre.lookup_instance(@agent, subject)
    assert {:ok, restored_checkpoint} = Spectre.checkpoint(restored)
    assert restored_checkpoint == work_completed.checkpoint

    assert {:ok, %View{status: :terminal, terminal_category: :completed}} =
             Spectre.loop(restored, work_ref)

    assert Enum.any?(Spectre.state(restored).planned_effects, fn effect ->
             effect.name == :publish_report and effect.status == :completed
           end)
  end

  defp analyze(%Bundle{} = bundle) do
    Enum.map(bundle.entries, fn entry ->
      checkpoint = Map.fetch!(bundle.objects, entry.blob_digest)
      assert {:ok, canonical} = Codec.decode(checkpoint)
      assert {:ok, flow} = Canonical.fetch(canonical, :flow)
      assert {:ok, runs} = Canonical.fetch(canonical, :runs)
      assert {:ok, work} = Canonical.fetch(canonical, :work)
      assert {:ok, events} = Canonical.fetch(canonical, :events)

      effect_statuses =
        flow.pending_effects
        |> Kernel.++(flow.planned_effects)
        |> Enum.map(& &1.status)
        |> Enum.uniq()

      work =
        Enum.map(work, fn {id, loop} ->
          %{
            id: id,
            controller_id: loop.controller_id,
            status: loop.status,
            result: loop.outcome && loop.outcome.result
          }
        end)

      operation_event_types =
        events
        |> Map.get(:records, [])
        |> Enum.map(& &1.type)
        |> Enum.uniq()

      %{
        revision: entry.revision,
        effect_statuses: effect_statuses,
        run_count: map_size(runs),
        work: work,
        operation_event_types: operation_event_types
      }
    end)
  end

  defp flush_current!(instance, attempts \\ 10)

  defp flush_current!(_instance, 0), do: flunk("checkpoint did not settle")

  defp flush_current!(instance, attempts) do
    assert {:ok, persisted_revision} = Spectre.flush_checkpoint(instance)
    assert {:ok, checkpoint} = Spectre.checkpoint(instance)
    assert {:ok, report} = Foundation.verify_instance_checkpoint(checkpoint)

    if report.revision == persisted_revision do
      %{revision: persisted_revision, checkpoint: checkpoint, digest: report.digest}
    else
      flush_current!(instance, attempts - 1)
    end
  end

  defp eventually_loop(instance, ref, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll_loop(instance, ref, deadline)
  end

  defp poll_loop(instance, ref, deadline) do
    case Spectre.loop(instance, ref) do
      {:ok, %View{status: :terminal}} = result ->
        result

      other ->
        if System.monotonic_time(:millisecond) >= deadline do
          flunk("Work did not finish: #{inspect(other)}")
        else
          Process.sleep(10)
          poll_loop(instance, ref, deadline)
        end
    end
  end

  defp assert_eventually(predicate, timeout \\ 2_000) do
    deadline = System.monotonic_time(:millisecond) + timeout
    poll_predicate(predicate, deadline)
  end

  defp poll_predicate(predicate, deadline) do
    cond do
      predicate.() ->
        :ok

      System.monotonic_time(:millisecond) >= deadline ->
        flunk("condition did not become true")

      true ->
        Process.sleep(10)
        poll_predicate(predicate, deadline)
    end
  end

  defp run_complete?(instance, run_id) do
    match?({:ok, %{status: :complete}}, Instance.run(instance, run_id))
  end

  defp ledger_opts(server, namespace) do
    [backend: EtsBackend, server: server, namespace: namespace]
  end

  defp unique_subject(prefix) do
    Subject.new("#{prefix}-#{System.unique_integer([:positive, :monotonic])}")
  end

  defp unique_namespace(prefix) do
    "#{prefix}.#{System.unique_integer([:positive, :monotonic])}"
  end
end
