defmodule SpectreLedger.InstanceIntegrationRenderer do
  @moduledoc false

  def render(prompt, input, _context), do: "#{prompt}:#{input.text}"
end

defmodule SpectreLedger.InstanceIntegrationAgent do
  @moduledoc false

  use Spectre.Agent, id: :spectre_ledger_instance_integration

  router(via: [:regex], semantic_cache?: false, classification_log?: false)

  flow :ledger_integration do
    on :HELLO, regex: ~r/^hello$/i do
      reply(:hello, renderer: {SpectreLedger.InstanceIntegrationRenderer, :render})
    end
  end
end

defmodule SpectreLedger.InstanceIntegrationTest do
  use ExUnit.Case, async: false

  alias Spectre.AgentRef
  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Backend.Memory
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.Config
  alias Spectre.Subject

  @agent SpectreLedger.InstanceIntegrationAgent

  test "a real Instance restores and continues from the Ledger checkpoint" do
    server = start_supervised!(Memory)
    subject = Subject.new("ledger-restart-#{System.unique_integer([:positive, :monotonic])}")
    namespace = "instance-restart"

    store =
      Spectre.Ledger.checkpoint_store(backend: :memory, server: server, namespace: namespace)

    ref = Ref.new(AgentRef.new(@agent), subject)

    assert {:ok, first_instance} =
             Spectre.summon(
               agent: @agent,
               subject: subject,
               checkpoint_store: store,
               checkpoint_mode: :manual,
               idle: false
             )

    assert {:ok, first_turn} = Spectre.turn(first_instance, "hello")
    assert {:reply, _result} = first_turn.decision

    assert {:ok, first_revision, first_checkpoint, first_report} =
             flush_current(first_instance)

    assert first_revision > 0

    assert :ok = GenServer.stop(first_instance, :normal)

    assert {:ok, restored_instance} =
             Spectre.summon(
               agent: @agent,
               subject: subject,
               checkpoint_store: store,
               checkpoint_mode: :manual,
               idle: false
             )

    on_exit(fn ->
      if Process.alive?(restored_instance), do: GenServer.stop(restored_instance, :normal)
    end)

    assert {:ok, ^first_checkpoint} = Spectre.checkpoint(restored_instance)
    assert Spectre.checkpoint_status(restored_instance).persisted_revision == first_revision

    assert {:ok, second_turn} = Spectre.turn(restored_instance, "hello")
    assert {:reply, _result} = second_turn.decision

    assert {:ok, second_revision, _second_checkpoint, _second_report} =
             flush_current(restored_instance)

    assert second_revision > first_revision

    assert {:ok, config} =
             Config.new(backend: :memory, server: server, namespace: namespace)

    assert {:ok, entries} = Memory.entries(config, ref.key, [])
    revisions = Enum.map(entries, & &1.revision)
    assert revisions == Enum.sort(revisions)
    assert first_revision in revisions
    assert List.last(revisions) == second_revision

    assert Enum.find(entries, &(&1.revision == first_revision)).checkpoint_digest ==
             first_report.digest

    assert {:ok, %{entry_count: entry_count, head_revision: ^second_revision}} =
             Chain.verify(entries)

    assert entry_count >= 2
  end

  defp flush_current(instance, attempts \\ 5)

  defp flush_current(_instance, 0), do: {:error, :checkpoint_did_not_settle}

  defp flush_current(instance, attempts) do
    with {:ok, persisted_revision} <- Spectre.flush_checkpoint(instance),
         {:ok, checkpoint} <- Spectre.checkpoint(instance),
         {:ok, report} <- Foundation.verify_instance_checkpoint(checkpoint) do
      if report.revision == persisted_revision,
        do: {:ok, persisted_revision, checkpoint, report},
        else: flush_current(instance, attempts - 1)
    end
  end
end
