defmodule SpectreLedger.InferenceUsageTest do
  use ExUnit.Case, async: true

  alias Spectre.Inference.FrozenSelection
  alias Spectre.Ledger
  alias Spectre.Ledger.Backend.Memory
  alias Spectre.Ledger.InferenceUsage
  alias Spectre.Ledger.ReceiptSink
  alias Spectre.Receipt.Envelope

  test "aggregates cumulative terminal usage and provider calls by frozen model" do
    server = start_supervised!(Memory)
    stream = "instance:usage-summary"
    opts = [backend: :memory, server: server, namespace: unique_namespace()]

    receipts = [
      selected(stream, 1, "inference-1", "invocation-1", "attempt-1", "gpt-ref", 1),
      terminal(
        stream,
        2,
        {"inference-1", "invocation-1", "attempt-1"},
        outcome: :completed,
        provider_started: true,
        usage: %{input_tokens: 10, output_tokens: 5, total_tokens: 15, duration_ms: 20},
        usage_quality: :provider
      ),
      selected(stream, 3, "inference-2", "invocation-2", "attempt-2", "gpt-ref", 2),
      terminal(
        stream,
        4,
        {"inference-2", "invocation-2", "attempt-2"},
        outcome: :failed,
        provider_started: true,
        usage: %{input_tokens: 4, total_tokens: 4, duration_ms: 8},
        usage_quality: :estimated
      ),
      selected(stream, 5, "inference-3", "invocation-3", "attempt-3", "claude-ref", 1),
      terminal(
        stream,
        6,
        {"inference-3", "invocation-3", "attempt-3"},
        outcome: :cancelled_before_provider_start,
        provider_started: false,
        usage: %{},
        usage_quality: :unavailable
      ),
      selected(stream, 7, "inference-open", "invocation-open", "attempt-open", "gpt-ref", 1)
    ]

    Enum.each(receipts, fn receipt ->
      assert {:ok, :appended} = ReceiptSink.append(receipt, opts)
    end)

    assert {:ok, summary} = Ledger.inference_usage(stream, opts)
    assert summary.instance_ref == stream
    assert summary.receipt_count == 7
    assert summary.selection_count == 4
    assert summary.terminal_attempts == 3
    assert summary.provider_calls == 2
    assert summary.completed_attempts == 1
    assert summary.failed_attempts == 1
    assert summary.cancelled_attempts == 1
    assert summary.open_attempts == 1
    assert summary.unmatched_terminal_attempts == 0
    assert summary.usage.input_tokens == 14
    assert summary.usage.output_tokens == 5
    assert summary.usage.total_tokens == 19
    assert summary.usage.duration_ms == 28
    assert summary.usage_quality == %{provider: 1, estimated: 1, unavailable: 1}

    assert [claude, gpt] = summary.models
    assert claude.provider == :anthropic
    assert claude.model == "claude-sonnet"
    assert claude.provider_calls == 0
    assert claude.cancelled_attempts == 1
    assert gpt.provider == :openai
    assert gpt.model == "gpt-studio"
    assert gpt.provider_calls == 2
    assert gpt.usage.total_tokens == 19
    assert gpt.usage_quality == %{provider: 1, estimated: 1, unavailable: 0}
  end

  test "reports unmatched terminal evidence and rejects partial totals" do
    terminal =
      terminal(
        "instance:unmatched",
        1,
        {"inference", "invocation", "attempt"},
        outcome: :failed,
        provider_started: true,
        usage: %{input_tokens: 2, total_tokens: 2},
        usage_quality: :provider
      )

    assert {:ok, summary} = InferenceUsage.summarize([terminal])
    assert summary.provider_calls == 1
    assert summary.unmatched_terminal_attempts == 1
    assert [%{model_ref: nil, usage: %{total_tokens: 2}}] = summary.models

    assert {:error, :partial_ledger_receipt_stream_not_verifiable} =
             Ledger.inference_usage("instance:unmatched", limit: 10)
  end

  test "rejects malformed or duplicate semantic evidence" do
    stream = "instance:invalid-usage"

    invalid =
      terminal(
        stream,
        1,
        {"inference", "invocation", "attempt"},
        outcome: :completed,
        provider_started: true,
        usage: %{},
        usage_quality: :provider
      )

    invalid = %{invalid | payload: Map.put(invalid.payload, :usage, %{total_tokens: -1})}
    invalid_id = invalid.id

    assert {:error, {:invalid_inference_terminal_usage, ^invalid_id}} =
             InferenceUsage.summarize([invalid])

    valid =
      terminal(
        stream,
        2,
        {"inference", "invocation", "attempt"},
        outcome: :completed,
        provider_started: true,
        usage: %{total_tokens: 1},
        usage_quality: :provider
      )

    duplicate = %{valid | id: valid.id <> "-duplicate"}
    duplicate_id = duplicate.id

    assert {:error, {:duplicate_inference_terminal, ^duplicate_id}} =
             InferenceUsage.summarize([valid, duplicate])
  end

  defp selected(stream, revision, inference_id, invocation_id, attempt_id, model_ref, attempt) do
    provider = if model_ref == "claude-ref", do: :anthropic, else: :openai
    model = if model_ref == "claude-ref", do: "claude-sonnet", else: "gpt-studio"

    selection = %FrozenSelection{
      request_id: "request-#{inference_id}",
      model_ref: model_ref,
      selector_ref: "Elixir.Spectre.Prism.Selector",
      profile_hash: "profile-#{model_ref}",
      attempt: attempt,
      metadata: %{prism_provider: provider, provider_model: model}
    }

    inference_envelope(
      :inference_selected,
      stream,
      revision,
      inference_id,
      invocation_id,
      attempt_id,
      "spectre.inference.selected/1",
      %{purpose: :response, attempt: attempt, selection: selection, recoverable?: true}
    )
  end

  defp terminal(stream, revision, {inference_id, invocation_id, attempt_id}, opts) do
    inference_envelope(
      :inference_attempt_terminal,
      stream,
      revision,
      inference_id,
      invocation_id,
      attempt_id,
      "spectre.inference.attempt-terminal/1",
      %{
        outcome: Keyword.fetch!(opts, :outcome),
        provider_started: Keyword.fetch!(opts, :provider_started),
        usage: Keyword.fetch!(opts, :usage),
        usage_quality: Keyword.fetch!(opts, :usage_quality)
      }
    )
  end

  defp inference_envelope(
         kind,
         stream,
         revision,
         inference_id,
         invocation_id,
         attempt_id,
         schema,
         payload
       ) do
    Envelope.new!(
      kind: kind,
      instance_ref: stream,
      run_id: "run-#{inference_id}",
      run_revision: revision,
      inference_id: inference_id,
      invocation_id: invocation_id,
      attempt_id: attempt_id,
      control_revision: 0,
      stream_epoch: "epoch-#{attempt_id}",
      canonical_revision: revision,
      correlation_id: "correlation-#{revision}",
      definition_ref: "definition:usage-test",
      pre_state_digest: digest(revision),
      post_state_digest: digest(revision + 1),
      payload_schema_ref: schema,
      payload: payload,
      privacy: :confidential,
      recorded_at: 1_800_000_000_000 + revision
    )
  end

  defp digest(value),
    do: :crypto.hash(:sha256, Integer.to_string(value)) |> Base.encode16(case: :lower)

  defp unique_namespace,
    do: "usage-#{System.unique_integer([:positive])}"
end
