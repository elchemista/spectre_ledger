defmodule Spectre.Ledger.InferenceUsage do
  @moduledoc """
  Read-only inference usage projection over validated Spectre receipts.

  Selection receipts identify the frozen model chosen for an attempt. Terminal
  receipts carry cumulative usage and its quality. This projection joins the
  two by their durable inference, invocation, and attempt identities and sums
  each terminal attempt exactly once. Ledger storage remains append-only.

  The `Spectre.Ledger.inference_usage/2` facade verifies the complete Ledger
  receipt chain before calling this projection. Direct callers of
  `summarize/1` are responsible for supplying validated envelopes.
  """

  alias Spectre.Inference.Usage
  alias Spectre.Receipt.Envelope

  @qualities [:provider, :estimated, :unavailable]

  @type summary :: %{
          required(:schema_version) => 1,
          required(:instance_ref) => String.t() | nil,
          required(:receipt_count) => non_neg_integer(),
          required(:selection_count) => non_neg_integer(),
          required(:terminal_attempts) => non_neg_integer(),
          required(:provider_calls) => non_neg_integer(),
          required(:completed_attempts) => non_neg_integer(),
          required(:failed_attempts) => non_neg_integer(),
          required(:cancelled_attempts) => non_neg_integer(),
          required(:open_attempts) => non_neg_integer(),
          required(:unmatched_terminal_attempts) => non_neg_integer(),
          required(:usage) => map(),
          required(:usage_quality) => map(),
          required(:models) => [map()]
        }

  @doc "Builds an inference usage summary from validated receipt envelopes."
  @spec summarize([Envelope.t()]) :: {:ok, summary()} | {:error, term()}
  def summarize(envelopes) when is_list(envelopes) do
    with {:ok, instance_ref} <- instance_ref(envelopes),
         {:ok, selections} <- selections(envelopes),
         {:ok, terminals} <- terminals(envelopes) do
      totals = Enum.reduce(terminals, empty_stats(), &add_terminal(&2, &1, selections))
      selection_keys = selections |> Map.keys() |> MapSet.new()
      terminal_keys = terminals |> Map.keys() |> MapSet.new()

      {:ok,
       %{
         schema_version: 1,
         instance_ref: instance_ref,
         receipt_count: length(envelopes),
         selection_count: map_size(selections),
         terminal_attempts: totals.attempts,
         provider_calls: totals.provider_calls,
         completed_attempts: totals.completed_attempts,
         failed_attempts: totals.failed_attempts,
         cancelled_attempts: totals.cancelled_attempts,
         open_attempts: MapSet.size(MapSet.difference(selection_keys, terminal_keys)),
         unmatched_terminal_attempts:
           MapSet.size(MapSet.difference(terminal_keys, selection_keys)),
         usage: Usage.to_map(totals.usage),
         usage_quality: totals.usage_quality,
         models: model_summaries(terminals, selections)
       }}
    end
  end

  def summarize(_envelopes), do: {:error, :invalid_ledger_inference_receipts}

  @spec selections([Envelope.t()]) :: {:ok, map()} | {:error, term()}
  defp selections(envelopes) do
    envelopes
    |> Enum.filter(&match?(%Envelope{kind: :inference_selected}, &1))
    |> Enum.reduce_while({:ok, %{}}, fn envelope, {:ok, selected} ->
      key = attempt_key(envelope)

      with :ok <- require_attempt_key(key, envelope),
           {:ok, selection} <- selection(envelope),
           false <- Map.has_key?(selected, key) do
        {:cont, {:ok, Map.put(selected, key, selection)}}
      else
        true -> {:halt, {:error, {:duplicate_inference_selection, envelope.id}}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec terminals([Envelope.t()]) :: {:ok, map()} | {:error, term()}
  defp terminals(envelopes) do
    envelopes
    |> Enum.filter(&match?(%Envelope{kind: :inference_attempt_terminal}, &1))
    |> Enum.reduce_while({:ok, %{}}, fn envelope, {:ok, terminal} ->
      key = attempt_key(envelope)

      with :ok <- require_attempt_key(key, envelope),
           {:ok, attempt} <- terminal(envelope),
           false <- Map.has_key?(terminal, key) do
        {:cont, {:ok, Map.put(terminal, key, attempt)}}
      else
        true -> {:halt, {:error, {:duplicate_inference_terminal, envelope.id}}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec selection(Envelope.t()) :: {:ok, map()} | {:error, term()}
  defp selection(%Envelope{} = envelope) do
    with payload when is_map(payload) <- envelope.payload,
         selection when is_map(selection) <- attr(payload, :selection),
         model_ref when is_binary(model_ref) and model_ref != "" <-
           attr(selection, :model_ref),
         metadata <- plain_map(attr(selection, :metadata, %{})) do
      {:ok,
       %{
         model_ref: model_ref,
         profile_hash: attr(selection, :profile_hash),
         provider: attr(metadata, :prism_provider, attr(metadata, :provider)),
         model: attr(metadata, :provider_model, attr(metadata, :model)),
         level: attr(selection, :level),
         attempt: attr(selection, :attempt),
         purpose: attr(payload, :purpose)
       }}
    else
      _invalid -> {:error, {:invalid_inference_selection_receipt, envelope.id}}
    end
  end

  @spec terminal(Envelope.t()) :: {:ok, map()} | {:error, term()}
  defp terminal(%Envelope{} = envelope) do
    with payload when is_map(payload) <- envelope.payload,
         {:ok, provider_started} <- boolean(attr(payload, :provider_started), envelope.id),
         {:ok, quality} <- quality(attr(payload, :usage_quality), envelope.id),
         {:ok, usage} <- usage(attr(payload, :usage, %{}), envelope.id),
         {:ok, outcome} <- outcome(attr(payload, :outcome), envelope.id) do
      {:ok,
       %{
         envelope_id: envelope.id,
         provider_started: provider_started,
         usage_quality: quality,
         usage: usage,
         outcome: outcome
       }}
    else
      {:error, _reason} = error -> error
      _invalid -> {:error, {:invalid_inference_terminal_receipt, envelope.id}}
    end
  end

  @spec usage(term(), String.t()) :: {:ok, Usage.t()} | {:error, term()}
  defp usage(value, envelope_id) do
    {:ok, Usage.new(value)}
  rescue
    ArgumentError -> {:error, {:invalid_inference_terminal_usage, envelope_id}}
  end

  @spec boolean(term(), String.t()) :: {:ok, boolean()} | {:error, term()}
  defp boolean(value, _envelope_id) when is_boolean(value), do: {:ok, value}
  defp boolean(_value, envelope_id), do: {:error, {:invalid_provider_started, envelope_id}}

  @spec quality(term(), String.t()) :: {:ok, atom()} | {:error, term()}
  defp quality(value, _envelope_id) when value in @qualities, do: {:ok, value}

  defp quality(value, envelope_id) when is_binary(value) do
    case Enum.find(@qualities, &(Atom.to_string(&1) == value)) do
      nil -> {:error, {:invalid_inference_usage_quality, envelope_id}}
      quality -> {:ok, quality}
    end
  end

  defp quality(_value, envelope_id),
    do: {:error, {:invalid_inference_usage_quality, envelope_id}}

  @spec outcome(term(), String.t()) :: {:ok, atom() | String.t()} | {:error, term()}
  defp outcome(value, _envelope_id) when is_atom(value) and not is_nil(value), do: {:ok, value}

  defp outcome(value, _envelope_id) when is_binary(value) and value != "",
    do: {:ok, value}

  defp outcome(_value, envelope_id), do: {:error, {:invalid_inference_outcome, envelope_id}}

  @spec add_terminal(map(), {term(), map()}, map()) :: map()
  defp add_terminal(stats, {key, terminal}, selections) do
    stats
    |> increment(:attempts)
    |> maybe_increment(:provider_calls, terminal.provider_started)
    |> increment_outcome(terminal.outcome)
    |> Map.update!(:usage, &Usage.add(&1, terminal.usage))
    |> update_in([:usage_quality, terminal.usage_quality], &(&1 + 1))
    |> Map.update!(:selection_keys, &MapSet.put(&1, selection_key(Map.get(selections, key))))
  end

  @spec model_summaries(map(), map()) :: [map()]
  defp model_summaries(terminals, selections) do
    terminals
    |> Enum.reduce(%{}, fn {key, terminal}, models ->
      selection = Map.get(selections, key)
      model_key = selection_key(selection)
      stats = Map.get(models, model_key, empty_stats())
      Map.put(models, model_key, add_terminal(stats, {key, terminal}, selections))
    end)
    |> Enum.map(fn {model_key, stats} -> model_summary(model_key, stats) end)
    |> Enum.sort_by(fn model ->
      {inspect(model.provider), inspect(model.model), inspect(model.model_ref)}
    end)
  end

  @spec model_summary(tuple(), map()) :: map()
  defp model_summary({model_ref, provider, model, profile_hash}, stats) do
    %{
      model_ref: model_ref,
      provider: provider,
      model: model,
      profile_hash: profile_hash,
      terminal_attempts: stats.attempts,
      provider_calls: stats.provider_calls,
      completed_attempts: stats.completed_attempts,
      failed_attempts: stats.failed_attempts,
      cancelled_attempts: stats.cancelled_attempts,
      usage: Usage.to_map(stats.usage),
      usage_quality: stats.usage_quality
    }
  end

  @spec empty_stats() :: map()
  defp empty_stats do
    %{
      attempts: 0,
      provider_calls: 0,
      completed_attempts: 0,
      failed_attempts: 0,
      cancelled_attempts: 0,
      usage: %Usage{},
      usage_quality: %{provider: 0, estimated: 0, unavailable: 0},
      selection_keys: MapSet.new()
    }
  end

  defp increment(stats, field), do: Map.update!(stats, field, &(&1 + 1))
  defp maybe_increment(stats, _field, false), do: stats
  defp maybe_increment(stats, field, true), do: increment(stats, field)

  defp increment_outcome(stats, outcome) when outcome in [:completed, "completed"],
    do: increment(stats, :completed_attempts)

  defp increment_outcome(stats, outcome)
       when outcome in [
              :cancelled,
              "cancelled",
              :cancelled_before_provider_start,
              "cancelled_before_provider_start"
            ],
       do: increment(stats, :cancelled_attempts)

  defp increment_outcome(stats, _outcome), do: increment(stats, :failed_attempts)

  @spec selection_key(map() | nil) :: tuple()
  defp selection_key(nil), do: {nil, nil, nil, nil}

  defp selection_key(selection),
    do: {selection.model_ref, selection.provider, selection.model, selection.profile_hash}

  @spec attempt_key(Envelope.t()) :: tuple()
  defp attempt_key(envelope),
    do: {envelope.inference_id, envelope.invocation_id, envelope.attempt_id}

  @spec require_attempt_key(tuple(), Envelope.t()) :: :ok | {:error, term()}
  defp require_attempt_key({inference_id, invocation_id, attempt_id}, _envelope)
       when is_binary(inference_id) and inference_id != "" and is_binary(invocation_id) and
              invocation_id != "" and is_binary(attempt_id) and attempt_id != "",
       do: :ok

  defp require_attempt_key(_key, envelope),
    do: {:error, {:invalid_inference_attempt_identity, envelope.id}}

  @spec instance_ref([Envelope.t()]) :: {:ok, String.t() | nil} | {:error, term()}
  defp instance_ref(envelopes) do
    refs = envelopes |> Enum.map(& &1.instance_ref) |> Enum.reject(&is_nil/1) |> Enum.uniq()

    case refs do
      [] -> {:ok, nil}
      [ref] -> {:ok, ref}
      _many -> {:error, :mixed_ledger_instance_receipts}
    end
  end

  defp plain_map(value) when is_map(value) and not is_struct(value), do: value
  defp plain_map(_value), do: %{}

  defp attr(map, key, default \\ nil),
    do: Map.get(map, key, Map.get(map, Atom.to_string(key), default))
end
