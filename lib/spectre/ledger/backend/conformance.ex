defmodule Spectre.Ledger.Backend.Conformance do
  @moduledoc """
  ExUnit-independent contract suite for complete Ledger backends.

  The runner first delegates checkpoint semantics and the concurrent CAS race
  to `Spectre.Instance.CheckpointStore.Conformance`. It then verifies the
  Ledger archive surface: ordered chain, immutable object set, deterministic
  bundle, import into an isolated namespace, exact retry, and readback.

  A run writes both its supplied Ref and a derived import namespace. Callers
  must supply a fresh Ref and caller-owned backend resources.
  """

  alias Spectre.Instance.CheckpointStore.Conformance, as: CoreConformance
  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Bundle
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.CheckpointStore
  alias Spectre.Ledger.Config

  @contract_version 1
  @callbacks [
    load: 2,
    compare_and_swap: 2,
    head: 2,
    entries: 3,
    objects: 3,
    put_stream: 4
  ]

  @type report :: %{
          required(:contract_version) => 1,
          required(:checkpoint_store) => map(),
          required(:entry_count) => pos_integer(),
          required(:head_revision) => non_neg_integer(),
          required(:head_entry_digest) => String.t(),
          required(:bundle_checksum) => String.t(),
          required(:import) => :verified
        }

  @doc "Runs checkpoint, chain, bundle, and import conformance for one backend."
  @spec run(keyword(), Ref.t()) :: {:ok, report()} | {:error, term()}
  def run(opts, %Ref{} = ref) when is_list(opts) do
    with {:ok, config} <- normalize_config(opts),
         :ok <- callbacks(config.backend),
         {:ok, core} <- core_conformance(opts, ref),
         {:ok, entries} <- entries(config, ref.key),
         {:ok, chain} <- chain(entries),
         {:ok, head} <- head(config, ref.key),
         :ok <- matching_head(head, chain),
         {:ok, objects} <- objects(config, ref.key),
         {:ok, encoded} <- bundle(entries, objects),
         {:ok, bundle} <- decode_bundle(encoded),
         {:ok, bundle_report} <- verify_bundle(bundle),
         :ok <- import(config, ref, bundle) do
      {:ok,
       %{
         contract_version: @contract_version,
         checkpoint_store: core,
         entry_count: chain.entry_count,
         head_revision: chain.head_revision,
         head_entry_digest: chain.head_entry_digest,
         bundle_checksum: bundle_report.checksum,
         import: :verified
       }}
    end
  rescue
    _exception -> failure(:callback, :raised)
  catch
    _kind, _reason -> failure(:callback, :failed)
  end

  def run(_opts, _ref), do: failure(:options, :invalid)

  defp normalize_config(opts) do
    case Config.new(opts) do
      {:ok, config} -> {:ok, config}
      {:error, _reason} -> failure(:configuration, :invalid)
    end
  end

  defp callbacks(backend) do
    cond do
      not Code.ensure_loaded?(backend) ->
        failure(:configuration, :backend_not_loaded)

      Enum.any?(@callbacks, fn {function, arity} ->
        not function_exported?(backend, function, arity)
      end) ->
        failure(:configuration, :callback_missing)

      true ->
        :ok
    end
  end

  defp core_conformance(opts, ref) do
    case CoreConformance.run({CheckpointStore, opts}, ref) do
      {:ok, report} -> {:ok, report}
      {:error, _reason} -> failure(:checkpoint_store, :failed)
    end
  end

  defp entries(config, stream_key) do
    case config.backend.entries(config, stream_key, []) do
      {:ok, entries} when is_list(entries) and entries != [] -> {:ok, entries}
      _other -> failure(:entries, :invalid_readback)
    end
  end

  defp chain(entries) do
    case Chain.verify(entries) do
      {:ok, report} -> {:ok, report}
      {:error, _reason} -> failure(:chain, :invalid)
    end
  end

  defp head(config, stream_key) do
    case config.backend.head(config, stream_key) do
      {:ok, head} -> {:ok, head}
      _other -> failure(:head, :invalid_readback)
    end
  end

  defp matching_head(head, chain) do
    if head.revision == chain.head_revision and head.entry_digest == chain.head_entry_digest,
      do: :ok,
      else: failure(:head, :mismatch)
  end

  defp objects(config, stream_key) do
    case config.backend.objects(config, stream_key, []) do
      {:ok, objects} when is_map(objects) and map_size(objects) > 0 -> {:ok, objects}
      _other -> failure(:objects, :invalid_readback)
    end
  end

  defp bundle(entries, objects) do
    case Bundle.export(entries, objects) do
      {:ok, encoded} -> {:ok, encoded}
      {:error, _reason} -> failure(:bundle, :export_failed)
    end
  end

  defp decode_bundle(encoded) do
    case Bundle.decode(encoded) do
      {:ok, bundle} -> {:ok, bundle}
      {:error, _reason} -> failure(:bundle, :decode_failed)
    end
  end

  defp verify_bundle(bundle) do
    case Bundle.verify(bundle) do
      {:ok, report} -> {:ok, report}
      {:error, _reason} -> failure(:bundle, :verification_failed)
    end
  end

  defp import(config, ref, bundle) do
    destination = %{config | namespace: import_namespace(config.namespace, ref.key)}
    stream_key = hd(bundle.entries).stream_key
    backend = config.backend

    with {:ok, :imported} <-
           backend.put_stream(destination, stream_key, bundle.entries, bundle.objects),
         {:ok, :idempotent} <-
           backend.put_stream(destination, stream_key, bundle.entries, bundle.objects),
         {:ok, source} <- backend.load(config, ref),
         {:ok, ^source} <- backend.load(destination, ref),
         {:ok, destination_head} <- backend.head(destination, stream_key),
         {:ok, destination_entries} <- backend.entries(destination, stream_key, []),
         {:ok, destination_objects} <- backend.objects(destination, stream_key, []),
         :ok <- imported_head(destination_head, bundle.entries),
         :ok <- imported_entries(destination_entries, bundle.entries),
         :ok <- imported_objects(destination_objects, bundle.objects) do
      :ok
    else
      _other -> failure(:import, :failed)
    end
  end

  defp imported_head(head, entries) do
    expected = List.last(entries)

    if head.entry_digest == expected.entry_digest and head.revision == expected.revision,
      do: :ok,
      else: failure(:import, :head_mismatch)
  end

  defp imported_entries(actual, expected) do
    actual_digests = Enum.map(actual, & &1.entry_digest)
    expected_digests = Enum.map(expected, & &1.entry_digest)

    if actual_digests == expected_digests,
      do: :ok,
      else: failure(:import, :entries_mismatch)
  end

  defp imported_objects(actual, expected) do
    if actual == expected,
      do: :ok,
      else: failure(:import, :objects_mismatch)
  end

  defp import_namespace(namespace, stream_key) do
    suffix =
      stream_key
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)
      |> binary_part(0, 16)

    base = String.slice(namespace, 0, 99)
    "#{base}.conformance.#{suffix}"
  end

  defp failure(phase, code),
    do: {:error, {:ledger_backend_conformance_failed, phase, code}}
end
