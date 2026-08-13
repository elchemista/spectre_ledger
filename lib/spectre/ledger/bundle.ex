defmodule Spectre.Ledger.Bundle do
  @moduledoc """
  Deterministic, bounded transport for one complete Ledger checkpoint chain.

  A bundle contains Ledger entries plus the exact checkpoint bytes addressed by
  each entry's `blob_digest`. Verification checks the envelope checksum, entry
  chain, raw byte digests, and Spectre Foundation checkpoint digests.

  Verification delegates checkpoint validation to
  `Spectre.Foundation.Conformance`. Foundation decodes canonical values, and
  `Spectre.Run.Value.prepare/1` may load an existing BEAM module named by a
  checkpoint; normal module loading can run that module's `@on_load` callback.
  `verify/2` is therefore a trusted/local-artifact operation, not a sandbox for
  untrusted bundles.

  Verification does not call `Spectre.Run.restore/1`, start an Instance,
  activate an Agent, or replay model, action, or effect execution. The manifest
  is consequently explicit that the artifact supports checkpoint playback,
  not every-revision capture or deterministic execution replay.
  """

  alias Spectre.Foundation.Conformance, as: Foundation
  alias Spectre.Ledger.Chain
  alias Spectre.Ledger.Entry
  alias Spectre.Ledger.Telemetry

  import Bitwise, only: [bor: 2, bxor: 2]

  @format "spectre/ledger-bundle"
  @version 1
  @digest ~r/\A[0-9a-f]{64}\z/
  @top_keys ~w(format bundle_version manifest entries objects checksum)
  @manifest %{
    "capture" => "persisted_checkpoints",
    "completeness" => "checkpoint_playback",
    "every_revision" => false,
    "deterministic_replay_claim" => false
  }
  @manifest_keys Map.keys(@manifest)

  @default_max_bytes 64 * 1_024 * 1_024
  @default_max_entries 10_000
  @default_max_objects 10_000
  @default_max_object_bytes 8 * 1_024 * 1_024
  @default_max_total_object_bytes 64 * 1_024 * 1_024
  @default_max_depth 32
  @limit_keys [
    :max_bytes,
    :max_entries,
    :max_objects,
    :max_object_bytes,
    :max_total_object_bytes,
    :max_depth
  ]
  @option_keys [:telemetry, :telemetry_handler | @limit_keys]

  @enforce_keys [:format, :bundle_version, :manifest, :entries, :objects, :checksum]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          format: String.t(),
          bundle_version: pos_integer(),
          manifest: map(),
          entries: [Entry.t()],
          objects: %{required(String.t()) => binary()},
          checksum: String.t()
        }

  @type verification_report :: %{
          required(:bundle_version) => pos_integer(),
          required(:capture) => :persisted_checkpoints,
          required(:completeness) => :checkpoint_playback,
          required(:every_revision) => false,
          required(:deterministic_replay_claim) => false,
          required(:entry_count) => pos_integer(),
          required(:object_count) => pos_integer(),
          required(:stream_key_digest) => String.t(),
          required(:head_revision) => non_neg_integer(),
          required(:head_entry_digest) => String.t(),
          required(:checksum) => String.t()
        }

  @doc "Returns the Ledger bundle schema version."
  @spec version() :: 1
  def version, do: @version

  @doc "Returns the immutable completeness claims embedded in every v1 bundle."
  @spec manifest() :: map()
  def manifest, do: @manifest

  @doc """
  Exports entries and their raw checkpoint objects as deterministic JSON.

  `objects` must be an exact map from every entry `blob_digest` to the raw
  checkpoint bytes. Extra or missing objects are rejected.

  Pass `telemetry: false` to suppress both the optional custom handler and the
  standard `:telemetry` event for this operation. The default is `true`.
  """
  @spec export([Entry.t()], %{String.t() => binary()}, keyword()) ::
          {:ok, binary()} | {:error, term()}
  def export(entries, objects, opts \\ []) do
    started_at = System.monotonic_time()

    result =
      with {:ok, limits} <- limits(opts),
           :ok <- collection_limits(entries, objects, limits),
           :ok <- verify_content(entries, objects),
           {:ok, unsigned} <- unsigned_data(entries, objects),
           {:ok, checksum} <- checksum(unsigned),
           data = Map.put(unsigned, "checksum", checksum),
           {:ok, encoded} <- canonical_json(data),
           :ok <- encoded_size(encoded, limits.max_bytes) do
        {:ok, encoded}
      end

    emit(:export, result, entries, objects, started_at, opts)
    result
  end

  @doc """
  Decodes a bounded v1 bundle and verifies its global checksum.

  This checks the closed envelope and content-addressed object encoding. Call
  `verify/2` to additionally verify the Ledger chain and Spectre checkpoints.
  `telemetry: false | true` is accepted so callers can share the same closed
  Bundle options across decode and verification; decode itself emits no event.
  """
  @spec decode(binary(), keyword()) :: {:ok, t()} | {:error, term()}
  def decode(encoded, opts \\ []) do
    with {:ok, limits} <- limits(opts) do
      decode_with_limits(encoded, limits)
    end
  end

  @doc """
  Verifies a JSON bundle or decoded bundle as a trusted/local artifact.

  This calls the Spectre Foundation verifier. Decoding canonical values may
  load existing modules named by the checkpoint and can therefore trigger
  module-load behavior such as `@on_load`. Do not use this function as an
  untrusted-input sandbox. Verify externally supplied bundles on an isolated
  node with a restricted, pre-vetted code path and module set.

  This function does not call `Spectre.Run.restore/1`, start an Instance,
  activate an Agent, or replay model, action, or effect execution.

  Pass `telemetry: false` to suppress both the optional custom handler and the
  standard `:telemetry` event for this operation. The default is `true`.
  """
  @spec verify(binary() | t(), keyword()) ::
          {:ok, verification_report()} | {:error, term()}
  def verify(bundle, opts \\ []) do
    started_at = System.monotonic_time()

    result =
      with {:ok, limits} <- limits(opts),
           {:ok, decoded} <- normalize_bundle(bundle, limits),
           :ok <- verify_content(decoded.entries, decoded.objects),
           {:ok, chain} <- Chain.verify(decoded.entries) do
        {:ok, report(decoded, chain)}
      end

    {entries, objects} = bundle_counts(bundle)
    emit(:verify, result, entries, objects, started_at, opts)
    result
  end

  @doc "Returns the closed JSON-safe bundle data shape."
  @spec to_data(t()) :: map()
  def to_data(%__MODULE__{} = bundle) do
    %{
      "format" => bundle.format,
      "bundle_version" => bundle.bundle_version,
      "manifest" => bundle.manifest,
      "entries" => Enum.map(bundle.entries, &Entry.to_data/1),
      "objects" =>
        Map.new(bundle.objects, fn {digest, bytes} -> {digest, Base.encode64(bytes)} end),
      "checksum" => bundle.checksum
    }
  end

  @spec normalize_bundle(binary() | t(), map()) :: {:ok, t()} | {:error, term()}
  defp normalize_bundle(encoded, limits) when is_binary(encoded),
    do: decode_with_limits(encoded, limits)

  defp normalize_bundle(%__MODULE__{} = bundle, limits) do
    with :ok <- collection_limits(bundle.entries, bundle.objects, limits),
         {:ok, encoded} <- canonical_json(to_data(bundle)),
         :ok <- encoded_size(encoded, limits.max_bytes) do
      decode_with_limits(encoded, limits)
    end
  end

  defp normalize_bundle(_bundle, _limits), do: {:error, :invalid_ledger_bundle}

  @spec decode_with_limits(term(), map()) :: {:ok, t()} | {:error, term()}
  defp decode_with_limits(encoded, limits) when is_binary(encoded) do
    with :ok <- encoded_size(encoded, limits.max_bytes),
         :ok <- json_depth(encoded, limits.max_depth),
         {:ok, ordered} <- decode_json(encoded),
         {:ok, data} <- ordered_to_plain(ordered),
         :ok <- envelope(data),
         :ok <- checksum_matches(data),
         {:ok, entries} <- decode_entries(Map.fetch!(data, "entries"), limits.max_entries),
         {:ok, objects} <- decode_objects(Map.fetch!(data, "objects"), limits),
         :ok <- collection_limits(entries, objects, limits) do
      {:ok,
       %__MODULE__{
         format: @format,
         bundle_version: @version,
         manifest: @manifest,
         entries: entries,
         objects: objects,
         checksum: Map.fetch!(data, "checksum")
       }}
    end
  end

  defp decode_with_limits(_encoded, _limits), do: {:error, :invalid_ledger_bundle}

  @spec unsigned_data([Entry.t()], map()) :: {:ok, map()} | {:error, term()}
  defp unsigned_data(entries, objects) do
    if Enum.all?(objects, fn {digest, bytes} -> digest?(digest) and is_binary(bytes) end) do
      {:ok,
       %{
         "format" => @format,
         "bundle_version" => @version,
         "manifest" => @manifest,
         "entries" => Enum.map(entries, &Entry.to_data/1),
         "objects" => Map.new(objects, fn {digest, bytes} -> {digest, Base.encode64(bytes)} end)
       }}
    else
      {:error, :invalid_ledger_bundle_objects}
    end
  rescue
    _exception -> {:error, :invalid_ledger_bundle_entries}
  end

  @spec envelope(term()) :: :ok | {:error, term()}
  defp envelope(data) when is_map(data) and not is_struct(data) do
    with :ok <- exact_keys(data, @top_keys, :bundle),
         @format <- Map.fetch!(data, "format"),
         @version <- Map.fetch!(data, "bundle_version"),
         :ok <- manifest(Map.fetch!(data, "manifest")),
         true <- is_list(Map.fetch!(data, "entries")),
         true <- is_map(Map.fetch!(data, "objects")),
         true <- digest?(Map.fetch!(data, "checksum")) do
      :ok
    else
      format when is_binary(format) and format != @format ->
        {:error, {:unsupported_ledger_bundle_format, format}}

      version when is_integer(version) and version != @version ->
        {:error, {:unsupported_ledger_bundle_version, version}}

      {:error, _reason} = error ->
        error

      _invalid ->
        {:error, :invalid_ledger_bundle_envelope}
    end
  end

  defp envelope(_data), do: {:error, :invalid_ledger_bundle_envelope}

  @spec manifest(term()) :: :ok | {:error, term()}
  defp manifest(manifest) when is_map(manifest) and not is_struct(manifest) do
    with :ok <- exact_keys(manifest, @manifest_keys, :manifest),
         true <- manifest == @manifest do
      :ok
    else
      {:error, _reason} = error -> error
      false -> {:error, :invalid_ledger_bundle_manifest}
    end
  end

  defp manifest(_manifest), do: {:error, :invalid_ledger_bundle_manifest}

  @spec checksum_matches(map()) :: :ok | {:error, term()}
  defp checksum_matches(data) do
    supplied = Map.fetch!(data, "checksum")

    with {:ok, expected} <- data |> Map.delete("checksum") |> checksum(),
         true <- secure_compare(supplied, expected) do
      :ok
    else
      false -> {:error, :ledger_bundle_checksum_mismatch}
      {:error, _reason} = error -> error
    end
  end

  @spec decode_entries(term(), pos_integer()) :: {:ok, [Entry.t()]} | {:error, term()}
  defp decode_entries(entries, max_entries) when is_list(entries) do
    with :ok <- count_limit(:entries, length(entries), max_entries),
         false <- entries == [] do
      decode_entry_list(entries)
    else
      true -> {:error, :empty_ledger_bundle}
      {:error, _reason} = error -> error
    end
  end

  defp decode_entries(_entries, _max_entries), do: {:error, :invalid_ledger_bundle_entries}

  defp decode_entry_list(entries) do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {data, index}, {:ok, decoded} ->
      case Entry.from_data(data) do
        {:ok, entry} -> {:cont, {:ok, [entry | decoded]}}
        {:error, reason} -> {:halt, {:error, {:invalid_ledger_bundle_entry, index, reason}}}
      end
    end)
    |> reverse_decoded()
  end

  defp reverse_decoded({:ok, decoded}), do: {:ok, Enum.reverse(decoded)}
  defp reverse_decoded({:error, _reason} = error), do: error

  @spec decode_objects(term(), map()) :: {:ok, map()} | {:error, term()}
  defp decode_objects(objects, limits) when is_map(objects) and not is_struct(objects) do
    with :ok <- count_limit(:objects, map_size(objects), limits.max_objects) do
      decode_object_map(objects, limits)
    end
  end

  defp decode_objects(_objects, _limits), do: {:error, :invalid_ledger_bundle_objects}

  defp decode_object_map(objects, limits) do
    objects
    |> Enum.sort_by(&elem(&1, 0))
    |> Enum.reduce_while({:ok, %{}, 0}, fn {digest, encoded}, {:ok, decoded, total} ->
      case decode_object(digest, encoded, limits, total) do
        {:ok, bytes, next_total} ->
          {:cont, {:ok, Map.put(decoded, digest, bytes), next_total}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> decoded_objects()
  end

  defp decoded_objects({:ok, decoded, _total}), do: {:ok, decoded}
  defp decoded_objects({:error, _reason} = error), do: error

  @spec decode_object(term(), term(), map(), non_neg_integer()) ::
          {:ok, binary(), non_neg_integer()} | {:error, term()}
  defp decode_object(digest, encoded, limits, total)
       when is_binary(encoded) and is_binary(digest) do
    max_encoded = 4 * div(limits.max_object_bytes + 2, 3)

    cond do
      not digest?(digest) ->
        {:error, :invalid_ledger_bundle_object_digest}

      byte_size(encoded) > max_encoded ->
        {:error, {:ledger_bundle_object_too_large, limits.max_object_bytes}}

      true ->
        with {:ok, bytes} <- decode_base64(encoded),
             :ok <- object_size(bytes, limits.max_object_bytes),
             next_total = total + byte_size(bytes),
             :ok <- total_object_size(next_total, limits.max_total_object_bytes) do
          {:ok, bytes, next_total}
        end
    end
  end

  defp decode_object(_digest, _encoded, _limits, _total),
    do: {:error, :invalid_ledger_bundle_object}

  @spec verify_content(term(), term()) :: :ok | {:error, term()}
  defp verify_content(entries, objects) when is_list(entries) and is_map(objects) do
    with {:ok, _chain} <- Chain.verify(entries),
         :ok <- exact_object_set(entries, objects) do
      verify_checkpoints(entries, objects)
    end
  end

  defp verify_content(_entries, _objects), do: {:error, :invalid_ledger_bundle_content}

  defp verify_checkpoints(entries, objects) do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {entry, index}, :ok ->
      case verify_checkpoint(entry, Map.fetch!(objects, entry.blob_digest)) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, {:invalid_ledger_bundle_object, index, reason}}}
      end
    end)
  end

  @spec verify_checkpoint(Entry.t(), binary()) :: :ok | {:error, term()}
  defp verify_checkpoint(%Entry{} = entry, checkpoint) when is_binary(checkpoint) do
    with true <- secure_compare(sha256(checkpoint), entry.blob_digest),
         {:ok, report} <-
           Foundation.verify_instance_checkpoint(checkpoint, entry.stream_key),
         true <- secure_compare(report.digest, entry.checkpoint_digest),
         true <- report.revision == entry.revision do
      :ok
    else
      false -> {:error, :ledger_bundle_content_mismatch}
      {:error, _reason} -> {:error, :invalid_spectre_checkpoint}
    end
  end

  defp verify_checkpoint(_entry, _checkpoint), do: {:error, :invalid_spectre_checkpoint}

  @spec exact_object_set([Entry.t()], map()) :: :ok | {:error, term()}
  defp exact_object_set(entries, objects) do
    expected = entries |> Enum.map(& &1.blob_digest) |> MapSet.new()
    supplied = objects |> Map.keys() |> MapSet.new()

    if expected == supplied,
      do: :ok,
      else: {:error, :ledger_bundle_object_set_mismatch}
  end

  @spec collection_limits(term(), term(), map()) :: :ok | {:error, term()}
  defp collection_limits(entries, objects, limits) when is_list(entries) and is_map(objects) do
    with :ok <- count_limit(:entries, length(entries), limits.max_entries),
         :ok <- count_limit(:objects, map_size(objects), limits.max_objects) do
      object_limits(objects, limits)
    end
  end

  defp collection_limits(_entries, _objects, _limits),
    do: {:error, :invalid_ledger_bundle_content}

  defp object_limits(objects, limits) do
    objects
    |> Map.values()
    |> Enum.reduce_while({:ok, 0}, fn
      bytes, {:ok, total} when is_binary(bytes) ->
        reduce_object_size(bytes, total, limits)

      _invalid, _total ->
        {:halt, {:error, :invalid_ledger_bundle_object}}
    end)
    |> case do
      {:ok, _total} -> :ok
      {:error, _reason} = error -> error
    end
  end

  defp reduce_object_size(bytes, total, limits) do
    next_total = total + byte_size(bytes)

    with :ok <- object_size(bytes, limits.max_object_bytes),
         :ok <- total_object_size(next_total, limits.max_total_object_bytes) do
      {:cont, {:ok, next_total}}
    else
      {:error, _reason} = error -> {:halt, error}
    end
  end

  @spec report(t(), map()) :: verification_report()
  defp report(bundle, chain) do
    %{
      bundle_version: @version,
      capture: :persisted_checkpoints,
      completeness: :checkpoint_playback,
      every_revision: false,
      deterministic_replay_claim: false,
      entry_count: length(bundle.entries),
      object_count: map_size(bundle.objects),
      stream_key_digest: sha256(chain.stream_key),
      head_revision: chain.head_revision,
      head_entry_digest: chain.head_entry_digest,
      checksum: bundle.checksum
    }
  end

  @spec limits(term()) :: {:ok, map()} | {:error, term()}
  defp limits(opts) when is_list(opts) do
    if Keyword.keyword?(opts) do
      keys = Keyword.keys(opts)

      cond do
        length(keys) != length(Enum.uniq(keys)) ->
          {:error, :duplicate_ledger_bundle_options}

        Enum.any?(keys, &(&1 not in @option_keys)) ->
          {:error, :unknown_ledger_bundle_options}

        true ->
          validated_limits(opts)
      end
    else
      {:error, :invalid_ledger_bundle_options}
    end
  end

  defp limits(_opts), do: {:error, :invalid_ledger_bundle_options}

  @spec validated_limits(keyword()) :: {:ok, map()} | {:error, term()}
  defp validated_limits(opts) do
    with :ok <- telemetry_option(opts) do
      build_limits(opts)
    end
  end

  @spec telemetry_option(keyword()) :: :ok | {:error, term()}
  defp telemetry_option(opts) do
    case Keyword.fetch(opts, :telemetry) do
      :error -> :ok
      {:ok, value} when is_boolean(value) -> :ok
      {:ok, _value} -> {:error, {:invalid_ledger_bundle_option, :telemetry}}
    end
  end

  @spec build_limits(keyword()) :: {:ok, map()} | {:error, term()}
  defp build_limits(opts) do
    defaults = %{
      max_bytes: @default_max_bytes,
      max_entries: @default_max_entries,
      max_objects: @default_max_objects,
      max_object_bytes: @default_max_object_bytes,
      max_total_object_bytes: @default_max_total_object_bytes,
      max_depth: @default_max_depth
    }

    Enum.reduce_while(@limit_keys, {:ok, defaults}, fn key, {:ok, result} ->
      value = Keyword.get(opts, key, Map.fetch!(defaults, key))

      if is_integer(value) and value > 0 do
        {:cont, {:ok, Map.put(result, key, value)}}
      else
        {:halt, {:error, {:invalid_ledger_bundle_limit, key}}}
      end
    end)
  end

  @spec exact_keys(map(), [String.t()], atom()) :: :ok | {:error, term()}
  defp exact_keys(data, expected, scope) do
    if MapSet.new(Map.keys(data)) == MapSet.new(expected),
      do: :ok,
      else: {:error, {:invalid_ledger_bundle_keys, scope}}
  end

  @spec count_limit(atom(), non_neg_integer(), pos_integer()) :: :ok | {:error, term()}
  defp count_limit(_kind, count, max) when count <= max, do: :ok
  defp count_limit(kind, _count, max), do: {:error, {:ledger_bundle_count_exceeded, kind, max}}

  @spec object_size(binary(), pos_integer()) :: :ok | {:error, term()}
  defp object_size(bytes, max) when byte_size(bytes) <= max, do: :ok
  defp object_size(_bytes, max), do: {:error, {:ledger_bundle_object_too_large, max}}

  @spec total_object_size(non_neg_integer(), pos_integer()) :: :ok | {:error, term()}
  defp total_object_size(total, max) when total <= max, do: :ok

  defp total_object_size(_total, max),
    do: {:error, {:ledger_bundle_objects_too_large, max}}

  @spec encoded_size(binary(), pos_integer()) :: :ok | {:error, term()}
  defp encoded_size(encoded, max) when byte_size(encoded) <= max, do: :ok
  defp encoded_size(_encoded, max), do: {:error, {:ledger_bundle_too_large, max}}

  @spec decode_base64(binary()) :: {:ok, binary()} | {:error, term()}
  defp decode_base64(encoded) do
    case Base.decode64(encoded) do
      {:ok, decoded} ->
        if Base.encode64(decoded) == encoded,
          do: {:ok, decoded},
          else: {:error, :noncanonical_ledger_bundle_base64}

      :error ->
        {:error, :invalid_ledger_bundle_base64}
    end
  end

  @spec decode_json(binary()) :: {:ok, term()} | {:error, term()}
  defp decode_json(encoded) do
    case Jason.decode(encoded, objects: :ordered_objects, strings: :copy) do
      {:ok, decoded} -> {:ok, decoded}
      {:error, _reason} -> {:error, :invalid_ledger_bundle_json}
    end
  rescue
    _exception -> {:error, :invalid_ledger_bundle_json}
  catch
    _kind, _reason -> {:error, :invalid_ledger_bundle_json}
  end

  @spec ordered_to_plain(term()) :: {:ok, term()} | {:error, term()}
  defp ordered_to_plain(%Jason.OrderedObject{values: values}) do
    keys = Enum.map(values, &elem(&1, 0))

    if length(keys) == length(Enum.uniq(keys)) do
      ordered_map_to_plain(values)
    else
      {:error, :duplicate_ledger_bundle_json_key}
    end
  end

  defp ordered_to_plain(values) when is_list(values) do
    Enum.reduce_while(values, {:ok, []}, fn value, {:ok, result} ->
      case ordered_to_plain(value) do
        {:ok, plain} -> {:cont, {:ok, [plain | result]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, result} -> {:ok, Enum.reverse(result)}
      {:error, _reason} = error -> error
    end
  end

  defp ordered_to_plain(value), do: {:ok, value}

  defp ordered_map_to_plain(values) do
    Enum.reduce_while(values, {:ok, %{}}, fn {key, value}, {:ok, result} ->
      case ordered_to_plain(value) do
        {:ok, plain} -> {:cont, {:ok, Map.put(result, key, plain)}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  @spec checksum(map()) :: {:ok, String.t()} | {:error, term()}
  defp checksum(data) do
    with {:ok, encoded} <- canonical_json(data), do: {:ok, sha256(encoded)}
  end

  @spec canonical_json(term()) :: {:ok, binary()} | {:error, term()}
  defp canonical_json(data) do
    case Jason.encode(ordered(data), maps: :strict) do
      {:ok, encoded} -> {:ok, encoded}
      {:error, _reason} -> {:error, :invalid_ledger_bundle_data}
    end
  rescue
    _exception -> {:error, :invalid_ledger_bundle_data}
  end

  defp ordered(value) when is_map(value) and not is_struct(value) do
    value
    |> Enum.sort_by(fn {key, _item} -> key end)
    |> Enum.map(fn {key, item} -> {key, ordered(item)} end)
    |> Jason.OrderedObject.new()
  end

  defp ordered(value) when is_list(value), do: Enum.map(value, &ordered/1)
  defp ordered(value), do: value

  @spec json_depth(binary(), pos_integer()) :: :ok | {:error, term()}
  defp json_depth(encoded, max), do: scan_json(encoded, max, 0, false, false)

  defp scan_json(<<>>, _max, _depth, _in_string, _escaped), do: :ok

  defp scan_json(<<_byte, rest::binary>>, max, depth, true, true),
    do: scan_json(rest, max, depth, true, false)

  defp scan_json(<<?\\, rest::binary>>, max, depth, true, false),
    do: scan_json(rest, max, depth, true, true)

  defp scan_json(<<?\", rest::binary>>, max, depth, true, false),
    do: scan_json(rest, max, depth, false, false)

  defp scan_json(<<_byte, rest::binary>>, max, depth, true, false),
    do: scan_json(rest, max, depth, true, false)

  defp scan_json(<<?\", rest::binary>>, max, depth, false, false),
    do: scan_json(rest, max, depth, true, false)

  defp scan_json(<<byte, _rest::binary>>, max, depth, false, false)
       when byte in [?{, ?[] and depth + 1 > max,
       do: {:error, {:ledger_bundle_depth_exceeded, max}}

  defp scan_json(<<byte, rest::binary>>, max, depth, false, false)
       when byte in [?{, ?[],
       do: scan_json(rest, max, depth + 1, false, false)

  defp scan_json(<<byte, rest::binary>>, max, depth, false, false)
       when byte in [?}, ?]] and depth > 0,
       do: scan_json(rest, max, depth - 1, false, false)

  defp scan_json(<<_byte, rest::binary>>, max, depth, false, false),
    do: scan_json(rest, max, depth, false, false)

  @spec emit(atom(), term(), term(), term(), integer(), keyword()) :: :ok
  defp emit(operation, result, entries, objects, started_at, opts) do
    measurements = %{
      duration_us:
        System.convert_time_unit(System.monotonic_time() - started_at, :native, :microsecond),
      entry_count: safe_count(entries),
      object_count: safe_count(objects)
    }

    metadata =
      case result do
        {:ok, _value} -> %{operation: operation, outcome: :ok}
        {:error, reason} -> %{operation: operation, outcome: :error, reason: reason}
      end

    Telemetry.emit([:bundle, operation, :stop], measurements, metadata, opts)
  end

  defp bundle_counts(%__MODULE__{entries: entries, objects: objects}), do: {entries, objects}
  defp bundle_counts(_bundle), do: {[], %{}}

  defp safe_count(value) when is_list(value), do: length(value)
  defp safe_count(value) when is_map(value), do: map_size(value)
  defp safe_count(_value), do: 0

  defp digest?(value), do: is_binary(value) and Regex.match?(@digest, value)

  defp secure_compare(left, right)
       when is_binary(left) and is_binary(right) and byte_size(left) == byte_size(right),
       do: constant_time_compare(left, right, 0) == 0

  defp secure_compare(_left, _right), do: false

  defp constant_time_compare(<<>>, <<>>, result), do: result

  defp constant_time_compare(
         <<left, left_rest::binary>>,
         <<right, right_rest::binary>>,
         result
       ),
       do: constant_time_compare(left_rest, right_rest, bor(result, bxor(left, right)))

  defp sha256(bytes),
    do: bytes |> then(&:crypto.hash(:sha256, &1)) |> Base.encode16(case: :lower)
end
