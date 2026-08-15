defmodule Spectre.Ledger.ReceiptEntry do
  @moduledoc """
  Immutable append-chain entry for one persisted boundary receipt.

  `sequence` is the physical sink append order. `canonical_revision` remains
  the logical runtime coordinate and may arrive out of order in observational
  mode, so Ledger records both and never conflates them.
  """

  alias Spectre.Canonical.Value
  alias Spectre.Ledger.ReceiptWrite
  alias Spectre.Receipt.Envelope
  alias Spectre.Receipt.Sink

  @format "spectre/ledger-receipt-entry"
  @version 1
  @digest ~r/\A[0-9a-f]{64}\z/
  @kinds Envelope.kinds()
  @kind_by_name Map.new(@kinds, fn kind -> {Atom.to_string(kind), kind} end)

  @enforce_keys [
    :format,
    :entry_version,
    :stream_key,
    :sequence,
    :receipt_id,
    :kind,
    :canonical_revision,
    :envelope_digest,
    :payload_ref,
    :previous_entry_digest,
    :recorded_at,
    :owner_fencing_token,
    :entry_digest
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          format: String.t(),
          entry_version: pos_integer(),
          stream_key: String.t(),
          sequence: pos_integer(),
          receipt_id: String.t(),
          kind: Envelope.kind(),
          canonical_revision: non_neg_integer() | nil,
          envelope_digest: String.t(),
          payload_ref: String.t(),
          previous_entry_digest: String.t() | nil,
          recorded_at: non_neg_integer(),
          owner_fencing_token: non_neg_integer() | nil,
          entry_digest: String.t()
        }

  @doc "Returns the receipt-entry schema version."
  @spec version() :: pos_integer()
  def version, do: @version

  @doc "Finalizes a receipt write against a backend-allocated chain position."
  @spec new(ReceiptWrite.t(), pos_integer(), String.t() | nil) ::
          {:ok, t()} | {:error, term()}
  def new(%ReceiptWrite{} = write, sequence, previous_entry_digest) do
    entry = %__MODULE__{
      format: @format,
      entry_version: @version,
      stream_key: write.stream_key,
      sequence: sequence,
      receipt_id: write.receipt_id,
      kind: write.receipt_kind,
      canonical_revision: write.canonical_revision,
      envelope_digest: write.envelope_digest,
      payload_ref: write.payload_ref,
      previous_entry_digest: previous_entry_digest,
      recorded_at: write.recorded_at,
      owner_fencing_token: write.owner_fencing_token,
      entry_digest: ""
    }

    with :ok <- validate_fields(entry),
         digest <- Value.digest!(identity(entry)) do
      {:ok, %{entry | entry_digest: digest}}
    end
  end

  @doc "Returns the closed portable representation used by storage and bundles."
  @spec to_data(t()) :: map()
  def to_data(%__MODULE__{} = entry) do
    identity(entry)
    |> Map.put("owner_fencing_token", entry.owner_fencing_token)
    |> Map.put("entry_digest", entry.entry_digest)
  end

  @doc "Decodes and verifies one closed receipt-entry representation."
  @spec from_data(map()) :: {:ok, t()} | {:error, term()}
  def from_data(
        %{
          "format" => @format,
          "entry_version" => @version,
          "stream_key" => stream_key,
          "sequence" => sequence,
          "receipt_id" => receipt_id,
          "kind" => kind,
          "canonical_revision" => canonical_revision,
          "envelope_digest" => envelope_digest,
          "payload_ref" => payload_ref,
          "previous_entry_digest" => previous_entry_digest,
          "recorded_at" => recorded_at,
          "owner_fencing_token" => owner_fencing_token,
          "entry_digest" => entry_digest
        } = data
      )
      when map_size(data) == 13 do
    with {:ok, kind} <- decode_kind(kind) do
      entry = %__MODULE__{
        format: @format,
        entry_version: @version,
        stream_key: stream_key,
        sequence: sequence,
        receipt_id: receipt_id,
        kind: kind,
        canonical_revision: canonical_revision,
        envelope_digest: envelope_digest,
        payload_ref: payload_ref,
        previous_entry_digest: previous_entry_digest,
        recorded_at: recorded_at,
        owner_fencing_token: owner_fencing_token,
        entry_digest: entry_digest
      }

      with :ok <- validate_fields(entry),
           true <- Value.digest!(identity(entry)) == entry.entry_digest do
        {:ok, entry}
      else
        false -> {:error, :ledger_receipt_entry_digest_mismatch}
        {:error, _reason} = error -> error
      end
    end
  end

  def from_data(%{"entry_version" => version}),
    do: {:error, {:unsupported_ledger_receipt_entry_version, version}}

  def from_data(_data), do: {:error, :invalid_ledger_receipt_entry}

  @doc "Verifies all fields and the deterministic entry digest."
  @spec verify(t()) :: :ok | {:error, term()}
  def verify(%__MODULE__{} = entry) do
    with :ok <- validate_fields(entry),
         true <- Value.digest!(identity(entry)) == entry.entry_digest do
      :ok
    else
      false -> {:error, :ledger_receipt_entry_digest_mismatch}
      {:error, _reason} = error -> error
    end
  end

  def verify(_entry), do: {:error, :invalid_ledger_receipt_entry}

  @doc "Returns whether an existing entry is the exact identity of a retried write."
  @spec matches_write?(t(), ReceiptWrite.t()) :: boolean()
  def matches_write?(%__MODULE__{} = entry, %ReceiptWrite{} = write) do
    entry.stream_key == write.stream_key and entry.receipt_id == write.receipt_id and
      entry.kind == write.receipt_kind and
      entry.canonical_revision == write.canonical_revision and
      entry.envelope_digest == write.envelope_digest and entry.payload_ref == write.payload_ref and
      entry.recorded_at == write.recorded_at
  end

  def matches_write?(_entry, _write), do: false

  @doc "Verifies that an entry addresses the supplied canonical envelope."
  @spec verify_envelope(t(), Envelope.t()) :: :ok | {:error, term()}
  def verify_envelope(%__MODULE__{} = entry, %Envelope{} = envelope) do
    cond do
      entry.receipt_id != envelope.id ->
        {:error, :ledger_receipt_id_mismatch}

      entry.kind != envelope.kind ->
        {:error, :ledger_receipt_kind_mismatch}

      entry.canonical_revision != envelope.canonical_revision ->
        {:error, :ledger_receipt_revision_mismatch}

      entry.envelope_digest != Envelope.digest(envelope) ->
        {:error, :ledger_receipt_digest_mismatch}

      entry.payload_ref != Sink.payload_ref(envelope) ->
        {:error, :ledger_receipt_payload_ref_mismatch}

      entry.recorded_at != envelope.recorded_at ->
        {:error, :ledger_receipt_recorded_at_mismatch}

      true ->
        :ok
    end
  end

  def verify_envelope(_entry, _envelope), do: {:error, :invalid_ledger_receipt_envelope}

  defp identity(entry) do
    %{
      "format" => @format,
      "entry_version" => @version,
      "stream_key" => entry.stream_key,
      "sequence" => entry.sequence,
      "receipt_id" => entry.receipt_id,
      "kind" => Atom.to_string(entry.kind),
      "canonical_revision" => entry.canonical_revision,
      "envelope_digest" => entry.envelope_digest,
      "payload_ref" => entry.payload_ref,
      "previous_entry_digest" => entry.previous_entry_digest,
      "recorded_at" => entry.recorded_at
    }
  end

  defp validate_fields(entry) do
    with :ok <- nonempty(entry.stream_key, :stream_key),
         :ok <- positive(entry.sequence, :sequence),
         :ok <- receipt_id(entry.receipt_id),
         :ok <- receipt_kind(entry.kind),
         :ok <- optional_revision(entry.canonical_revision),
         :ok <- digest(entry.envelope_digest, :envelope_digest),
         :ok <- payload_ref(entry.payload_ref, entry.envelope_digest),
         :ok <- optional_digest(entry.previous_entry_digest),
         :ok <- non_negative(entry.recorded_at, :recorded_at),
         :ok <- optional_non_negative(entry.owner_fencing_token, :owner_fencing_token) do
      entry_digest(entry.entry_digest)
    end
  end

  defp nonempty(value, _field) when is_binary(value) and value != "", do: :ok
  defp nonempty(_value, field), do: {:error, {:invalid_ledger_receipt_entry_field, field}}

  defp positive(value, _field) when is_integer(value) and value > 0, do: :ok
  defp positive(_value, field), do: {:error, {:invalid_ledger_receipt_entry_field, field}}

  defp non_negative(value, _field) when is_integer(value) and value >= 0, do: :ok
  defp non_negative(_value, field), do: {:error, {:invalid_ledger_receipt_entry_field, field}}

  defp optional_non_negative(nil, _field), do: :ok
  defp optional_non_negative(value, field), do: non_negative(value, field)

  defp optional_revision(nil), do: :ok
  defp optional_revision(value), do: non_negative(value, :canonical_revision)

  defp receipt_id("receipt:" <> rest) when rest != "", do: :ok
  defp receipt_id(_value), do: {:error, :invalid_ledger_receipt_id}

  defp receipt_kind(kind) when kind in @kinds, do: :ok
  defp receipt_kind(_kind), do: {:error, :invalid_ledger_receipt_kind}

  defp payload_ref("receipt-payload:" <> digest, digest), do: :ok
  defp payload_ref(_payload_ref, _digest), do: {:error, :invalid_ledger_receipt_payload_ref}

  defp digest(value, _field) when is_binary(value) do
    if Regex.match?(@digest, value), do: :ok, else: {:error, :invalid_ledger_receipt_digest}
  end

  defp digest(_value, _field), do: {:error, :invalid_ledger_receipt_digest}

  defp optional_digest(nil), do: :ok
  defp optional_digest(value), do: digest(value, :previous_entry_digest)

  defp entry_digest(""), do: :ok
  defp entry_digest(value), do: digest(value, :entry_digest)

  defp decode_kind(kind) when is_binary(kind) do
    case Map.fetch(@kind_by_name, kind) do
      {:ok, decoded} -> {:ok, decoded}
      :error -> {:error, :invalid_ledger_receipt_kind}
    end
  end

  defp decode_kind(_kind), do: {:error, :invalid_ledger_receipt_kind}
end
