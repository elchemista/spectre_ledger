defmodule Spectre.Ledger.Entry do
  @moduledoc """
  Immutable checkpoint-chain entry owned by Ledger.

  It references a Spectre checkpoint by both its Foundation semantic digest
  and its raw byte digest. It does not duplicate the checkpoint schema.
  """

  alias Spectre.Canonical.Value
  alias Spectre.Ledger.Write

  @format "spectre/ledger-entry"
  @version 1
  @digest ~r/\A[0-9a-f]{64}\z/

  @enforce_keys [
    :format,
    :entry_version,
    :stream_key,
    :kind,
    :expected_revision,
    :revision,
    :checkpoint_digest,
    :blob_digest,
    :previous_entry_digest,
    :source_entry_digest,
    :owner_fencing_token,
    :entry_digest
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          format: String.t(),
          entry_version: pos_integer(),
          stream_key: String.t(),
          kind: :checkpoint | :migration,
          expected_revision: non_neg_integer(),
          revision: non_neg_integer(),
          checkpoint_digest: String.t(),
          blob_digest: String.t(),
          previous_entry_digest: String.t() | nil,
          source_entry_digest: String.t() | nil,
          owner_fencing_token: non_neg_integer() | nil,
          entry_digest: String.t()
        }

  @doc "Returns the Ledger entry schema version."
  @spec version() :: pos_integer()
  def version, do: @version

  @doc "Finalizes a prepared write against the current stream head."
  @spec new(Write.t(), String.t() | nil) :: {:ok, t()} | {:error, term()}
  def new(%Write{} = write, previous_entry_digest) do
    entry = %__MODULE__{
      format: @format,
      entry_version: @version,
      stream_key: write.ref.key,
      kind: write.kind,
      expected_revision: write.expected_revision,
      revision: write.revision,
      checkpoint_digest: write.checkpoint_digest,
      blob_digest: write.blob_digest,
      previous_entry_digest: previous_entry_digest,
      source_entry_digest: write.source_entry_digest,
      owner_fencing_token: write.owner_fencing_token,
      entry_digest: ""
    }

    with :ok <- validate_fields(entry),
         digest <- Value.digest!(identity(entry)) do
      {:ok, %{entry | entry_digest: digest}}
    end
  end

  @doc "Returns the canonical data shape used by bundles and PostgreSQL rows."
  @spec to_data(t()) :: map()
  def to_data(%__MODULE__{} = entry) do
    identity(entry)
    |> Map.put("entry_digest", entry.entry_digest)
  end

  @doc "Decodes a closed entry data shape without creating atoms."
  @spec from_data(map()) :: {:ok, t()} | {:error, term()}
  def from_data(
        %{
          "format" => @format,
          "entry_version" => @version,
          "stream_key" => stream_key,
          "kind" => kind,
          "expected_revision" => expected_revision,
          "revision" => revision,
          "checkpoint_digest" => checkpoint_digest,
          "blob_digest" => blob_digest,
          "previous_entry_digest" => previous_entry_digest,
          "source_entry_digest" => source_entry_digest,
          "owner_fencing_token" => owner_fencing_token,
          "entry_digest" => entry_digest
        } = data
      )
      when map_size(data) == 12 do
    with {:ok, kind} <- decode_kind(kind) do
      entry = %__MODULE__{
        format: @format,
        entry_version: @version,
        stream_key: stream_key,
        kind: kind,
        expected_revision: expected_revision,
        revision: revision,
        checkpoint_digest: checkpoint_digest,
        blob_digest: blob_digest,
        previous_entry_digest: previous_entry_digest,
        source_entry_digest: source_entry_digest,
        owner_fencing_token: owner_fencing_token,
        entry_digest: entry_digest
      }

      with :ok <- validate_fields(entry),
           true <- Value.digest!(identity(entry)) == entry.entry_digest do
        {:ok, entry}
      else
        false -> {:error, :ledger_entry_digest_mismatch}
        {:error, _reason} = error -> error
      end
    end
  end

  def from_data(%{"entry_version" => version}),
    do: {:error, {:unsupported_ledger_entry_version, version}}

  def from_data(_data), do: {:error, :invalid_ledger_entry}

  @doc "Verifies an entry and its content digest."
  @spec verify(t()) :: :ok | {:error, term()}
  def verify(%__MODULE__{} = entry) do
    with :ok <- validate_fields(entry),
         true <- Value.digest!(identity(entry)) == entry.entry_digest do
      :ok
    else
      false -> {:error, :ledger_entry_digest_mismatch}
      {:error, _reason} = error -> error
    end
  end

  @spec identity(t()) :: map()
  defp identity(%__MODULE__{} = entry) do
    %{
      "format" => @format,
      "entry_version" => @version,
      "stream_key" => entry.stream_key,
      "kind" => Atom.to_string(entry.kind),
      "expected_revision" => entry.expected_revision,
      "revision" => entry.revision,
      "checkpoint_digest" => entry.checkpoint_digest,
      "blob_digest" => entry.blob_digest,
      "previous_entry_digest" => entry.previous_entry_digest,
      "source_entry_digest" => entry.source_entry_digest,
      "owner_fencing_token" => entry.owner_fencing_token
    }
  end

  @spec validate_fields(t()) :: :ok | {:error, term()}
  defp validate_fields(%__MODULE__{} = entry) do
    cond do
      not (is_binary(entry.stream_key) and entry.stream_key != "") ->
        {:error, :invalid_ledger_stream_key}

      entry.kind not in [:checkpoint, :migration] ->
        {:error, :invalid_ledger_entry_kind}

      not non_negative?(entry.expected_revision) or not non_negative?(entry.revision) ->
        {:error, :invalid_ledger_revision}

      entry.revision <= entry.expected_revision ->
        {:error, :non_advancing_ledger_revision}

      not digest?(entry.checkpoint_digest) or not digest?(entry.blob_digest) ->
        {:error, :invalid_ledger_content_digest}

      not optional_digest?(entry.previous_entry_digest) or
          not optional_digest?(entry.source_entry_digest) ->
        {:error, :invalid_ledger_chain_digest}

      not (is_nil(entry.owner_fencing_token) or non_negative?(entry.owner_fencing_token)) ->
        {:error, :invalid_owner_fencing_token}

      entry.entry_digest != "" and not digest?(entry.entry_digest) ->
        {:error, :invalid_ledger_entry_digest}

      true ->
        :ok
    end
  end

  defp decode_kind("checkpoint"), do: {:ok, :checkpoint}
  defp decode_kind("migration"), do: {:ok, :migration}
  defp decode_kind(_kind), do: {:error, :invalid_ledger_entry_kind}

  defp non_negative?(value), do: is_integer(value) and value >= 0
  defp digest?(value), do: is_binary(value) and Regex.match?(@digest, value)
  defp optional_digest?(nil), do: true
  defp optional_digest?(value), do: digest?(value)
end
