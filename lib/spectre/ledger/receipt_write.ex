defmodule Spectre.Ledger.ReceiptWrite do
  @moduledoc """
  Validated handoff from the Spectre receipt sink to a Ledger backend.

  The write carries canonical envelope bytes and their content address. The
  backend allocates append sequence and chain linkage atomically; it must not
  derive a second receipt identity.
  """

  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.ReceiptCodec
  alias Spectre.Receipt.Envelope
  alias Spectre.Receipt.Sink

  @enforce_keys [
    :envelope,
    :encoded,
    :byte_size,
    :stream_key,
    :receipt_id,
    :receipt_kind,
    :canonical_revision,
    :envelope_digest,
    :payload_ref,
    :recorded_at,
    :owner_fencing_token
  ]
  defstruct @enforce_keys

  @type t :: %__MODULE__{
          envelope: Envelope.t(),
          encoded: binary(),
          byte_size: pos_integer(),
          stream_key: String.t(),
          receipt_id: String.t(),
          receipt_kind: Envelope.kind(),
          canonical_revision: non_neg_integer() | nil,
          envelope_digest: String.t(),
          payload_ref: String.t(),
          recorded_at: non_neg_integer(),
          owner_fencing_token: non_neg_integer() | nil
        }

  @doc "Builds a bounded, canonical backend write from a core receipt envelope."
  @spec new(Envelope.t(), keyword(), Config.t()) :: {:ok, t()} | {:error, term()}
  def new(%Envelope{} = envelope, opts, %Config{} = config) when is_list(opts) do
    with {:ok, ^envelope} <- Envelope.new(envelope),
         {:ok, stream_key} <- stream_key(envelope, Keyword.get(opts, :instance_ref)),
         {:ok, owner_fencing_token} <-
           owner_fencing_token(Keyword.get(opts, :owner_fencing_token)),
         {:ok, encoded} <- ReceiptCodec.encode(envelope),
         :ok <- receipt_size(encoded, config.max_receipt_bytes) do
      {:ok,
       %__MODULE__{
         envelope: envelope,
         encoded: encoded,
         byte_size: byte_size(encoded),
         stream_key: stream_key,
         receipt_id: envelope.id,
         receipt_kind: envelope.kind,
         canonical_revision: envelope.canonical_revision,
         envelope_digest: Envelope.digest(envelope),
         payload_ref: Sink.payload_ref(envelope),
         recorded_at: envelope.recorded_at,
         owner_fencing_token: owner_fencing_token
       }}
    else
      {:ok, _normalized} -> {:error, :noncanonical_ledger_receipt}
      {:error, _reason} = error -> error
    end
  end

  def new(_envelope, _opts, _config), do: {:error, :invalid_ledger_receipt_write}

  @doc "Revalidates a write at a backend trust boundary."
  @spec validate(t(), Config.t()) :: :ok | {:error, term()}
  def validate(%__MODULE__{} = write, %Config{} = config) do
    opts = [
      instance_ref: write.stream_key,
      owner_fencing_token: write.owner_fencing_token
    ]

    with {:ok, expected} <- new(write.envelope, opts, config),
         true <- expected == write do
      :ok
    else
      false -> {:error, :invalid_ledger_receipt_write}
      {:error, _reason} -> {:error, :invalid_ledger_receipt_write}
    end
  end

  def validate(_write, _config), do: {:error, :invalid_ledger_receipt_write}

  defp stream_key(%Envelope{instance_ref: key}, nil) when is_binary(key) and key != "",
    do: {:ok, key}

  defp stream_key(%Envelope{instance_ref: key}, %Ref{key: key}), do: {:ok, key}
  defp stream_key(%Envelope{instance_ref: nil}, %Ref{key: key}), do: {:ok, key}

  defp stream_key(%Envelope{instance_ref: key}, key) when is_binary(key) and key != "",
    do: {:ok, key}

  defp stream_key(%Envelope{instance_ref: nil}, key) when is_binary(key) and key != "",
    do: {:ok, key}

  defp stream_key(%Envelope{instance_ref: nil, correlation_id: correlation_id}, nil) do
    digest =
      correlation_id
      |> then(&:crypto.hash(:sha256, &1))
      |> Base.encode16(case: :lower)

    {:ok, "receipt:" <> digest}
  end

  defp stream_key(%Envelope{instance_ref: key}, %Ref{}),
    do: {:error, {:ledger_receipt_instance_ref_mismatch, key}}

  defp stream_key(%Envelope{instance_ref: key}, _provided) when is_binary(key),
    do: {:error, {:ledger_receipt_instance_ref_mismatch, key}}

  defp stream_key(_envelope, _provided), do: {:error, :invalid_ledger_receipt_stream_key}

  defp owner_fencing_token(nil), do: {:ok, nil}

  defp owner_fencing_token(value) when is_integer(value) and value >= 0,
    do: {:ok, value}

  defp owner_fencing_token(_value), do: {:error, :invalid_owner_fencing_token}

  defp receipt_size(encoded, max_bytes)
       when byte_size(encoded) > 0 and byte_size(encoded) <= max_bytes,
       do: :ok

  defp receipt_size(_encoded, max_bytes),
    do: {:error, {:ledger_receipt_too_large, max_bytes}}
end
