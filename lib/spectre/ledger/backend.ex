defmodule Spectre.Ledger.Backend do
  @moduledoc """
  Storage contract used by the Ledger checkpoint and receipt adapters.

  Checkpoint and boundary-receipt chains have separate heads. A backend
  atomically advances the relevant head and appends one immutable entry. It
  does not schedule Spectre Instances or retry ambiguous writes.

  Receipt callbacks are optional so checkpoint-only backends remain valid.
  Selecting `Spectre.Ledger.ReceiptSink` requires the complete receipt callback
  set and fails closed when any callback is absent.
  """

  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.Entry
  alias Spectre.Ledger.Receipt
  alias Spectre.Ledger.ReceiptEntry
  alias Spectre.Ledger.ReceiptWrite
  alias Spectre.Ledger.Write

  @callback load(Config.t(), Ref.t()) ::
              :not_found | {:ok, binary()} | {:error, term()}
  @callback compare_and_swap(Config.t(), Write.t()) ::
              {:ok, Receipt.t()} | {:error, term()}
  @callback head(Config.t(), String.t()) ::
              :not_found | {:ok, Entry.t()} | {:error, term()}
  @callback entries(Config.t(), String.t(), keyword()) ::
              {:ok, [Entry.t()]} | {:error, term()}
  @callback objects(Config.t(), String.t(), keyword()) ::
              {:ok, %{String.t() => binary()}} | {:error, term()}
  @callback migrate(Config.t(), Ref.t(), Ref.t(), binary() | map(), Write.t()) ::
              {:ok, :moved | :aliased} | {:error, term()}
  @callback put_stream(Config.t(), String.t(), [Entry.t()], %{String.t() => binary()}) ::
              {:ok, :imported | :idempotent} | {:error, term()}

  @callback append_receipt(Config.t(), ReceiptWrite.t()) ::
              {:ok, :appended | :idempotent} | {:error, term()}
  @callback lookup_receipt(Config.t(), String.t()) ::
              {:ok, binary()} | :not_found | {:error, term()}
  @callback put_receipt_payload(Config.t(), ReceiptWrite.t()) ::
              {:ok, String.t()} | {:error, term()}
  @callback get_receipt_payload(Config.t(), String.t()) ::
              {:ok, binary()} | :not_found | {:error, term()}
  @callback receipt_entries(Config.t(), String.t(), keyword()) ::
              {:ok, [ReceiptEntry.t()]} | {:error, term()}
  @callback receipt_objects(Config.t(), String.t(), keyword()) ::
              {:ok, %{String.t() => binary()}} | {:error, term()}

  @optional_callbacks migrate: 5,
                      append_receipt: 2,
                      lookup_receipt: 2,
                      put_receipt_payload: 2,
                      get_receipt_payload: 2,
                      receipt_entries: 3,
                      receipt_objects: 3
end
