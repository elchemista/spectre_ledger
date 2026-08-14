defmodule Spectre.Ledger.Backend do
  @moduledoc """
  Storage contract used by the Ledger Checkpoint Store.

  A backend atomically advances one stream head and appends one immutable
  entry. It does not schedule Spectre Instances or retry ambiguous writes.
  """

  alias Spectre.Instance.Ref
  alias Spectre.Ledger.Config
  alias Spectre.Ledger.Entry
  alias Spectre.Ledger.Receipt
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

  @optional_callbacks migrate: 5
end
