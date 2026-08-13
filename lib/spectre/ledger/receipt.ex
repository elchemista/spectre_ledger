defmodule Spectre.Ledger.Receipt do
  @moduledoc "A portable acknowledgement for one Ledger checkpoint append."

  @enforce_keys [:stream_key, :revision, :checkpoint_digest, :entry_digest, :status]
  defstruct [:stream_key, :revision, :checkpoint_digest, :entry_digest, :status]

  @type t :: %__MODULE__{
          stream_key: String.t(),
          revision: non_neg_integer(),
          checkpoint_digest: String.t(),
          entry_digest: String.t(),
          status: :appended | :idempotent
        }
end
