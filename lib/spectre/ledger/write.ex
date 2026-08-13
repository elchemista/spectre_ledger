defmodule Spectre.Ledger.Write do
  @moduledoc """
  Validated internal handoff from the Spectre Checkpoint Store to a backend.

  Applications normally construct this through
  `Spectre.Ledger.CheckpointStore`; it is public so third-party Ledger backends
  can implement the backend contract without depending on private structs.
  """

  alias Spectre.Instance.Ref

  @enforce_keys [
    :ref,
    :checkpoint,
    :expected_revision,
    :revision,
    :checkpoint_digest,
    :blob_digest,
    :byte_size,
    :kind
  ]
  defstruct [
    :ref,
    :checkpoint,
    :expected_revision,
    :revision,
    :checkpoint_digest,
    :blob_digest,
    :byte_size,
    :owner_fencing_token,
    :source_entry_digest,
    :kind
  ]

  @type t :: %__MODULE__{
          ref: Ref.t(),
          checkpoint: binary(),
          expected_revision: non_neg_integer(),
          revision: non_neg_integer(),
          checkpoint_digest: String.t(),
          blob_digest: String.t(),
          byte_size: pos_integer(),
          owner_fencing_token: non_neg_integer() | nil,
          source_entry_digest: String.t() | nil,
          kind: :checkpoint | :migration
        }
end
