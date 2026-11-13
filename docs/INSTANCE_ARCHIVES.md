# Spectre Ledger — standalone and retained runtime reference

This guide describes the retained Instance checkpoint and boundary-receipt archives, including Memory/PostgreSQL backends, bundles and receipt inspection. They are separate from the compacting AgentServer `ServerStore`; options and revision formats are not interchangeable.

Start with the [current README](../README.md) and [0.4 integration guide](SPECTRE_0_4.md) for new AgentServer applications.

Spectre Ledger is an append-only durable checkpoint and boundary-receipt ledger
for Spectre. It implements two existing core boundaries:
`Spectre.Instance.CheckpointStore` and `Spectre.Receipt.Sink`. Spectre remains
the only runtime owner, scheduler, receipt-outbox owner, and recovery authority.

Ledger records checkpoints that Spectre actually persists and the
nondeterministic or authority boundaries for which Spectre emits receipts. It
does **not** claim to record every runtime revision, provide deterministic
execution replay, or make model calls and external effects exactly once.

## Installation

Use the matching development-branch dependencies in the [current README](../README.md).

## Configure checkpoints and receipts

The Memory backend is caller-owned and intentionally volatile:

```elixir
{:ok, ledger} = Spectre.Ledger.Backend.Memory.start_link()

ledger_opts = [
  backend: :memory,
  server: ledger,
  namespace: "development"
]

checkpoint_store = Spectre.Ledger.checkpoint_store(ledger_opts)
receipt_sink = Spectre.Ledger.receipt_sink(ledger_opts)

{:ok, instance} =
  Spectre.summon(
    agent: MyApp.Agent,
    subject: subject,
    checkpoint_store: checkpoint_store,
    receipt_mode: :required,
    receipt_sink: receipt_sink
  )
```

Use `:observational` when receipt delivery must never gate a Run. Use
`:required` when Spectre must stage the payload, commit its canonical outbox,
cross the checkpoint durability barrier, append idempotently, and acknowledge
the outbox before completing the boundary. Required mode needs both the
checkpoint store and the payload-capable receipt sink shown above.

Read and verify one Instance's persisted evidence with the same backend
options:

```elixir
{:ok, envelopes} = Spectre.Ledger.receipts(instance_ref, ledger_opts)
{:ok, report} = Spectre.Ledger.verify_receipts(instance_ref, ledger_opts)
```

`receipt_entries/2` exposes physical append order. The receipt envelope keeps
its separate `canonical_revision`, because asynchronous observational delivery
may arrive out of canonical order. Verification reports that distinction
instead of silently reordering evidence.

Aggregate model calls and cumulative usage from one complete Instance receipt
stream without changing Ledger's append-only storage contract:

```elixir
{:ok, usage} = Spectre.Ledger.inference_usage(instance_ref, ledger_opts)

usage.provider_calls
usage.usage.total_tokens
usage.models
```

The projection joins `:inference_selected` and
`:inference_attempt_terminal` receipts by durable attempt identity. It counts
only attempts with `provider_started: true` as provider calls, includes usage
from completed and failed attempts, and keeps `:provider`, `:estimated`, and
`:unavailable` quality counts explicit.

## PostgreSQL

For production, use `backend: :postgres` with a host-owned, already running
Ecto Repo. Ledger neither configures nor supervises that Repo. Generate and run
the package migration first:

```console
mix spectre_ledger.gen.migration
mix ecto.migrate
```

Storage schema v2 adds isolated receipt stream, entry, and payload tables. The
generated SQL upgrades an existing schema v1 in place and keeps checkpoint
tables and data. Writes require PostgreSQL `READ COMMITTED` isolation.

See [Architecture](ARCHITECTURE.md), [Operations](OPERATIONS.md), and
the normative [Public API](PUBLIC_API.md) for the complete contracts.

## Checkpoint Bundle v1

Bundle v1 transports one complete persisted-checkpoint chain. It does not yet
contain the separate boundary-receipt chain.

```elixir
{:ok, json} = Spectre.Ledger.Bundle.export(entries, objects)
{:ok, report} = Spectre.Ledger.Bundle.verify(json)
```

The v1 manifest permanently says:

- capture: `persisted_checkpoints`
- completeness: `checkpoint_playback`
- every revision: `false`
- deterministic replay claim: `false`

Verification is bounded by encoded-size, entry-count, object-count,
per-object, total-object, and nesting-depth limits. It validates the global
checksum, Ledger entry chain, raw object digests, and Spectre Foundation
checkpoint digests. Foundation canonical decoding may load an existing BEAM
module named by the checkpoint, including its `@on_load` callback. Treat
direct bundle verification as a trusted/local-artifact operation; isolate
verification of external input.

Bundle verification does not call `Spectre.Run.restore/1`, start an Instance,
activate an Agent, or replay model, action, or effect execution.

## Doctor and telemetry

```console
mix spectre_ledger.doctor
mix spectre_ledger.doctor --backend postgres --repo MyApp.Repo --strict
mix spectre_ledger.doctor --format json
```

Doctor composes `Spectre.Doctor`, checks checkpoint and receipt backend
capabilities, warns that Memory is volatile, and performs a read-only
PostgreSQL schema probe. It never starts resources or writes storage.

Ledger emits privacy-safe events under `[:spectre, :ledger, ...]`, including
checkpoint and receipt append/read operations. Raw stream keys, receipt
payloads, checkpoints, credentials, Repo configuration, and raw errors are not
telemetry metadata.

## Security

Treat checkpoints, receipt envelopes, and staged payload objects as sensitive
application state. Spectre redacts constitutionally sensitive receipt paths,
but ordinary admitted input and model output are still confidential evidence.
Content addressing proves integrity, not confidentiality or authorization.
Encrypt and restrict the backing store as required, retain payload objects for
as long as an outbox or receipt entry can reference them, and verify every
artifact before use. See [SECURITY.md](../SECURITY.md).

## License

Apache-2.0. See the repository `LICENSE` file.
