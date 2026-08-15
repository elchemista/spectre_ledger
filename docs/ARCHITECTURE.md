# Architecture

Spectre Ledger is a satellite package around existing Spectre boundaries. It
does not add a scheduler, a second canonical history, an outbox, or a replay
engine to Spectre core.

## Ownership

- Spectre owns Instances, runtime transitions, receipt creation, receipt-outbox
  commits, checkpoint timing, delivery policy, and recovery.
- Ledger implements `Spectre.Instance.CheckpointStore` and
  `Spectre.Receipt.Sink`; it owns durable checkpoint and receipt-chain storage.
- The host owns process supervision, Ecto Repo configuration, credentials,
  migration execution, encryption, retention, authorization, and backup.
- Lab or another consumer may inspect verified evidence offline. Ledger does
  not restore or replay Runs for it.

Ecto SQL and Postgrex are optional package dependencies. Memory, bundle,
chain, and conformance consumers do not fetch them transitively. A PostgreSQL
host declares both dependencies and continues to own its Repo.

## Two independent chains

Checkpoint history and boundary evidence use independent append heads. Keeping
them separate preserves the semantics of both core boundaries:

```text
checkpoint stream                    receipt stream
revision 3 -> revision 8             sequence 1 -> sequence 2 -> sequence 3
coalesced persisted state            physical sink append order
```

A checkpoint Entry v1 identifies one successfully persisted checkpoint by its
opaque stream key, expected/resulting revisions, Foundation semantic digest,
exact-byte SHA-256 digest, previous entry digest, and optional migration-source
digest. Revision gaps are valid because Spectre may coalesce checkpoints.

A ReceiptEntry v1 identifies one successfully appended envelope by its
physical sequence, deterministic receipt id, kind, canonical revision,
envelope digest, content-addressed payload reference, recorded time, and prior
receipt-entry digest. `sequence` is deliberately not `canonical_revision`:
observational delivery can append receipts out of canonical order. Verification
returns `canonical_ordered: false` rather than sorting or rejecting valid
physical evidence.

Owner fencing tokens are stored as operational metadata on both entry types.
They do not participate in portable entry identity because retries and recovery
may use a later owner token without changing the underlying durable boundary.

## Required receipt flow

Ledger does not duplicate Spectre's receipt outbox. With
`receipt_mode: :required`, the flow remains:

1. Spectre creates and redacts the portable envelope.
2. Ledger stages canonical envelope bytes at
   `receipt-payload:<Envelope.digest(envelope)>`.
3. Spectre commits the boundary plus a compact canonical outbox entry.
4. Spectre crosses its checkpoint durability barrier.
5. Ledger appends the receipt idempotently to the physical chain.
6. Spectre commits the outbox acknowledgement.

If append acknowledgement is lost, Spectre calls `lookup/2`. Ledger verifies
the stored entry, content-addressed bytes, envelope digest, receipt id, and
entry-to-envelope linkage before returning it. Exact retries return
`:idempotent`; the same id with different evidence fails closed.

A crash after step 2 but before the outbox commit can leave an unreferenced
staged payload. It is intentionally absent from `receipt_objects/3`, which
returns only objects referenced by committed receipt entries. Retention tooling
may garbage-collect old staged objects only after proving that no active or
recoverable outbox can still reference them.

## Backend atomicity

Memory serializes operations in its caller-owned GenServer. It is suitable for
tests and ephemeral runtimes, not durability.

PostgreSQL uses one locked stream-head row per chain. A receipt append stages or
verifies the content-addressed payload, locks the receipt stream, resolves an
exact retry, inserts the immutable entry, and advances the head inside one
`READ COMMITTED` transaction. The unique receipt id is scoped by namespace.
Unknown transaction outcomes are reported as ambiguous so Spectre can
reconcile instead of blindly creating new evidence.

PostgreSQL storage schema v2 adds three receipt tables to the five checkpoint
tables. The migration accepts schema v1, creates the new tables, and advances
the storage marker to v2 without rewriting checkpoint entries.

## Verification and claims

`verify/2` verifies a complete checkpoint chain. `verify_receipts/2` verifies a
complete receipt chain, the exact referenced object set, canonical envelope
decoding, and every entry-to-envelope link. Receipt reports count boundaries
whose pre/post state digests are both present and explicitly keep these claims
false:

- deterministic replay;
- every-revision capture;
- exactly-once provider calls;
- exactly-once external effects.

State roots prove linkage to canonical state around recorded boundaries. They
do not make the work between roots deterministic.

## Bundle v1

`Spectre.Ledger.Bundle` exports a closed JSON envelope containing one complete
checkpoint Entry chain, its exact object map, an honest completeness manifest,
and a global checksum. Receipt chains are not part of Bundle v1.

Verification bounds encoded bytes and nesting, rejects duplicate/unknown keys,
checks the checksum, decodes bounded objects, verifies the chain and object
set, and calls Spectre Foundation for checkpoint semantics. It does not call
`Spectre.Run.restore/1` or replay runtime work.

Foundation checkpoint validation and receipt decoding both process Spectre
portable values. `Spectre.Run.Value.prepare/1` may load an existing BEAM module
named by those values, and loading can run `@on_load`. These are
trusted/local-artifact boundaries, not untrusted-input sandboxes. External
artifacts require an isolated restricted node and a pre-vetted module set.
