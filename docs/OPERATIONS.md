# Operations

## Readiness

Run Ledger Doctor after configuration and before serving traffic:

```console
mix spectre_ledger.doctor --backend postgres --repo MyApp.Repo --strict
```

The command composes Spectre's public Doctor, verifies the Ledger package and
checkpoint Bundle contracts, reports whether the backend implements the
optional receipt sink/archive capability, then asks PostgreSQL for a read-only
schema inventory. It starts no process, writes no row, and runs no migration.
Memory always produces a volatility warning; strict mode turns warnings into a
non-zero command result.

## PostgreSQL lifecycle

Declare `ecto_sql ~> 3.14` and `postgrex ~> 0.22.4` in the host application.
They remain optional Ledger dependencies. Selecting PostgreSQL without Ecto SQL
installed fails closed with
`{:ledger_postgres_dependency_unavailable, :ecto_sql}`.

The host starts and supervises its Ecto Repo. Generate the Ledger migration,
review it, apply it through the host's normal deployment process, and only then
configure the adapters. Keep `:schema` and `:table_prefix` identical between
migration generation, Doctor, checkpoint store, and receipt sink.

Storage schema v2 owns eight tables:

- checkpoint: `meta`, `streams`, `blobs`, `entries`, `aliases`;
- receipts: `receipt_streams`, `receipt_payloads`, `receipt_entries`.

Running v2 migration SQL on an existing v1 installation preserves checkpoint
tables and data, creates the receipt tables, and changes only the storage schema
marker from `1` to `2`. Apply the upgrade before enabling receipt delivery.
The generated down migration removes all Ledger-owned tables; it is an
uninstall operation and is destructive to both chains.

Ledger write transactions require PostgreSQL `READ COMMITTED` and verify the
effective isolation before mutation. Use the host's normal statement timeout,
backup, restore, and monitoring policy. An unknown transaction outcome may have
committed: allow Spectre's checkpoint or receipt reconciliation to read durable
state instead of issuing a divergent retry.

## Receipt modes and retention

Use the same namespace and backend identity for an Instance's checkpoint store
and receipt sink. `:required` mode needs a durable checkpoint store and the
payload-capable sink; `:observational` mode does not gate the Run on delivery.

`max_receipt_bytes` defaults to 8,000,000 bytes and independently bounds one
canonical receipt envelope. Lower it when application evidence is known to be
smaller. It does not change Spectre's own receipt outbox capacity.

Receipt payload objects must outlive every canonical outbox entry and receipt
entry that can reference them. A process loss after staging and before outbox
commit can leave an orphan object. Do not delete staged objects merely because
they are not yet in `receipt_entries`; use a deployment-specific grace period
that exceeds recovery and backup windows. Ledger 0.1.0 intentionally provides
no automatic garbage collector.

Receipt envelopes can contain admitted input, model output, and effect
arguments/results. Encrypt storage and backups when required, enforce tenant
authorization outside Ledger, and keep namespaces isolated. Content addresses
are integrity identifiers, not secrets or access tokens.

## Monitoring

Ledger emits events under `[:spectre, :ledger, ...]` for checkpoint operations,
receipt append/lookup/payload operations, bundles, and Doctor. Measurements are
numeric. Metadata is limited to enum-like fields, non-negative revisions or
sequences, module names, classified errors, and digested stream identifiers.

It excludes checkpoint bytes, receipt payloads, raw stream and receipt ids,
Repo options, SQL parameters, credentials, and raw adapter errors. Telemetry is
observational: handler failures do not change storage outcomes.

Operational signals worth alerting on include:

- ambiguous PostgreSQL transactions or repeated receipt reconciliation;
- receipt id/payload conflicts, which indicate divergent evidence;
- schema version mismatch or incomplete table inventory;
- growth in staged payload storage compared with committed receipt entries;
- required receipt outbox saturation reported by Spectre core.

## Conformance

Third-party backends can run two independent suites:

```elixir
Spectre.Ledger.Backend.Conformance.run(opts, fresh_ref)
Spectre.Ledger.ReceiptBackend.Conformance.run(opts, fresh_stream_key)
```

The first covers checkpoint CAS, chain, bundle, import, and readback. The
second covers the optional receipt sink/archive, including concurrent initial
append and payload verification. Use a fresh namespace and identifiers for each
run. Passing conformance does not certify deployment durability, encryption,
retention, authorization, backup, or topology.

## Bundle handling

- retain bundles as sensitive application-state backups;
- set limits below defaults when deployment constraints permit;
- verify every bundle before import, directly only for trusted/local artifacts;
- isolate external verification with restricted code and no production
  credentials because Foundation decoding can load checkpoint-named modules;
- authorize the stream and source independently of checksum validity;
- keep original bytes for investigation when verification fails;
- never present Bundle v1 as receipt export, deterministic replay, or an
  every-transition log.
