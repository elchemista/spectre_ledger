# Operations

## Readiness

Run Ledger Doctor after configuration and before serving traffic:

```console
mix spectre_ledger.doctor --backend postgres --repo MyApp.Repo --strict
```

The command composes Spectre's public Doctor, verifies Ledger package and
bundle contracts, then asks the PostgreSQL backend for a read-only schema
inventory. It starts no process, writes no database row, and runs no migration.
Memory always produces a volatility warning; strict mode turns warnings into a
non-zero command result.

## PostgreSQL lifecycle

The host starts and supervises its Ecto Repo. Generate the Ledger migration,
review it, apply it through the host's normal deployment process, and only then
configure the checkpoint store. Keep schema and table-prefix options identical
between migration generation, Doctor, and runtime.

Use the host's normal transaction, statement-timeout, backup, restore, and
monitoring policy. An unknown transaction outcome may have committed; allow
Spectre's checkpoint-store reconciliation to read the durable head instead of
blindly retrying a different payload.

Ledger writes require PostgreSQL `READ COMMITTED` isolation and check the
effective isolation at the start of every transaction. This is required for
the row-lock plus alias-visibility protocol; a host override to repeatable-read
or serializable is rejected before any write.

## Monitoring

Ledger emits events under `[:spectre, :ledger, ...]`. Bundle stop events expose
duration and counts. Doctor exposes duration and status. Backends can report
append/load measurements through the same boundary.

Metadata contains only bounded enum-like operational fields, integer revisions,
module names, classified errors, and digested stream identifiers. It excludes
checkpoint bytes, raw stream keys, Repo options, SQL parameters, credentials,
and raw adapter errors.

## Bundle handling

- retain bundles as sensitive application-state backups;
- set limits lower than the defaults when deployment constraints permit;
- call `Bundle.verify/2` before import;
- authorize the stream and source independently of checksum validity;
- keep the original bytes for audit if verification or import fails;
- never present Bundle v1 as deterministic replay or an every-transition log.
