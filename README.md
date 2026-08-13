# Spectre Ledger

Spectre Ledger 0.1.0 is an append-only durable checkpoint ledger for
Spectre 0.3.1. It implements the existing
`Spectre.Instance.CheckpointStore` boundary; Spectre remains the only runtime
owner and scheduler.

Ledger records checkpoints that Spectre actually persists. Because Spectre's
checkpoint manager may coalesce revisions, Ledger does **not** claim to record
every runtime revision, provide deterministic execution replay, or make model
and external side effects exactly once.

## Installation

```elixir
def deps do
  [
    {:spectre, "~> 0.3.1"},
    {:spectre_ledger, "~> 0.1.0"}
  ]
end
```

The PostgreSQL adapter is optional. Consumers that only verify or inspect
bundles do not pull Ecto SQL or Postgrex transitively. Applications selecting
the PostgreSQL backend must declare the database dependencies themselves:

```elixir
def deps do
  [
    {:spectre_ledger, "~> 0.1.0"},
    {:ecto_sql, "~> 3.14"},
    {:postgrex, "~> 0.22.4"}
  ]
end
```

During development against the adjacent Spectre checkout:

```console
SPECTRE_PATH=../spectre mix deps.get
SPECTRE_PATH=../spectre mix test
```

## Checkpoint store

The in-memory backend is caller-owned and intentionally volatile:

```elixir
{:ok, ledger} = Spectre.Ledger.Backend.Memory.start_link()

store =
  Spectre.Ledger.checkpoint_store(
    backend: :memory,
    server: ledger,
    namespace: "development"
  )
```

For production, use `backend: :postgres` with those optional dependencies and
a host-owned, already running Ecto Repo. Ledger neither configures nor
supervises that Repo. Generate and run the package migration first:

```console
mix spectre_ledger.gen.migration
mix ecto.migrate
```

See [Architecture](docs/ARCHITECTURE.md), [Operations](docs/OPERATIONS.md), and
the normative [Public API](docs/PUBLIC_API.md) for the contracts and
operational model.

## Bundle v1

A bundle transports one complete persisted-checkpoint chain. It stores
`Entry.to_data/1` entries and checkpoint bytes addressed by raw SHA-256 digest,
then covers the complete envelope with a deterministic checksum.

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
checkpoint digests. Foundation decodes canonical values during this last step;
`Spectre.Run.Value.prepare/1` may load an existing BEAM module named by the
checkpoint, and normal module loading can run that module's `@on_load`
callback. Treat `Bundle.verify/2` as a trusted/local-artifact operation. Verify
externally supplied artifacts only on an isolated node with a restricted,
pre-vetted code path and module set.

Bundle verification does not call `Spectre.Run.restore/1`, start an Instance,
activate an Agent, or replay model, action, or effect execution.

## Doctor and telemetry

Run read-only diagnostics with:

```console
mix spectre_ledger.doctor
mix spectre_ledger.doctor --backend postgres --repo MyApp.Repo --strict
mix spectre_ledger.doctor --format json
```

Doctor composes `Spectre.Doctor`, checks Ledger's package and bundle contracts,
warns that Memory is volatile, and performs a read-only PostgreSQL schema
probe. It never starts resources or writes storage.

Ledger telemetry is emitted under `[:spectre, :ledger, ...]`. Measurements are
numeric and metadata is redacted: raw stream keys, checkpoints, credentials,
Repo configuration, and raw errors are not emitted. Bundle `export/3`,
`decode/2`, and `verify/2` accept the closed boolean option `telemetry: false`
when an offline consumer must suppress both custom and standard telemetry;
the default is `true`.

## Security

Treat bundles and database checkpoints as potentially sensitive application
state even though identifiers are content-addressed. Size and shape limits do
not make Foundation-backed verification safe for untrusted input. Keep normal
verification to trusted/local artifacts; use an isolated, restricted node for
externally supplied bundles, and verify every bundle before import. See
[SECURITY.md](SECURITY.md).

## License

Apache-2.0. See the repository `LICENSE` file.
