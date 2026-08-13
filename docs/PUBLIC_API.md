# Spectre Ledger public API — 0.1.0

This file is the normative public API manifest for Spectre Ledger `0.1.0`,
which targets Spectre `~> 0.3.1`. Compatibility guarantees apply only to the
modules and callables listed below. Anything absent from this manifest is an
implementation detail, even when it is exported by the BEAM or visible in
generated documentation.

Default arguments are expanded into every callable arity. For listed modules,
documented types and documented struct fields are also public. A module with no
callable row exposes only its documented module, type, or struct contract.

Backend callback implementations remain callable through the
`Spectre.Ledger.Backend` behaviour; listing a backend module here does not turn
its OTP callbacks or internal helpers into an additional API. The Memory
backend is caller-owned and volatile. The PostgreSQL backend uses a host-owned
Repo and requires the host to declare Ledger's optional `ecto_sql` and
`postgrex` dependencies.

`test/public_api_manifest_test.exs` parses this manifest after a clean compile
and verifies every declared module, function, macro, and callback against the
application's compiled BEAM surface.

## Manifest

- `Mix.Tasks.SpectreLedger.Doctor`
- `Mix.Tasks.SpectreLedger.Gen.Migration`
- `Spectre.Ledger`
  - functions: `version/0`, `checkpoint_store/0`, `checkpoint_store/1`, `head/1`, `head/2`, `entries/1`, `entries/2`, `verify/1`, `verify/2`, `export_bundle/1`, `export_bundle/2`, `import_bundle/1`, `import_bundle/2`
- `Spectre.Ledger.Backend`
  - callbacks: `load/2`, `compare_and_swap/2`, `head/2`, `entries/3`, `objects/3`, `migrate/5`, `put_stream/4`
- `Spectre.Ledger.Backend.Conformance`
  - functions: `run/2`
- `Spectre.Ledger.Backend.Memory`
  - functions: `start_link/0`, `start_link/1`
- `Spectre.Ledger.Backend.Postgres`
- `Spectre.Ledger.Backend.Postgres.Migration`
  - functions: `schema_version/0`, `up_sql/0`, `up_sql/1`, `down_sql/0`, `down_sql/1`
- `Spectre.Ledger.Bundle`
  - functions: `version/0`, `manifest/0`, `export/2`, `export/3`, `decode/1`, `decode/2`, `verify/1`, `verify/2`, `to_data/1`
  - options: `telemetry: false | true` on `export/3`, `decode/2`, and `verify/2`;
    `false` suppresses both custom-handler and standard `:telemetry` emission
- `Spectre.Ledger.Chain`
  - functions: `verify/1`
- `Spectre.Ledger.CheckpointStore`
  - functions: `load/2`, `compare_and_swap/5`, `migrate_instance_key/5`
- `Spectre.Ledger.Config`
  - functions: `new/1`, `fetch_backend/2`, `get_backend/2`, `get_backend/3`
- `Spectre.Ledger.Doctor`
  - functions: `run/0`, `run/1`, `contract_version/0`
- `Spectre.Ledger.Doctor.Report`
  - functions: `to_map/1`, `format/1`, `format/2`, `acceptable?/1`, `acceptable?/2`
- `Spectre.Ledger.Entry`
  - functions: `version/0`, `new/2`, `to_data/1`, `from_data/1`, `verify/1`
- `Spectre.Ledger.Receipt`
- `Spectre.Ledger.Telemetry`
  - functions: `emit/1`, `emit/2`, `emit/3`, `emit/4`, `id_digest/1`, `reason_class/1`
- `Spectre.Ledger.Write`

## Contract boundaries

Ledger archives only checkpoints that Spectre actually persists. Its public
contract does not claim every runtime revision, deterministic execution replay,
or exactly-once model and external side effects. Bundle v1 is a verified,
bounded transport for persisted-checkpoint playback. `Bundle.verify/2` does not
call `Spectre.Run.restore/1`, start an Instance, activate an Agent, or replay
model, action, or effect execution. Its Foundation-backed canonical decoding
may nevertheless load an existing module named by the checkpoint, including
module `@on_load` behavior. Verification is therefore supported directly only
for trusted/local artifacts; externally supplied artifacts require an isolated,
restricted node and a pre-vetted module set.

The public backend conformance runner is ExUnit-independent and is the common
contract suite for third-party backends. It extends Spectre's existing
checkpoint-store conformance boundary; it does not add another scheduler,
history model, or outbox to Spectre core.
