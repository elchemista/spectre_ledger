# Spectre Ledger public API — 0.1.0

This file is the normative public API manifest for Spectre Ledger `0.1.0`,
which targets Spectre `~> 0.3.2`. Compatibility guarantees apply only to the
modules and callables listed below. Anything absent from this manifest is an
implementation detail, even when exported by the BEAM.

Default arguments are expanded into every callable arity. For listed modules,
documented types and documented struct fields are also public. A module with no
callable row exposes only its documented module, type, or struct contract.

Backend callback implementations remain callable through the
`Spectre.Ledger.Backend` behaviour; listing a backend module here does not turn
its OTP callbacks or internal helpers into an additional API. Receipt callbacks
are an optional capability so existing checkpoint-only backends remain valid.
The Memory backend is caller-owned and volatile. PostgreSQL uses a host-owned
Repo and requires the host to declare Ledger's optional `ecto_sql` and
`postgrex` dependencies.

`test/public_api_manifest_test.exs` parses this manifest after a clean compile
and verifies every declared module, function, macro, and callback against the
application's compiled BEAM surface.

## Manifest

- `Mix.Tasks.SpectreLedger.Doctor`
- `Mix.Tasks.SpectreLedger.Gen.Migration`
- `Spectre.Ledger`
  - functions: `version/0`, `checkpoint_store/0`, `checkpoint_store/1`, `receipt_sink/0`, `receipt_sink/1`, `receipt/1`, `receipt/2`, `receipt_payload/1`, `receipt_payload/2`, `receipt_entries/1`, `receipt_entries/2`, `receipts/1`, `receipts/2`, `inference_usage/1`, `inference_usage/2`, `verify_receipts/1`, `verify_receipts/2`, `head/1`, `head/2`, `entries/1`, `entries/2`, `verify/1`, `verify/2`, `export_bundle/1`, `export_bundle/2`, `import_bundle/1`, `import_bundle/2`
- `Spectre.Ledger.Backend`
  - callbacks: `load/2`, `compare_and_swap/2`, `head/2`, `entries/3`, `objects/3`, `migrate/5`, `put_stream/4`, `append_receipt/2`, `lookup_receipt/2`, `put_receipt_payload/2`, `get_receipt_payload/2`, `receipt_entries/3`, `receipt_objects/3`
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
- `Spectre.Ledger.InferenceUsage`
  - functions: `summarize/1`
- `Spectre.Ledger.ReceiptBackend.Conformance`
  - functions: `run/2`
- `Spectre.Ledger.ReceiptChain`
  - functions: `verify/1`
- `Spectre.Ledger.ReceiptEntry`
  - functions: `version/0`, `new/3`, `to_data/1`, `from_data/1`, `verify/1`, `matches_write?/2`, `verify_envelope/2`
- `Spectre.Ledger.ReceiptSink`
  - functions: `append/2`, `lookup/2`, `put_payload/2`, `get_payload/2`
- `Spectre.Ledger.ReceiptWrite`
  - functions: `new/3`, `validate/2`
- `Spectre.Ledger.Telemetry`
  - functions: `emit/1`, `emit/2`, `emit/3`, `emit/4`, `id_digest/1`, `reason_class/1`
- `Spectre.Ledger.Write`

## Contract boundaries

Checkpoint entries and receipt entries are independent immutable chains with
independent heads. Checkpoint `revision` is the persisted canonical revision;
receipt `sequence` is physical sink append order. A receipt retains its own
`canonical_revision`, which can be out of physical order in observational
mode. Verification reports this without rewriting history.

`Spectre.Ledger.ReceiptSink` persists Spectre's portable, already-redacted
envelopes. In required mode, Spectre—not Ledger—owns payload staging, the
canonical receipt outbox, durability barriers, delivery retry, and recovery.
Ledger provides idempotent content-addressed storage and verified readback.

Ledger archives only checkpoints and boundary receipts that Spectre actually
emits. It does not claim every runtime revision, deterministic execution
replay, or exactly-once model and external side effects. Bundle v1 remains a
verified, bounded transport for the checkpoint chain only; receipt archive
queries are separate in 0.1.0.

Foundation-backed checkpoint verification and receipt-envelope decoding may
load an existing module named by portable values, including module `@on_load`
behavior. Direct verification is supported for trusted/local artifacts;
external artifacts require an isolated, restricted node and pre-vetted module
set.

The public conformance runners are ExUnit-independent. Backend conformance
tests the required checkpoint archive. Receipt-backend conformance separately
tests the optional sink and receipt archive capability. Neither runner
certifies deployment durability, encryption, retention, authorization, backup,
or topology.
