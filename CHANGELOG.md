# Changelog

All notable changes to Spectre Ledger are documented here.

## 0.1.0

Initial release for Spectre 0.3.2.

### Added

- append-only `Spectre.Instance.CheckpointStore` and `Spectre.Receipt.Sink`
  adapters;
- caller-owned Memory backend and host-owned PostgreSQL backend;
- optional Ecto SQL and Postgrex dependencies, keeping bundle-only and Memory
  consumers free of the PostgreSQL dependency stack;
- atomic CAS, exact-retry reconciliation, coalesced revision support, migration
  aliases, and portable receipts;
- content-addressed Entry v1 chains;
- independent content-addressed ReceiptEntry v1 chains that preserve physical
  append order and the separate canonical revision;
- required-mode payload staging, idempotent append/lookup reconciliation, and
  verified receipt archive queries without duplicating Spectre's canonical
  outbox;
- bounded canonical receipt codec, exact object-set verification, namespace
  isolation, and honest state-digest linkage reports;
- optional receipt-backend capability and ExUnit-independent conformance suite;
- PostgreSQL storage schema v2 with in-place upgrade from checkpoint schema v1,
  three receipt tables, atomic concurrent append, and verified readback;
- real Spectre Instance integration covering required receipts, checkpoint
  durability, shutdown, restore, and continued execution;
- deterministic Bundle v1 export and bounded offline verification;
- closed `telemetry: false | true` Bundle option for offline consumers, with
  suppression of both custom and standard telemetry sinks when disabled;
- explicit bundle completeness claims: persisted checkpoints and checkpoint
  playback only, never every-revision or deterministic replay;
- explicit Bundle v1 trust boundary: Foundation verification may load existing
  checkpoint-named BEAM modules (including `@on_load`), so direct verification
  is for trusted/local artifacts and external input requires isolation;
- read-only Ledger Doctor, PostgreSQL schema probe, and privacy-safe telemetry;
- public Spectre checkpoint-store and receipt-sink conformance coverage plus
  Memory and PostgreSQL backend integration tests;
- normative public API manifest and Hex release contract gates.
