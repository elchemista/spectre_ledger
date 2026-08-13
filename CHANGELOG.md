# Changelog

All notable changes to Spectre Ledger are documented here.

## 0.1.0

Initial release for Spectre 0.3.1.

### Added

- append-only `Spectre.Instance.CheckpointStore` adapter;
- caller-owned Memory backend and host-owned PostgreSQL backend;
- optional Ecto SQL and Postgrex dependencies, keeping bundle-only and Memory
  consumers free of the PostgreSQL dependency stack;
- atomic CAS, exact-retry reconciliation, coalesced revision support, migration
  aliases, and portable receipts;
- content-addressed Entry v1 chains;
- deterministic Bundle v1 export and bounded offline verification;
- closed `telemetry: false | true` Bundle option for offline consumers, with
  suppression of both custom and standard telemetry sinks when disabled;
- explicit bundle completeness claims: persisted checkpoints and checkpoint
  playback only, never every-revision or deterministic replay;
- explicit Bundle v1 trust boundary: Foundation verification may load existing
  checkpoint-named BEAM modules (including `@on_load`), so direct verification
  is for trusted/local artifacts and external input requires isolation;
- read-only Ledger Doctor, PostgreSQL schema probe, and privacy-safe telemetry;
- public Spectre checkpoint-store conformance coverage and backend integration
  tests;
- normative public API manifest and Hex release contract gates.
