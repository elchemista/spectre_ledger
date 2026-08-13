# Changelog

All notable changes to Spectre Ledger are documented here.

## 0.1.0

Initial release for Spectre 0.3.1.

### Added

- append-only `Spectre.Instance.CheckpointStore` adapter;
- caller-owned Memory backend and host-owned PostgreSQL backend;
- atomic CAS, exact-retry reconciliation, coalesced revision support, migration
  aliases, and portable receipts;
- content-addressed Entry v1 chains;
- deterministic Bundle v1 export and bounded offline verification;
- explicit bundle completeness claims: persisted checkpoints and checkpoint
  playback only, never every-revision or deterministic replay;
- read-only Ledger Doctor, PostgreSQL schema probe, and privacy-safe telemetry;
- public Spectre checkpoint-store conformance coverage and backend integration
  tests;
- normative public API manifest and Hex release contract gates.
