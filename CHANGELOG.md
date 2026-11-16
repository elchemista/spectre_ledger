# Changelog

## Unreleased — Spectre 0.4

### Added

- AgentServer coordination Store with opaque CAS revisions, file compaction and
  metadata-only checkpoint inspection.
- PostgreSQL coordination storage on a host-owned Ecto Repo, with asynchronous
  query workers, bounded keyset pages and revision tombstones.
- `gam_domain/1` discovery for canonical GAM history and the persisted Domain read
  model on the same Repo; `gam_ledger/1` remains available for history-only callers.
- Atomic, revision-fenced projection updates and verified rebuild from canonical
  history. Historical records can leave the live Domain after each command;
  foundation, active frontiers and required dependency closures still consume RAM.
- Real PostgreSQL integration and process/memory profiling probes, including
  outage recovery, independent Domains and shared multi-agent resources.

### Changed

- Configured Stack resources supply Managed GAM coordination and approval storage.
  Inbox history has no automatic purge or mandatory quota.
- File compaction removes superseded values and tombstones while preserving a
  global revision watermark, private file initialization and durability barriers.
- Require Spectre `~> 0.4.0` and Ecto 3.14.3; support explicit local source overrides.

### Compatibility

- Install coordination, GAM history and Domain projection tables through
  application migrations; upgrade existing derived indexes before startup.
- Existing SPLS1/SPLS2 files remain readable. Compaction writes checkpoint-v2;
  disk history is not automatically migrated into PostgreSQL.
- Instance checkpoint/receipt archives and Bundle v1 remain separate APIs.

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
