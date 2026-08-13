# Architecture

Spectre Ledger is a satellite package around an existing Spectre boundary. It
does not add history records, an outbox, a second scheduler, or a replay engine
to Spectre core.

## Ownership

- Spectre owns Instances, runtime transitions, checkpoint timing, and recovery.
- Ledger implements `Spectre.Instance.CheckpointStore` and owns durable
  checkpoint-chain storage.
- The host owns process supervision, Ecto Repo configuration, credentials,
  migration execution, retention, authorization, and backup.
- Lab may consume verified bundles offline; Ledger does not decode executable
  Runs for it.

Ecto SQL and Postgrex are optional package dependencies. Bundle, chain,
checkpoint conformance, and Memory consumers do not fetch them transitively.
The host opts into the PostgreSQL backend by declaring both dependencies and
continues to own its Repo.

## Entry chain

Each Entry v1 identifies one successful persisted checkpoint by:

- opaque stream key;
- expected and resulting checkpoint revisions (gaps are valid);
- Spectre Foundation semantic checkpoint digest;
- SHA-256 digest of the exact checkpoint bytes;
- prior entry digest;
- optional migration-source digest.

Owner fencing tokens are operational metadata. They are stored for diagnostics
but do not participate in the portable entry identity.

Backends atomically append the blob and entry and advance the stream head.
Exact retries are idempotent; divergent same-revision writes conflict; stale
expected revisions are rejected. Ambiguous outcomes are returned to Spectre's
existing reconciliation boundary.

## Bundle v1

`Spectre.Ledger.Bundle` exports a closed JSON envelope containing one complete
entry chain, an exact object map, an honest completeness manifest, and a global
checksum. Canonical JSON recursively sorts object keys. Bundle bytes therefore
do not vary with Elixir map enumeration order.

Verification proceeds without executing checkpoint contents:

1. bound encoded bytes and JSON nesting;
2. reject duplicate/unknown keys and verify the checksum;
3. decode bounded objects from canonical base64;
4. verify the Entry chain and exact object set;
5. compare each raw object digest;
6. call the public Spectre Foundation checkpoint verifier and compare semantic
   digest and revision.

The Foundation verifier restores the canonical Instance state but does not
perform Agent activation. Bundle code never calls `Spectre.Run.restore`.

The Bundle v1 manifest claims only persisted checkpoint playback. Spectre may
coalesce checkpoint revisions, and checkpoint playback does not reproduce model
or external side effects; every-revision and deterministic-replay flags are
therefore permanently false in v1.
