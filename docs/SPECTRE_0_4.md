# Ledger with Spectre 0.4 AgentServer

[README](../README.md) · [Migration](MIGRATING_TO_0_4.md) · [Public API](PUBLIC_API.md)

## Helper contract and confidentiality

| Input | Result |
| --- | --- |
| `{:head}` or `{:head, current_key}` | `{:ok, %{revision: token, digest: sha256, bytes: count}}` or `:not_found` |
| `{:entries}` / `{:entries, current_key}` / `{:entries, current_key, limit}` | `{:ok, metadata_list}` |

The runtime passes `%{adapter: StoreModule, store: reference, key: current_key}`
directly to the worker from the actual Server checkpoint configuration. It does
not come from input, assigns, or application service options. A foreign key is
rejected with `:checkpoint_scope_mismatch`; missing configuration reports
`:server_checkpoint_required`. The helper cannot enumerate other conversations
or return saved payload bytes. Native Store access remains a host-level API.

Head's budget is 64 KiB, entry metadata 1 MiB; the history limit defaults to 50
and is bounded at 1000. Metadata includes sequence, action, key, revision and digest.
Compaction keeps only the latest entry per live key. On a Store without an entries
interface the helper reports `:checkpoint_history_unavailable`; head uses the common
Store fetch port. For snapshot counts use Lab's logical `{:inspect, :checkpoint}`.

## Store and GAM journal wiring

The Server coordination binding is a triple:

```elixir
checkpoint = {Spectre.Ledger.ServerStore, MyApp.Checkpoints, conversation_key}
```

`MyApp.Checkpoints` can be a supervised Store name. GAM's journal port can use the
same Store process, with its own journal keys, through a pair:

```elixir
gam = [
  storage: {:disk, "var/gam/" <> conversation_key},
  store: {Spectre.Ledger.ServerStore, MyApp.Checkpoints},
  operations: allowed_operations
]
```

Pass `checkpoint: checkpoint` and `gam: gam` to the Server. AgentServer creates
its authority ledger separately; both the journal and authority history must
have matching durability. Without custom storage, Spectre supplies a coherent
checkpoint/journal automatically.
The Store does not supply a GAM scope, invent mandates or approve a Candidate.
Do not confuse coordination/journal persistence with an immutable GAM evidence
archive or the legacy `Spectre.Instance.CheckpointStore` adapter.

## Optional Stack-owned lifecycle

Install Ledger normally, then give its runtime process options to the Stack runtime:

```elixir
{:ok, runtime} = Spectre.Stack.Runtime.start_link(MyApp.AI,
  packages: [spectre_ledger: [server_store: [path: "data/support.ledger"]]])
{:ok, ref} = Spectre.Stack.resolve(MyApp.AI, :resource, {:spectre_ledger, :server_store})
{:ok, store} = Spectre.Stack.Runtime.resolve(runtime, ref)
```

Use that `store` in the actual Server checkpoint binding. A Stack install does not
start a Store by itself. Alternatively supervise `ServerStore` directly as in the
README. Do not start two writers for the same file, and keep credentials/live
resources outside compiled installation data. Supervision and stable registration
names belong to the host application.

## Durability, compaction and format compatibility

`put_new/3`, `compare_and_swap/4` and `delete/3` acknowledge only after persistence.
Revision tokens are opaque. Fresh/recreated keys use a global sequence watermark;
live-key CAS/delete advance the current revision exactly once. This prevents an
old token matching a recreated key after its tombstone has been removed.

At append capacity the Store atomically compacts including the pending mutation.
`compact/1` requests this explicitly. It keeps every live key, removes superseded
values and deleted keys from disk **and memory**, and retains the global watermark.
If live values cannot fit the capacity, the mutation is rejected without replacing
the previously acknowledged state. The default bound is 256 MiB.

Initialization/compaction write an exclusive temporary file, set mode 0600 before
payload bytes, sync the file, rename atomically, and sync the containing directory.
Reads wait through compaction. Symlink and hardlink aliases are rejected. This is
a local single-owner Store, not a multi-host filesystem transaction service.

SPLS1 and old SPLS2/checkpoint-v1 are readable. New compaction writes
SPLS2/checkpoint-v2 so old binaries fail closed instead of allocating unsafe reused
revisions. Live keys retain their old values/tokens; the global watermark also
survives an empty compacted snapshot. Corrupt/truncated frames fail closed because
replay cannot know whether damaged bytes were already acknowledged. Preserve the
file for inspection and perform host-controlled recovery; do not silently truncate.

Deletion is a tombstone until compaction. Compaction removes old payload bytes from
the active file, not filesystem snapshots, backups or physical storage. Apply the
host's retention controls separately. The legacy PostgreSQL/Memory archive backends remain
[separate Instance APIs](INSTANCE_ARCHIVES.md). The PostgreSQL coordination
adapter below implements the AgentServer Store contract directly.

## Mounted Skill runtimes

The `ledger` helper reads the calling Server's checkpoint metadata, including in
mounted Skills. A mount shares its root's GAM Domain while keeping a distinct
checkpoint. Stack resources remain host-supervised. See the
[core Skill guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/SKILLS.md) and
[Server command lifecycle](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/STACK_TOOLS.md).

## PostgreSQL coordination Store and approval inbox

For growing histories, use a host-owned Ecto Repo. Declare `ecto_sql` and
`postgrex` dependencies in the application and supervise its Repo before the Stack.
Create the table through an application migration (never implicitly at startup):

```elixir
{:ok, ddl} = Spectre.Ledger.ServerStore.Postgres.migration_sql(
  schema: "public", table: "spectre_server_store")
# In an Ecto.Migration up/0: Enum.each(ddl, &execute/1).
# Keep this as an application-owned, reviewed migration.
```

Configure the resource when starting the Stack runtime:

```elixir
{Spectre.Stack.Runtime,
 stack: MyApp.AI,
 packages: [spectre_ledger: [server_store: [backend: :postgres,
   repo: MyApp.Repo, table: "spectre_server_store"]]]}
```

Managed GAM discovers this active resource through the existing Stack contract.
An explicit `gam.store` wins, followed by an explicit checkpoint adapter; merely
installing the package without configuring its resource does not choose storage.
Use `gam: [storage: {:disk, authority_directory}]` for file authority, or
`gam: [storage: :ledger]` for canonical GAM history in the same Repo (see below).
Checkpoint and Managed GAM coordination use the selected port, without a second
local command journal. Store lifetime belongs to the Stack, not an individual Server.

### Canonical GAM history in PostgreSQL

The coordination Store implements optional `gam_domain/1`, returning the existing
`Spectre.GAM.Ledger.Store.Postgres` adapter and `Spectre.Ledger.DomainStore` on
the same Repo. `gam_ledger/1` remains available for history-only callers. Select
`gam: [storage: :ledger]` on an AgentServer with a stable identity and this active
Stack resource. No duplicate history behavior or Repo process is needed.

Install the GAM tables in an application-owned migration, in addition to the
coordination DDL above:

```elixir
{:ok, %{up: up, down: down}} =
  Spectre.GAM.Ledger.Store.Postgres.migration_sql(
    schema: "public", table_prefix: "spectre_gam")
# In the migration's up/0: Enum.each(up, &execute/1).
# In down/0: Enum.each(down, &execute/1); dropping history is destructive.

# Also install the derived Domain read model, including on existing databases:
{:ok, %{up: projection_up, down: projection_down}} =
  Spectre.Ledger.DomainStore.migration_sql(
    schema: "public", table_prefix: "spectre_gam")
# up/0: Enum.each(projection_up, &execute/1).
# down/0: Enum.each(projection_down, &execute/1).
```

The resource option `gam_table_prefix: "spectre_gam"` and `schema:` must match the
migration. The combined Domain backend allows prefixes up to 46 bytes so its
derived table names fit PostgreSQL's identifier limit. GAM query/transaction
timeouts inherit `query_timeout`; authority
transactions run through the Domain's existing serialized commit boundary and
the shared Repo pool, separately from the coordination owner's `max_workers`.
`rebuild_timeout` (default 60 seconds) separately bounds the transaction which
fences and clears the disposable projection on cold startup. Ordinary reads and
commits retain `query_timeout`. Cold replay is proportional to history and uses
those ordinary per-operation timeouts; it is not a constant-time snapshot restore.
This does not turn GAM commits into background acknowledgements or make all
Server transitions independent of DB latency.

The immutable foundation profile lives in coordination storage; canonical GAM
batches use dedicated append-only tables, CAS, batch identity and digest checks.
Reopening verifies the ledger before using its derived State, preserving Acts,
Outcomes, revocations and consumed nonces. Store/Repo ownership stays with the
Stack/application. Neither bootstrap nor queries create schemas, erase history,
or silently fall back to an in-memory authority during an outage.
An unresolved ambiguous canonical append halts its GAM Domain under the existing
kernel contract. Its Managed owner remains available. Retry recovery with application backoff
after the database returns; normal recovery verifies the committed prefix.
Managed hosts can explicitly select a fresh RAM Domain through `recovery_policy`
or `Spectre.Server.recover_storage/2`; it never reuses the old Domain grants or
merges divergent histories. See the core Domain storage guide. This
differs from an inbox-only failure, which reports the policy event without
requiring a Domain halt. Tests exercise both paths with an actual PostgreSQL
stop/start and verify that authority is not recreated or advanced in volatile RAM.

Existing `{:disk, path}` Domains keep their files. Changing to `:ledger` is not
an automatic migration: history, foundation profile and checkpoint/journal state
must move coherently, or use a new Domain. The core rejects an existing default
checkpoint without its matching database authority profile. A custom host can
use `{:ledger, {Adapter, opts}}`; file ServerStore returns `:not_configured` from
`gam_ledger/1` and continues to use explicit disk authority.

The Domain publishes individual current records through
`Spectre.GAM.Domain.Projection.Store`: foundation metadata, authorities, scopes,
Acts, Decisions, Evidence, Presentations, Outcomes, Duties, consumed nonces,
Meter and dispatch state. The head and row changes commit atomically after the
canonical batch is verified. Pages and exact multi-key reads require the exact
Domain/generation/revision and have row/byte budgets. Standalone reads use one
repeatable-read, read-only transaction for their head and data. This avoids WAL
writes from locking the head for each historical lookup while retaining a
consistent snapshot. Inside an existing application transaction, the adapter
keeps that transaction and the original shared head lock. Writes still lock
and compare the current head before committing. On restart only these
derived tables are rebuilt; history is untouched. This prevents a corrupt saved
projection from becoming authority. Kernel/Fold semantics remain shared with audit.

```elixir
alias Spectre.GAM.Domain.Projection.{Rows, Store}
{:ok, %{projection: projection}} = Spectre.Ledger.ServerStore.gam_domain(store)
{:ok, %{handle: handle, cursor: cursor}} = Store.head(projection, domain_ref)
{:ok, records, next} = Store.page(projection, handle, cursor, :acts, limit: 20)
decoded = Enum.map(records, fn {key, bytes} -> {key, Rows.decode(bytes)} end)
```

These are trusted host-side reads, not redacted public diagnostics. Values may
contain sensitive operation material. `Rows.decode/1` accepts only fixed GAM
record types; it does not deserialize runtime capabilities. A changed generation
or revision invalidates previous cursors. Startup performs no schema DDL.

**Memory:** the Domain now uses these rows during admission and releases
historical records after each command. Its retained State contains the Genesis
authority roots and active frontiers. Relevant dependency closures and active
work can still grow; full inspection APIs can materialize history temporarily.
Without a projection adapter the original RAM mode is unchanged. The pure Fold
and kernel use the same rules in both modes. Existing projection installations
must apply `Spectre.Ledger.DomainStore.upgrade_sql/1` in a migration before
starting the updated runtime. These are derived indexes, not another journal. See the core's
[profile](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/OTP_PROFILE_0_4.md).

### Coordination query execution

The PostgreSQL owner stores only configuration and active query references.
Operations run in monitored tasks using the Repo's existing pool; unrelated reads
can proceed while a row is locked. `max_workers` (default 16) and `query_timeout`
(default 4000 ms) bound concurrent I/O and waiting, not request history. Writes
use transactions with `synchronous_commit=on`; deployment durability still depends
on PostgreSQL/filesystem configuration. A timeout can leave the outcome unknown:
retry using the same identity and CAS rules, never assume a failed acknowledgement
means that a write did not commit. The adapter does not create a Repo or run DDL.

`ServerStore.list(store, tuple_prefix, limit: 50, max_bytes: 1_048_576, after: cursor)`
returns `{:ok, [{key, %{revision: token, value: value}}], next_cursor}`.
Keyset pagination uses an indexed namespace range and bounds payload bytes before
returning them from SQL. It never loads the complete inbox to find one page.

Approval requests retain the frozen request, digest, rendering acknowledgement
and exact signed terminal decision in the same record. IDs are scoped by receiver
and sender; atomic insertion rejects conflicting retransmissions. There is no
automatic expiry deletion, inbox quota or cache of historical records in the
AgentServer. UI pages and developer policy callbacks are documented in the
[core approval guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/EXTERNAL_APPROVALS.md).
Explicit Store deletion preserves a SQL revision tombstone; database retention
and any eventual physical removal remain application policy. PostgreSQL exposes
current records and opaque revisions, not the file log's `entries/3` or compaction.
Neither backend promises an audit record for every intermediate CAS revision.

Database failures feed the core's `handle_approval_inbox/2` as `:storage_error`
events with operation/reference and the unchanged request when receiving one.
A host may opt into private volatile buffering and retry after recovery; the
library never acknowledges that buffer as a durable approval. The callback's
outage branch may update private state only. The authenticated consumer receives
the technical error event even without a custom callback. See the
[database outage policy](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/EXTERNAL_APPROVALS.md#database-outage-policy).

## Payloads and storage policy

Ledger does not automatically encrypt saved values. GAM rows and frozen approval
requests can contain operation material and rendered arguments. Configure the
Server's `payload_store: {MyApp.Payloads, options}` to keep protected bodies in
application-owned storage; the handler implements `put/3` and `fetch/3` and chooses
its reference format, encryption and retention. See
[payload storage](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/PAYLOAD_STORAGE.md). Protect database access and backups
according to the application's disclosure policy.

## Reproducible OTP diagnostics

From this checkout with PostgreSQL binaries installed:

```sh
SPECTRE_PATH=../spectre MIX_ENV=test ERL_FLAGS='+S 4:4' mix run bench/otp_profile.exs
```

The developer probe starts an isolated temporary PostgreSQL cluster, two real
AgentServers and Skill processes with independent Domains. It grows the inbox,
executes human-consented operations and samples every live BEAM process after GC,
including reductions, heaps, memory, mailbox lengths, links, monitors and retained
binary bytes. It samples transient peaks, checks Gate task cleanup and measures
idle/teardown phases. Set `PROFILE_OUTPUT`, `PROFILE_BATCHES` and
`PROFILE_BATCH_SIZE` and `PROFILE_IDLE_MS` for reproducible artifacts and larger workloads. Full GC is
intrusive; run this in an isolated VM, never against a production node.

Set `PROFILE_EPROF=true` with a smaller representative workload to capture OTP
`:eprof` function and process CPU costs. The sustained memory probe runs without
call tracing so profiling overhead does not distort the baseline. Both Gate and
PostgreSQL query tasks have an owner-linked Task.Supervisor: forced owner death
also terminates its active tasks, including a hung custom Store call.
The PostgreSQL test fixture uses `fsync=off` for speed; these probes validate
process/storage semantics, not power-loss durability of a production deployment.
