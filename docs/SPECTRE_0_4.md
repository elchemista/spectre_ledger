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
bridge = [scope: gam_scope,
  store: {Spectre.Ledger.ServerStore, MyApp.Checkpoints},
  bindings: host_bindings]
```

Pass `checkpoint: checkpoint` and `gam: {Spectre.Server.GAM, bridge}` to the Server.
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
host's retention controls separately. PostgreSQL/Memory archive backends remain
[separate Instance APIs](INSTANCE_ARCHIVES.md); they are not alternative backend
options for this `ServerStore` implementation.

## Who owns the loop

`Spectre.Stack` installs immutable capabilities and tool descriptors. The Agent
selects that Stack and declares routing; `Spectre.Server` owns the GenServer,
conversation, pending continuations and asynchronous results. Use
`use Spectre.Server, tools: MyApp.AI` to generate private helpers. A helper returns
an updated context containing a command; return that context from the callback.
It does not synchronously return the package's result. Tuple input chooses the
operation, and `on_result:` identifies the continuation in `handle_result/3`.

Preparation validates ordinary input before dispatch. The Server records command
identity/outbox state, invokes a worker, projects a bounded portable result and
acquires that result before the callback. Durable recovery needs a configured
checkpoint Store. Keep live clients and process handles in supervised resources
or process-local private context, not durable assigns. Native OTP callbacks remain
available; they use the same command-return boundary.

## Where GAM intervenes

The [Governed Act Model](https://spectre.guide/en/governed-act-model) separates
authority (Mandate), supporting facts (Evidence), the admitted execution record
(Act), and unresolved obligations (Duty). Installing a package connects a
capability; it does not grant authority to act on behalf of the end user.

A cognitive descriptor uses the tool worker path, without treating every model
call or classifier as a consent request. A governed descriptor becomes `propose`
before dispatch and uses the standard `Spectre.Server.GAM` bridge. Host-declared
surfaces still determine which effects, protected disclosures or resource uses
need governance; read-oriented operations are not categorically exempt from those
application boundaries. Ordinary configured cognition is not automatically a new
end-user authorization ceremony.

The host supplies a real GAM scope, Candidate factories, mandates/evidence,
Presentations or interpretation when needed, and admitted executors. `bind:` names
that host operation only. Native package calls outside the Server do not acquire
these guarantees automatically. Do not call an effect executor directly from a
flow callback and describe that call as governed.

## Recovery and errors

These packages' cognitive helper operations declare safe replay; the runtime can
retry interrupted cognitive work according to the command lifecycle. A result is
still scoped to its original command. Application input rejections arrive as
`{:error, {:tool_rejected, tool, operation, reason}}`; library domain errors retain
the package's tagged result. Invalid installation/configuration is a developer
error, not a successful acquisition. The general `{:tool_unknown, details}` result
belongs to nonrepeatable tool operations; it is not an instruction to retry effects.

Governed effects use GAM recovery and their original command identities. An
ambiguous outcome remains unresolved: it must not be reported as proof of failure
or sent again blindly. A changed checkpoint fingerprint needs an explicit host
migration. Contract drift is handled by the core recovery rules rather than
invoking new adapter code with old arguments. See the
[core lifecycle](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/STACK_TOOLS.md)
and [AgentServer guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/AGENT_SERVER.md).
