# Spectre Ledger

Ledger provides durable storage and inspection at Spectre's persistence boundaries.
For Spectre 0.4, `Spectre.Ledger.ServerStore` stores AgentServer coordination state
and GAM journal values with CAS revisions, checksums, synced writes and atomic
compaction. It is a storage port; it does not become another agent runtime.

The package also retains **separate Instance checkpoint and receipt archives**,
Memory/PostgreSQL adapters and verified bundles. Those append-only archive APIs
are useful directly, but their formats and options are not the AgentServer Store.

## Installation on the 0.4 development branch

This branch targets the **unreleased Spectre 0.4 architecture**. Use matching Git
branches; the examples do not assume that Spectre 0.4 is available from Hex.
For reproducible deployments, replace branch selectors with reviewed commit refs.

```elixir
def deps do
  [
    {:spectre, github: "elchemista/spectre",
     branch: "feature/agent-server-gam", override: true},
    {:spectre_ledger, github: "elchemista/spectre_ledger",
     branch: "feature/stack-tools-0.4"}
  ]
end
```

The manifest requirement `{:spectre, "~> 0.4.0"}` describes compatibility, not
publication availability. Local contributors can use `SPECTRE_PATH=../spectre` for this repository's dependency override.

## Persist and inspect an AgentServer

```elixir
defmodule MyApp.AI do
  use Spectre.Stack
  install Spectre.Ledger
end

defmodule MyApp.SupportServer do
  use Spectre.Server, tools: MyApp.AI

  def handle_flow({:support, :checkpoint}, _input, ctx),
    do: ledger(ctx, {:head}, on_result: :checkpoint)

  def handle_result(:checkpoint, {:ok, head}, ctx),
    do: respond(ctx, "Checkpoint revision #{head.revision}: #{head.bytes} bytes")

  def handle_result(tag, result, ctx), do: super(tag, result, ctx)
end

defmodule MyApp.SupportAgent do
  use Spectre.Agent, stack: MyApp.AI, server: MyApp.SupportServer
  flow :support do
    on :checkpoint, regex: ~r/^checkpoint$/i
  end
end

{:ok, store} = Spectre.Ledger.ServerStore.start_link(path: "data/support.ledger")
{:ok, server} = Spectre.Server.start_link(
  agent: MyApp.SupportAgent,
  identity: %{subject: "conversation-42"},
  gam: [storage: {:disk, "data/support-gam/conversation-42"}],
  checkpoint: {Spectre.Ledger.ServerStore, store, {:support, "conversation-42"}},
  consumer: self())
{:ok, interaction} = Spectre.Server.input(server, "checkpoint")
```

Put both processes under your supervision tree in an application. The stable
identity and durable GAM directory preserve matching authority and coordination
history across restarts; an external checkpoint must not use an ephemeral GAM
ledger. `head` returns
only `%{revision: ..., digest: ..., bytes: ...}` for this Server's checkpoint.
It does not copy saved messages, candidates or assigns into a tool result. Explicit
foreign keys are rejected. `{:entries}` returns metadata, not an immutable audit trail.

## A native revisioned Store

```elixir
{:ok, revision} = Spectre.Ledger.ServerStore.put_new(store, :example, %{value: 1})
{:ok, next_revision} = Spectre.Ledger.ServerStore.compare_and_swap(
  store, :example, revision, %{value: 2})
:ok = Spectre.Ledger.ServerStore.delete(store, :example, next_revision)
:ok = Spectre.Ledger.ServerStore.compact(store)
```

Revision tokens are opaque and monotonic; a new key need not start at 1.
Compaction removes superseded values and deleted keys from memory and disk while a
global sequence prevents stale CAS tokens matching recreated keys. It runs automatically
at append capacity and can be scheduled explicitly. Live values must still fit capacity
(default 256 MiB). Writes, initialization and compaction use durability barriers;
reads wait for a busy Store. Corrupt/torn logs fail closed for operator inspection.

Compaction is not forensic erasure of filesystem snapshots or backups. The
[Store and recovery guide](docs/SPECTRE_0_4.md) covers format upgrades, GAM journal
configuration, Stack-owned resources and the distinct legacy archive contracts.

## Documentation

- [AgentServer integration, helpers and authority](docs/SPECTRE_0_4.md)
- [Migration from the 0.3 integration](docs/MIGRATING_TO_0_4.md)
- [Standalone and retained runtime reference](docs/INSTANCE_ARCHIVES.md)
- [Public API](docs/PUBLIC_API.md) and [changelog](CHANGELOG.md)
- [Governed Act Model](https://spectre.guide/en/governed-act-model) and [core tool lifecycle](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/STACK_TOOLS.md)

## Use inside a reusable Skill

The `ledger` helper is generated in a Skill callback module with
`use Spectre.Server, tools: MyApp.AI`, using the same installed Stack. A
`Spectre.Skill` with `server:` has a separate GenServer, local assigns, workers,
streams, pending intentions and checkpoint key.

**It uses the root AgentServer's same automatic GAM integration, Domain and
authority ledger.** Configure `gam:` once on the root. Mounting a Skill creates
no new Domain or grant and needs no additional Host or consent ingress. Shared
Stack resources stay in their existing supervised runtime.

Agent and Skill workers can run concurrently; the shared kernel orders admission.
A governed effect must be enabled in root `operations` and permitted by the
mount's `governed:` references. Each Skill proposal records its mount and
invocation provenance. Consent identifies the originating Skill and exact
presentation: approving an email cannot approve another Skill's memory write.
Timer closure is recorded in the same Domain and does not erase a Duty.
Ordinary cognitive operations retain their configured provider boundary.

See the [core Skill ownership and consent guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/SKILLS.md),
[automatic GAM responsibilities](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/MANAGED_GAM.md)
and [two-Skill LiveView showcase](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/DEMO.md).
