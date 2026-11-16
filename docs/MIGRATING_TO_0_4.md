# Migrate Ledger integration from 0.3 to 0.4

This guide targets the current unreleased development branch, not a published
Hex release. Start with the matching Git dependencies in the [README](../README.md).
The package's compatibility requirement is Spectre `~> 0.4.0`; use the core
`feature/agent-server-gam` and satellite `feature/stack-tools-0.4` branches together.

## Move ownership, preserve capabilities

| Earlier integration | AgentServer 0.4 |
| --- | --- |
| `checkpoint_store: Ledger.checkpoint_store(opts)` for an Instance | `checkpoint: {Ledger.ServerStore, store, key}` for an AgentServer |
| Archive revision starts at a familiar sequential value | Treat Server Store revisions as opaque CAS tokens; fresh keys may start above 1 |
| Read arbitrary raw checkpoint by helper key | Helper is fenced to current key and returns only metadata |
| Append history or tombstones kept forever | Coordination log compacts; immutable archives remain the separate legacy API |
| Earlier SPLS1/SPLS2 checkpoint-v1 file | Read compatibly; compaction upgrades marker and retains global revision fences |

Keep routes in the Agent and move execution callbacks to
`use Spectre.Server, tools: MyApp.AI`. Return the context produced by `ledger`
and handle its tagged result in `handle_result/3`. The
[core migration guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/MIGRATING_TO_0_4.md) covers shared Server, GAM and
checkpoint changes; old Instance snapshots are not AgentServer snapshots.

## What remains available

Ledger retains the Instance checkpoint and boundary-receipt archives, including Memory/PostgreSQL backends, bundles and receipt inspection. They are separate from the compacting AgentServer `ServerStore`; options and revision formats are not interchangeable.

See the [retained reference](INSTANCE_ARCHIVES.md) for these APIs and
[0.4 helper/lifecycle contracts](SPECTRE_0_4.md) for the new integration. Migration
is an ownership/API change, not a promise that every legacy convenience now
implicitly routes through AgentServer or GAM.

## Skill Servers

Mounted Skills share their root's GAM Domain and retain separate checkpoints.
Follow the [core Skill guide](https://github.com/elchemista/spectre/blob/feature/agent-server-gam/docs/SKILLS.md); this package requires no separate
Skill host or second GAM configuration.
