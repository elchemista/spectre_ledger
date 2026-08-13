# Security policy

## Reporting

Please report suspected vulnerabilities privately through the repository's
security advisory channel. Do not include credentials, production checkpoints,
or customer data in a public issue.

## Trust boundaries

Ledger checkpoints and bundles can contain application state. Content
addressing proves integrity, not confidentiality, authorization, provenance, or
safe execution. Encrypt storage and transport where required, restrict database
and bundle access, and apply application retention policy independently.

Bundle verification:

- enforces total JSON, entry, object, per-object, total decoded object, and JSON
  nesting limits before accepting content;
- rejects duplicate JSON keys, unknown envelope fields, non-canonical base64,
  incomplete object sets, broken entry chains, and digest mismatches;
- validates checkpoints through `Spectre.Foundation.Conformance`;
- does not restore or execute `Spectre.Run` values embedded in checkpoints.

Verification does not authorize a bundle for a particular Agent or deployment.
Hosts must apply their own manifest and policy allowlists before playback or
import.

PostgreSQL credentials and Repo configuration belong to the host application.
Ledger never persists them in entries or bundles. Doctor is read-only and
telemetry emits classified errors and digested identifiers, not raw error
terms, stream keys, checkpoints, or connection configuration.

## Supported versions

Security fixes are provided for the latest released Spectre Ledger 0.1.x
version while it remains compatible with Spectre 0.3.1.
