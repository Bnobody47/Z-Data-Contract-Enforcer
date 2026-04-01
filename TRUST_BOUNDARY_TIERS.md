# Trust Boundary Tiers for Data Contracts

This document defines how the Week 7 Data Contract Enforcer architecture behaves under different trust boundaries. It clarifies where lineage traversal works and where contract registry patterns are required.

## Tier 1 - Same Team, Same Repo

Scope:
- Single team controls producer and consumer systems.
- Full lineage graph is available from the Week 4 Cartographer output.

Enforcement model:
- ContractGenerator publishes contract clauses from profiled data.
- ValidationRunner executes structural and statistical checks on snapshots.
- ViolationAttributor traverses lineage and correlates with git history.

Blast radius model:
- Computed directly from lineage graph downstream nodes and contract lineage metadata.

## Tier 2 - Different Teams, Same Company

Scope:
- Teams share governance context but not full internal implementation details.
- Lineage is partial and often siloed by domain.

Enforcement model:
- Keep Tier 1 checks where lineage is visible.
- Add a central contract registry where teams subscribe to contract IDs/versions.
- Notify subscribers on breaking or near-breaking changes.

Blast radius model:
- Combined output:
  - internal known blast radius from visible lineage
  - subscriber blast radius from contract registry dependency graph

## Tier 3 - Different Companies

Scope:
- No shared lineage graph across organizations.
- Producers cannot introspect consumer internals.

Enforcement model:
- Contract registry is the interoperability layer.
- Consumers register dependencies and acceptable version ranges.
- Producers publish versioned contracts and compatibility metadata.
- Breaking changes require major version changes and deprecation windows.

Blast radius model:
- Cross-company blast radius is registry-based:
  - number of subscribed consumers
  - affected contract versions
  - notification delivery status
- Each consumer computes internal impact independently.

## Operational Implications

1. Do not rely on lineage traversal beyond trust boundaries.
2. Enforce compatibility before release where possible.
3. Treat subscriber notifications as first-class compliance artifacts.
4. Keep both structural and semantic/statistical checks active:
   - structural catches schema/type breakage
   - statistical catches meaning drift (for example 0.0-1.0 to 0-100 scale changes)

## Alignment with Real Tooling

- Confluent Schema Registry:
  - Focuses on compatibility enforcement at registration time.
  - Blocks disallowed breaking changes before production writes.

- dbt Mesh:
  - Works well for Tier 2 where dependencies are centrally modeled.
  - Impact can be computed from governed DAG relationships.

- Pact:
  - Consumer-driven contract testing model.
  - Producer CI fails when consumer expectations are violated.

## Recommended Pattern for This Repo

- Current default: Tier 1 mode (lineage-based attribution).
- Planned extension: add registry mode for Tier 2/3:
  - `contracts/attributor.py --mode lineage`
  - `contracts/attributor.py --mode registry`

This preserves educational fidelity for Week 7 while documenting a production-ready expansion path.
