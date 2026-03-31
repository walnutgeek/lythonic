# Documentation Revamp: Compose-Forward Reframing

## Motivation

The `compose` package has grown to 7 modules (namespace, DAG runner, provenance,
cached, namespace_config, cli, logic) while `state` remains at 2. All existing
documentation — README, index.md, pyproject.toml description, tutorials — frames
lythonic as a SQLite/Pydantic ORM. The compose side has API reference pages but
zero tutorials or examples. This revamp reframes the project as a two-pillar
toolkit with compose as the lead.

## New Tagline

**"Lightweight pythonic toolkit for building complex workflows"**

Replaces: "Light-weight pythonic SQLite integration with Pydantic"

Used in: `pyproject.toml` description, `README.md`, `docs/index.md`.

## Deliverables

### 1. Compose Tutorial (`docs/tutorials/compose-pipeline.md`)

A single end-to-end tutorial that threads through the compose stack by building
a data-enrichment pipeline. Each section builds on the previous with runnable
code at each stage.

**Sections:**

1. **Register callables in a namespace** — `Namespace.register()`, dot-path
   access, `GlobalRef`-style naming (`"pipeline.data:fetch"`)
2. **Define a DAG** — connect nodes with `>>`, fan-out/fan-in, type-checked
   edges via `DagNode`, `DagEdge`, `Dag`
3. **Execute the DAG** — `DagRunner` with async execution, automatic output
   wiring between nodes, `DagRunResult`
4. **Add provenance** — switch from `NullProvenance` to `DagProvenance`,
   show SQLite-backed run history, query past runs
5. **Add caching** — wrap an expensive callable with
   `register_cached_callable`, show TTL behavior (min/max), probabilistic
   refresh

**Example domain:** Data enrichment pipeline — fetch data, transform, validate,
store. Concrete enough to be realistic, simple enough to not distract from the
compose concepts.

**Style:** Follow existing tutorial conventions (see `first-schema.md`,
`crud-operations.md`). Each step shows code, explains what it does, shows
output. No separate "explanation" blocks — interleave code and prose.

### 2. Rewrite `docs/index.md` (Docs Site Landing Page)

**Structure:**

1. Title + new tagline
2. Brief paragraph: what lythonic is, naming both pillars (compose and state)
3. **Compose example first** — ~15 lines excerpted/adapted from the tutorial
   opening. Show namespace registration + DAG definition + execution. Enough
   to demonstrate the flavor.
4. **State example second** — trimmed version of the current DbModel + CRUD
   example. Shorter than current.
5. "Next steps" links: compose tutorial, state tutorials, API reference

### 3. Rewrite `README.md` (GitHub Landing Page)

**Structure:**

1. Project name + new tagline
2. One paragraph: what lythonic is, two pillars
3. Installation: `uv add lythonic`
4. Two "at a glance" sections:
   - **Compose** — 3-4 bullets (namespaces, DAGs, execution, caching)
   - **State** — 3-4 bullets (Pydantic models, SQLite, CRUD, multi-tenant)
5. Link to documentation site
6. No full code examples — keep it punchy, drive traffic to docs

### 4. Update `pyproject.toml`

Change the `description` field to the new tagline.

### 5. Reorder `mkdocs.yml` Navigation

```yaml
nav:
  - Home: index.md
  - Getting Started: getting-started.md
  - Tutorials:
      - Build a Pipeline: tutorials/compose-pipeline.md
      - Your First Schema: tutorials/first-schema.md
      - CRUD Operations: tutorials/crud-operations.md
      - Cashflow Tracking: tutorials/cashflow-example.md
  - How-To Guides:
      - Define a Schema: how-to/define-schema.md
      - Multi-Tenant Apps: how-to/multi-tenant.md
  - API Reference:
      - lythonic: reference/core.md
      - compose:
          - lythonic.compose: reference/compose.md
          - "...namespace": reference/compose-namespace.md
          - "...namespace_config": reference/compose-namespace-config.md
          - "...dag_runner": reference/compose-dag-runner.md
          - "...dag_provenance": reference/compose-dag-provenance.md
          - "...cached": reference/compose-cached.md
          - "...cli": reference/compose-cli.md
          - "...logic": reference/compose-logic.md
      - state:
          - lythonic.state: reference/state.md
          - "...user": reference/user.md
      - lythonic.types: reference/types.md
      - lythonic.periodic: reference/periodic.md
      - lythonic.misc: reference/misc.md
  - Release Notes:
      - v0.0.10: release_notes/v0.0.10.md
      - v0.0.9: release_notes/v0.0.9.md
```

Key changes: compose tutorial listed first, compose reference grouped and
before state, reference sections organized by pillar.

## Out of Scope

- Existing state tutorials (first-schema, CRUD, cashflow) — untouched,
  just reordered after compose in nav
- Existing how-to guides — no changes
- API reference pages — no changes (auto-generated from module docstrings)
- Module docstrings — no changes
- New how-to guides for compose — deferred to a future release
