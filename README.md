# lythonic

[![PyPI version](https://img.shields.io/pypi/v/lythonic.svg?label=lythonic&color=blue)](https://pypi.org/project/lythonic/)
[![Documentation](https://img.shields.io/badge/docs-GitHub%20Pages-blue)](https://walnutgeek.github.io/lythonic/)

**Lightweight pythonic toolkit for building complex workflows.**

Lythonic combines composable callable pipelines with structured SQLite
persistence — two pillars that work together or independently.

## Installation

```bash
uv add lythonic
```

## Compose

Build callable pipelines and DAGs with automatic execution and provenance.

- **Namespace** — hierarchical registry for callables with dot-path access
- **DAG** — wire nodes with `>>`, validate types and cycles, fan-out/fan-in
- **DagRunner** — async execution with output wiring, pause/restart/replay
- **Caching** — SQLite-backed cache with probabilistic TTL refresh

## State

Structured data persistence powered by SQLite and Pydantic.

- **DbModel** — define tables as Pydantic models with automatic DDL generation
- **Schema** — manage multiple tables with referential integrity
- **CRUD** — insert, select, update, delete with typed filtering
- **Multi-tenant** — built-in user-scoped access patterns via `UserOwned`

## Documentation

Full documentation at [walnutgeek.github.io/lythonic](https://walnutgeek.github.io/lythonic/).
