# Documentation Strategy

## Overview

Lythonic uses a two-tier documentation approach:

1. **Module docstrings** - API reference lives in source code (e.g., `src/lythonic/state/__init__.py`)
2. **Static site** - Tutorials, how-tos, and rendered API docs via MkDocs

## Stack

- **MkDocs** - Static site generator
- **Material for MkDocs** - Theme with navigation, search, dark mode
- **mkdocstrings** - Extracts docstrings from Python modules

## Directory Structure

```
docs/
  index.md                    # Landing page with feature list and quick example
  getting-started.md          # Installation + first working example

  tutorials/                  # Step-by-step learning
    first-schema.md           # Schema definition in depth
    crud-operations.md        # All database operations
    cashflow-example.md       # Real-world multi-tenant example

  how-to/                     # Task-focused guides
    define-schema.md          # Quick reference for schema patterns
    multi-tenant.md           # Multi-tenant app patterns

  reference/                  # API docs (auto-extracted from module docstrings)
    core.md                   # ::: lythonic (GlobalRef, Result, etc.)
    state.md                  # ::: lythonic.state (DbModel, Schema, etc.)
    user.md                   # ::: lythonic.state.user (UserOwned, UserContext)
    types.md                  # ::: lythonic.types (KnownType, MapPair, etc.)
    compose.md                # ::: lythonic.compose (Method, ArgInfo, etc.)
    compose-logic.md          # ::: lythonic.compose.logic (LogicNode, LogicGraph)
    compose-cli.md            # ::: lythonic.compose.cli (ActionTree, RunContext)
    periodic.md               # ::: lythonic.periodic (Frequency, Interval, etc.)
    misc.md                   # ::: lythonic.misc (ensure_dir, tabula_rasa_path)

mkdocs.yml                    # Site configuration and nav structure
```

## Content Types

Following the [Diataxis](https://diataxis.fr/) framework:

| Type | Purpose | Location |
|------|---------|----------|
| **Tutorials** | Learning-oriented, follow along | `docs/tutorials/` |
| **How-To Guides** | Task-oriented, solve a problem | `docs/how-to/` |
| **Reference** | API documentation | `docs/reference/` + module docstrings |
| **Explanation** | Background, design decisions | `docs/explanation/` (future) |

## What Goes Where

### Module docstrings (source code)

The **primary home** for API documentation. Every public module should have a
docstring in its `__init__.py` (or the module file itself) that covers:

- One-line summary of what the module does
- `## Quick Start` section with a minimal working example
- Sections explaining key concepts, organized by topic
- Usage examples for non-obvious patterns

mkdocstrings extracts these automatically into the static site. This means the
source code is always the source of truth for API docs.

**When to update module docstrings:**
- Adding or changing public API (classes, functions, constants)
- Changing behavior that users depend on
- Adding a new module (also create a `docs/reference/<module>.md` page)

**What belongs here:**
- API descriptions, parameters, return types
- Usage examples that show how to call the API
- Caveats, gotchas, and non-obvious behavior

**What does NOT belong here:**
- Step-by-step learning guides (use tutorials)
- Design rationale or architecture discussion (use explanation)

### Tutorials (`docs/tutorials/`)

Long-form, sequential guides that teach a concept from scratch. A reader follows
start to finish.

**When to write a tutorial:**
- Introducing a major feature (e.g., schema definition, CRUD, multi-tenant)
- A concept that requires context and progressive building (not just API calls)

**Style:**
- Complete runnable examples throughout
- Build on previous steps sequentially
- Use concrete, realistic models (Task, Author, Book, Account)
- Focus on the learning path, not exhaustive API coverage

### How-To Guides (`docs/how-to/`)

Short, task-focused pages. A reader wants to accomplish a specific goal.

**When to write a how-to:**
- A common task that combines multiple API calls
- A pattern that is not obvious from the API reference alone

**Style:**
- Answer "How do I X?"
- Short and focused on one task
- Link to reference for full API details
- No sequential learning assumed

### Reference Pages (`docs/reference/`)

Thin wrappers that pull docstrings from source code via mkdocstrings. The page
itself is minimal:

```markdown
# lythonic.state

Core module for SQLite ORM functionality.

::: lythonic.state
    options:
      show_root_heading: false
      members:
        - DbModel
        - Schema
```

**When to add a reference page:**
- Adding a new module to the project
- Also add it to `nav:` in `mkdocs.yml`

### Design Specs and Plans (`docs/superpowers/`)

Technical design documents and implementation plans for features, generated
during brainstorming and planning phases:

- `docs/superpowers/specs/` - Design specs (requirements, architecture, data models)
- `docs/superpowers/plans/` - Implementation plans (task-by-task breakdown)

These are working documents, not user-facing.

Directory `docs/superpowers/` preserved over release cycle, and wiped clean right 
after release. So if you inspect release tag you can find all specs and tags that
went into release. 

## Writing Guidelines

### Module Docstrings

Format for `__init__.py` files:

```python
"""
Module summary.

## Quick Start
Brief example.

## Section
Detailed docs...
"""
```

Follow the docstring conventions in `CLAUDE.md` (concise, no arg/return
repetition when obvious, backticks for code, triple-quote on own lines).

### Class and Function Docstrings

- Public classes and functions SHOULD have docstrings.
- Internal/private ones only need docstrings if purpose is non-obvious.
- Do NOT repeat what is obvious from names and type annotations.
- DO explain "why", caveats, and non-obvious behavior.

### Doctests in Docstrings

Docstrings can include `>>>` examples that serve as both documentation and
executable tests. pytest runs these automatically via `--doctest-modules`.

**When to use doctests:**
- Pure functions with simple, deterministic inputs and outputs
- Parsing, formatting, conversion, and validation logic
- Any function where showing concrete input/output examples clarifies usage
  better than prose

**When NOT to use doctests:**
- Tests needing setup/teardown (temp files, databases)
- Non-deterministic output (timestamps, UUIDs)
- Error/exception testing
- Tests needing mutable module state

**Style:**
- Keep examples to 1-5 lines of `>>>` each
- Use realistic but minimal inputs
- Order from simple to complex
- Each example should be self-contained within its docstring block

**Example:**

```python
def _parse_nsref(nsref: str) -> tuple[list[str], str]:
    """
    Parse nsref into (branch_parts, leaf_name).

    >>> _parse_nsref('market.data:fetch_prices')
    (['market', 'data'], 'fetch_prices')
    >>> _parse_nsref('market:fetch_prices')
    (['market'], 'fetch_prices')
    >>> _parse_nsref('fetch_prices')
    ([], 'fetch_prices')

    """
```

When a standalone inline test (below `## Tests`) only verifies input/output
on a pure function, consider converting it to a doctest. This improves
documentation while keeping the test. Remove the standalone test after
adding the doctest to avoid duplication.

See `testing.md` for the full testing strategy, including when to use
doctests vs. inline tests vs. separate test files.

### Tutorials and How-Tos

- Use fenced code blocks with `python` language tag.
- All code examples should be complete and runnable.
- Use admonitions (`!!! note`, `!!! warning`) sparingly and only when needed.

## Build Commands

```bash
# Install docs dependencies
uv sync --group docs

# Build static site to site/
make docs

# Local preview at http://localhost:8000
make docs-serve

# Deploy to GitHub Pages
make docs-deploy
```

## Adding New Documentation

### New Module

1. Add docstring to the module's `__init__.py`
2. Create `docs/reference/<module>.md` with mkdocstrings directive
3. Add to `nav:` in `mkdocs.yml`

### New Tutorial

1. Create `docs/tutorials/my-topic.md`
2. Add to `nav:` in `mkdocs.yml`
3. Link from related pages

### New How-To

1. Create `docs/how-to/my-task.md`
2. Add to `nav:` in `mkdocs.yml`

## Configuration

See `mkdocs.yml` for site metadata, theme settings, navigation structure,
mkdocstrings handler options, and Markdown extensions.

## Deployment

Options:

1. **GitHub Pages** - `make docs-deploy` pushes to `gh-pages` branch
2. **Netlify** - Connect repo, build command: `make docs`
3. **Read the Docs** - Supports MkDocs natively

## Search

Material theme includes Lunr.js search. No configuration needed.
For larger sites, consider Algolia DocSearch (free for open source).
