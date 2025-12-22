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
  index.md                    # Landing page
  getting-started.md          # Quick start guide

  tutorials/                  # Step-by-step learning
    first-schema.md
    crud-operations.md
    cashflow-example.md

  how-to/                     # Task-focused guides
    define-schema.md
    multi-tenant.md

  reference/                  # API docs (auto-extracted)
    state.md                  # ::: lythonic.state
    user.md                   # ::: lythonic.state.user
    types.md                  # ::: lythonic.types

mkdocs.yml                    # Site configuration
```

## Content Types

Following the [Diátaxis](https://diataxis.fr/) framework:

| Type | Purpose | Location |
|------|---------|----------|
| **Tutorials** | Learning-oriented, follow along | `docs/tutorials/` |
| **How-To Guides** | Task-oriented, solve a problem | `docs/how-to/` |
| **Reference** | API documentation | `docs/reference/` + module docstrings |
| **Explanation** | Background, design decisions | `docs/explanation/` (future) |

## Writing Guidelines

### Module Docstrings

Keep comprehensive API docs in module `__init__.py` files. Format:

```python
"""
Module summary.

## Quick Start
Brief example.

## Section
Detailed docs...
"""
```

mkdocstrings extracts these automatically.

### Tutorials

- Step-by-step, sequential
- Include complete runnable examples
- Focus on learning path, not API completeness

### How-To Guides

- Short, focused on one task
- Answer "How do I X?"
- Link to reference for details

### Reference Pages

Use mkdocstrings directive:

```markdown
# lythonic.state

::: lythonic.state
    options:
      members:
        - DbModel
        - Schema
```

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

## Configuration

See `mkdocs.yml` for:

- Site metadata and theme settings
- Navigation structure
- mkdocstrings handler options
- Markdown extensions

## Deployment

Options:

1. **GitHub Pages** - `make docs-deploy` pushes to `gh-pages` branch
2. **Netlify** - Connect repo, build command: `make docs`
3. **Read the Docs** - Supports MkDocs natively

## Adding New Documentation

### New Tutorial

1. Create `docs/tutorials/my-topic.md`
2. Add to `nav:` in `mkdocs.yml`
3. Link from related pages

### New How-To

1. Create `docs/how-to/my-task.md`
2. Add to `nav:` in `mkdocs.yml`

### New Module Reference

1. Add docstring to module `__init__.py`
2. Create `docs/reference/module.md` with mkdocstrings directive
3. Add to `nav:` in `mkdocs.yml`

## Search

Material theme includes Lunr.js search. No configuration needed.
For larger sites, consider Algolia DocSearch (free for open source).
