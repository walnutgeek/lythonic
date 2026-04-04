# Namespace Tags Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add tagging support to `NamespaceNode` with a tag query API on `Namespace` supporting `&` (AND), `|` (OR), `~` (NOT) operators.

**Architecture:** Tags are stored as `frozenset[str]` on `NamespaceNode`. A `_validate_tags()` helper rejects strings and invalid characters. `Namespace.query()` parses a simple expression language with standard precedence (`~` > `&` > `|`), collects all leaves recursively, and filters. Config models gain an optional `tags` field for round-trip persistence.

**Tech Stack:** Python 3.11+, Pydantic, pytest

---

## File Structure

| File | Responsibility |
|---|---|
| `src/lythonic/compose/namespace.py` | `NamespaceNode.tags`, `_validate_tags()`, `register()`/`register_all()` tags param, `Namespace.query()`, `_parse_tag_expr()`, `_eval_tag_expr()` |
| `src/lythonic/compose/namespace_config.py` | `NamespaceEntryConfig.tags`, `load_namespace()` tags pass-through, `dump_namespace()` tags output |
| `tests/test_namespace.py` | Tag registration, validation, query tests |
| `tests/test_namespace_config.py` | Config round-trip with tags |

---

### Task 1: Tag validation helper and NamespaceNode tags field

**Files:**
- Modify: `src/lythonic/compose/namespace.py:101-123`
- Test: `tests/test_namespace.py`

- [ ] **Step 1: Write failing tests for tag validation and storage**

Add to the end of `tests/test_namespace.py`:

```python
# Tags


def test_namespace_node_tags_default():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    node = NamespaceNode(method=method, nsref="test:sample_fn", namespace=ns)
    assert node.tags == frozenset()


def test_namespace_node_tags_set():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    node = NamespaceNode(
        method=method, nsref="test:sample_fn", namespace=ns, tags=frozenset({"slow", "market"})
    )
    assert node.tags == frozenset({"slow", "market"})


def test_namespace_node_tags_rejects_string():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    try:
        NamespaceNode(
            method=method, nsref="test:sample_fn", namespace=ns, tags="slow"  # pyright: ignore[reportArgumentType]
        )
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "str" in str(e)


def test_namespace_node_tags_rejects_invalid_chars():
    from lythonic.compose import Method
    from lythonic.compose.namespace import Namespace, NamespaceNode

    method = Method(_get_sample_fn())
    ns = Namespace()
    for bad_tag in ["slow&fast", "a|b", "~experimental", "has space"]:
        try:
            NamespaceNode(
                method=method, nsref="test:sample_fn", namespace=ns, tags=frozenset({bad_tag})
            )
            raise AssertionError(f"Expected ValueError for tag {bad_tag!r}")
        except ValueError as e:
            assert "tag" in str(e).lower()
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_namespace_node_tags_default tests/test_namespace.py::test_namespace_node_tags_set tests/test_namespace.py::test_namespace_node_tags_rejects_string tests/test_namespace.py::test_namespace_node_tags_rejects_invalid_chars -v`
Expected: FAIL — `__init__` doesn't accept `tags` yet.

- [ ] **Step 3: Implement `_validate_tags()` and update `NamespaceNode.__init__`**

In `src/lythonic/compose/namespace.py`, add the validation helper before the `NamespaceNode` class:

```python
_INVALID_TAG_CHARS = frozenset("&|~")


def _validate_tags(tags: frozenset[str] | set[str] | list[str] | None) -> frozenset[str]:
    """
    Validate and normalize tags to `frozenset[str]`. Rejects `str` input
    (common mistake) and tags containing operator characters or whitespace.
    """
    if tags is None:
        return frozenset()
    if isinstance(tags, str):
        raise TypeError("tags must be a collection of strings, not a str")
    result = frozenset(tags)
    for tag in result:
        if any(c in _INVALID_TAG_CHARS for c in tag):
            raise ValueError(f"Tag {tag!r} contains invalid characters (&, |, or ~)")
        if any(c.isspace() for c in tag):
            raise ValueError(f"Tag {tag!r} contains whitespace")
    return result
```

Update `NamespaceNode.__init__` to accept and store tags:

```python
def __init__(
    self,
    method: Method,
    nsref: str,
    namespace: Namespace,
    decorated: Callable[..., Any] | None = None,
    tags: frozenset[str] | set[str] | list[str] | None = None,
) -> None:
    self.method = method
    self.nsref = nsref
    self.namespace = namespace
    self._decorated = decorated
    self.metadata: dict[str, Any] = {}
    self.tags: frozenset[str] = _validate_tags(tags)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py::test_namespace_node_tags_default tests/test_namespace.py::test_namespace_node_tags_set tests/test_namespace.py::test_namespace_node_tags_rejects_string tests/test_namespace.py::test_namespace_node_tags_rejects_invalid_chars -v`
Expected: PASS

- [ ] **Step 5: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass, zero warnings.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "feat: add tags field to NamespaceNode with validation"
```

---

### Task 2: Pass tags through register() and register_all()

**Files:**
- Modify: `src/lythonic/compose/namespace.py:180-278`
- Test: `tests/test_namespace.py`

- [ ] **Step 1: Write failing tests for register with tags**

Add to `tests/test_namespace.py`:

```python
def test_register_with_tags():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register(
        this_module._sample_fn, nsref="test:fetch", tags={"slow", "market"}  # pyright: ignore[reportPrivateUsage]
    )
    assert node.tags == frozenset({"slow", "market"})


def test_register_without_tags():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    node = ns.register(this_module._sample_fn, nsref="test:fetch")  # pyright: ignore[reportPrivateUsage]
    assert node.tags == frozenset()


def test_register_all_with_tags():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    nodes = ns.register_all(
        this_module._sample_fn, this_module._another_fn, tags={"batch"}  # pyright: ignore[reportPrivateUsage]
    )
    assert all(n.tags == frozenset({"batch"}) for n in nodes)


def test_register_tags_validation():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    try:
        ns.register(this_module._sample_fn, nsref="test:fetch", tags="slow")  # pyright: ignore[reportPrivateUsage, reportArgumentType]
        raise AssertionError("Expected TypeError")
    except TypeError:
        pass
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_register_with_tags tests/test_namespace.py::test_register_without_tags tests/test_namespace.py::test_register_all_with_tags tests/test_namespace.py::test_register_tags_validation -v`
Expected: FAIL — `register()` doesn't accept `tags` yet.

- [ ] **Step 3: Add tags parameter to register(), _register_dag(), and register_all()**

In `src/lythonic/compose/namespace.py`, update `register()`:

```python
def register(
    self,
    c: Callable[..., Any] | str | Dag,
    nsref: str | None = None,
    decorate: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
    tags: frozenset[str] | set[str] | list[str] | None = None,
) -> NamespaceNode:
```

Inside `register()`, pass `tags` to the `NamespaceNode` constructor on line 214:

```python
node = NamespaceNode(method=method, nsref=nsref, namespace=self, decorated=decorated, tags=tags)
```

And pass `tags` through to `_register_dag()`:

```python
if isinstance(c, Dag):
    return self._register_dag(c, nsref, tags=tags)
```

Update `_register_dag()` signature:

```python
def _register_dag(self, dag: Dag, nsref: str | None, tags: frozenset[str] | set[str] | list[str] | None = None) -> NamespaceNode:
```

And pass tags to its `NamespaceNode` constructor on line 260:

```python
node = NamespaceNode(method=method, nsref=nsref, namespace=self, tags=tags)
```

Update `register_all()`:

```python
def register_all(
    self,
    *cc: Callable[..., Any],
    decorate: Callable[[Callable[..., Any]], Callable[..., Any]] | None = None,
    tags: frozenset[str] | set[str] | list[str] | None = None,
) -> list[NamespaceNode]:
    """Bulk register callables using derived paths."""
    return [self.register(c, decorate=decorate, tags=tags) for c in cc]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py::test_register_with_tags tests/test_namespace.py::test_register_without_tags tests/test_namespace.py::test_register_all_with_tags tests/test_namespace.py::test_register_tags_validation -v`
Expected: PASS

- [ ] **Step 5: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "feat: add tags parameter to register() and register_all()"
```

---

### Task 3: Tag query expression parser and Namespace.query()

**Files:**
- Modify: `src/lythonic/compose/namespace.py`
- Test: `tests/test_namespace.py`

- [ ] **Step 1: Write failing tests for query()**

Add to `tests/test_namespace.py`:

```python
def _setup_tagged_namespace():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:fast_market", tags={"fast", "market"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:slow_market", tags={"slow", "market"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._sample_fn, nsref="b:fast_exp", tags={"fast", "experimental"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="b:untagged")  # pyright: ignore[reportPrivateUsage]
    return ns


def test_query_single_tag():
    ns = _setup_tagged_namespace()
    result = ns.query("slow")
    assert [n.nsref for n in result] == ["a:slow_market"]


def test_query_and():
    ns = _setup_tagged_namespace()
    result = ns.query("fast & market")
    assert [n.nsref for n in result] == ["a:fast_market"]


def test_query_or():
    ns = _setup_tagged_namespace()
    result = ns.query("slow | experimental")
    nsrefs = sorted(n.nsref for n in result)
    assert nsrefs == ["a:slow_market", "b:fast_exp"]


def test_query_not():
    ns = _setup_tagged_namespace()
    result = ns.query("~market")
    nsrefs = sorted(n.nsref for n in result)
    assert nsrefs == ["b:fast_exp", "b:untagged"]


def test_query_combined_precedence():
    """slow & ~experimental | fast evaluates as (slow & ~experimental) | fast"""
    ns = _setup_tagged_namespace()
    result = ns.query("slow & ~experimental | fast")
    nsrefs = sorted(n.nsref for n in result)
    assert nsrefs == ["a:fast_market", "a:slow_market", "b:fast_exp"]


def test_query_no_matches():
    ns = _setup_tagged_namespace()
    result = ns.query("nonexistent")
    assert result == []


def test_query_empty_raises():
    ns = _setup_tagged_namespace()
    try:
        ns.query("")
        raise AssertionError("Expected ValueError")
    except ValueError:
        pass

    try:
        ns.query("   ")
        raise AssertionError("Expected ValueError")
    except ValueError:
        pass


def test_query_malformed_raises():
    ns = _setup_tagged_namespace()
    for bad_expr in ["& slow", "slow &", "~ &", "| fast"]:
        try:
            ns.query(bad_expr)
            raise AssertionError(f"Expected ValueError for {bad_expr!r}")
        except ValueError:
            pass


def test_query_recursive_across_branches():
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="deep.nested.branch:fn", tags={"deep"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="top:fn", tags={"deep"})  # pyright: ignore[reportPrivateUsage]
    result = ns.query("deep")
    nsrefs = sorted(n.nsref for n in result)
    assert nsrefs == ["deep.nested.branch:fn", "top:fn"]


def test_query_untagged_nodes_match_not():
    """Nodes with no tags match ~sometag (they lack the tag)."""
    from lythonic.compose.namespace import Namespace

    ns = Namespace()
    ns.register(this_module._sample_fn, nsref="a:tagged", tags={"x"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_fn, nsref="a:untagged")  # pyright: ignore[reportPrivateUsage]
    result = ns.query("~x")
    assert [n.nsref for n in result] == ["a:untagged"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace.py::test_query_single_tag tests/test_namespace.py::test_query_and tests/test_namespace.py::test_query_or tests/test_namespace.py::test_query_not -v`
Expected: FAIL — `query()` doesn't exist yet.

- [ ] **Step 3: Implement the expression parser and query()**

Add these functions and the method to `src/lythonic/compose/namespace.py`. Place the two module-level functions before the `Namespace` class definition (after `_validate_tags`):

```python
def _parse_tag_expr(expr: str) -> list[str]:
    """
    Tokenize a tag query expression into a list of tokens. Tokens are
    tag names, `&`, `|`, or `~`. Raises `ValueError` for empty or
    whitespace-only expressions.
    """
    tokens: list[str] = []
    for part in expr.split():
        i = 0
        while i < len(part):
            if part[i] in ("&", "|", "~"):
                tokens.append(part[i])
                i += 1
            else:
                j = i
                while j < len(part) and part[j] not in ("&", "|", "~"):
                    j += 1
                tokens.append(part[j - 1 : j] if j == i + 1 else part[i:j])
                i = j
    if not tokens:
        raise ValueError("Tag query expression is empty")
    return tokens


def _eval_tag_expr(tokens: list[str], tags: frozenset[str]) -> bool:
    """
    Evaluate a tokenized tag expression against a set of tags.
    Precedence: ~ (NOT, highest) > & (AND) > | (OR, lowest).
    No parentheses grouping.

    Grammar:
        or_expr  = and_expr ("|" and_expr)*
        and_expr = not_expr ("&" not_expr)*
        not_expr = "~" not_expr | tag
    """
    pos = 0

    def _or_expr() -> bool:
        nonlocal pos
        result = _and_expr()
        while pos < len(tokens) and tokens[pos] == "|":
            pos += 1
            result = _and_expr() or result
        return result

    def _and_expr() -> bool:
        nonlocal pos
        result = _not_expr()
        while pos < len(tokens) and tokens[pos] == "&":
            pos += 1
            result = _not_expr() and result
        return result

    def _not_expr() -> bool:
        nonlocal pos
        if pos < len(tokens) and tokens[pos] == "~":
            pos += 1
            return not _not_expr()
        if pos >= len(tokens) or tokens[pos] in ("&", "|"):
            raise ValueError(f"Expected tag name at position {pos}")
        tag = tokens[pos]
        pos += 1
        return tag in tags

    result = _or_expr()
    if pos < len(tokens):
        raise ValueError(f"Unexpected token {tokens[pos]!r} at position {pos}")
    return result
```

Add `query()` and `_all_leaves()` to the `Namespace` class:

```python
def _all_leaves(self) -> list[NamespaceNode]:
    """Recursively collect all leaf nodes from this namespace and all branches."""
    leaves: list[NamespaceNode] = list(self._leaves.values())
    for branch in self._branches.values():
        leaves.extend(branch._all_leaves())
    return leaves

def query(self, expr: str) -> list[NamespaceNode]:
    """
    Return all nodes matching a tag expression. Supports `&` (AND),
    `|` (OR), `~` (NOT) with standard precedence (`~` > `&` > `|`).
    """
    tokens = _parse_tag_expr(expr)
    return [
        node for node in self._all_leaves()
        if _eval_tag_expr(tokens, node.tags)
    ]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace.py -k "query" -v`
Expected: All PASS.

- [ ] **Step 5: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass.

- [ ] **Step 6: Commit**

```bash
git add src/lythonic/compose/namespace.py tests/test_namespace.py
git commit -m "feat: add Namespace.query() with tag expression parsing"
```

---

### Task 4: Config round-trip (namespace_config.py)

**Files:**
- Modify: `src/lythonic/compose/namespace_config.py:51-76, 88-172, 241-290`
- Test: `tests/test_namespace_config.py`

- [ ] **Step 1: Write failing tests for config with tags**

Add to `tests/test_namespace_config.py`:

```python
def test_config_entry_with_tags():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "market:fetch",
                    "gref": "json:dumps",
                    "tags": ["slow", "market"],
                },
            ]
        }
    )
    assert config.entries[0].tags == ["slow", "market"]


def test_config_entry_without_tags():
    from lythonic.compose.namespace_config import NamespaceConfig

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {"nsref": "market:fetch", "gref": "json:dumps"},
            ]
        }
    )
    assert config.entries[0].tags is None


def test_load_namespace_with_tags():
    from lythonic.compose.namespace_config import NamespaceConfig, load_namespace

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "math:double",
                    "gref": "tests.test_namespace_config:_plain_fn",
                    "tags": ["fast", "math"],
                },
            ]
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        node = ns.get("math:double")
        assert node.tags == frozenset({"fast", "math"})


def test_dump_namespace_with_tags():
    from lythonic.compose.namespace import Namespace
    from lythonic.compose.namespace_config import dump_namespace

    ns = Namespace()
    ns.register(this_module._plain_fn, nsref="math:double", tags={"beta", "alpha"})  # pyright: ignore[reportPrivateUsage]
    ns.register(this_module._another_plain_fn, nsref="fmt:to_str")  # pyright: ignore[reportPrivateUsage]

    dumped = dump_namespace(ns)
    # Entry with tags should have sorted tags list
    math_entry = next(e for e in dumped.entries if e.nsref == "fmt:to_str")
    assert math_entry.tags is None
    tagged_entry = next(e for e in dumped.entries if e.nsref == "math:double")
    assert tagged_entry.tags == ["alpha", "beta"]


def test_dump_load_round_trip_with_tags():
    from lythonic.compose.namespace_config import NamespaceConfig, dump_namespace, load_namespace

    config = NamespaceConfig.model_validate(
        {
            "entries": [
                {
                    "nsref": "math:double",
                    "gref": "tests.test_namespace_config:_plain_fn",
                    "tags": ["fast", "math"],
                },
                {
                    "nsref": "fmt:to_str",
                    "gref": "tests.test_namespace_config:_another_plain_fn",
                },
            ]
        }
    )

    with tempfile.TemporaryDirectory() as tmp:
        ns = load_namespace(config, Path(tmp))
        dumped = dump_namespace(ns)

        tagged = next(e for e in dumped.entries if e.nsref == "math:double")
        assert tagged.tags == ["fast", "math"]

        untagged = next(e for e in dumped.entries if e.nsref == "fmt:to_str")
        assert untagged.tags is None
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_namespace_config.py::test_config_entry_with_tags tests/test_namespace_config.py::test_load_namespace_with_tags tests/test_namespace_config.py::test_dump_namespace_with_tags tests/test_namespace_config.py::test_dump_load_round_trip_with_tags -v`
Expected: FAIL — `tags` field doesn't exist on `NamespaceEntryConfig` yet.

- [ ] **Step 3: Add tags to NamespaceEntryConfig**

In `src/lythonic/compose/namespace_config.py`, add `tags` field to `NamespaceEntryConfig`:

```python
class NamespaceEntryConfig(BaseModel):
    """
    One entry in the namespace config. Determined by fields present:

    - `gref` set, no `dag` -> callable (plain or cached)
    - `dag` set, no `gref` -> DAG entry
    - Both or neither -> validation error
    """

    nsref: str
    gref: str | None = None
    cache: CacheEntryConfig | None = None
    dag: DagEntryConfig | None = None
    tags: list[str] | None = None
```

- [ ] **Step 4: Update load_namespace() to pass tags**

In `load_namespace()`, update the callable registration (pass 1) to include tags. Change the `else` branch (plain callable) at line 130:

```python
else:
    ns.register(entry.gref, nsref=entry.nsref, tags=entry.tags)
```

And the cached branch — `register_cached_callable` doesn't accept tags, so add tags after registration. Change lines 119-128:

```python
if entry.cache is not None:
    if cache_db is None:
        raise ValueError(
            f"Entry '{entry.nsref}' has cache config but storage.cache_db is not set"
        )
    from lythonic.compose.cached import register_cached_callable

    node = register_cached_callable(
        ns,
        gref=entry.gref,
        nsref=entry.nsref,
        min_ttl=entry.cache.min_ttl,
        max_ttl=entry.cache.max_ttl,
        db_path=cache_db,
    )
    if entry.tags:
        from lythonic.compose.namespace import _validate_tags
        node.tags = _validate_tags(entry.tags)
```

- [ ] **Step 5: Update dump_namespace() to output tags**

In `dump_namespace()`, update the leaf collection to include tags. In the `else` branch (plain callable) at line 278:

```python
else:
    entries.append(
        NamespaceEntryConfig(
            nsref=node.nsref,
            gref=str(node.method.gref),
            tags=sorted(node.tags) if node.tags else None,
        )
    )
```

Similarly, update the `"cache"` branch at line 269:

```python
elif "cache" in node.metadata:
    cache_info = node.metadata["cache"]
    entries.append(
        NamespaceEntryConfig(
            nsref=node.nsref,
            gref=str(node.method.gref),
            cache=CacheEntryConfig(**cache_info),
            tags=sorted(node.tags) if node.tags else None,
        )
    )
```

And the `"dag"` branch at line 261:

```python
if "dag" in node.metadata:
    dag_info = node.metadata["dag"]
    entries.append(
        NamespaceEntryConfig(
            nsref=node.nsref,
            dag=dag_info,
            tags=sorted(node.tags) if node.tags else None,
        )
    )
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `uv run pytest tests/test_namespace_config.py -k "tags" -v`
Expected: All PASS.

- [ ] **Step 7: Run full test suite and lint**

Run: `make lint && make test`
Expected: All pass.

- [ ] **Step 8: Commit**

```bash
git add src/lythonic/compose/namespace.py src/lythonic/compose/namespace_config.py tests/test_namespace_config.py
git commit -m "feat: add tags to namespace config for round-trip persistence"
```
