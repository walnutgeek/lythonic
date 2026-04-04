# Namespace Tags

Add tagging support to `NamespaceNode` with a query API on `Namespace` that supports
`&` (AND), `|` (OR), and `~` (NOT) operators.

## Tags on NamespaceNode

`NamespaceNode.__init__` gets an optional `tags` parameter stored as
`self.tags: frozenset[str]`. Defaults to `frozenset()`.

Validation at construction time:

- `TypeError` if `tags` is a `str` (guards against the common mistake of passing a
  string, which is iterable over characters).
- `ValueError` if any tag contains `&`, `|`, `~`, or whitespace.

## register() and register_all()

`register()` gets an optional `tags: frozenset[str] | set[str] | list[str] | None = None`
parameter. If provided, validated and converted to `frozenset[str]` before passing to
the `NamespaceNode` constructor. `_register_dag()` also accepts and passes through
`tags`.

`register_all()` gets a `tags` parameter applied uniformly to all callables in the
batch. For per-callable tags in bulk registration, use individual `register()` calls.

## query()

New method `Namespace.query(expr: str) -> list[NamespaceNode]`.

### Tokenization

Split expression on whitespace. Recognize `&`, `|`, `~` as operators. Everything else
is a tag literal.

### Evaluation

Standard precedence (no parentheses grouping):

- `~` (NOT) — highest, unary prefix
- `&` (AND) — middle
- `|` (OR) — lowest

### Recursive collection

Collects all leaves from all nested branches and filters by the expression.

### Examples

```python
ns.query("slow")                        # all nodes tagged "slow"
ns.query("slow & market")               # nodes with both tags
ns.query("slow | fast")                 # nodes with either tag
ns.query("~experimental")               # nodes without "experimental"
ns.query("slow & ~experimental | fast") # (slow & ~experimental) | fast
```

## Error handling

- **Tag validation** (registration): `TypeError` if `tags` is a `str`. `ValueError`
  if any tag contains `&`, `|`, `~`, or whitespace.
- **Query validation**: `ValueError` for empty/whitespace-only expressions.
  `ValueError` for malformed expressions (e.g., `"& slow"`, `"slow &"`, `"~ &"`).
- **No matches**: returns empty list.
- **Nodes without tags**: match `~sometag` (they lack the tag), do not match any
  positive tag query.

## Config round-trip (namespace_config.py)

`NamespaceEntryConfig` gets an optional `tags: list[str] | None = None` field.
`load_namespace()` passes tags to `register()`. `dump_namespace()` reads `node.tags`
and includes them in the entry config if non-empty (sorted for determinism).

## Files changed

- `src/lythonic/compose/namespace.py` — `NamespaceNode`, `Namespace.register()`,
  `Namespace.register_all()`, `Namespace._register_dag()`, new `Namespace.query()`
- `src/lythonic/compose/namespace_config.py` — `NamespaceEntryConfig`,
  `load_namespace()`, `dump_namespace()`
- `tests/test_namespace.py` — tag registration, query, validation, edge cases
- `tests/test_namespace_config.py` — config round-trip with tags
