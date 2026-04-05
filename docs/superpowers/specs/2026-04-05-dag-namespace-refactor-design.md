# Dag-Namespace Refactoring

Stabilize the Dag-owns-Namespace pattern, clean up node copying on
registration, and remove the clone/merge composition mechanism.

## Dag Initialization and Auto-Registration

`Dag.__init__` creates `self.namespace = Namespace()`. When `dag.node(callable)`
receives a raw callable (not a `NamespaceNode`):

1. Builds a `GlobalRef` from the callable.
2. Tries `self.namespace.get(str(gref))`.
3. On `KeyError`, calls `self.namespace.register(callable)` (auto-derives nsref
   from module and name).
4. Uses the resulting `NamespaceNode` to create the `DagNode`.

Calling `dag.node(same_fn)` twice reuses the same `NamespaceNode` while creating
two distinct `DagNode`s with different labels.

## `_register_dag` Node Copying and Reference Update

When `Namespace.register(dag, nsref=...)` calls `_register_dag`:

1. Raises `ValueError` if `dag.namespace is self` (prevents double-registration).
2. Iterates `dag.namespace._all_leaves()` and for each node:
   - Parses its nsref into branch/leaf.
   - If the leaf already exists in the target branch: compares `method.gref` --
     skip if identical callable, raise `ValueError` if different.
   - If it does not exist: creates a new `NamespaceNode` copy in the parent
     namespace.
3. Updates all `DagNode.ns_node` references in `dag.nodes` to point to the
   parent namespace's copies (looks up by nsref).
4. Sets `dag.namespace = self`.
5. Proceeds with existing `DagRunner` wrapper creation.

## Remove clone, merge, and DagNode >> Dag

- Delete `Dag.clone(prefix)`.
- Delete `Dag._merge_and_wire()`.
- Simplify `DagNode.__rshift__` to only accept `DagNode`, remove the
  `isinstance(other, Dag)` branch.
- Delete all related tests (~10 tests covering clone, merge, label collision,
  chained composition).

## Test Coverage

New tests:

- **Auto-registration basics**: `dag.node(callable)` auto-registers in
  `dag.namespace`, second call reuses same `NamespaceNode`.
- **Register dag copies nodes**: after `ns.register(dag, nsref=...)`, the DAG's
  nodes appear in the parent namespace.
- **DagNode references updated**: after registration,
  `dag.nodes[label].ns_node.namespace is ns` (points to parent).
- **Skip identical**: parent already has a node for the same callable -- no
  error, node is reused.
- **Conflict raises**: parent has a node with the same nsref but different
  callable -- `ValueError`.
- **Double-registration guard**: calling `ns.register(dag)` when
  `dag.namespace is ns` raises `ValueError`.
- **dag.node with NamespaceNode**: passing an already-registered
  `NamespaceNode` to `dag.node()` still works (does not re-register).
- **Existing DAG runner tests**: must still pass -- execution, fan-in/fan-out,
  provenance, pause/restart.

## Files Changed

- `src/lythonic/compose/namespace.py` -- `Dag.__init__`, `Dag.node()`,
  `Namespace._register_dag()`, delete `Dag.clone()`, `Dag._merge_and_wire()`,
  simplify `DagNode.__rshift__`.
- `tests/test_namespace.py` -- delete clone/merge tests, add new
  auto-registration and node-copying tests.
- `tests/test_dag_runner.py` -- verify existing tests still pass.
