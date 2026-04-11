# open questions / issues / design decisions

[ ] We need API that inspect DAGs that are in process of execution or were executed recently.

* LabelSwitch - It's a string ID that instructs SwitchNode to invoke one of the predefined DAGs within the switch node. So, switch node is collection of different compatible DAGs/workflows. All DAGs have with a same single source types in a single compatible sink type. And then when data come to the switch node, data has to include LabelSwitch that matches one of the DAGs. All source data exept LabelSwitch passed to matching DAG
* flatMap - option on map node creation. DAGs inside of map not only one element, but list of elements, those elements will be flattened into resulting list after map processing. or it could be implemented as flatMap() method.



## Validate 

[ ] ---
* Ensure that DAG updates to  SQLite are done in executor rather than inside of event loop, which can cause delay with the overall performance of the system.
* Builds on top rule: Use `open_sqlite_db()` context manager ... in the sence of... Batch related operations into a single open/close cycle rather than opening and closing per operation.  one of the case when it's not true I know it's when task completes and there is an update on node execution and there is also update on next node activation and I think they done as a separate open_sqlite_db cycles. But I think we need to revisit existing code in that sense. Maybe just ask claude to do that.
---
Done. Three changes:

  1. Private cursor helpers — _insert_run, _update_node_complete, _insert_edge_traversal, etc. Take a cursor, no commit. Building blocks for batching.

  2. Batch public methods:
  - complete_node_with_edges(run_id, label, output_json, edges) — node completion + all outgoing edge traversals in one DB cycle
  - fail_node_and_finish_run(run_id, label, error) — node failure + run finish in one DB cycle

  3. Executor offloading — all provenance writes in DagRunner._execute() now use await loop.run_in_executor(None, ...) to keep SQLite I/O off the event loop.

  Before: N+2 DB open/close cycles per node (start + complete + N edges), all blocking the event loop.
  After: 2 DB open/close cycles per node (start + batch complete), all in thread executor.

  226 tests pass, 0 lint warnings.
---