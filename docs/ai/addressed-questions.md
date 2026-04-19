# open questions / issues / design decisions


# API Facade 

[x] We need api_facade module will be implemneted in  in woodglue.

## Non-practical E2E example

I want to create an end-to-end test using do_sleep_repeat.py example. Extract the yaml file from a do_sleep_repeat.py doc string, save it in a temporary directory, and run lyth start on this directory, ensure that lyth is running, keep it running for two minutes, then run CLI lyth stop command to shut down the server. Ensure that it stopped. instantiat dag_provenance on that directory and run test that use DagRun and other pydantic objects to query runs that already completed.

[x] Validated 


## Sub-DAG awareness in provenance inspection models

 @src/lythonic/compose/dag_provenance.py I added few to do's. I want a DAGs to be fully aware of sub DAGs that is running. It means that we need to capture edge traversals to sub_dags as well on composite
  node executions.

For Label Path - Compute at load time, Especially that dag_nsref is is not only NSREF, but it's NSREF with Label Path to dag so all we need is just to add node_label.
For Back-refs - It was bad idea. I removed them


About Current sub-DAG nsref patterns (from dag_runner.py):
- MapNode: f"{dag_nsref}/{map_node.label}[{k}]" → e.g., "pipeline/chunks[0]" -> ok

- CallNode: f"{dag_nsref}/{call_node.label}/call" → e.g., "pipeline/enrich/call" -> I don't like that extra path element was introduced. It might create impression that there is two DAGs 'enrich' and 'call' stacked on top of eachother. Just "pipeline/enrich" or "pipeline/enrich[]" would be better. 

- SwitchNode: f"{dag_nsref}/{switch_node.label}[{switch_label}]" → e.g., "pipeline/router[text]" -> ok

- MapSwitchNode: f"{dag_nsref}/{node.label}[{idx}:{switch_label}]" → e.g., "pipeline/map_route[0:text]" -> ok

So for EdgeTraversal for composite nodes,  we can generate one for {upsream.label} -> {composite.label} when we left upstream node, same as with any other node.

sub_dags now in NodeExecution

sub_dags: dict[str,"DagRun"]|None # TODO not None for composite nodes only. Key is label with item qualifier {composite.label}[...whatever] infered from dag_nsref so no need for schema change.

❯ I'm still not getting necessity of dictionary to capture edges.  Talking about "Build edges as dict[str, list[EdgeTraversal]] (keyed by downstream_label)". Code Assume there is many edges possible with same downstream label which is possible in a database but should not happening from from a
  business logic sense. I would rather store them as a list and enforce it in database as a composite primary key for all 3: run_id, upstream_label, downstream_label. We don't want to store labels with item qualifiers like "enrich[]" as edges. they are not structural edges and not captured in
  edge_traversals. they barely are dag invocations within composite nodes and captured only in dag_nsref in dag_runs. And let keep convention for call node to have empty item qualifier so we always see dags invocations in the path:  "_run_sub_dag key param at call site (line 389):"    should be ->
  f"{call_node.label}/call" -> f"{call_node.label}[]"

[ ] Validated 


## Schema tweaks and, Json lazy loads. 

1. on dag_runs we need: sink_outputs_json TEXT to present consolidated outputs from sinks
[x] Validated 

2. node_type cannot capture all types. Node could be source, sink and composite at the same time. Knowing that it is source and sink is very important. Fact that it is composite is not very important.
[x] Validated 

3. *_json fields coud be fairly big so we should not load them by default.
[ ] Validated 

4. input_json is wrong for CompositeNodes. consider:

```
$ sqlite3 data/dags.db 'select * from node_executions' |grep 807d1392-abfc-4dfe-86bc-21dcf881c0cf\|map
807d1392-abfc-4dfe-86bc-21dcf881c0cf|map|completed|composite|{"composite": "MapSwitchNode"}|["a", "b", "d", "e", "w", "x", "y", "z", "A B C", "X Y Z"]|1776492844.9625|1776492845.98714|
```
input_json is not what "807d1392-abfc-4dfe-86bc-21dcf881c0cf|input|" returned.
[x] Validated

4. JSON fields in node execution and in the DAG runs could be fairly big. So, we should not load them by default. By default, we should set those fields to None. There should be a method that allows to load a fully populated object with JSONs and it should be done for specified nodes or specified DAGs, not for whole hierarchy.
[ ] Validated 


```
$ sqlite3 data/dags.db 'select * from dag_runs'
...
807d1392-abfc-4dfe-86bc-21dcf881c0cf|examples:map_switch_flat_map||completed|1776492844.95916|1776492845.98841|{}
26e9e0f7-f084-4eeb-932e-710b447158ae|examples:map_switch_flat_map/map[0:split_dag]|807d1392-abfc-4dfe-86bc-21dcf881c0cf|completed|1776492844.96415|1776492844.97169|{"split": {"ss": {"s1": "a/b", "s2": "d/e"}}}
5c9ba9c3-c408-4259-a5d4-e6bfb90e24fa|examples:map_switch_flat_map/map[3:join]|807d1392-abfc-4dfe-86bc-21dcf881c0cf|completed|1776492844.96496|1776492845.98031|{"join": {"ss": {"s1": "X Y", "s2": "Z"}}}
4aae6ab3-de63-45d3-8d8e-8b4e4ed1cb78|examples:map_switch_flat_map/map[1:split]|807d1392-abfc-4dfe-86bc-21dcf881c0cf|completed|1776492844.96428|1776492844.97372|{"split": {"ss": {"s1": "w/x", "s2": "y/z"}}}
5b92466f-c192-4290-984e-94d0ed00db55|examples:map_switch_flat_map/map[2:join_dag]|807d1392-abfc-4dfe-86bc-21dcf881c0cf|completed|1776492844.96445|1776492845.98206|{"join": {"ss": {"s1": "A", "s2": "B C"}}}
...
$ sqlite3 data/dags.db 'select * from node_executions'
...
807d1392-abfc-4dfe-86bc-21dcf881c0cf|input|completed|source|{}|[["split_dag", {"s1": "a/b", "s2": "d/e"}], ["split", {"s1": "w/x", "s2": "y/z"}], ["join_dag", {"s1": "A", "s2": "B C"}], ["join", {"s1": "X Y", "s2": "Z"}]]|1776492844.96032|1776492844.96155|
807d1392-abfc-4dfe-86bc-21dcf881c0cf|map|completed|composite|{"composite": "MapSwitchNode"}|["a", "b", "d", "e", "w", "x", "y", "z", "A B C", "X Y Z"]|1776492844.9625|1776492845.98714|
4aae6ab3-de63-45d3-8d8e-8b4e4ed1cb78|split|completed|source|{"ss": {"s1": "w/x", "s2": "y/z"}}|["w", "x", "y", "z"]|1776492844.9696|1776492844.97052|
26e9e0f7-f084-4eeb-932e-710b447158ae|split|completed|source|{"ss": {"s1": "a/b", "s2": "d/e"}}|["a", "b", "d", "e"]|1776492844.96666|1776492844.97098|
5c9ba9c3-c408-4259-a5d4-e6bfb90e24fa|join|completed|source|{"ss": {"s1": "X Y", "s2": "Z"}}|"X Y Z"|1776492844.96868|1776492845.97528|
5b92466f-c192-4290-984e-94d0ed00db55|join|completed|source|{"ss": {"s1": "A", "s2": "B C"}}|"A B C"|1776492844.97501|1776492845.98038|
...
$ sqlite3 data/dags.db 'select * from edge_traversals'
807d1392-abfc-4dfe-86bc-21dcf881c0cf|input|map|1776492844.96184
...
$ 
```

### Plan: Schema tweaks and lazy JSON loading for provenance models

Context

Follow-up to the sub-DAG awareness work. Four issues to address:

1. dag_runs needs sink_outputs_json to surface consolidated sink outputs
2. node_type is a single string but nodes can be source AND sink AND composite
simultaneously. Source/sink matter; composite is derivable from sub_dags.
3. *_json fields can be large — don't load them by default in inspection models
4. Composite nodes record {"composite": "MapNode"} as input_json instead of
the actual upstream data

Files to modify

1. src/lythonic/compose/dag_provenance.py — schema, models, load logic
2. src/lythonic/compose/dag_runner.py — node flags, composite input, sink outputs
3. tests/test_dag_runner.py — update assertions
4. tests/test_lyth_e2e.py — update node_type assertions

Changes

1. Schema changes

_NODE_EXECUTIONS_DDL: replace node_type TEXT with:
is_source INTEGER DEFAULT 0,
is_sink INTEGER DEFAULT 0,

_DAG_RUNS_DDL: add sink_outputs_json TEXT

2. Add IoPayload model, update NodeExecution and DagRun

class IoPayload(BaseModel):
    """Input/output JSON payload. Loaded on demand via `load_io()`."""
    input_json: str | None = None
    output_json: str | None = None

NodeExecution — replace node_type with flags, replace input_json/
output_json with io:
class NodeExecution(BaseModel):
    node_label: str
    status: str
    is_source: bool = False
    is_sink: bool = False
    started_at: datetime | None = None
    finished_at: datetime | None = None
    error: str | None = None
    edges: list[EdgeTraversal] = []
    sub_dags: dict[str, "DagRun"] | None = None
    io: IoPayload | None = None

DagRun — replace source_inputs_json with io:
class DagRun(BaseModel):
    run_id: str
    dag_nsref: str
    parent_run_id: str | None = None
    status: str
    started_at: datetime
    finished_at: datetime | None = None
    nodes: dict[str, NodeExecution] = {}
    io: IoPayload | None = None  # input=source_inputs, output=sink_outputs

3. Update private DB helpers

_insert_node_start: change node_type: str | None to
is_source: bool = False, is_sink: bool = False. Update SQL columns.

_finish_run: add sink_outputs_json: str | None = None param.
Update SQL to also SET sink_outputs_json.

4. Update DagProvenance public API

record_node_start: node_type → is_source/is_sink.

finish_run: add sink_outputs_json param.

get_run raw dict: add sink_outputs_json to SELECT and result.

get_node_executions raw dict: return is_source/is_sink instead
of node_type.

NullProvenance: update matching signatures.

5. _load_dag_runs — lazy JSON, no node_type column

_load_dag_runs never loads JSON fields. io stays None on all
returned models.

Node query selects only:
run_id, node_label, status, is_source, is_sink, started_at, finished_at, error

Run rows (from _fetch_run_rows) select only:
run_id, dag_nsref, parent_run_id, status, started_at, finished_at
(drop source_inputs_json from the SELECT)

6. Add load_io method

def load_io(
    self,
    dag_run: DagRun,
    node_labels: Sequence[str] | None = None,
) -> None:

Single DB round-trip populates:
- dag_run.io — from source_inputs_json and sink_outputs_json columns
on the dag_runs row
- node.io — from input_json and output_json columns on
node_executions rows, filtered by node_labels if provided
(all nodes if None)

Does NOT recurse into sub-DAGs. To load sub-DAG IO, call load_io on
the sub-DAG's DagRun separately.

7. Detect composite nodes from child runs

In _load_dag_runs: build all nodes with sub_dags=None, then query for
child runs across ALL loaded run_ids. When a child matches a node via
expansion key prefix, lazily init sub_dags={} on that node and attach.

No DB column needed for composite detection.

8. Fix composite node input recording (dag_runner.py)

Replace _node_type() with _node_flags():
def _node_flags(label: str) -> tuple[bool, bool]:
    return (label in source_labels, label in sink_labels)

Composite node record_node_start: get upstream output first via
_get_upstream_output(), record it as input_json (instead of the
{"composite": "MapNode"} placeholder).

finish_run call: compute sink outputs dict and pass as
sink_outputs_json.

All record_node_start calls: pass is_source=..., is_sink=...
instead of node_type=....

9. Update tests

test_provenance_node_type → test_provenance_node_flags:
assert is_source/is_sink in raw dict results.

test_edge_traversals_recorded_during_dag_run: change
node.node_type assertions to node.is_source / node.is_sink.

test_sub_dags_loaded_on_composite_nodes: remove
mapped_node.node_type == "composite".

test_lyth_e2e.py: change node.node_type is not None to check
boolean flags.

Add test_io_lazy_loading: verify io is None by default,
populated after load_io().

Add test_sink_outputs_json_recorded: verify raw run dict has
sink_outputs_json after completion.

10. Raw dict APIs used by restart()/replay() in dag_runner.py

These methods use get_run() (returns dict with source_inputs_json)
and get_node_executions() (returns dicts with output_json). These
raw dict APIs continue to return the JSON columns directly — they are
unaffected by the IoPayload change since they don't use inspection models.

Verification

make lint
make test



----


[ ] Validated

## Get runs data

src/lythonic/compose/dag_provenance.py 
 
> We need pydatic models matching dag_runs, node_executions, and edge_traversals tables. DagRun has list[NodeExecution], and  NodeExecution has -> list[EdgeTraversal], that will allow you to skip repeating fields: run_id in NodeExecution and upstream_label in EdgeTraversal. Express all times in `datetime` timezone aware in utc. Heve Json blob fields optional even if they NOT NULL in db

Have couple functionds defined on DagRun: 
* `latest_update()->datetime` = basically max of all datetime's on all nodes and traversals.
* `nodes_changed_since(dt:datetime)->list[NodeExecution]

Let's refactor def get_recent_runs(..) to return list[DagRun]
* that inspect DAGs that are in process of execution or were executed recently.
* watch what trigger executed

> About get_child_runs() can we make it recursive we can use CTE syntax to do that efficiently

[x] Validated

