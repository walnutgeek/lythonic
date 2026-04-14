# open questions / issues / design decisions


# API Facade 

[x] We need api_facade module will be implemneted in  in woodglue.

## Non-practical E2E example

I want to create an end-to-end test using do_sleep_repeat.py example. Extract the yaml file from a do_sleep_repeat.py doc string, save it in a temporary directory, and run lyth start on this directory, ensure that lyth is running, keep it running for two minutes, then run CLI lyth stop command to shut down the server. Ensure that it stopped. instantiat dag_provenance on that directory and run test that use DagRun and other pydantic objects to query runs that already completed.

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

[ ] Validated

