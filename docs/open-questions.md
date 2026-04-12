# open questions / issues / design decisions

* LabelSwitch - It's a string ID that instructs SwitchNode to invoke one of the predefined DAGs within the switch node. So, switch node is collection of different compatible DAGs/workflows. All DAGs have with a same single source types in a single compatible sink type. And then when data come to the switch node, data has to include LabelSwitch that matches one of the DAGs. All source data exept LabelSwitch passed to matching DAG

* flatMap - option on map node creation. DAGs inside of map not only one element, but list of elements, those elements will be flattened into resulting list after map processing. or it could be implemented as flatMap() method.


We need api_facade module. Maybe in woodglue.


## Optimize open/close cycles and keep SQLite I/O off eventloop 

* Ensure that DAG updates to  SQLite are done in executor rather than inside of event loop, which can cause delay with the overall performance of the system.

* Builds on top rule: Use `open_sqlite_db()` context manager ... in the sence of... Batch related operations into a single open/close cycle rather than opening and closing per operation.  one of the case when it's not true I know it's when task completes and there is an update on node execution and there is also update on next node activation and I think they done as a separate open_sqlite_db cycles. But I think we need to revisit existing code in that sense. Maybe just ask claude to do that.

[ ] Validate 

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

## Singleton DAG

> Singleton DAG feature: DAG automatically created out of ns_ref out of single function that scheduled through trigger. It has same ns_ref. Actually let's do it on namespace so we have a get method we probably should have `get_as_dag() -> Dag` method. And it makes it easier to track what happened with DAG provenace tracking in/outs timing and execution. We can clean out the code for a special handling function when the scheduled through triggers.


> Issue: A couple things. 
1. there is no dag_run inserts. I don't see any. Even so, I do see both dag and bare function starts. So, both of them should create dag run inserts, updates, node executions, etc.
2. There is a delay when triggers start to fire... for about a minute. I'm wondering why. Both are scheduled to fire every 15 seconds.

```
$ uv run lyth start 
$ head data/lyth.log
2026-04-12 10:13:46,866 DEBUG    [asyncio] run= node= Using selector: KqueueSelector
2026-04-12 10:13:46,887 INFO     [lythonic.state] run= node= Opening database data/triggers.db
2026-04-12 10:13:46,890 DEBUG    [lythonic.state] run= node= execute: CREATE TABLE IF NOT EXISTS trigger_activations (
    name TEXT PRIMARY KEY,
    dag_nsref TEXT NOT NULL,
    trigger_type TEXT NOT NULL,
    status TEXT NOT NULL,
    last_run_at REAL,
    next_run_at REAL,
    last_run_id TEXT,
$ grep -B2 'node=task1 Starting task1' data/lyth.log|head
2026-04-12 10:15:00,200 INFO     [lythonic.state] run= node= Opening database data/triggers.db
2026-04-12 10:15:00,201 DEBUG    [lythonic.state] run= node= execute: SELECT * FROM trigger_activations WHERE name = ? -- with args: (('task1_repeat',),)
2026-04-12 10:15:00,210 INFO     [lythonic.examples.do_sleep_repeat] run=5da2565a-a9f7-454c-bc60-6480ff6502a9 node=task1 Starting task1
--
2026-04-12 10:15:02,223 INFO     [lythonic.state] run= node= Opening database data/triggers.db
2026-04-12 10:15:02,224 DEBUG    [lythonic.state] run= node= execute: SELECT * FROM trigger_activations WHERE name = ? -- with args: (('dag1_repeat',),)
2026-04-12 10:15:02,225 INFO     [lythonic.examples.do_sleep_repeat] run=ec72af9b-1d52-4530-bbc3-a8d519a843e7 node=task1 Starting task1
--
2026-04-12 10:15:05,241 INFO     [lythonic.state] run= node= Opening database data/triggers.db
2026-04-12 10:15:05,242 DEBUG    [lythonic.state] run= node= execute: SELECT * FROM trigger_activations WHERE name = ? -- with args: (('task1_repeat',),)
$ grep 'INSERT INTO dag_runs' data/lyth.log 
$ 
```

[x] Validated both fixes

----