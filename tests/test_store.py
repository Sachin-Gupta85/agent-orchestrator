import os

from orchestrator.dag import DAG, Task
from orchestrator.engine import Engine
from orchestrator.store import InMemoryStore, JSONFileStore, RunState


def test_inmemory_roundtrip_is_isolated():
    store = InMemoryStore()
    st = RunState("r", results={"a": 1})
    store.save(st)
    st.results["a"] = 999                     # mutate after save
    loaded = store.load("r")
    assert loaded.results["a"] == 1           # checkpoint is a snapshot, not a live ref


def test_inmemory_missing_run_is_none():
    assert InMemoryStore().load("nope") is None


def test_checkpoint_written_per_task():
    store = InMemoryStore()
    dag = DAG()
    dag.add(Task("a", lambda ctx: 1))
    dag.add(Task("b", lambda ctx: 2, deps=("a",)))
    Engine(store).run(dag, "r", inputs={})
    # 1 initial save + 1 per completed task
    assert store.saves == 3


def test_jsonfile_store_persists_across_instances(tmp_path):
    path = os.path.join(tmp_path, "runs.json")
    dag = DAG()
    calls = {"a": 0}

    def a(ctx):
        calls["a"] += 1
        return "done"

    dag.add(Task("a", a))
    dag.add(Task("b", lambda ctx: (_ for _ in ()).throw(RuntimeError("x")), deps=("a",)))

    Engine(JSONFileStore(path)).run(dag, "run-1", inputs={})
    assert os.path.exists(path)

    # a brand-new store instance (simulating a process restart) resumes
    dag2 = DAG()
    dag2.add(Task("a", a))
    dag2.add(Task("b", lambda ctx: "shipped", deps=("a",)))
    res = Engine(JSONFileStore(path)).run(dag2, "run-1", inputs={})
    assert res.status == "completed"
    assert res.results["b"] == "shipped"
    assert calls["a"] == 1                     # 'a' survived the restart, not re-run
