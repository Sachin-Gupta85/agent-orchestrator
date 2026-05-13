import pytest

from orchestrator.dag import DAG, Task
from orchestrator.engine import Engine
from orchestrator.store import InMemoryStore


def test_happy_path_passes_results_downstream():
    dag = DAG()
    dag.add(Task("load", lambda ctx: ctx.inputs["x"]))
    dag.add(Task("double", lambda ctx: ctx["load"] * 2, deps=("load",)))
    dag.add(Task("plus1", lambda ctx: ctx["double"] + 1, deps=("double",)))

    res = Engine().run(dag, "run-1", inputs={"x": 10})
    assert res.status == "completed"
    assert res.results["plus1"] == 21
    assert res.executed == ["load", "double", "plus1"]   # topo order


def test_retry_recovers_transient_failure():
    attempts = {"n": 0}

    def flaky(ctx):
        attempts["n"] += 1
        if attempts["n"] < 3:
            raise RuntimeError("transient")
        return "ok"

    dag = DAG().add(Task("t", flaky, retries=5))
    res = Engine().run(dag, "r", inputs={})
    assert res.status == "completed"
    assert res.results["t"] == "ok"
    assert attempts["n"] == 3


def test_resume_skips_completed_tasks_after_crash():
    calls = {"a": 0, "b": 0}

    def a(ctx):
        calls["a"] += 1
        return 1

    def b(ctx):
        calls["b"] += 1
        if calls["b"] == 1:
            raise RuntimeError("boom on first attempt")   # crash after 'a' checkpointed
        return ctx["a"] + 1

    store = InMemoryStore()
    dag = DAG()
    dag.add(Task("a", a))
    dag.add(Task("b", b, deps=("a",)))

    first = Engine(store).run(dag, "run-x", inputs={})
    assert first.status == "failed" and first.failed_task == "b"

    second = Engine(store).run(dag, "run-x", inputs={})   # resume
    assert second.status == "completed"
    assert second.results["b"] == 2
    assert calls["a"] == 1                                 # 'a' not re-run on resume
    assert second.executed == ["b"]                        # only 'b' ran this time


def test_deterministic_replay_does_not_reinvoke():
    side_effects = {"n": 0}

    def t(ctx):
        side_effects["n"] += 1
        return 42

    store = InMemoryStore()
    dag = DAG().add(Task("t", t))

    r1 = Engine(store).run(dag, "run-r", inputs={})
    r2 = Engine(store).run(dag, "run-r", inputs={})        # fully recorded -> replay
    assert r1.results == r2.results == {"t": 42}
    assert side_effects["n"] == 1                          # body ran exactly once
    assert r2.executed == []                               # nothing executed on replay


def test_saga_compensation_rolls_back_in_reverse():
    rollbacks = []

    dag = DAG()
    dag.add(Task("charge", lambda ctx: "charged",
                 compensate=lambda ctx, r: rollbacks.append("refund")))
    dag.add(Task("reserve", lambda ctx: "reserved", deps=("charge",),
                 compensate=lambda ctx, r: rollbacks.append("release")))
    dag.add(Task("ship", lambda ctx: (_ for _ in ()).throw(RuntimeError("no stock")),
                 deps=("reserve",)))

    res = Engine().run(dag, "order-1", inputs={})
    assert res.status == "failed"
    assert res.failed_task == "ship"
    # completed order was [charge, reserve]; rollback runs reverse
    assert res.compensated == ["reserve", "charge"]
    assert rollbacks == ["release", "refund"]


def test_permanent_failure_reports_error():
    dag = DAG().add(Task("boom", lambda ctx: 1 / 0))
    res = Engine().run(dag, "r", inputs={})
    assert res.status == "failed"
    assert res.failed_task == "boom"
    assert "division" in res.error
