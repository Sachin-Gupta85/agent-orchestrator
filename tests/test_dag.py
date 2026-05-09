import pytest

from orchestrator.dag import DAG, DAGError, Task


def noop(ctx):
    return None


def build(edges):
    """edges: dict name -> deps"""
    dag = DAG()
    for name, deps in edges.items():
        dag.add(Task(name, noop, deps=tuple(deps)))
    return dag


def test_topological_order_respects_deps():
    dag = build({"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]})
    order = dag.topological_order()
    assert order.index("a") < order.index("b")
    assert order.index("b") < order.index("d")
    assert order.index("c") < order.index("d")


def test_order_is_deterministic():
    dag = build({"a": [], "b": [], "c": []})
    assert dag.topological_order() == ["a", "b", "c"]      # alphabetical tie-break


def test_levels_group_parallelizable_tasks():
    dag = build({"a": [], "b": ["a"], "c": ["a"], "d": ["b", "c"]})
    assert dag.levels() == [["a"], ["b", "c"], ["d"]]


def test_cycle_is_rejected():
    dag = build({"a": ["b"], "b": ["a"]})
    with pytest.raises(DAGError):
        dag.validate()


def test_unknown_dependency_is_rejected():
    dag = DAG()
    dag.add(Task("a", noop, deps=("ghost",)))
    with pytest.raises(DAGError):
        dag.validate()


def test_duplicate_task_name_is_rejected():
    dag = DAG()
    dag.add(Task("a", noop))
    with pytest.raises(DAGError):
        dag.add(Task("a", noop))
