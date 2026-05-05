"""The task graph — definition, validation, and scheduling order.

A workflow is a DAG of ``Task``s connected by dependencies. This module owns the
graph concerns only (cycle detection, topological order, parallelizable levels);
*executing* the graph is the engine's job.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

# A task body receives a Context and returns a JSON-serialisable result.
TaskFn = Callable[["Context"], Any]
CompensateFn = Callable[["Context", Any], None]


@dataclass
class Context:
    """What a task sees: the run inputs plus the results of its dependencies."""

    inputs: dict
    upstream: dict = field(default_factory=dict)   # dep name -> result

    def __getitem__(self, dep: str) -> Any:
        return self.upstream[dep]

    def get(self, dep: str, default: Any = None) -> Any:
        return self.upstream.get(dep, default)


@dataclass
class Task:
    name: str
    run: TaskFn
    deps: tuple[str, ...] = ()
    retries: int = 0                                # extra attempts beyond the first
    compensate: CompensateFn | None = None          # saga rollback for this task


class DAGError(ValueError):
    pass


class DAG:
    def __init__(self) -> None:
        self._tasks: dict[str, Task] = {}

    def add(self, task: Task) -> "DAG":
        if task.name in self._tasks:
            raise DAGError(f"duplicate task name: {task.name!r}")
        self._tasks[task.name] = task
        return self

    def task(self, name: str) -> Task:
        return self._tasks[name]

    @property
    def names(self) -> list[str]:
        return list(self._tasks)

    def validate(self) -> None:
        for t in self._tasks.values():
            for d in t.deps:
                if d not in self._tasks:
                    raise DAGError(f"task {t.name!r} depends on unknown task {d!r}")
        self.topological_order()                    # raises on a cycle

    def topological_order(self) -> list[str]:
        """Kahn's algorithm; names broken ties alphabetically for determinism."""
        indeg = {n: 0 for n in self._tasks}
        for t in self._tasks.values():
            for _ in t.deps:
                indeg[t.name] += 1
        ready = sorted(n for n, d in indeg.items() if d == 0)
        order: list[str] = []
        while ready:
            n = ready.pop(0)
            order.append(n)
            for m in self._tasks.values():
                if n in m.deps:
                    indeg[m.name] -= 1
                    if indeg[m.name] == 0:
                        ready.append(m.name)
            ready.sort()
        if len(order) != len(self._tasks):
            raise DAGError("cycle detected in task graph")
        return order

    def levels(self) -> list[list[str]]:
        """Group tasks into dependency levels — each level could run in parallel."""
        order = self.topological_order()
        level_of: dict[str, int] = {}
        for n in order:
            deps = self._tasks[n].deps
            level_of[n] = 0 if not deps else 1 + max(level_of[d] for d in deps)
        out: list[list[str]] = []
        for n in order:
            lvl = level_of[n]
            while len(out) <= lvl:
                out.append([])
            out[lvl].append(n)
        return [sorted(group) for group in out]
