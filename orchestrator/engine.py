"""The durable execution engine.

Runs a DAG to completion with four guarantees that matter for agent/LLM
pipelines, where steps are slow, expensive, and have side effects:

  * **Checkpointing** — state is saved after every task.
  * **Resume** — re-running a run_id skips completed tasks and reuses their
    results, so a crash costs you at most the in-flight task.
  * **Deterministic replay** — a fully-recorded run re-runs without invoking any
    task body again (side effects fire exactly once).
  * **Saga compensation** — if a task fails for good, the engine rolls back the
    tasks that already succeeded, in reverse order, via their compensators.

Execution is deterministic (topological order, names breaking ties); no real
clock or randomness is involved.
"""
from __future__ import annotations

from dataclasses import dataclass, field

from .dag import DAG, Context, Task
from .store import InMemoryStore, RunState, StateStore


class TaskFailed(RuntimeError):
    def __init__(self, task: str, cause: BaseException) -> None:
        super().__init__(f"task {task!r} failed: {cause}")
        self.task = task
        self.cause = cause


@dataclass
class RunResult:
    run_id: str
    status: str                              # "completed" | "failed"
    results: dict = field(default_factory=dict)
    failed_task: str | None = None
    error: str | None = None
    compensated: list[str] = field(default_factory=list)
    executed: list[str] = field(default_factory=list)   # tasks actually run this invocation


class Engine:
    def __init__(self, store: StateStore | None = None) -> None:
        self.store = store or InMemoryStore()

    def _context(self, dag: DAG, task: Task, state: RunState) -> Context:
        upstream = {d: state.results[d] for d in task.deps if d in state.results}
        return Context(inputs=state.inputs, upstream=upstream)

    def _exec_with_retry(self, task: Task, ctx: Context):
        attempts = task.retries + 1
        last: BaseException | None = None
        for _ in range(attempts):
            try:
                return task.run(ctx)
            except Exception as exc:         # noqa: BLE001 - engine boundary
                last = exc
        assert last is not None
        raise last

    def _compensate(self, dag: DAG, state: RunState) -> list[str]:
        done = []
        for name in reversed(state.order):
            if state.statuses.get(name) != "completed":
                continue
            task = dag.task(name)
            if task.compensate is None:
                continue
            ctx = self._context(dag, task, state)
            task.compensate(ctx, state.results[name])
            state.statuses[name] = "compensated"
            done.append(name)
        return done

    def run(self, dag: DAG, run_id: str, inputs: dict | None = None) -> RunResult:
        dag.validate()
        state = self.store.load(run_id)
        if state is None:
            state = RunState(run_id, inputs=dict(inputs or {}))
            self.store.save(state)

        executed: list[str] = []
        for name in dag.topological_order():
            if state.statuses.get(name) == "completed":
                continue                     # resume / replay: reuse recorded result

            task = dag.task(name)
            ctx = self._context(dag, task, state)
            try:
                result = self._exec_with_retry(task, ctx)
            except Exception as exc:         # noqa: BLE001
                state.statuses[name] = "failed"
                self.store.save(state)
                compensated = self._compensate(dag, state)
                self.store.save(state)
                return RunResult(run_id, "failed", dict(state.results),
                                 failed_task=name, error=str(exc),
                                 compensated=compensated, executed=executed)

            state.results[name] = result
            state.statuses[name] = "completed"
            state.order.append(name)
            executed.append(name)
            self.store.save(state)           # checkpoint after every task

        return RunResult(run_id, "completed", dict(state.results), executed=executed)
