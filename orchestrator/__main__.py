"""Offline demo — a durable 'place order' saga that fails and rolls back,
then a crash-and-resume run. No clock, no randomness, no network.

    python -m orchestrator
"""
from __future__ import annotations

from .dag import DAG, Task
from .engine import Engine
from .store import InMemoryStore


def build_order_dag(out_of_stock: bool) -> DAG:
    log: list[str] = []

    def charge(ctx):
        log.append("charged card")
        return {"txn": "tx_123"}

    def reserve(ctx):
        log.append("reserved inventory")
        return {"hold": "h_9"}

    def ship(ctx):
        if out_of_stock:
            raise RuntimeError("item out of stock")
        log.append("shipped")
        return {"tracking": "TRK1"}

    dag = DAG()
    dag.add(Task("charge", charge, compensate=lambda c, r: log.append("REFUNDED card")))
    dag.add(Task("reserve", reserve, deps=("charge",),
                 compensate=lambda c, r: log.append("RELEASED inventory")))
    dag.add(Task("ship", ship, deps=("reserve",)))
    dag._log = log  # type: ignore[attr-defined]
    return dag


def main() -> None:
    print("=== 1) saga rollback on failure ===")
    dag = build_order_dag(out_of_stock=True)
    res = Engine().run(dag, "order-42", inputs={})
    for line in dag._log:  # type: ignore[attr-defined]
        print(f"   {line}")
    print(f"   -> status={res.status}, failed at {res.failed_task!r}, "
          f"compensated={res.compensated}\n")

    print("=== 2) crash then resume (work is not repeated) ===")
    store = InMemoryStore()
    calls = {"step_a": 0}

    def step_a(ctx):
        calls["step_a"] += 1
        return "A"

    def step_b(ctx):
        # fail the first time (simulating a crash), succeed on resume
        if store.load("job-7").statuses.get("b") != "failed":
            raise RuntimeError("crashed mid-flight")
        return "B"

    d = DAG()
    d.add(Task("a", step_a))
    d.add(Task("b", step_b, deps=("a",)))

    r1 = Engine(store).run(d, "job-7", inputs={})
    print(f"   first run : status={r1.status}, executed={r1.executed}")
    r2 = Engine(store).run(d, "job-7", inputs={})
    print(f"   resume    : status={r2.status}, executed={r2.executed}")
    print(f"   step_a ran {calls['step_a']} time(s) total (resume skipped it)")


if __name__ == "__main__":
    main()
