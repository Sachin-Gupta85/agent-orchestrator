# 🧬 agent-orchestrator — a durable DAG workflow engine

> A small, deterministic orchestration engine for agent / LLM pipelines, where
> steps are slow, costly and have side effects. Define a graph of tasks with
> dependencies, retries and **saga compensators**; the engine runs it with
> **checkpointing**, crash-safe **resume**, **deterministic replay**, and
> **rollback on failure**. Pure standard library.

![Python](https://img.shields.io/badge/Python-3.10%2B-blue)
![Focus](https://img.shields.io/badge/focus-durable%20execution-6f42c1)
![Tests](https://img.shields.io/badge/tests-pytest%20(16)-brightgreen)

## 🧠 The four guarantees
| Guarantee | How | Why it matters for LLM work |
|---|---|---|
| **Checkpointing** | state saved after every task (`store`) | a 6-step agent run that dies at step 5 doesn't restart from zero |
| **Resume** | re-running a `run_id` skips completed tasks and reuses their results | you re-pay for at most the in-flight step, not the whole run |
| **Deterministic replay** | a fully-recorded run re-runs without invoking any task body | side effects (charges, emails, writes) fire **exactly once** |
| **Saga compensation** | on permanent failure, completed tasks are rolled back in reverse via their `compensate` fn | a failed multi-step action unwinds cleanly instead of leaving half-done state |

## 🧩 Graph concerns (`dag`)
Cycle detection, deterministic topological order (names break ties), and
`levels()` — the dependency layers that *could* run in parallel:

```python
DAG: a -> {b, c} -> d
dag.levels()  == [["a"], ["b", "c"], ["d"]]
```

## 🚀 Run
```bash
python -m orchestrator
```
The demo runs a "place order" saga (charge → reserve → ship). When `ship` fails,
`reserve` and `charge` are **compensated in reverse** (release inventory, refund).
Then it crashes a job mid-flight and resumes it, showing the already-done step is
**not** re-executed.

## ✅ Test
```bash
pip install -r requirements-dev.txt && python -m pytest -q
```

## Why it's Level 5
This is the durable-execution substrate (think Temporal / Step Functions, in
miniature) that a platform team builds so expensive, side-effectful agent
workflows survive crashes and fail safely. The hard parts — exactly-once replay,
resume-from-checkpoint, and saga rollback — are implemented and tested here, not
hand-waved.

---
MIT licensed. Part of a portfolio of runnable systems projects.
