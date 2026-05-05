"""Durable run state — the checkpoint that makes execution resumable.

After each task the engine saves a ``RunState`` snapshot. If the process dies,
a later run loads the snapshot and skips the tasks already done, so work is never
repeated and side effects fire exactly once. An in-memory store suffices for
tests; the JSON file store survives a restart.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class RunState:
    run_id: str
    inputs: dict = field(default_factory=dict)
    results: dict[str, Any] = field(default_factory=dict)
    statuses: dict[str, str] = field(default_factory=dict)   # pending/completed/failed/compensated
    order: list[str] = field(default_factory=list)           # completion order (for rollback)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "inputs": self.inputs,
            "results": self.results,
            "statuses": self.statuses,
            "order": self.order,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "RunState":
        return cls(d["run_id"], d.get("inputs", {}), d.get("results", {}),
                   d.get("statuses", {}), d.get("order", []))


class StateStore(Protocol):
    def load(self, run_id: str) -> RunState | None:
        ...

    def save(self, state: RunState) -> None:
        ...


class InMemoryStore:
    def __init__(self) -> None:
        self._data: dict[str, dict] = {}
        self.saves = 0                       # observable: how many checkpoints were written

    def load(self, run_id: str) -> RunState | None:
        raw = self._data.get(run_id)
        return RunState.from_dict(raw) if raw is not None else None

    def save(self, state: RunState) -> None:
        # Deep-copy via round-trip so later mutation of the live state can't
        # retroactively change a checkpoint (mirrors a real serialising store).
        self._data[state.run_id] = json.loads(json.dumps(state.to_dict()))
        self.saves += 1


class JSONFileStore:
    def __init__(self, path: str) -> None:
        self.path = path
        self.saves = 0

    def load(self, run_id: str) -> RunState | None:
        try:
            with open(self.path, encoding="utf-8") as fh:
                blob = json.load(fh)
        except FileNotFoundError:
            return None
        raw = blob.get(run_id)
        return RunState.from_dict(raw) if raw is not None else None

    def save(self, state: RunState) -> None:
        try:
            with open(self.path, encoding="utf-8") as fh:
                blob = json.load(fh)
        except (FileNotFoundError, json.JSONDecodeError):
            blob = {}
        blob[state.run_id] = state.to_dict()
        with open(self.path, "w", encoding="utf-8") as fh:
            json.dump(blob, fh, indent=2)
        self.saves += 1
