from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import json


@dataclass(frozen=True)
class WatchSnapshot:
  source: str
  page: str
  commission_rate: float
  target_profit: float
  stop_loss: float
  position_count: int
  watch_count: int
  watches: list[dict[str, Any]]
  captured_at: datetime


def append_watch_snapshot(events_path: Path, snapshot: WatchSnapshot) -> None:
  event = {
    "captured_at": snapshot.captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "source": snapshot.source,
    "kind": "watch_plan_snapshot",
    "page": snapshot.page,
    "commission_rate": snapshot.commission_rate,
    "target_profit": snapshot.target_profit,
    "stop_loss": snapshot.stop_loss,
    "position_count": snapshot.position_count,
    "watch_count": snapshot.watch_count,
    "watches": snapshot.watches,
  }
  with events_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(event) + "\n")
