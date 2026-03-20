from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import json


def append_operator_interaction_event(
  events_path: Path,
  *,
  action: str,
  status: str,
  detail: str,
  request_id: str | None = None,
  reference_id: str | None = None,
  source: str = "operator_console",
  metadata: dict[str, Any] | None = None,
) -> None:
  event = {
    "captured_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "source": source,
    "kind": "operator_interaction",
    "page": "worker_request",
    "action": action,
    "status": status,
    "detail": detail,
    "request_id": request_id,
    "reference_id": reference_id,
    "metadata": metadata or {},
  }
  with events_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(event) + "\n")
