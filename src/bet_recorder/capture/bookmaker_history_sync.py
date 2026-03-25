from __future__ import annotations

from pathlib import Path
import json


def append_bookmaker_history_sync_event(events_path: Path, report: dict) -> None:
  event = {
    "captured_at": str(report.get("finished_at", "") or report.get("started_at", "") or ""),
    "kind": "bookmaker_history_sync",
    "source": "bookmaker_history",
    "page": str(report.get("page", "") or ""),
    "action": str(report.get("venue", "") or ""),
    "status": str(report.get("status", "") or ""),
    "request_id": "",
    "reference_id": str(report.get("venue", "") or ""),
    "summary": str(report.get("summary", "") or ""),
    "detail": str(report.get("detail", "") or ""),
    "metadata": {
      "url": str(report.get("url", "") or ""),
      "rows_extracted": int(report.get("rows_extracted", 0) or 0),
      "trace": [dict(step) for step in report.get("trace", []) if isinstance(step, dict)],
    },
  }
  with events_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(event) + "\n")
