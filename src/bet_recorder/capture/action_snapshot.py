from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
import json


@dataclass(frozen=True)
class ActionSnapshot:
  source: str
  page: str
  action: str
  target: str
  status: str
  url: str
  document_title: str
  body_text: str
  interactive_snapshot: list[dict[str, Any]]
  links: list[str]
  inputs: dict[str, Any]
  visible_actions: list[str]
  resource_hosts: list[str]
  local_storage_keys: list[str]
  screenshot_path: str | None
  notes: list[str]
  metadata: dict[str, Any]
  captured_at: datetime


def append_action_snapshot(events_path: Path, snapshot: ActionSnapshot) -> None:
  event = {
    "captured_at": snapshot.captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "source": snapshot.source,
    "kind": "action_snapshot",
    "page": snapshot.page,
    "action": snapshot.action,
    "target": snapshot.target,
    "status": snapshot.status,
    "url": snapshot.url,
    "document_title": snapshot.document_title,
    "body_text": snapshot.body_text,
    "interactive_snapshot": snapshot.interactive_snapshot,
    "links": snapshot.links,
    "inputs": snapshot.inputs,
    "visible_actions": snapshot.visible_actions,
    "resource_hosts": snapshot.resource_hosts,
    "local_storage_keys": snapshot.local_storage_keys,
    "screenshot_path": snapshot.screenshot_path,
    "notes": snapshot.notes,
    "metadata": snapshot.metadata,
  }
  with events_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(event) + "\n")
