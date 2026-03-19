from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
import json


@dataclass(frozen=True)
class PageSnapshot:
  source: str
  kind: str
  page: str
  url: str
  document_title: str
  body_text: str
  interactive_snapshot: list[dict[str, str]]
  links: list[str]
  inputs: dict[str, str]
  visible_actions: list[str]
  resource_hosts: list[str]
  local_storage_keys: list[str]
  screenshot_path: str | None
  notes: list[str]
  captured_at: datetime
  metadata: dict = field(default_factory=dict)


def append_page_snapshot(events_path: Path, snapshot: PageSnapshot) -> None:
  event = {
    "captured_at": snapshot.captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    "source": snapshot.source,
    "kind": snapshot.kind,
    "page": snapshot.page,
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
  }
  if snapshot.metadata:
    event["metadata"] = snapshot.metadata
  with events_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(event) + "\n")
