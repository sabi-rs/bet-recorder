from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


def _to_utc_z(value: datetime) -> str:
  return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def _dedupe(values: list[str]) -> list[str]:
  return list(dict.fromkeys(values))


@dataclass(slots=True)
class BrowserPageState:
  captured_at: datetime
  page: str
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

  def to_payload(self) -> dict[str, Any]:
    return {
      "captured_at": _to_utc_z(self.captured_at),
      "page": self.page,
      "url": self.url,
      "document_title": self.document_title,
      "body_text": self.body_text,
      "interactive_snapshot": self.interactive_snapshot,
      "links": self.links,
      "inputs": self.inputs,
      "visible_actions": self.visible_actions,
      "resource_hosts": _dedupe(self.resource_hosts),
      "local_storage_keys": _dedupe(self.local_storage_keys),
      "screenshot_path": self.screenshot_path,
      "notes": self.notes,
    }


@dataclass(slots=True)
class BrowserScreenshot:
  page: str
  path: str
  mime_type: str
  width: int | None = None
  height: int | None = None

  def to_payload(self) -> dict[str, Any]:
    return {
      "page": self.page,
      "path": self.path,
      "mime_type": self.mime_type,
      "width": self.width,
      "height": self.height,
    }


@dataclass(slots=True)
class BrowserTransportEvent:
  captured_at: datetime
  event_type: str
  headers: dict[str, Any]
  preview: str
  metadata: dict[str, Any]

  def to_payload(self) -> dict[str, Any]:
    return {
      "captured_at": _to_utc_z(self.captured_at),
      "type": self.event_type,
      "headers": self.headers,
      "preview": self.preview,
      "metadata": self.metadata,
    }


@dataclass(slots=True)
class BrowserActionState:
  captured_at: datetime
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

  def to_payload(self) -> dict[str, Any]:
    return {
      "captured_at": _to_utc_z(self.captured_at),
      "page": self.page,
      "action": self.action,
      "target": self.target,
      "status": self.status,
      "url": self.url,
      "document_title": self.document_title,
      "body_text": self.body_text,
      "interactive_snapshot": self.interactive_snapshot,
      "links": self.links,
      "inputs": self.inputs,
      "visible_actions": self.visible_actions,
      "resource_hosts": _dedupe(self.resource_hosts),
      "local_storage_keys": _dedupe(self.local_storage_keys),
      "screenshot_path": self.screenshot_path,
      "notes": self.notes,
      "metadata": self.metadata,
    }
