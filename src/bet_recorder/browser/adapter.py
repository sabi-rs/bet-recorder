from __future__ import annotations

from datetime import datetime
from typing import Any

from bet_recorder.browser.models import (
  BrowserActionState,
  BrowserPageState,
  BrowserScreenshot,
  BrowserTransportEvent,
)


def build_page_payload(
  *,
  captured_at: datetime,
  page: str,
  url: str,
  document_title: str,
  body_text: str,
  interactive_snapshot: list[dict[str, Any]],
  links: list[str],
  inputs: dict[str, Any],
  visible_actions: list[str],
  resource_hosts: list[str],
  local_storage_keys: list[str],
  screenshot: BrowserScreenshot | None = None,
  notes: list[str] | None = None,
) -> dict[str, Any]:
  return BrowserPageState(
    captured_at=captured_at,
    page=page,
    url=url,
    document_title=document_title,
    body_text=body_text,
    interactive_snapshot=interactive_snapshot,
    links=links,
    inputs=inputs,
    visible_actions=visible_actions,
    resource_hosts=resource_hosts,
    local_storage_keys=local_storage_keys,
    screenshot_path=screenshot.path if screenshot else None,
    notes=notes or [],
  ).to_payload()


def build_transport_payload(
  *,
  captured_at: datetime,
  event_type: str,
  headers: dict[str, Any],
  preview: str,
  metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return BrowserTransportEvent(
    captured_at=captured_at,
    event_type=event_type,
    headers=headers,
    preview=preview,
    metadata=metadata or {},
  ).to_payload()


def build_action_payload(
  *,
  captured_at: datetime,
  page: str,
  action: str,
  target: str,
  status: str,
  url: str,
  document_title: str,
  body_text: str,
  interactive_snapshot: list[dict[str, Any]],
  links: list[str],
  inputs: dict[str, Any],
  visible_actions: list[str],
  resource_hosts: list[str],
  local_storage_keys: list[str],
  screenshot: BrowserScreenshot | None = None,
  notes: list[str] | None = None,
  metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
  return BrowserActionState(
    captured_at=captured_at,
    page=page,
    action=action,
    target=target,
    status=status,
    url=url,
    document_title=document_title,
    body_text=body_text,
    interactive_snapshot=interactive_snapshot,
    links=links,
    inputs=inputs,
    visible_actions=visible_actions,
    resource_hosts=resource_hosts,
    local_storage_keys=local_storage_keys,
    screenshot_path=screenshot.path if screenshot else None,
    notes=notes or [],
    metadata=metadata or {},
  ).to_payload()
