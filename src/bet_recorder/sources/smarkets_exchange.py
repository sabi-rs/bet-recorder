from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime

from bet_recorder.capture.page_snapshot import PageSnapshot, append_page_snapshot
from bet_recorder.capture.run_bundle import RunBundle

PAGE_KINDS = {
  "market": "market_snapshot",
  "open_positions": "positions_snapshot",
  "history": "history_snapshot",
  "settlement": "settlement_snapshot",
}


@dataclass(frozen=True)
class SmarketsExchangePageCapture:
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
  metadata: dict | None = None


def capture_smarkets_exchange_page(
  bundle: RunBundle,
  capture: SmarketsExchangePageCapture,
) -> None:
  kind = PAGE_KINDS.get(capture.page)
  if kind is None:
    raise ValueError(f"Unsupported Smarkets page: {capture.page}")

  append_page_snapshot(
    bundle.events_path,
    PageSnapshot(
      source="smarkets_exchange",
      kind=kind,
      page=capture.page,
      url=capture.url,
      document_title=capture.document_title,
      body_text=capture.body_text,
      interactive_snapshot=capture.interactive_snapshot,
      links=capture.links,
      inputs=capture.inputs,
      visible_actions=capture.visible_actions,
      resource_hosts=capture.resource_hosts,
      local_storage_keys=capture.local_storage_keys,
      screenshot_path=capture.screenshot_path,
      notes=capture.notes,
      captured_at=capture.captured_at,
      metadata=capture.metadata or {},
    ),
  )
