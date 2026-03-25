from __future__ import annotations

from datetime import datetime

from bet_recorder.capture.bets_observations import refresh_bets_observations
from bet_recorder.capture.action_snapshot import ActionSnapshot, append_action_snapshot
from bet_recorder.capture.page_snapshot import PageSnapshot, append_page_snapshot
from bet_recorder.capture.run_bundle import RunBundle
from bet_recorder.capture.watch_snapshot import WatchSnapshot, append_watch_snapshot
from bet_recorder.sources.betway_uk import BetwayPageCapture, capture_betway_page
from bet_recorder.sources.smarkets_exchange import (
  SmarketsExchangePageCapture,
  capture_smarkets_exchange_page,
)
from bet_recorder.transport.writer import append_transport_event


def record_live_page(*, source: str, bundle: RunBundle, payload: dict) -> None:
  captured_at = datetime.fromisoformat(payload["captured_at"].replace("Z", "+00:00"))

  if source == "betway_uk":
    capture_betway_page(
      bundle,
      BetwayPageCapture(captured_at=captured_at, **_common_payload_kwargs(payload)),
    )
  elif source in {
    "bet365",
    "betuk",
    "betfred",
    "betdaq",
    "coral",
    "ladbrokes",
    "kwik",
    "bet600",
  }:
    append_page_snapshot(
      bundle.events_path,
      PageSnapshot(
        source=source,
        kind=_generic_page_kind(payload["page"]),
        captured_at=captured_at,
        **_common_payload_kwargs(payload),
      ),
    )
  elif source == "smarkets_exchange":
    capture_smarkets_exchange_page(
      bundle,
      SmarketsExchangePageCapture(captured_at=captured_at, **_common_payload_kwargs(payload)),
    )
  else:
    raise ValueError(f"Unsupported source: {source}")
  refresh_bets_observations(
    run_dir=bundle.run_dir,
    events_path=bundle.events_path,
    transport_path=bundle.transport_path,
  )


def record_live_action(*, source: str, bundle: RunBundle, payload: dict) -> None:
  captured_at = datetime.fromisoformat(payload["captured_at"].replace("Z", "+00:00"))
  append_action_snapshot(
    bundle.events_path,
    ActionSnapshot(
      source=source,
      page=payload["page"],
      action=payload["action"],
      target=payload["target"],
      status=payload["status"],
      url=payload["url"],
      document_title=payload["document_title"],
      body_text=payload["body_text"],
      interactive_snapshot=payload["interactive_snapshot"],
      links=payload["links"],
      inputs=payload["inputs"],
      visible_actions=payload["visible_actions"],
      resource_hosts=payload["resource_hosts"],
      local_storage_keys=payload["local_storage_keys"],
      screenshot_path=payload["screenshot_path"],
      notes=payload["notes"],
      metadata=payload.get("metadata", {}),
      captured_at=captured_at,
    ),
  )


def record_live_transport(*, bundle: RunBundle, payload: dict) -> None:
  if bundle.transport_path is None:
    raise ValueError("Run bundle does not have transport capture enabled.")
  append_transport_event(bundle.transport_path, payload)
  refresh_bets_observations(
    run_dir=bundle.run_dir,
    events_path=bundle.events_path,
    transport_path=bundle.transport_path,
  )


def record_watch_plan(*, source: str, bundle: RunBundle, payload: dict) -> None:
  captured_at = datetime.fromisoformat(payload["captured_at"].replace("Z", "+00:00"))
  append_watch_snapshot(
    bundle.events_path,
    WatchSnapshot(
      source=source,
      page=payload["page"],
      commission_rate=float(payload["commission_rate"]),
      target_profit=float(payload["target_profit"]),
      stop_loss=float(payload["stop_loss"]),
      position_count=int(payload["position_count"]),
      watch_count=int(payload["watch_count"]),
      watches=payload["watches"],
      captured_at=captured_at,
    ),
  )


def _common_payload_kwargs(payload: dict) -> dict:
  return {
    "page": payload["page"],
    "url": payload["url"],
    "document_title": payload["document_title"],
    "body_text": payload["body_text"],
    "interactive_snapshot": payload["interactive_snapshot"],
    "links": payload["links"],
    "inputs": payload["inputs"],
    "visible_actions": payload["visible_actions"],
    "resource_hosts": payload["resource_hosts"],
    "local_storage_keys": payload["local_storage_keys"],
    "screenshot_path": payload["screenshot_path"],
    "notes": payload["notes"],
    "metadata": payload.get("metadata", {}),
  }


def _generic_page_kind(page: str) -> str:
  if page in {"my_bets", "open_positions"}:
    return "positions_snapshot"
  if page == "settlement":
    return "settlement_snapshot"
  if page == "market":
    return "market_snapshot"
  if page == "betslip":
    return "betslip_snapshot"
  if page == "confirmation":
    return "confirmation_snapshot"
  raise ValueError(f"Unsupported generic page kind: {page}")
