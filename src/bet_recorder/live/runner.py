from __future__ import annotations

from datetime import datetime

from bet_recorder.capture.action_snapshot import ActionSnapshot, append_action_snapshot
from bet_recorder.capture.run_bundle import RunBundle
from bet_recorder.capture.watch_snapshot import WatchSnapshot, append_watch_snapshot
from bet_recorder.sources.betway_uk import BetwayPageCapture, capture_betway_page
from bet_recorder.sources.fairodds_terminal import (
  FairOddsPageCapture,
  capture_fairodds_page,
)
from bet_recorder.sources.profitmaximiser import (
  ProfitMaximiserPageCapture,
  capture_profitmaximiser_page,
)
from bet_recorder.sources.rebelbetting_rb import RbPageCapture, capture_rb_page
from bet_recorder.sources.rebelbetting_vb import VbPageCapture, capture_vb_page
from bet_recorder.sources.smarkets_exchange import (
  SmarketsExchangePageCapture,
  capture_smarkets_exchange_page,
)
from bet_recorder.transport.writer import append_transport_event


def record_live_page(*, source: str, bundle: RunBundle, payload: dict) -> None:
  captured_at = datetime.fromisoformat(payload["captured_at"].replace("Z", "+00:00"))

  if source == "rebelbetting_vb":
    capture_vb_page(
      bundle,
      VbPageCapture(captured_at=captured_at, **_common_payload_kwargs(payload)),
    )
  elif source == "rebelbetting_rb":
    capture_rb_page(
      bundle,
      RbPageCapture(captured_at=captured_at, **_common_payload_kwargs(payload)),
    )
  elif source == "fairodds_terminal":
    capture_fairodds_page(
      bundle,
      FairOddsPageCapture(captured_at=captured_at, **_common_payload_kwargs(payload)),
    )
  elif source == "profitmaximiser_members":
    capture_profitmaximiser_page(
      bundle,
      ProfitMaximiserPageCapture(captured_at=captured_at, **_common_payload_kwargs(payload)),
    )
  elif source == "betway_uk":
    capture_betway_page(
      bundle,
      BetwayPageCapture(captured_at=captured_at, **_common_payload_kwargs(payload)),
    )
  elif source == "smarkets_exchange":
    capture_smarkets_exchange_page(
      bundle,
      SmarketsExchangePageCapture(captured_at=captured_at, **_common_payload_kwargs(payload)),
    )
  else:
    raise ValueError(f"Unsupported source: {source}")


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
  }
