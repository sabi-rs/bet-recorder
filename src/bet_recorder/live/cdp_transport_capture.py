from __future__ import annotations

from collections.abc import Callable

from bet_recorder.browser.cdp import (
  DEFAULT_DEBUG_BASE_URL,
  DebugTarget,
  capture_transport_events,
  list_debug_targets,
  select_debug_target,
)
from bet_recorder.capture.bets_observations import refresh_bets_observations
from bet_recorder.capture.run_bundle import RunBundle
from bet_recorder.transport.writer import append_transport_event

TargetLister = Callable[[str], list[DebugTarget]]
EventCapturer = Callable[[str, int, bool], list[dict]]


def capture_cdp_transport(
  *,
  source: str,
  bundle: RunBundle,
  debug_base_url: str = DEFAULT_DEBUG_BASE_URL,
  duration_ms: int,
  reload: bool = False,
  url_contains: str | None = None,
  list_targets: TargetLister | None = None,
  capture_events: EventCapturer | None = None,
) -> int:
  if bundle.transport_path is None:
    raise ValueError("Run bundle does not have transport capture enabled.")

  lister = list_targets or _list_targets
  capturer = capture_events or _capture_events

  target = select_debug_target(
    source=source,
    targets=lister(debug_base_url),
    url_contains=url_contains,
  )
  events = capturer(target.websocket_debugger_url, duration_ms, reload)
  for event in events:
    append_transport_event(
      bundle.transport_path,
      {
        **event,
        "target_id": target.target_id,
        "target_title": target.title,
        "target_url": target.url,
      },
    )
  refresh_bets_observations(
    run_dir=bundle.run_dir,
    events_path=bundle.events_path,
    transport_path=bundle.transport_path,
  )
  return len(events)


def _list_targets(debug_base_url: str) -> list[DebugTarget]:
  return list_debug_targets(debug_base_url=debug_base_url)


def _capture_events(websocket_debugger_url: str, duration_ms: int, reload: bool) -> list[dict]:
  return capture_transport_events(
    websocket_debugger_url=websocket_debugger_url,
    duration_ms=duration_ms,
    reload=reload,
  )
