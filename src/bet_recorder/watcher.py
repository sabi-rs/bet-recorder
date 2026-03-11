from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json
import time

from bet_recorder.analysis.position_watch import build_smarkets_watch_plan
from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.capture.run_bundle import load_run_bundle
from bet_recorder.exchange_worker import analyze_positions_payload, build_exchange_panel_snapshot
from bet_recorder.live.agent_browser_capture import capture_agent_browser_page
from bet_recorder.watcher_state import build_watcher_state, write_watcher_state


@dataclass(frozen=True)
class WatcherConfig:
  run_dir: Path
  session: str
  interval_seconds: float
  commission_rate: float
  target_profit: float
  stop_loss: float


CaptureOnce = Callable[[WatcherConfig, datetime], dict]
Sleep = Callable[[float], None]
Now = Callable[[], datetime]


def run_smarkets_watcher(
  config: WatcherConfig,
  *,
  capture_once: CaptureOnce | None = None,
  sleep: Sleep | None = None,
  now: Now | None = None,
  max_iterations: int | None = None,
) -> dict:
  effective_capture_once = capture_once or capture_current_smarkets_open_positions
  effective_sleep = sleep or time.sleep
  effective_now = now or _utc_now

  ensure_watcher_run_dir(config.run_dir)

  latest_state: dict | None = None
  iteration = 0
  while max_iterations is None or iteration < max_iterations:
    iteration += 1
    captured_at = effective_now()
    payload = effective_capture_once(config, captured_at)
    snapshot = build_exchange_snapshot_from_payload(payload, config)
    latest_state = build_watcher_state(
      source="smarkets_exchange",
      run_dir=config.run_dir,
      interval_seconds=config.interval_seconds,
      iteration=iteration,
      snapshot=snapshot,
      captured_at=captured_at,
    )
    write_watcher_state(config.run_dir / "watcher-state.json", latest_state)
    if max_iterations is not None and iteration >= max_iterations:
      break
    effective_sleep(config.interval_seconds)

  assert latest_state is not None
  return latest_state


def capture_current_smarkets_open_positions(config: WatcherConfig, captured_at: datetime) -> dict:
  bundle = load_run_bundle(source="smarkets_exchange", run_dir=config.run_dir)
  client = AgentBrowserClient(session=config.session)
  return capture_agent_browser_page(
    source="smarkets_exchange",
    bundle=bundle,
    page="open_positions",
    captured_at=captured_at,
    client=client,
    notes=["watcher-loop"],
  )


def build_exchange_snapshot_from_payload(payload: dict, config: WatcherConfig) -> dict:
  positions_analysis = analyze_positions_payload(payload)
  watch = build_smarkets_watch_plan(
    positions=positions_analysis["positions"],
    commission_rate=config.commission_rate,
    target_profit=config.target_profit,
    stop_loss=config.stop_loss,
  )
  snapshot = build_exchange_panel_snapshot(watch)
  snapshot["account_stats"] = positions_analysis.get("account_stats")
  snapshot["open_positions"] = positions_analysis["positions"]
  snapshot["other_open_bets"] = positions_analysis.get("other_open_bets", [])
  snapshot["worker"]["detail"] = (
    f'Watcher iteration captured {watch["watch_count"]} watch groups from '
    f'{watch["position_count"]} positions.'
  )
  snapshot["status_line"] = (
    f'Watcher updated {watch["watch_count"]} Smarkets watch groups at '
    f'{payload["captured_at"]}.'
  )
  return snapshot


def _utc_now() -> datetime:
  return datetime.now(UTC)


def ensure_watcher_run_dir(run_dir: Path) -> None:
  run_dir.mkdir(parents=True, exist_ok=True)
  (run_dir / "screenshots").mkdir(parents=True, exist_ok=True)
  (run_dir / "events.jsonl").touch(exist_ok=True)
  metadata_path = run_dir / "metadata.json"
  if not metadata_path.exists():
    metadata_path.write_text(json.dumps({"source": "smarkets_exchange"}) + "\n")
