from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json

from bet_recorder.analysis.position_watch import build_smarkets_watch_plan
from bet_recorder.analysis.smarkets_exchange import analyze_smarkets_page
from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.capture.run_bundle import load_run_bundle
from bet_recorder.live.agent_browser_capture import capture_agent_browser_page


@dataclass(frozen=True)
class WorkerConfig:
  positions_payload_path: Path | None
  run_dir: Path | None
  account_payload_path: Path | None
  open_bets_payload_path: Path | None
  agent_browser_session: str | None
  commission_rate: float
  target_profit: float
  stop_loss: float

  @classmethod
  def from_dict(cls, payload: dict) -> WorkerConfig:
    if payload.get("positions_payload_path") is None and payload.get("run_dir") is None:
      raise ValueError("Worker config requires positions_payload_path or run_dir.")
    try:
      return cls(
        positions_payload_path=(
          Path(payload["positions_payload_path"])
          if payload.get("positions_payload_path") is not None
          else None
        ),
        run_dir=Path(payload["run_dir"]) if payload.get("run_dir") is not None else None,
        account_payload_path=(
          Path(payload["account_payload_path"])
          if payload.get("account_payload_path") is not None
          else None
        ),
        open_bets_payload_path=(
          Path(payload["open_bets_payload_path"])
          if payload.get("open_bets_payload_path") is not None
          else None
        ),
        agent_browser_session=(
          str(payload["agent_browser_session"])
          if payload.get("agent_browser_session") is not None
          else None
        ),
        commission_rate=float(payload["commission_rate"]),
        target_profit=float(payload["target_profit"]),
        stop_loss=float(payload["stop_loss"]),
      )
    except KeyError as exc:
      raise ValueError(f"Missing worker config field: {exc.args[0]}") from exc
    except (TypeError, ValueError) as exc:
      raise ValueError("Worker config fields must be valid JSON scalars.") from exc


def load_watch_snapshot(
  *,
  payload_path: Path,
  commission_rate: float,
  target_profit: float,
  stop_loss: float,
) -> dict:
  payload = json.loads(payload_path.read_text())
  analysis = analyze_smarkets_page(
    page=payload["page"],
    body_text=payload["body_text"],
    inputs=payload.get("inputs", {}),
    visible_actions=payload.get("visible_actions", []),
  )
  if analysis["page"] != "open_positions":
    raise ValueError("watch-open-positions requires an open_positions payload.")

  return build_smarkets_watch_plan(
    positions=analysis["positions"],
    commission_rate=commission_rate,
    target_profit=target_profit,
    stop_loss=stop_loss,
  )


def load_positions_analysis(payload_path: Path) -> dict:
  payload = json.loads(payload_path.read_text())
  return analyze_positions_payload(payload)


def analyze_positions_payload(payload: dict) -> dict:
  analysis = analyze_smarkets_page(
    page=payload["page"],
    body_text=payload["body_text"],
    inputs=payload.get("inputs", {}),
    visible_actions=payload.get("visible_actions", []),
  )
  if analysis["page"] != "open_positions":
    raise ValueError("positions payload must be an open_positions capture.")
  return analysis


def load_latest_positions_payload_from_run_dir(run_dir: Path) -> dict:
  events_path = run_dir / "events.jsonl"
  if not events_path.exists():
    raise ValueError(f"Run bundle does not contain events.jsonl: {events_path}")

  latest_payload: dict | None = None
  for line in events_path.read_text().splitlines():
    if not line.strip():
      continue
    event = json.loads(line)
    if event.get("kind") != "positions_snapshot":
      continue
    if event.get("page") != "open_positions":
      continue
    latest_payload = event

  if latest_payload is None:
    raise ValueError(f"No positions_snapshot event found in run bundle: {run_dir}")
  return latest_payload


def load_positions_analysis_for_config(config: WorkerConfig) -> dict:
  if config.positions_payload_path is not None:
    return load_positions_analysis(config.positions_payload_path)
  if config.run_dir is not None:
    return analyze_positions_payload(load_latest_positions_payload_from_run_dir(config.run_dir))
  raise ValueError("Worker config requires positions_payload_path or run_dir.")


def capture_current_smarkets_open_positions(config: WorkerConfig) -> None:
  if config.run_dir is None or config.agent_browser_session is None:
    return

  bundle = load_run_bundle(source="smarkets_exchange", run_dir=config.run_dir)
  client = AgentBrowserClient(session=config.agent_browser_session)
  capture_agent_browser_page(
    source="smarkets_exchange",
    bundle=bundle,
    page="open_positions",
    captured_at=datetime.now(UTC),
    client=client,
    notes=["exchange-worker-refresh"],
  )


def build_exchange_panel_snapshot(watch: dict) -> dict:
  unique_market_count = len({row["market"] for row in watch["watches"]})
  return {
    "worker": {
      "name": "bet-recorder",
      "status": "ready",
      "detail": (
        f'Loaded {watch["watch_count"]} watch groups from {watch["position_count"]} positions.'
      ),
    },
    "venues": [
      {
        "id": "smarkets",
        "label": "Smarkets",
        "status": "ready",
        "detail": f'{watch["watch_count"]} grouped watches across {unique_market_count} markets',
        "event_count": watch["watch_count"],
        "market_count": unique_market_count,
      },
    ],
    "selected_venue": "smarkets",
    "events": [
      {
        "id": f'{row["market"]}::{row["contract"]}',
        "label": row["contract"],
        "competition": row["market"],
        "start_time": (
          f'profit {row["profit_take_back_odds"]:.2f} stop {row["stop_loss_back_odds"]:.2f}'
        ),
        "url": "",
      }
      for row in watch["watches"]
    ],
    "markets": [
      {
        "name": row["market"],
        "contract_count": row["position_count"],
      }
      for row in watch["watches"]
    ],
    "preflight": None,
    "status_line": f'Loaded {watch["watch_count"]} Smarkets watch groups from bet-recorder.',
    "account_stats": None,
    "open_positions": [],
    "other_open_bets": [],
    "watch": watch,
  }


def load_watch_snapshot_for_config(config: WorkerConfig) -> dict:
  positions_analysis = load_positions_analysis_for_config(config)
  return build_smarkets_watch_plan(
    positions=positions_analysis["positions"],
    commission_rate=config.commission_rate,
    target_profit=config.target_profit,
    stop_loss=config.stop_loss,
  )


def load_open_positions(payload_path: Path) -> list[dict]:
  return load_positions_analysis(payload_path)["positions"]


def load_account_stats(payload_path: Path | None) -> dict | None:
  if payload_path is None:
    return None
  payload = json.loads(payload_path.read_text())
  required_fields = ("available_balance", "exposure", "unrealized_pnl", "currency")
  missing_fields = [field for field in required_fields if field not in payload]
  if missing_fields:
    raise ValueError(
      f"Account stats payload is missing fields: {', '.join(missing_fields)}",
    )
  return {
    "available_balance": float(payload["available_balance"]),
    "exposure": float(payload["exposure"]),
    "unrealized_pnl": float(payload["unrealized_pnl"]),
    "currency": str(payload["currency"]),
  }


def load_other_open_bets(payload_path: Path | None) -> list[dict]:
  if payload_path is None:
    return []
  payload = json.loads(payload_path.read_text())
  bets = payload.get("bets")
  if not isinstance(bets, list):
    raise ValueError("Open bets payload must contain a bets list.")
  return [
    {
      "label": str(bet["label"]),
      "market": str(bet["market"]),
      "side": str(bet["side"]),
      "odds": float(bet["odds"]),
      "stake": float(bet["stake"]),
      "status": str(bet["status"]),
    }
    for bet in bets
  ]


def load_exchange_snapshot_for_config(config: WorkerConfig) -> dict:
  capture_current_smarkets_open_positions(config)
  positions_analysis = load_positions_analysis_for_config(config)
  watch = build_smarkets_watch_plan(
    positions=positions_analysis["positions"],
    commission_rate=config.commission_rate,
    target_profit=config.target_profit,
    stop_loss=config.stop_loss,
  )
  snapshot = build_exchange_panel_snapshot(watch)
  snapshot["account_stats"] = (
    positions_analysis.get("account_stats") or load_account_stats(config.account_payload_path)
  )
  snapshot["open_positions"] = positions_analysis["positions"]
  snapshot["other_open_bets"] = (
    positions_analysis.get("other_open_bets") or load_other_open_bets(config.open_bets_payload_path)
  )
  if snapshot["account_stats"] is not None:
    snapshot["worker"]["detail"] = (
      f'Loaded {watch["watch_count"]} watch groups and richer account state from '
      f'{watch["position_count"]} positions.'
    )
  return snapshot


def parse_worker_request(request: str | dict) -> tuple[str, dict | None]:
  if isinstance(request, str) and request in {"LoadDashboard", "Refresh"}:
    return request, None

  if isinstance(request, dict) and len(request) == 1:
    request_name, request_payload = next(iter(request.items()))
    if request_name in {"LoadDashboard", "Refresh"}:
      if request_payload is None:
        return request_name, None
      if not isinstance(request_payload, dict):
        raise ValueError(f"{request_name} payload must be an object when provided.")
      return request_name, request_payload

    if request_name == "SelectVenue":
      if not isinstance(request_payload, dict):
        raise ValueError("SelectVenue payload must be an object.")
      return request_name, request_payload

  raise ValueError(f"Unsupported worker request: {request}")


def require_worker_config(config: WorkerConfig | None, request_name: str) -> WorkerConfig:
  if config is None:
    raise ValueError(
      f"{request_name} requires worker config. Send LoadDashboard with config first.",
    )
  return config


def handle_worker_request(
  *,
  request: str | dict,
  config: WorkerConfig | None,
) -> tuple[dict, WorkerConfig]:
  request_name, request_payload = parse_worker_request(request)

  if request_name == "LoadDashboard":
    next_config = config
    if request_payload is not None:
      request_config = request_payload.get("config")
      if request_config is not None:
        if not isinstance(request_config, dict):
          raise ValueError("LoadDashboard config must be an object.")
        next_config = WorkerConfig.from_dict(request_config)
    resolved_config = require_worker_config(next_config, request_name)
    return {"snapshot": load_exchange_snapshot_for_config(resolved_config)}, resolved_config

  resolved_config = require_worker_config(config, request_name)

  if request_name == "Refresh":
    return {"snapshot": load_exchange_snapshot_for_config(resolved_config)}, resolved_config

  if request_name == "SelectVenue":
    assert request_payload is not None
    try:
      venue = request_payload["venue"]
    except KeyError as exc:
      raise ValueError("SelectVenue payload must include venue.") from exc
    if venue != "smarkets":
      raise ValueError(f"Unsupported venue for recorder worker: {venue}")
    return {"snapshot": load_exchange_snapshot_for_config(resolved_config)}, resolved_config

  raise AssertionError(f"Unhandled worker request type: {request_name}")


def handle_worker_request_line(
  *,
  request_line: str,
  config: WorkerConfig | None,
) -> tuple[dict, WorkerConfig]:
  return handle_worker_request(
    request=json.loads(request_line),
    config=config,
  )


def iter_worker_session_responses(
  *,
  request_lines: Iterable[str],
) -> Iterator[dict]:
  config: WorkerConfig | None = None
  for request_line in request_lines:
    normalized_request_line = request_line.strip()
    if not normalized_request_line:
      continue
    response, config = handle_worker_request_line(
      request_line=normalized_request_line,
      config=config,
    )
    yield response
