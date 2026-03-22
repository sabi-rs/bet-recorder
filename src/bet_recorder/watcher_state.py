from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json
import os


def build_watcher_state(
  *,
  source: str,
  run_dir: Path,
  interval_seconds: float,
  iteration: int,
  snapshot: dict,
  captured_at: datetime,
) -> dict:
  watch = snapshot.get("watch") or {}
  decisions = [
    _build_decision(
      watch=row,
      target_profit=float(watch.get("target_profit", 0.0)),
      stop_loss=float(watch.get("stop_loss", 0.0)),
    )
    for row in watch.get("watches", [])
  ]
  updated_at = captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
  return {
    "source": source,
    "run_dir": str(run_dir),
    "updated_at": updated_at,
    "interval_seconds": interval_seconds,
    "iteration": iteration,
    "worker": snapshot.get("worker"),
    "account_stats": snapshot.get("account_stats"),
    "open_positions": snapshot.get("open_positions", []),
    "other_open_bets": snapshot.get("other_open_bets", []),
    "watch": snapshot.get("watch"),
    "warnings": snapshot.get("warnings", []),
    "decision_count": len(decisions),
    "decisions": decisions,
  }


def build_watcher_error_state(
  *,
  source: str,
  run_dir: Path,
  interval_seconds: float,
  iteration: int,
  captured_at: datetime,
  error: str,
) -> dict:
  updated_at = captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
  return {
    "source": source,
    "run_dir": str(run_dir),
    "updated_at": updated_at,
    "interval_seconds": interval_seconds,
    "iteration": iteration,
    "worker": {
      "name": "bet-recorder",
      "status": "error",
      "detail": error,
    },
    "account_stats": None,
    "open_positions": [],
    "other_open_bets": [],
    "watch": {
      "position_count": 0,
      "watch_count": 0,
      "commission_rate": 0.0,
      "target_profit": 0.0,
      "stop_loss": 0.0,
      "watches": [],
    },
    "warnings": [],
    "decision_count": 0,
    "decisions": [],
  }


def write_watcher_state(output_path: Path, state: dict) -> None:
  if output_path.parent:
    output_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
      output_path.parent.chmod(0o700)
    except OSError:
      pass
  temp_path = output_path.with_name(f".{output_path.name}.tmp")
  temp_path.write_text(json.dumps(state, indent=2) + "\n")
  try:
    os.chmod(temp_path, 0o600)
  except OSError:
    pass
  temp_path.replace(output_path)
  try:
    os.chmod(output_path, 0o600)
  except OSError:
    pass


def _build_decision(*, watch: dict, target_profit: float, stop_loss: float) -> dict:
  current_pnl = float(watch["current_pnl_amount"])
  current_back_odds = watch.get("current_back_odds")
  if not bool(watch.get("can_trade_out")):
    status = "monitor_only"
    reason = "trade_out_unavailable"
  elif current_back_odds is not None:
    current_back_odds = float(current_back_odds)
    if current_back_odds >= float(watch["profit_take_back_odds"]):
      status = "take_profit_ready"
    elif current_back_odds <= float(watch["stop_loss_back_odds"]):
      status = "stop_loss_ready"
    else:
      status = "hold"
    reason = "current_back_odds"
  elif current_pnl >= target_profit:
    status = "take_profit_ready"
    reason = "current_pnl_amount"
  elif current_pnl <= -stop_loss:
    status = "stop_loss_ready"
    reason = "current_pnl_amount"
  else:
    status = "hold"
    reason = "current_pnl_amount"

  return {
    "contract": watch["contract"],
    "market": watch["market"],
    "status": status,
    "reason": reason,
    "current_pnl_amount": current_pnl,
    "current_back_odds": current_back_odds,
    "profit_take_back_odds": float(watch["profit_take_back_odds"]),
    "stop_loss_back_odds": float(watch["stop_loss_back_odds"]),
  }
