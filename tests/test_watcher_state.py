from datetime import UTC, datetime
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.watcher_state import (  # noqa: E402
  build_watcher_state,
  write_watcher_state,
)


def test_build_watcher_state_marks_profit_and_stop_loss_readiness() -> None:
  state = build_watcher_state(
    source="smarkets_exchange",
    run_dir=Path("/tmp/smarkets-run"),
    interval_seconds=5.0,
    iteration=3,
    snapshot={
      "watch": {
        "target_profit": 1.0,
        "stop_loss": 1.0,
        "watches": [
          {
            "contract": "Draw",
            "market": "Full-time result",
            "current_pnl_amount": 2.4,
            "can_trade_out": True,
            "profit_take_back_odds": 4.8,
            "stop_loss_back_odds": 2.8,
          },
          {
            "contract": "1 - 1",
            "market": "Correct score",
            "current_pnl_amount": -1.2,
            "can_trade_out": True,
            "profit_take_back_odds": 10.87,
            "stop_loss_back_odds": 5.38,
          },
          {
            "contract": "Arsenal",
            "market": "Match odds",
            "current_pnl_amount": 0.1,
            "can_trade_out": False,
            "profit_take_back_odds": 2.0,
            "stop_loss_back_odds": 1.8,
          },
        ],
      },
    },
    captured_at=datetime(2026, 3, 11, 12, 5, tzinfo=UTC),
  )

  assert state["decision_count"] == 3
  assert state["decisions"][0]["status"] == "take_profit_ready"
  assert state["decisions"][1]["status"] == "stop_loss_ready"
  assert state["decisions"][2]["status"] == "monitor_only"


def test_write_watcher_state_persists_latest_json(tmp_path: Path) -> None:
  output_path = tmp_path / "watcher-state.json"

  write_watcher_state(
    output_path,
    {
      "source": "smarkets_exchange",
      "run_dir": str(tmp_path),
      "updated_at": "2026-03-11T12:05:00Z",
      "decision_count": 1,
      "decisions": [{"contract": "Draw", "status": "hold"}],
    },
  )

  persisted = json.loads(output_path.read_text())
  assert persisted["decision_count"] == 1
  assert persisted["decisions"][0]["contract"] == "Draw"
