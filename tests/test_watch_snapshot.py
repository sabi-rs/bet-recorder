from datetime import UTC, datetime
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.capture.watch_snapshot import WatchSnapshot, append_watch_snapshot  # noqa: E402


def test_append_watch_snapshot_writes_watch_plan_event(tmp_path: Path) -> None:
  events_path = tmp_path / "events.jsonl"
  append_watch_snapshot(
    events_path,
    WatchSnapshot(
      source="smarkets_exchange",
      page="open_positions",
      commission_rate=0.0,
      target_profit=1.0,
      stop_loss=1.0,
      position_count=3,
      watch_count=2,
      watches=[
        {
          "contract": "1 - 1",
          "market": "Correct score",
          "profit_take_back_odds": 10.87,
          "stop_loss_back_odds": 5.38,
        },
      ],
      captured_at=datetime(2026, 3, 9, 20, 45, tzinfo=UTC),
    ),
  )

  event = json.loads(events_path.read_text())
  assert event["kind"] == "watch_plan_snapshot"
  assert event["page"] == "open_positions"
  assert event["watch_count"] == 2

