from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.capture.action_snapshot import ActionSnapshot, append_action_snapshot  # noqa: E402


def test_append_action_snapshot_writes_generic_action_event(tmp_path: Path) -> None:
  events_path = tmp_path / "events.jsonl"

  append_action_snapshot(
    events_path,
    ActionSnapshot(
      source="betway_uk",
      page="confirmation",
      action="place_bet",
      target="place_bet_button",
      status="confirmed",
      url="https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
      document_title="Almeria - Cultural Leonesa Betting Odds, Football Betting at Betway",
      body_text="Bet placed",
      interactive_snapshot=[{"ref": "e1", "role": "button", "name": "Done"}],
      links=[],
      inputs={"stake": "10"},
      visible_actions=["Done"],
      resource_hosts=["betway.com"],
      local_storage_keys=["theme"],
      screenshot_path="screenshots/confirmation-20260309T201500Z.png",
      notes=["free-bet"],
      metadata={"selection": "Almeria 2-0", "bookmaker": "Betway"},
      captured_at=datetime(2026, 3, 9, 20, 15, tzinfo=UTC),
    ),
  )

  event = json.loads(events_path.read_text())

  assert event["kind"] == "action_snapshot"
  assert event["action"] == "place_bet"
  assert event["target"] == "place_bet_button"
  assert event["status"] == "confirmed"
  assert event["metadata"]["selection"] == "Almeria 2-0"
