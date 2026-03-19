from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json

from bet_recorder.capture.page_snapshot import PageSnapshot, append_page_snapshot


def test_append_page_snapshot_writes_expected_event_shape(tmp_path: Path) -> None:
  events_path = tmp_path / "events.jsonl"

  snapshot = PageSnapshot(
    source="betway_uk",
    kind="market_snapshot",
    page="market",
    url="https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
    document_title="Betway event",
    body_text="Correct Score",
    interactive_snapshot=[{"tag": "BUTTON", "text": "Add to Bet Slip"}],
    links=["https://betway.com/gb/en/sports"],
    inputs={"stake": ""},
    visible_actions=["Add to Bet Slip"],
    resource_hosts=["betway.com"],
    local_storage_keys=["theme"],
    screenshot_path="/tmp/market.png",
    notes=["example"],
    captured_at=datetime(2026, 3, 9, 10, 0, 0, tzinfo=UTC),
  )

  append_page_snapshot(events_path, snapshot)

  event = json.loads(events_path.read_text())

  assert event == {
    "captured_at": "2026-03-09T10:00:00Z",
    "source": "betway_uk",
    "kind": "market_snapshot",
    "page": "market",
    "url": "https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
    "document_title": "Betway event",
    "body_text": "Correct Score",
    "interactive_snapshot": [{"tag": "BUTTON", "text": "Add to Bet Slip"}],
    "links": ["https://betway.com/gb/en/sports"],
    "inputs": {"stake": ""},
    "visible_actions": ["Add to Bet Slip"],
    "resource_hosts": ["betway.com"],
    "local_storage_keys": ["theme"],
    "screenshot_path": "/tmp/market.png",
    "notes": ["example"],
  }


def test_append_page_snapshot_appends_without_overwriting(tmp_path: Path) -> None:
  events_path = tmp_path / "events.jsonl"

  append_page_snapshot(
    events_path,
    PageSnapshot(
      source="betway_uk",
      kind="market_snapshot",
      page="market",
      url="https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
      document_title="Betway event",
      body_text="Correct Score",
      interactive_snapshot=[],
      links=[],
      inputs={},
      visible_actions=[],
      resource_hosts=["betway.com"],
      local_storage_keys=["theme"],
      screenshot_path=None,
      notes=[],
      captured_at=datetime(2026, 3, 9, 10, 5, 0, tzinfo=UTC),
    ),
  )
  append_page_snapshot(
    events_path,
    PageSnapshot(
      source="bet365",
      kind="positions_snapshot",
      page="my_bets",
      url="https://www.bet365.com/#/IP/B16",
      document_title="bet365 my bets",
      body_text="Open Bets",
      interactive_snapshot=[],
      links=[],
      inputs={},
      visible_actions=[],
      resource_hosts=["bet365.com"],
      local_storage_keys=["mybets"],
      screenshot_path=None,
      notes=[],
      captured_at=datetime(2026, 3, 9, 10, 6, 0, tzinfo=UTC),
    ),
  )

  lines = events_path.read_text().splitlines()

  assert len(lines) == 2
  assert json.loads(lines[0])["source"] == "betway_uk"
  assert json.loads(lines[1])["source"] == "bet365"
