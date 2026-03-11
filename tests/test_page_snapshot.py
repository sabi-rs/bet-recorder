from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json

from bet_recorder.capture.page_snapshot import PageSnapshot, append_page_snapshot


def test_append_page_snapshot_writes_expected_event_shape(tmp_path: Path) -> None:
  events_path = tmp_path / "events.jsonl"

  snapshot = PageSnapshot(
    source="profitmaximiser_members",
    kind="training_page_snapshot",
    page="training",
    url="https://profitmaximiser.co.uk/members/pages/training",
    document_title="Training",
    body_text="TRAINING\\nBookmaker offer basics",
    interactive_snapshot=[{"tag": "A", "text": "Start Here"}],
    links=["https://profitmaximiser.co.uk/members/pages/starthere"],
    inputs={"search_title": ""},
    visible_actions=["Start Here"],
    resource_hosts=["profitmaximiser.co.uk"],
    local_storage_keys=["search_text_input"],
    screenshot_path="/tmp/training.png",
    notes=["example"],
    captured_at=datetime(2026, 3, 9, 10, 0, 0, tzinfo=UTC),
  )

  append_page_snapshot(events_path, snapshot)

  event = json.loads(events_path.read_text())

  assert event == {
    "captured_at": "2026-03-09T10:00:00Z",
    "source": "profitmaximiser_members",
    "kind": "training_page_snapshot",
    "page": "training",
    "url": "https://profitmaximiser.co.uk/members/pages/training",
    "document_title": "Training",
    "body_text": "TRAINING\\nBookmaker offer basics",
    "interactive_snapshot": [{"tag": "A", "text": "Start Here"}],
    "links": ["https://profitmaximiser.co.uk/members/pages/starthere"],
    "inputs": {"search_title": ""},
    "visible_actions": ["Start Here"],
    "resource_hosts": ["profitmaximiser.co.uk"],
    "local_storage_keys": ["search_text_input"],
    "screenshot_path": "/tmp/training.png",
    "notes": ["example"],
  }


def test_append_page_snapshot_appends_without_overwriting(tmp_path: Path) -> None:
  events_path = tmp_path / "events.jsonl"

  append_page_snapshot(
    events_path,
    PageSnapshot(
      source="rebelbetting_vb",
      kind="dashboard_snapshot",
      page="dashboard",
      url="https://vb.rebelbetting.com/",
      document_title="Value betting by RebelBetting",
      body_text="25 value bets",
      interactive_snapshot=[],
      links=[],
      inputs={},
      visible_actions=[],
      resource_hosts=["vb.rebelbetting.com"],
      local_storage_keys=["Token"],
      screenshot_path=None,
      notes=[],
      captured_at=datetime(2026, 3, 9, 10, 5, 0, tzinfo=UTC),
    ),
  )
  append_page_snapshot(
    events_path,
    PageSnapshot(
      source="fairodds_terminal",
      kind="ui_state_snapshot",
      page="drops",
      url="https://app.fairoddsterminal.com/pro#/drop",
      document_title="FairOdds Terminal",
      body_text="Pinnacle Dropping Odds",
      interactive_snapshot=[],
      links=[],
      inputs={},
      visible_actions=[],
      resource_hosts=["app.fairoddsterminal.com"],
      local_storage_keys=["fo_layout:v12"],
      screenshot_path=None,
      notes=[],
      captured_at=datetime(2026, 3, 9, 10, 6, 0, tzinfo=UTC),
    ),
  )

  lines = events_path.read_text().splitlines()

  assert len(lines) == 2
  assert json.loads(lines[0])["source"] == "rebelbetting_vb"
  assert json.loads(lines[1])["source"] == "fairodds_terminal"
