from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json

import pytest

from bet_recorder.capture.run_bundle import initialize_run_bundle
from bet_recorder.sources.fairodds_terminal import (
  FairOddsPageCapture,
  capture_fairodds_page,
)


def test_capture_fairodds_drops_page_writes_ui_state_snapshot(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="fairodds_terminal",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 12, 0, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="default",
    transport_capture_enabled=False,
  )

  capture_fairodds_page(
    bundle,
    FairOddsPageCapture(
      page="drops",
      url="https://app.fairoddsterminal.com/pro#/drop",
      document_title="FairOdds Terminal",
      body_text="Pinnacle Dropping Odds",
      interactive_snapshot=[{"tag": "BUTTON", "text": "Watchlists"}],
      links=[],
      inputs={"search": ""},
      visible_actions=["Watchlists", "Widget Settings"],
      resource_hosts=["app.fairoddsterminal.com"],
      local_storage_keys=["fo_layout:v12"],
      screenshot_path="/tmp/drops.png",
      notes=["live"],
      captured_at=datetime(2026, 3, 9, 12, 1, 0, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["source"] == "fairodds_terminal"
  assert event["kind"] == "ui_state_snapshot"
  assert event["page"] == "drops"


def test_capture_fairodds_ev_page_writes_dashboard_snapshot(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="fairodds_terminal",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 12, 10, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="default",
    transport_capture_enabled=False,
  )

  capture_fairodds_page(
    bundle,
    FairOddsPageCapture(
      page="ev",
      url="https://app.fairoddsterminal.com/pro#/drop",
      document_title="FairOdds Terminal",
      body_text="Positive Expected Value (EV) Bets",
      interactive_snapshot=[],
      links=[],
      inputs={},
      visible_actions=["Search event..."],
      resource_hosts=["app.fairoddsterminal.com"],
      local_storage_keys=["value:lastRows:live"],
      screenshot_path=None,
      notes=[],
      captured_at=datetime(2026, 3, 9, 12, 11, 0, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["kind"] == "dashboard_snapshot"
  assert event["page"] == "ev"


def test_capture_fairodds_page_rejects_unknown_pages(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="fairodds_terminal",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 12, 20, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="default",
    transport_capture_enabled=False,
  )

  with pytest.raises(ValueError, match="Unsupported FairOdds page"):
    capture_fairodds_page(
      bundle,
      FairOddsPageCapture(
        page="unknown",
        url="https://app.fairoddsterminal.com/pro#/drop",
        document_title="FairOdds Terminal",
        body_text="",
        interactive_snapshot=[],
        links=[],
        inputs={},
        visible_actions=[],
        resource_hosts=[],
        local_storage_keys=[],
        screenshot_path=None,
        notes=[],
        captured_at=datetime(2026, 3, 9, 12, 21, 0, tzinfo=UTC),
      ),
    )
