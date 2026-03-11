from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.capture.run_bundle import initialize_run_bundle  # noqa: E402
from bet_recorder.sources.betway_uk import BetwayPageCapture, capture_betway_page  # noqa: E402


def test_capture_betway_betslip_page_writes_betslip_snapshot(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="betway_uk",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 20, 10, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  capture_betway_page(
    bundle,
    BetwayPageCapture(
      page="betslip",
      url="https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
      document_title="Betway Betslip",
      body_text="4 Bet Slip Odds 7/1 Almeria Under 2.5 BTTS No Almeria over 1.5",
      interactive_snapshot=[],
      links=[],
      inputs={"stake": "10"},
      visible_actions=["Place Bet"],
      resource_hosts=["betway.com"],
      local_storage_keys=["theme"],
      screenshot_path="screenshots/betslip-20260309T201100Z.png",
      notes=[],
      captured_at=datetime(2026, 3, 9, 20, 11, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["source"] == "betway_uk"
  assert event["kind"] == "betslip_snapshot"
  assert event["page"] == "betslip"


def test_capture_betway_page_rejects_unknown_pages(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="betway_uk",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 20, 10, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  with pytest.raises(ValueError, match="Unsupported Betway page"):
    capture_betway_page(
      bundle,
      BetwayPageCapture(
        page="unknown",
        url="https://betway.com/gb/en/sports/",
        document_title="Betway",
        body_text="",
        interactive_snapshot=[],
        links=[],
        inputs={},
        visible_actions=[],
        resource_hosts=[],
        local_storage_keys=[],
        screenshot_path=None,
        notes=[],
        captured_at=datetime(2026, 3, 9, 20, 12, tzinfo=UTC),
      ),
    )
