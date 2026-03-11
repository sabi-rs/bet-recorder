from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json

import pytest

from bet_recorder.capture.run_bundle import initialize_run_bundle
from bet_recorder.sources.rebelbetting_rb import RbPageCapture, capture_rb_page


def test_capture_rb_dashboard_page_writes_dashboard_snapshot(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="rebelbetting_rb",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 11, 30, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  capture_rb_page(
    bundle,
    RbPageCapture(
      page="dashboard",
      url="https://rb.rebelbetting.com/",
      document_title="Sure betting by RebelBetting",
      body_text="56 sure bets",
      interactive_snapshot=[{"tag": "A", "text": "Filters"}],
      links=["https://rb.rebelbetting.com/filters"],
      inputs={"search": ""},
      visible_actions=["Filters"],
      resource_hosts=["rb.rebelbetting.com"],
      local_storage_keys=["Token"],
      screenshot_path="/tmp/rb-dashboard.png",
      notes=["trial-mode"],
      captured_at=datetime(2026, 3, 9, 11, 31, 0, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["source"] == "rebelbetting_rb"
  assert event["kind"] == "dashboard_snapshot"
  assert event["page"] == "dashboard"


def test_capture_rb_reports_page_writes_reports_snapshot(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="rebelbetting_rb",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 11, 40, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  capture_rb_page(
    bundle,
    RbPageCapture(
      page="reports",
      url="https://rb.rebelbetting.com/reports",
      document_title="Sure betting reports",
      body_text="Bets per Bookmaker",
      interactive_snapshot=[],
      links=[],
      inputs={},
      visible_actions=["Download CSV"],
      resource_hosts=["rb.rebelbetting.com"],
      local_storage_keys=["Token"],
      screenshot_path=None,
      notes=[],
      captured_at=datetime(2026, 3, 9, 11, 41, 0, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["kind"] == "reports_snapshot"
  assert event["page"] == "reports"


def test_capture_rb_page_rejects_unknown_pages(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="rebelbetting_rb",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 11, 50, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  with pytest.raises(ValueError, match="Unsupported RB page"):
    capture_rb_page(
      bundle,
      RbPageCapture(
        page="unknown",
        url="https://rb.rebelbetting.com/unknown",
        document_title="Unknown",
        body_text="",
        interactive_snapshot=[],
        links=[],
        inputs={},
        visible_actions=[],
        resource_hosts=[],
        local_storage_keys=[],
        screenshot_path=None,
        notes=[],
        captured_at=datetime(2026, 3, 9, 11, 51, 0, tzinfo=UTC),
      ),
    )
