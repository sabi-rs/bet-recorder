from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json

import pytest

from bet_recorder.capture.run_bundle import initialize_run_bundle
from bet_recorder.sources.rebelbetting_vb import VbPageCapture, capture_vb_page


def test_capture_vb_dashboard_page_writes_dashboard_snapshot(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="rebelbetting_vb",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 11, 0, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  capture_vb_page(
    bundle,
    VbPageCapture(
      page="dashboard",
      url="https://vb.rebelbetting.com/",
      document_title="Value betting by RebelBetting",
      body_text="25 value bets",
      interactive_snapshot=[{"tag": "A", "text": "Filters"}],
      links=["https://vb.rebelbetting.com/filters"],
      inputs={"search": ""},
      visible_actions=["Filters"],
      resource_hosts=["vb.rebelbetting.com"],
      local_storage_keys=["Token"],
      screenshot_path="/tmp/dashboard.png",
      notes=["trial-mode"],
      captured_at=datetime(2026, 3, 9, 11, 1, 0, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["source"] == "rebelbetting_vb"
  assert event["kind"] == "dashboard_snapshot"
  assert event["page"] == "dashboard"
  assert event["document_title"] == "Value betting by RebelBetting"


def test_capture_vb_filters_page_writes_filters_snapshot(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="rebelbetting_vb",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 11, 10, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  capture_vb_page(
    bundle,
    VbPageCapture(
      page="filters",
      url="https://vb.rebelbetting.com/filters",
      document_title="Filters - Value betting by RebelBetting",
      body_text="Default Filter",
      interactive_snapshot=[],
      links=[],
      inputs={},
      visible_actions=["Save", "Duplicate"],
      resource_hosts=["vb.rebelbetting.com"],
      local_storage_keys=["Token"],
      screenshot_path=None,
      notes=[],
      captured_at=datetime(2026, 3, 9, 11, 11, 0, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["kind"] == "filters_snapshot"
  assert event["page"] == "filters"


def test_capture_vb_page_rejects_unknown_pages(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="rebelbetting_vb",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 11, 20, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  with pytest.raises(ValueError, match="Unsupported VB page"):
    capture_vb_page(
      bundle,
      VbPageCapture(
        page="tutorial",
        url="https://vb.rebelbetting.com/tutorial",
        document_title="Tutorial",
        body_text="",
        interactive_snapshot=[],
        links=[],
        inputs={},
        visible_actions=[],
        resource_hosts=[],
        local_storage_keys=[],
        screenshot_path=None,
        notes=[],
        captured_at=datetime(2026, 3, 9, 11, 21, 0, tzinfo=UTC),
      ),
    )
