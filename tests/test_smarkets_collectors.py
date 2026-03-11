from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.capture.run_bundle import initialize_run_bundle  # noqa: E402
from bet_recorder.sources.smarkets_exchange import (  # noqa: E402
  SmarketsExchangePageCapture,
  capture_smarkets_exchange_page,
)


def test_capture_smarkets_open_positions_page_writes_positions_snapshot(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="smarkets_exchange",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 20, 20, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  capture_smarkets_exchange_page(
    bundle,
    SmarketsExchangePageCapture(
      page="open_positions",
      url="https://smarkets.com/event/44919794",
      document_title="Lazio vs Sassuolo",
      body_text="Sell Draw Sell 1-1 Trade out",
      interactive_snapshot=[],
      links=[],
      inputs={},
      visible_actions=["Trade out"],
      resource_hosts=["smarkets.com"],
      local_storage_keys=["token"],
      screenshot_path="screenshots/open-positions-20260309T202100Z.png",
      notes=[],
      captured_at=datetime(2026, 3, 9, 20, 21, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["source"] == "smarkets_exchange"
  assert event["kind"] == "positions_snapshot"
  assert event["page"] == "open_positions"


def test_capture_smarkets_page_rejects_unknown_pages(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="smarkets_exchange",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 20, 20, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  with pytest.raises(ValueError, match="Unsupported Smarkets page"):
    capture_smarkets_exchange_page(
      bundle,
      SmarketsExchangePageCapture(
        page="unknown",
        url="https://smarkets.com/",
        document_title="Smarkets",
        body_text="",
        interactive_snapshot=[],
        links=[],
        inputs={},
        visible_actions=[],
        resource_hosts=[],
        local_storage_keys=[],
        screenshot_path=None,
        notes=[],
        captured_at=datetime(2026, 3, 9, 20, 22, tzinfo=UTC),
      ),
    )
