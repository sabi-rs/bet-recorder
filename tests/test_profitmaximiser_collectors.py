from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json

import pytest

from bet_recorder.capture.run_bundle import initialize_run_bundle
from bet_recorder.sources.profitmaximiser import (
  ProfitMaximiserPageCapture,
  capture_profitmaximiser_page,
)


def test_capture_profitmaximiser_training_page_writes_training_snapshot(
  tmp_path: Path,
) -> None:
  bundle = initialize_run_bundle(
    source="profitmaximiser_members",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 12, 30, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  capture_profitmaximiser_page(
    bundle,
    ProfitMaximiserPageCapture(
      page="training",
      url="https://profitmaximiser.co.uk/members/pages/training",
      document_title="Training",
      body_text="TRAINING",
      interactive_snapshot=[{"tag": "A", "text": "Bookmaker New Account Offers"}],
      links=["https://profitmaximiser.co.uk/members/pages/bookmakeroffers"],
      inputs={},
      visible_actions=["Bookmaker New Account Offers"],
      resource_hosts=["profitmaximiser.co.uk"],
      local_storage_keys=["search_text_input"],
      screenshot_path="/tmp/training.png",
      notes=[],
      captured_at=datetime(2026, 3, 9, 12, 31, 0, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["source"] == "profitmaximiser_members"
  assert event["kind"] == "training_page_snapshot"
  assert event["page"] == "training"


def test_capture_profitmaximiser_calculator_page_writes_calculator_snapshot(
  tmp_path: Path,
) -> None:
  bundle = initialize_run_bundle(
    source="profitmaximiser_members",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 12, 40, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  capture_profitmaximiser_page(
    bundle,
    ProfitMaximiserPageCapture(
      page="calculator",
      url="https://profitmaximiser.co.uk/members/user/mb_calculator",
      document_title="Profit Maximiser",
      body_text="MATCHED BETTING CALCULATOR",
      interactive_snapshot=[],
      links=[],
      inputs={"Bet Amount": "10"},
      visible_actions=["Calculate"],
      resource_hosts=["profitmaximiser.co.uk"],
      local_storage_keys=["options"],
      screenshot_path=None,
      notes=[],
      captured_at=datetime(2026, 3, 9, 12, 41, 0, tzinfo=UTC),
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["kind"] == "calculator_snapshot"
  assert event["page"] == "calculator"


def test_capture_profitmaximiser_page_rejects_unknown_pages(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="profitmaximiser_members",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 12, 50, 0, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  with pytest.raises(ValueError, match="Unsupported ProfitMaximiser page"):
    capture_profitmaximiser_page(
      bundle,
      ProfitMaximiserPageCapture(
        page="unknown",
        url="https://profitmaximiser.co.uk/members/unknown",
        document_title="Profit Maximiser",
        body_text="",
        interactive_snapshot=[],
        links=[],
        inputs={},
        visible_actions=[],
        resource_hosts=[],
        local_storage_keys=[],
        screenshot_path=None,
        notes=[],
        captured_at=datetime(2026, 3, 9, 12, 51, 0, tzinfo=UTC),
      ),
    )
