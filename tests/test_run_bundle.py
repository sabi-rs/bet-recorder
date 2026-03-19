from __future__ import annotations

from datetime import UTC, datetime
import json

from bet_recorder.capture.run_bundle import finalize_run_bundle, initialize_run_bundle


def test_initialize_run_bundle_creates_expected_layout(tmp_path) -> None:
  started_at = datetime(2026, 3, 9, 7, 15, 30, tzinfo=UTC)

  bundle = initialize_run_bundle(
    source="betway_uk",
    root_dir=tmp_path,
    started_at=started_at,
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=True,
  )

  assert bundle.run_dir == (
    tmp_path
    / "captures"
    / "betway_uk"
    / "2026"
    / "2026-03-09"
    / "run-20260309T071530Z"
  )
  assert bundle.events_path == bundle.run_dir / "events.jsonl"
  assert bundle.metadata_path == bundle.run_dir / "metadata.json"
  assert bundle.transport_path == bundle.run_dir / "transport.jsonl"
  assert bundle.screenshots_dir == bundle.run_dir / "screenshots"

  assert bundle.run_dir.is_dir()
  assert bundle.screenshots_dir.is_dir()
  assert bundle.events_path.is_file()
  assert bundle.transport_path.is_file()
  assert bundle.metadata_path.is_file()


def test_initialize_run_bundle_writes_run_metadata(tmp_path) -> None:
  started_at = datetime(2026, 3, 9, 8, 0, 0, tzinfo=UTC)

  bundle = initialize_run_bundle(
    source="bet365",
    root_dir=tmp_path,
    started_at=started_at,
    collector_version="test-v2",
    browser_profile_used="default",
    transport_capture_enabled=False,
  )

  metadata = json.loads(bundle.metadata_path.read_text())

  assert metadata == {
    "source": "bet365",
    "started_at": "2026-03-09T08:00:00Z",
    "ended_at": None,
    "collector_version": "test-v2",
    "browser_profile_used": "default",
    "page_count": 0,
    "screenshot_count": 0,
    "transport_capture_enabled": False,
    "events_path": str(bundle.events_path),
    "transport_path": None,
    "screenshots_dir": str(bundle.screenshots_dir),
  }


def test_initialize_run_bundle_is_idempotent_for_existing_directory(tmp_path) -> None:
  started_at = datetime(2026, 3, 9, 9, 45, 0, tzinfo=UTC)

  first = initialize_run_bundle(
    source="smarkets_exchange",
    root_dir=tmp_path,
    started_at=started_at,
    collector_version="test-v3",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  first.events_path.write_text('{"kind":"seed"}\n')

  second = initialize_run_bundle(
    source="smarkets_exchange",
    root_dir=tmp_path,
    started_at=started_at,
    collector_version="test-v3",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  assert second.run_dir == first.run_dir
  assert second.events_path.read_text() == '{"kind":"seed"}\n'


def test_finalize_run_bundle_updates_metadata_counts(tmp_path) -> None:
  started_at = datetime(2026, 3, 9, 9, 45, 0, tzinfo=UTC)

  bundle = initialize_run_bundle(
    source="smarkets_exchange",
    root_dir=tmp_path,
    started_at=started_at,
    collector_version="test-v4",
    browser_profile_used="helium-copy",
    transport_capture_enabled=True,
  )

  bundle.events_path.write_text('{"kind":"positions_snapshot"}\n{"kind":"watch_plan_snapshot"}\n')
  (bundle.screenshots_dir / "open-positions.png").write_text("png")
  (bundle.screenshots_dir / "history.png").write_text("png")
  bundle.transport_path.write_text('{"kind":"ws_frame_received"}\n')

  finalize_run_bundle(
    bundle,
    ended_at=datetime(2026, 3, 9, 10, 5, 0, tzinfo=UTC),
  )

  metadata = json.loads(bundle.metadata_path.read_text())

  assert metadata["ended_at"] == "2026-03-09T10:05:00Z"
  assert metadata["page_count"] == 2
  assert metadata["screenshot_count"] == 2
  assert metadata["transport_event_count"] == 1
