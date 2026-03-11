from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json

from bet_recorder.capture.file_layout import run_directory


@dataclass(frozen=True)
class RunBundle:
  source: str
  run_dir: Path
  events_path: Path
  metadata_path: Path
  transport_path: Path | None
  screenshots_dir: Path


def initialize_run_bundle(
  *,
  source: str,
  root_dir: Path,
  started_at: datetime,
  collector_version: str,
  browser_profile_used: str,
  transport_capture_enabled: bool,
) -> RunBundle:
  normalized_started_at = started_at.astimezone(UTC)
  bundle_dir = run_directory(root_dir, source, normalized_started_at)
  screenshots_dir = bundle_dir / "screenshots"
  events_path = bundle_dir / "events.jsonl"
  metadata_path = bundle_dir / "metadata.json"
  transport_path = bundle_dir / "transport.jsonl" if transport_capture_enabled else None

  bundle_dir.mkdir(parents=True, exist_ok=True)
  screenshots_dir.mkdir(parents=True, exist_ok=True)
  events_path.touch(exist_ok=True)
  if transport_path is not None:
    transport_path.touch(exist_ok=True)

  metadata = {
    "source": source,
    "started_at": normalized_started_at.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "ended_at": None,
    "collector_version": collector_version,
    "browser_profile_used": browser_profile_used,
    "page_count": 0,
    "screenshot_count": 0,
    "transport_capture_enabled": transport_capture_enabled,
    "events_path": str(events_path),
    "transport_path": str(transport_path) if transport_path is not None else None,
    "screenshots_dir": str(screenshots_dir),
  }
  metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")

  return RunBundle(
    source=source,
    run_dir=bundle_dir,
    events_path=events_path,
    metadata_path=metadata_path,
    transport_path=transport_path,
    screenshots_dir=screenshots_dir,
  )


def load_run_bundle(*, source: str, run_dir: Path) -> RunBundle:
  return RunBundle(
    source=source,
    run_dir=run_dir,
    events_path=run_dir / "events.jsonl",
    metadata_path=run_dir / "metadata.json",
    transport_path=(run_dir / "transport.jsonl") if (run_dir / "transport.jsonl").exists() else None,
    screenshots_dir=run_dir / "screenshots",
  )


def finalize_run_bundle(bundle: RunBundle, *, ended_at: datetime) -> None:
  metadata = json.loads(bundle.metadata_path.read_text())
  metadata["ended_at"] = ended_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
  metadata["page_count"] = _line_count(bundle.events_path)
  metadata["screenshot_count"] = len(list(bundle.screenshots_dir.iterdir()))
  metadata["transport_event_count"] = (
    _line_count(bundle.transport_path) if bundle.transport_path is not None else 0
  )
  bundle.metadata_path.write_text(json.dumps(metadata, indent=2) + "\n")


def _line_count(path: Path) -> int:
  return sum(1 for _ in path.open(encoding="utf-8"))
