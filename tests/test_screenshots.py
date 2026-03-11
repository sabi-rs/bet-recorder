from datetime import UTC, datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.capture.run_bundle import initialize_run_bundle  # noqa: E402
from bet_recorder.capture.screenshots import write_screenshot  # noqa: E402


def test_write_screenshot_persists_bundle_owned_file_and_relative_path(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="rebelbetting_vb",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 10, 15, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  relative_path = write_screenshot(
    screenshots_dir=bundle.screenshots_dir,
    page="Dashboard View",
    captured_at=datetime(2026, 3, 9, 10, 16, tzinfo=UTC),
    image_bytes=b"png-bytes",
    mime_type="image/png",
  )

  assert relative_path == "screenshots/dashboard-view-20260309T101600Z.png"
  assert (bundle.run_dir / relative_path).read_bytes() == b"png-bytes"


def test_write_screenshot_uses_jpeg_extension_for_jpeg_images(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="profitmaximiser_members",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 10, 15, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  relative_path = write_screenshot(
    screenshots_dir=bundle.screenshots_dir,
    page="offer_table",
    captured_at=datetime(2026, 3, 9, 10, 16, 30, tzinfo=UTC),
    image_bytes=b"jpeg-bytes",
    mime_type="image/jpeg",
  )

  assert relative_path == "screenshots/offer-table-20260309T101630Z.jpg"
  assert (bundle.run_dir / relative_path).read_bytes() == b"jpeg-bytes"
