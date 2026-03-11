from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import re


def write_screenshot(
  *,
  screenshots_dir: Path,
  page: str,
  captured_at: datetime,
  image_bytes: bytes,
  mime_type: str,
) -> str:
  filename = (
    f"{_slugify(page)}-"
    f"{captured_at.astimezone(UTC).strftime('%Y%m%dT%H%M%SZ')}"
    f"{_extension_for_mime_type(mime_type)}"
  )
  output_path = screenshots_dir / filename
  output_path.write_bytes(image_bytes)
  return str(Path("screenshots") / filename)


def _slugify(value: str) -> str:
  slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
  return slug or "page"


def _extension_for_mime_type(mime_type: str) -> str:
  if mime_type == "image/jpeg":
    return ".jpg"
  if mime_type == "image/png":
    return ".png"
  raise ValueError(f"Unsupported screenshot mime type: {mime_type}")
