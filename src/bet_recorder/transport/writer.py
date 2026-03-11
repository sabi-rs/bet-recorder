from __future__ import annotations

from pathlib import Path
import json
import re

JWT_RE = re.compile(r"eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+")


def append_transport_event(transport_path: Path, event: dict) -> None:
  sanitized = _sanitize_value(event)
  with transport_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(sanitized) + "\n")


def _sanitize_value(value):
  if isinstance(value, dict):
    return {k: _sanitize_header(k, v) for k, v in value.items()}
  if isinstance(value, list):
    return [_sanitize_value(item) for item in value]
  if isinstance(value, str):
    return _sanitize_text(value)
  return value


def _sanitize_header(key: str, value):
  if key.lower() in {"cookie", "set-cookie", "authorization"}:
    return "[REDACTED]"
  return _sanitize_value(value)


def _sanitize_text(text: str) -> str:
  text = JWT_RE.sub("[REDACTED]", text)
  text = re.sub(r'("access_token"\s*:\s*")([^"]+)(")', r"\1[REDACTED]\3", text, flags=re.I)
  text = re.sub(r'("refresh_token"\s*:\s*")([^"]+)(")', r"\1[REDACTED]\3", text, flags=re.I)
  text = re.sub(r"(Bearer\s+)[A-Za-z0-9._~-]+", r"\1[REDACTED]", text, flags=re.I)
  return text
