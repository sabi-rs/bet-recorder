from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import json
import re

JWT_RE = re.compile(r"eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9._-]+\.[a-zA-Z0-9._-]+")
SENSITIVE_TEXT_KEYS = (
  "access_token",
  "refresh_token",
  "id_token",
  "password",
  "passphrase",
  "client_secret",
  "api_key",
  "apikey",
  "session_token",
)


def append_transport_event(transport_path: Path, event: dict) -> None:
  sanitized = _sanitize_value(event)
  with transport_path.open("a", encoding="utf-8") as handle:
    handle.write(json.dumps(sanitized) + "\n")


def append_transport_marker(
  transport_path: Path,
  *,
  action: str,
  phase: str,
  detail: str,
  request_id: str | None = None,
  reference_id: str | None = None,
  metadata: dict | None = None,
) -> None:
  append_transport_event(
    transport_path,
    {
      "captured_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
      "kind": "interaction_marker",
      "action": action,
      "phase": phase,
      "detail": detail,
      "request_id": request_id,
      "reference_id": reference_id,
      "metadata": metadata or {},
    },
  )


def _sanitize_value(value):
  if isinstance(value, dict):
    return {k: _sanitize_header(k, v) for k, v in value.items()}
  if isinstance(value, list):
    return [_sanitize_value(item) for item in value]
  if isinstance(value, str):
    return _sanitize_text(value)
  return value


def _sanitize_header(key: str, value):
  lowered = key.lower()
  if lowered in {
    "cookie",
    "set-cookie",
    "authorization",
    "proxy-authorization",
    "x-api-key",
    "x-auth-token",
  }:
    return "[REDACTED]"
  return _sanitize_value(value)


def _sanitize_text(text: str) -> str:
  text = JWT_RE.sub("[REDACTED]", text)
  for key in SENSITIVE_TEXT_KEYS:
    text = re.sub(
      rf'("{re.escape(key)}"\s*:\s*")([^"]+)(")',
      r"\1[REDACTED]\3",
      text,
      flags=re.I,
    )
  query_key_pattern = "|".join(re.escape(key) for key in SENSITIVE_TEXT_KEYS)
  text = re.sub(
    rf"([?&](?:{query_key_pattern})=)([^&#\s]+)",
    r"\1[REDACTED]",
    text,
    flags=re.I,
  )
  text = re.sub(r"(Bearer\s+)[A-Za-z0-9._~-]+", r"\1[REDACTED]", text, flags=re.I)
  return text
