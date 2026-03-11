from __future__ import annotations

from pathlib import Path
import json

from bet_recorder.transport.writer import append_transport_event


def test_append_transport_event_redacts_sensitive_fields(tmp_path: Path) -> None:
  transport_path = tmp_path / "transport.jsonl"

  append_transport_event(
    transport_path,
    {
      "type": "ws_handshake_request",
      "headers": {
        "Cookie": "abc=123",
        "Authorization": "Bearer secret-token",
      },
      "preview": '{"access_token":"abc","refresh_token":"def","jwt":"eyJabc.def.ghi"}',
    },
  )

  event = json.loads(transport_path.read_text())

  assert event["headers"]["Cookie"] == "[REDACTED]"
  assert event["headers"]["Authorization"] == "[REDACTED]"
  assert "[REDACTED]" in event["preview"]
  assert "secret-token" not in event["preview"]


def test_append_transport_event_appends_jsonl(tmp_path: Path) -> None:
  transport_path = tmp_path / "transport.jsonl"

  append_transport_event(transport_path, {"type": "a", "preview": "one"})
  append_transport_event(transport_path, {"type": "b", "preview": "two"})

  lines = transport_path.read_text().splitlines()

  assert len(lines) == 2
  assert json.loads(lines[0])["type"] == "a"
  assert json.loads(lines[1])["type"] == "b"
