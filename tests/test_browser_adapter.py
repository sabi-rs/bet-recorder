from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.adapter import (  # noqa: E402
  build_page_payload,
  build_transport_payload,
)
from bet_recorder.browser.models import BrowserScreenshot  # noqa: E402


def test_build_page_payload_matches_cli_contract() -> None:
  payload = build_page_payload(
    captured_at=datetime(2026, 3, 9, 10, 16, tzinfo=timezone.utc),
    page="dashboard",
    url="https://vb.rebelbetting.com/",
    document_title="Value betting by RebelBetting",
    body_text="25 value bets",
    interactive_snapshot=[{"tag": "A", "text": "Filters"}],
    links=["https://vb.rebelbetting.com/filters"],
    inputs={"search": ""},
    visible_actions=["Filters"],
    resource_hosts=["vb.rebelbetting.com", "cdn.rebelbetting.com", "vb.rebelbetting.com"],
    local_storage_keys=["Token", "Bookmakers", "Token"],
    screenshot=BrowserScreenshot(
      page="dashboard",
      path="/tmp/dashboard.png",
      mime_type="image/png",
      width=1440,
      height=900,
    ),
    notes=["trial-mode"],
  )

  assert payload == {
    "captured_at": "2026-03-09T10:16:00Z",
    "page": "dashboard",
    "url": "https://vb.rebelbetting.com/",
    "document_title": "Value betting by RebelBetting",
    "body_text": "25 value bets",
    "interactive_snapshot": [{"tag": "A", "text": "Filters"}],
    "links": ["https://vb.rebelbetting.com/filters"],
    "inputs": {"search": ""},
    "visible_actions": ["Filters"],
    "resource_hosts": ["vb.rebelbetting.com", "cdn.rebelbetting.com"],
    "local_storage_keys": ["Token", "Bookmakers"],
    "screenshot_path": "/tmp/dashboard.png",
    "notes": ["trial-mode"],
  }


def test_build_transport_payload_preserves_metadata() -> None:
  payload = build_transport_payload(
    captured_at=datetime(2026, 3, 9, 10, 16, 30, tzinfo=timezone.utc),
    event_type="ws_handshake_request",
    headers={"Cookie": "abc=123"},
    preview='{"protocol":"blazorpack","version":1}',
    metadata={
      "url": "wss://vb.rebelbetting.com/_blazor?id=123",
      "source": "rebelbetting_vb",
    },
  )

  assert payload == {
    "captured_at": "2026-03-09T10:16:30Z",
    "type": "ws_handshake_request",
    "headers": {"Cookie": "abc=123"},
    "preview": '{"protocol":"blazorpack","version":1}',
    "metadata": {
      "url": "wss://vb.rebelbetting.com/_blazor?id=123",
      "source": "rebelbetting_vb",
    },
  }
