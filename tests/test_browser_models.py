from datetime import datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.models import (  # noqa: E402
  BrowserPageState,
  BrowserScreenshot,
  BrowserTransportEvent,
)


def test_browser_page_state_to_payload_matches_manual_contract() -> None:
  state = BrowserPageState(
    captured_at=datetime(2026, 3, 9, 10, 16, tzinfo=timezone.utc),
    page="dashboard",
    url="https://vb.rebelbetting.com/",
    document_title="Value betting by RebelBetting",
    body_text="25 value bets",
    interactive_snapshot=[{"tag": "A", "text": "Filters"}],
    links=[
      "https://vb.rebelbetting.com/filters",
      "https://vb.rebelbetting.com/filters",
    ],
    inputs={"search": ""},
    visible_actions=["Filters"],
    resource_hosts=["vb.rebelbetting.com", "cdn.rebelbetting.com", "vb.rebelbetting.com"],
    local_storage_keys=["Token", "Token", "Bookmakers"],
    screenshot_path="/tmp/dashboard.png",
    notes=["trial-mode"],
  )

  payload = state.to_payload()

  assert payload == {
    "captured_at": "2026-03-09T10:16:00Z",
    "page": "dashboard",
    "url": "https://vb.rebelbetting.com/",
    "document_title": "Value betting by RebelBetting",
    "body_text": "25 value bets",
    "interactive_snapshot": [{"tag": "A", "text": "Filters"}],
    "links": [
      "https://vb.rebelbetting.com/filters",
      "https://vb.rebelbetting.com/filters",
    ],
    "inputs": {"search": ""},
    "visible_actions": ["Filters"],
    "resource_hosts": ["vb.rebelbetting.com", "cdn.rebelbetting.com"],
    "local_storage_keys": ["Token", "Bookmakers"],
    "screenshot_path": "/tmp/dashboard.png",
    "notes": ["trial-mode"],
  }


def test_browser_screenshot_to_payload_exposes_metadata() -> None:
  screenshot = BrowserScreenshot(
    page="dashboard",
    path="/tmp/dashboard.png",
    mime_type="image/png",
    width=1440,
    height=900,
  )

  assert screenshot.to_payload() == {
    "page": "dashboard",
    "path": "/tmp/dashboard.png",
    "mime_type": "image/png",
    "width": 1440,
    "height": 900,
  }


def test_browser_transport_event_to_payload_preserves_event_fields() -> None:
  event = BrowserTransportEvent(
    captured_at=datetime(2026, 3, 9, 10, 16, 30, tzinfo=timezone.utc),
    event_type="ws_handshake_request",
    headers={"Cookie": "abc=123"},
    preview='{"protocol":"blazorpack","version":1}',
    metadata={"url": "wss://vb.rebelbetting.com/_blazor?id=123"},
  )

  assert event.to_payload() == {
    "captured_at": "2026-03-09T10:16:30Z",
    "type": "ws_handshake_request",
    "headers": {"Cookie": "abc=123"},
    "preview": '{"protocol":"blazorpack","version":1}',
    "metadata": {"url": "wss://vb.rebelbetting.com/_blazor?id=123"},
  }
