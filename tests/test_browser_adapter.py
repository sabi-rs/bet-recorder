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
    page="market",
    url="https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
    document_title="Betway event",
    body_text="Correct Score",
    interactive_snapshot=[{"tag": "BUTTON", "text": "Add to Bet Slip"}],
    links=["https://betway.com/gb/en/sports"],
    inputs={"stake": ""},
    visible_actions=["Add to Bet Slip"],
    resource_hosts=["betway.com", "cdn.betway.com", "betway.com"],
    local_storage_keys=["theme", "betslip", "theme"],
    screenshot=BrowserScreenshot(
      page="market",
      path="/tmp/market.png",
      mime_type="image/png",
      width=1440,
      height=900,
    ),
    notes=["free-bet"],
  )

  assert payload == {
    "captured_at": "2026-03-09T10:16:00Z",
    "page": "market",
    "url": "https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
    "document_title": "Betway event",
    "body_text": "Correct Score",
    "interactive_snapshot": [{"tag": "BUTTON", "text": "Add to Bet Slip"}],
    "links": ["https://betway.com/gb/en/sports"],
    "inputs": {"stake": ""},
    "visible_actions": ["Add to Bet Slip"],
    "resource_hosts": ["betway.com", "cdn.betway.com"],
    "local_storage_keys": ["theme", "betslip"],
    "screenshot_path": "/tmp/market.png",
    "notes": ["free-bet"],
  }


def test_build_transport_payload_preserves_metadata() -> None:
  payload = build_transport_payload(
    captured_at=datetime(2026, 3, 9, 10, 16, 30, tzinfo=timezone.utc),
    event_type="ws_handshake_request",
    headers={"Cookie": "abc=123"},
    preview='{"protocol":"json","version":1}',
    metadata={
      "url": "wss://smarkets.com/ws?id=123",
      "source": "smarkets_exchange",
    },
  )

  assert payload == {
    "captured_at": "2026-03-09T10:16:30Z",
    "type": "ws_handshake_request",
    "headers": {"Cookie": "abc=123"},
    "preview": '{"protocol":"json","version":1}',
    "metadata": {
      "url": "wss://smarkets.com/ws?id=123",
      "source": "smarkets_exchange",
    },
  }
