from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.cdp import (  # noqa: E402
  DebugTarget,
  list_debug_targets,
  select_debug_target,
)


def test_list_debug_targets_parses_devtools_endpoint() -> None:
  def fetch_text(url: str) -> str:
    assert url == "http://127.0.0.1:9222/json/list"
    return json.dumps(
      [
        {
          "id": "page-1",
          "type": "page",
          "title": "Smarkets",
          "url": "https://smarkets.com/event/123",
          "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/page-1",
        },
        {
          "id": "worker-1",
          "type": "service_worker",
          "title": "worker",
          "url": "https://smarkets.com/sw.js",
          "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/worker-1",
        },
      ],
    )

  targets = list_debug_targets(fetch_text=fetch_text)

  assert targets == [
    DebugTarget(
      target_id="page-1",
      target_type="page",
      title="Smarkets",
      url="https://smarkets.com/event/123",
      websocket_debugger_url="ws://127.0.0.1:9222/devtools/page/page-1",
    ),
  ]


def test_select_debug_target_prefers_explicit_url_fragment() -> None:
  targets = [
    DebugTarget(
      target_id="betway",
      target_type="page",
      title="Betway",
      url="https://betway.com/gb/en/sports",
      websocket_debugger_url="ws://127.0.0.1:9222/devtools/page/betway",
    ),
    DebugTarget(
      target_id="smarkets",
      target_type="page",
      title="Smarkets",
      url="https://smarkets.com/event/123",
      websocket_debugger_url="ws://127.0.0.1:9222/devtools/page/smarkets",
    ),
  ]

  target = select_debug_target(
    source="smarkets_exchange",
    targets=targets,
    url_contains="smarkets.com",
  )

  assert target == targets[1]
