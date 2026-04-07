from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.cdp import (  # noqa: E402
    DebugTarget,
    list_debug_targets,
    _select_default_execution_context_id,
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


def test_list_debug_targets_can_include_iframe_targets() -> None:
    def fetch_text(url: str) -> str:
        assert url == "http://127.0.0.1:9222/json/list"
        return json.dumps(
            [
                {
                    "id": "page-1",
                    "type": "page",
                    "title": "Casino",
                    "url": "https://games.example.com/casino",
                    "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/page-1",
                },
                {
                    "id": "frame-1",
                    "type": "iframe",
                    "title": "Blackjack",
                    "url": "https://games.example.com/blackjack",
                    "webSocketDebuggerUrl": "ws://127.0.0.1:9222/devtools/page/frame-1",
                },
            ],
        )

    targets = list_debug_targets(
        fetch_text=fetch_text,
        target_types=("page", "iframe"),
    )

    assert targets == [
        DebugTarget(
            target_id="page-1",
            target_type="page",
            title="Casino",
            url="https://games.example.com/casino",
            websocket_debugger_url="ws://127.0.0.1:9222/devtools/page/page-1",
        ),
        DebugTarget(
            target_id="frame-1",
            target_type="iframe",
            title="Blackjack",
            url="https://games.example.com/blackjack",
            websocket_debugger_url="ws://127.0.0.1:9222/devtools/page/frame-1",
        ),
    ]


def test_select_default_execution_context_id_prefers_matching_frame() -> None:
    context_id = _select_default_execution_context_id(
        events=[
            {
                "method": "Runtime.executionContextCreated",
                "params": {
                    "context": {
                        "id": 11,
                        "auxData": {
                            "frameId": "other-frame",
                            "isDefault": True,
                        },
                    },
                },
            },
            {
                "method": "Runtime.executionContextCreated",
                "params": {
                    "context": {
                        "id": 27,
                        "auxData": {
                            "frameId": "blackjack-frame",
                            "isDefault": False,
                        },
                    },
                },
            },
            {
                "method": "Runtime.executionContextCreated",
                "params": {
                    "context": {
                        "id": 33,
                        "auxData": {
                            "frameId": "blackjack-frame",
                            "isDefault": True,
                        },
                    },
                },
            },
        ],
        frame_id="blackjack-frame",
    )

    assert context_id == 33
