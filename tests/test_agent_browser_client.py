from datetime import UTC, datetime
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.agent_browser import (  # noqa: E402
    AgentBrowserClient,
    BODY_TEXT_JS,
    INPUTS_JS,
    LINKS_JS,
    RESOURCE_HOSTS_JS,
    VISIBLE_ACTIONS_JS,
)


class FakeCompletedProcess:
    def __init__(self, stdout: str, returncode: int = 0, stderr: str = "") -> None:
        self.stdout = stdout
        self.returncode = returncode
        self.stderr = stderr


def test_capture_page_state_uses_agent_browser_json_contract() -> None:
    commands: list[list[str]] = []

    def runner(command: list[str]) -> FakeCompletedProcess:
        commands.append(command)
        key = tuple(command[-2:]) if command[-2] != "eval" else ("eval", command[-1])
        responses = {
            ("get", "title"): FakeCompletedProcess(
                '{"success":true,"data":{"title":"Value betting by RebelBetting"},"error":null}',
            ),
            ("get", "url"): FakeCompletedProcess(
                '{"success":true,"data":{"url":"https://vb.rebelbetting.com/"},"error":null}',
            ),
            ("snapshot", "-i"): FakeCompletedProcess(
                '{"success":true,"data":{"refs":{"e1":{"name":"Filters","role":"link"},'
                '"e2":{"name":"Search","role":"textbox"}},"snapshot":"- link \\"Filters\\" [ref=e1]"},'
                '"error":null}',
            ),
            ("eval", BODY_TEXT_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":"25 value bets"},"error":null}',
            ),
            ("eval", LINKS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":["https://vb.rebelbetting.com/filters"]},"error":null}',
            ),
            ("eval", INPUTS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":{"search":""}},"error":null}',
            ),
            ("eval", VISIBLE_ACTIONS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":["Filters","Log out"]},"error":null}',
            ),
            ("eval", RESOURCE_HOSTS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":["vb.rebelbetting.com","cdn.rebelbetting.com"]},"error":null}',
            ),
            ("storage", "local"): FakeCompletedProcess(
                '{"success":true,"data":{"data":{"Token":"abc","Bookmakers":"{}"}},"error":null}',
            ),
        }
        response = responses.get(key)
        if response is None:
            raise AssertionError(f"Unexpected command: {command}")
        return response

    client = AgentBrowserClient(session="helium-copy", runner=runner)

    state = client.capture_page_state(
        page="dashboard",
        captured_at=datetime(2026, 3, 9, 10, 16, tzinfo=UTC),
        screenshot_path="screenshots/dashboard-20260309T101600Z.png",
        notes=["trial-mode"],
    )

    assert state.to_payload() == {
        "captured_at": "2026-03-09T10:16:00Z",
        "page": "dashboard",
        "url": "https://vb.rebelbetting.com/",
        "document_title": "Value betting by RebelBetting",
        "body_text": "25 value bets",
        "interactive_snapshot": [
            {"ref": "e1", "role": "link", "name": "Filters"},
            {"ref": "e2", "role": "textbox", "name": "Search"},
        ],
        "links": ["https://vb.rebelbetting.com/filters"],
        "inputs": {"search": ""},
        "visible_actions": ["Filters", "Log out"],
        "resource_hosts": ["vb.rebelbetting.com", "cdn.rebelbetting.com"],
        "local_storage_keys": ["Token", "Bookmakers"],
        "screenshot_path": "screenshots/dashboard-20260309T101600Z.png",
        "notes": ["trial-mode"],
    }
    assert commands[0][:4] == ["agent-browser", "--session", "helium-copy", "--json"]


def test_capture_screenshot_writes_requested_target_path(tmp_path: Path) -> None:
    requested_commands: list[list[str]] = []

    def runner(command: list[str]) -> FakeCompletedProcess:
        requested_commands.append(command)
        return FakeCompletedProcess(
            stdout='{"success":true,"data":{"path":"'
            + str(tmp_path / "page.png")
            + '"},"error":null}',
        )

    client = AgentBrowserClient(session="helium-copy", runner=runner)

    screenshot_path = client.capture_screenshot(tmp_path / "page.png")

    assert screenshot_path == tmp_path / "page.png"
    assert requested_commands == [
        [
            "agent-browser",
            "--session",
            "helium-copy",
            "--json",
            "screenshot",
            str(tmp_path / "page.png"),
        ],
    ]


def test_open_url_uses_profile_argument(tmp_path: Path) -> None:
    requested_commands: list[list[str]] = []

    def runner(command: list[str]) -> FakeCompletedProcess:
        requested_commands.append(command)
        return FakeCompletedProcess('{"success":true,"data":{},"error":null}')

    client = AgentBrowserClient(profile_path=tmp_path / "owned-profile", runner=runner)

    client.open_url("https://smarkets.com/portfolio/")

    assert requested_commands == [
        [
            "agent-browser",
            "--profile",
            str(tmp_path / "owned-profile"),
            "--json",
            "open",
            "https://smarkets.com/portfolio/",
        ]
    ]


def test_click_and_fill_use_agent_browser_json_contract() -> None:
    requested_commands: list[list[str]] = []

    def runner(command: list[str]) -> FakeCompletedProcess:
        requested_commands.append(command)
        return FakeCompletedProcess('{"success":true,"data":{},"error":null}')

    client = AgentBrowserClient(session="helium-copy", runner=runner)

    client.fill('input[type="email"]', "user@example.com")
    client.click('button[type="submit"]')

    assert requested_commands == [
        [
            "agent-browser",
            "--session",
            "helium-copy",
            "--json",
            "fill",
            'input[type="email"]',
            "user@example.com",
        ],
        [
            "agent-browser",
            "--session",
            "helium-copy",
            "--json",
            "click",
            'button[type="submit"]',
        ],
    ]


def test_capture_page_state_tolerates_local_storage_access_denied() -> None:
    def runner(command: list[str]) -> FakeCompletedProcess:
        key = tuple(command[-2:]) if command[-2] != "eval" else ("eval", command[-1])
        responses = {
            ("get", "title"): FakeCompletedProcess(
                '{"success":true,"data":{"title":"Smarkets"},"error":null}',
            ),
            ("get", "url"): FakeCompletedProcess(
                '{"success":true,"data":{"url":"https://smarkets.com/"},"error":null}',
            ),
            ("snapshot", "-i"): FakeCompletedProcess(
                '{"success":true,"data":{"refs":{},"snapshot":""},"error":null}',
            ),
            ("eval", BODY_TEXT_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":"Open positions"},"error":null}',
            ),
            ("eval", LINKS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":[]},"error":null}',
            ),
            ("eval", INPUTS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":{}},"error":null}',
            ),
            ("eval", VISIBLE_ACTIONS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":["Trade out"]},"error":null}',
            ),
            ("eval", RESOURCE_HOSTS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":["smarkets.com"]},"error":null}',
            ),
            ("storage", "local"): FakeCompletedProcess(
                '{"success":false,"data":null,"error":"page.evaluate: SecurityError: Failed to read '
                'the \\"localStorage\\" property from \\"Window\\""}',
            ),
        }
        response = responses.get(key)
        if response is None:
            raise AssertionError(f"Unexpected command: {command}")
        return response

    client = AgentBrowserClient(session="helium-copy", runner=runner)

    state = client.capture_page_state(
        page="open_positions",
        captured_at=datetime(2026, 3, 11, 18, 30, tzinfo=UTC),
        screenshot_path=None,
        notes=["watcher-loop"],
    )

    assert state.local_storage_keys == []
    assert state.page == "open_positions"
    assert state.visible_actions == ["Trade out"]


def test_capture_page_state_includes_bets_storage_metadata_when_available() -> None:
    def runner(command: list[str]) -> FakeCompletedProcess:
        key = tuple(command[-2:]) if command[-2] != "eval" else ("eval", command[-1])
        responses = {
            ("get", "title"): FakeCompletedProcess(
                '{"success":true,"data":{"title":"Bets"},"error":null}',
            ),
            ("get", "url"): FakeCompletedProcess(
                '{"success":true,"data":{"url":"https://app.fairoddsterminal.com/bets"},"error":null}',
            ),
            ("snapshot", "-i"): FakeCompletedProcess(
                '{"success":true,"data":{"refs":{},"snapshot":""},"error":null}',
            ),
            ("eval", BODY_TEXT_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":"3 bets"},"error":null}',
            ),
            ("eval", LINKS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":[]},"error":null}',
            ),
            ("eval", INPUTS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":{}},"error":null}',
            ),
            ("eval", VISIBLE_ACTIONS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":["Filters"]},"error":null}',
            ),
            ("eval", RESOURCE_HOSTS_JS): FakeCompletedProcess(
                '{"success":true,"data":{"result":["app.fairoddsterminal.com"]},"error":null}',
            ),
            ("storage", "local"): FakeCompletedProcess(
                '{"success":true,"data":{"data":{'
                '"widget_cache_bets_data":"{\\"data\\":{\\"bets\\":[{\\"id\\":1}]},\\"timestamp\\":\\"2026-03-09T20:21:22Z\\",\\"version\\":3}",'
                '"fo_colwidths:bets:v3":"{\\"stake\\":120}"'
                '}},"error":null}',
            ),
        }
        response = responses.get(key)
        if response is None:
            raise AssertionError(f"Unexpected command: {command}")
        return response

    client = AgentBrowserClient(session="helium-copy", runner=runner)

    state = client.capture_page_state(
        page="bets",
        captured_at=datetime(2026, 3, 25, 11, 0, tzinfo=UTC),
        screenshot_path=None,
        notes=[],
    )

    assert state.metadata["page_kind"] == "bets"
    assert state.metadata["trackerCacheProbe"]["betCount"] == 1
    assert set(state.metadata["storage_snapshot"].keys()) == {
        "widget_cache_bets_data",
        "fo_colwidths:bets:v3",
    }


def test_resource_hosts_script_matches_verified_agent_browser_expression() -> None:
    assert RESOURCE_HOSTS_JS == (
        "Array.from(new Set(performance.getEntriesByType('resource').map((entry) => {"
        "try { return new URL(entry.name).hostname; } catch { return null; }"
        "}).filter(Boolean)))"
    )
