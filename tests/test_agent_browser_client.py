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
      stdout='{"success":true,"data":{"path":"' + str(tmp_path / "page.png") + '"},"error":null}',
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


def test_resource_hosts_script_matches_verified_agent_browser_expression() -> None:
  assert RESOURCE_HOSTS_JS == (
    "Array.from(new Set(performance.getEntriesByType('resource').map((entry) => {"
    "try { return new URL(entry.name).hostname; } catch { return null; }"
    "}).filter(Boolean)))"
  )
