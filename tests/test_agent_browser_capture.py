from datetime import UTC, datetime
from pathlib import Path
import json
import sys

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.models import BrowserPageState  # noqa: E402
from bet_recorder.capture.run_bundle import initialize_run_bundle  # noqa: E402
from bet_recorder.cli import app  # noqa: E402
from bet_recorder.live.agent_browser_capture import (  # noqa: E402
    capture_agent_browser_action,
    capture_agent_browser_page,
    capture_agent_browser_page_state,
)


class FakeAgentBrowserClient:
    def __init__(self, screenshot_bytes: bytes = b"png-bytes") -> None:
        self.screenshot_bytes = screenshot_bytes
        self.requested_screenshot_path: Path | None = None

    def capture_screenshot(self, output_path: Path) -> Path:
        self.requested_screenshot_path = output_path
        output_path.write_bytes(self.screenshot_bytes)
        return output_path

    def capture_page_state(
        self,
        *,
        page: str,
        captured_at: datetime,
        screenshot_path: str | None,
        notes: list[str],
    ) -> BrowserPageState:
        return BrowserPageState(
            captured_at=captured_at,
            page=page,
            url="https://vb.rebelbetting.com/",
            document_title="Value betting by RebelBetting",
            body_text="25 value bets",
            interactive_snapshot=[{"ref": "e1", "role": "link", "name": "Filters"}],
            links=["https://vb.rebelbetting.com/filters"],
            inputs={"search": ""},
            visible_actions=["Filters"],
            resource_hosts=["vb.rebelbetting.com"],
            local_storage_keys=["Token"],
            screenshot_path=screenshot_path,
            notes=notes,
        )


class NoScreenshotAgentBrowserClient(FakeAgentBrowserClient):
    def capture_screenshot(self, output_path: Path) -> Path:
        raise AssertionError("watcher polling should not capture screenshots")


def test_capture_agent_browser_page_state_records_event_without_screenshot(
    tmp_path: Path,
) -> None:
    bundle = initialize_run_bundle(
        source="smarkets_exchange",
        root_dir=tmp_path,
        started_at=datetime(2026, 3, 12, 10, 15, tzinfo=UTC),
        collector_version="test-v1",
        browser_profile_used="helium-copy",
        transport_capture_enabled=False,
    )
    client = NoScreenshotAgentBrowserClient()

    payload = capture_agent_browser_page_state(
        source="smarkets_exchange",
        bundle=bundle,
        page="open_positions",
        captured_at=datetime(2026, 3, 12, 10, 16, tzinfo=UTC),
        client=client,
        notes=["watcher-loop"],
    )

    event = json.loads(bundle.events_path.read_text())

    assert payload["screenshot_path"] is None
    assert event["kind"] == "positions_snapshot"
    assert event["screenshot_path"] is None


def test_capture_agent_browser_page_persists_screenshot_and_records_event(
    tmp_path: Path,
) -> None:
    bundle = initialize_run_bundle(
        source="rebelbetting_vb",
        root_dir=tmp_path,
        started_at=datetime(2026, 3, 9, 10, 15, tzinfo=UTC),
        collector_version="test-v1",
        browser_profile_used="helium-copy",
        transport_capture_enabled=False,
    )
    client = FakeAgentBrowserClient()

    payload = capture_agent_browser_page(
        source="rebelbetting_vb",
        bundle=bundle,
        page="dashboard",
        captured_at=datetime(2026, 3, 9, 10, 16, tzinfo=UTC),
        client=client,
        notes=["trial-mode"],
    )

    event = json.loads(bundle.events_path.read_text())

    assert client.requested_screenshot_path is not None
    assert not client.requested_screenshot_path.exists()
    assert payload["screenshot_path"] == "screenshots/dashboard-20260309T101600Z.png"
    assert (bundle.run_dir / payload["screenshot_path"]).read_bytes() == b"png-bytes"
    assert event["kind"] == "dashboard_snapshot"
    assert event["screenshot_path"] == payload["screenshot_path"]


def test_capture_agent_browser_page_cli_command_uses_current_browser_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()

    init_result = runner.invoke(
        app,
        [
            "init-run",
            "--source",
            "rebelbetting_vb",
            "--root-dir",
            str(tmp_path),
            "--started-at",
            "2026-03-09T10:15:00Z",
            "--collector-version",
            "test-v1",
            "--browser-profile-used",
            "helium-copy",
        ],
    )
    assert init_result.exit_code == 0
    init_payload = json.loads(init_result.output)

    captured = {}

    class StubAgentBrowserClient(FakeAgentBrowserClient):
        def __init__(self, session=None, cdp_port=None, executable_path=None):
            super().__init__()
            captured["session"] = session
            captured["cdp_port"] = cdp_port
            captured["executable_path"] = executable_path

    monkeypatch.setattr("bet_recorder.cli.AgentBrowserClient", StubAgentBrowserClient)

    result = runner.invoke(
        app,
        [
            "capture-agent-browser-page",
            "--source",
            "rebelbetting_vb",
            "--run-dir",
            init_payload["run_dir"],
            "--page",
            "dashboard",
            "--captured-at",
            "2026-03-09T10:16:00Z",
            "--session",
            "helium-copy",
            "--note",
            "trial-mode",
        ],
    )

    assert result.exit_code == 0
    assert captured == {
        "session": "helium-copy",
        "cdp_port": None,
        "executable_path": None,
    }
    event = json.loads(Path(init_payload["events_path"]).read_text())
    assert event["kind"] == "dashboard_snapshot"


def test_capture_agent_browser_action_records_current_browser_state(
    tmp_path: Path,
) -> None:
    bundle = initialize_run_bundle(
        source="betway_uk",
        root_dir=tmp_path,
        started_at=datetime(2026, 3, 9, 20, 15, tzinfo=UTC),
        collector_version="test-v1",
        browser_profile_used="helium-copy",
        transport_capture_enabled=False,
    )
    client = FakeAgentBrowserClient()

    payload = capture_agent_browser_action(
        source="betway_uk",
        bundle=bundle,
        page="confirmation",
        action="place_bet",
        target="place_bet_button",
        status="confirmed",
        captured_at=datetime(2026, 3, 9, 20, 16, tzinfo=UTC),
        client=client,
        notes=["free-bet"],
        metadata={"selection": "Almeria 2-0"},
    )

    event = json.loads(bundle.events_path.read_text())

    assert payload["action"] == "place_bet"
    assert event["kind"] == "action_snapshot"
    assert event["metadata"]["selection"] == "Almeria 2-0"
