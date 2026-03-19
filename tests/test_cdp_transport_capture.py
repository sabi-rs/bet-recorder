from datetime import UTC, datetime
from pathlib import Path
import json
import sys

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.cdp import DebugTarget  # noqa: E402
from bet_recorder.capture.run_bundle import initialize_run_bundle  # noqa: E402
from bet_recorder.cli import app  # noqa: E402
from bet_recorder.live.cdp_transport_capture import capture_cdp_transport  # noqa: E402


def test_capture_cdp_transport_selects_target_and_appends_sanitized_events(
  tmp_path: Path,
) -> None:
  bundle = initialize_run_bundle(
    source="smarkets_exchange",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 10, 15, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=True,
  )

  def fake_list_targets(debug_base_url: str) -> list[DebugTarget]:
    assert debug_base_url == "http://127.0.0.1:9222"
    return [
      DebugTarget(
        target_id="smarkets",
        target_type="page",
        title="Smarkets",
        url="https://smarkets.com/event/123",
        websocket_debugger_url="ws://127.0.0.1:9222/devtools/page/smarkets",
      ),
    ]

  def fake_capture_events(
    websocket_debugger_url: str,
    duration_ms: int,
    reload: bool,
  ) -> list[dict]:
    assert websocket_debugger_url == "ws://127.0.0.1:9222/devtools/page/smarkets"
    assert duration_ms == 2500
    assert reload is True
    return [
      {
        "method": "Network.webSocketFrameReceived",
        "params": {
          "response": {
            "opcode": 1,
            "payloadData": '{"access_token":"abc","protocol":"json"}',
          },
        },
      },
    ]

  count = capture_cdp_transport(
    source="smarkets_exchange",
    bundle=bundle,
    debug_base_url="http://127.0.0.1:9222",
    duration_ms=2500,
    reload=True,
    list_targets=fake_list_targets,
    capture_events=fake_capture_events,
  )

  lines = bundle.transport_path.read_text().splitlines()
  assert count == 1
  assert len(lines) == 1
  event = json.loads(lines[0])
  assert event["method"] == "Network.webSocketFrameReceived"
  assert "[REDACTED]" in event["params"]["response"]["payloadData"]


def test_capture_cdp_transport_cli_command_uses_live_target_capture(
  tmp_path: Path,
  monkeypatch,
) -> None:
  runner = CliRunner()

  init_result = runner.invoke(
    app,
    [
      "init-run",
      "--source",
      "smarkets_exchange",
      "--root-dir",
      str(tmp_path),
      "--started-at",
      "2026-03-09T10:15:00Z",
      "--collector-version",
      "test-v1",
      "--browser-profile-used",
      "helium-copy",
      "--transport",
    ],
  )
  assert init_result.exit_code == 0
  init_payload = json.loads(init_result.output)

  captured = {}

  def stub_capture_cdp_transport(**kwargs):
    captured.update(kwargs)
    return 3

  monkeypatch.setattr("bet_recorder.cli.capture_cdp_transport", stub_capture_cdp_transport)

  result = runner.invoke(
    app,
    [
      "capture-cdp-transport",
      "--source",
      "smarkets_exchange",
      "--run-dir",
      init_payload["run_dir"],
      "--duration-ms",
      "2500",
      "--debug-base-url",
      "http://127.0.0.1:9222",
      "--url-contains",
      "smarkets.com",
      "--reload",
    ],
  )

  assert result.exit_code == 0
  assert captured["source"] == "smarkets_exchange"
  assert str(captured["bundle"].run_dir) == init_payload["run_dir"]
  assert captured["duration_ms"] == 2500
  assert captured["debug_base_url"] == "http://127.0.0.1:9222"
  assert captured["url_contains"] == "smarkets.com"
  assert captured["reload"] is True
  assert json.loads(result.output) == {
    "source": "smarkets_exchange",
    "transport_path": str(Path(init_payload["run_dir"]) / "transport.jsonl"),
    "captured_event_count": 3,
  }
