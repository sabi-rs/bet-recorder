from datetime import UTC, datetime
from pathlib import Path
import json
import sys

import pytest
from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.adapter import build_page_payload  # noqa: E402
from bet_recorder.capture.run_bundle import initialize_run_bundle  # noqa: E402
from bet_recorder.cli import app  # noqa: E402
from bet_recorder.live.runner import (  # noqa: E402
  record_live_page,
  record_live_transport,
  record_watch_plan,
)


def test_record_live_page_appends_source_specific_event(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="betway_uk",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 10, 15, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  record_live_page(
    source="betway_uk",
    bundle=bundle,
    payload=build_page_payload(
      captured_at=datetime(2026, 3, 9, 10, 16, tzinfo=UTC),
      page="market",
      url="https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
      document_title="Betway event",
      body_text="Almeria Correct Score",
      interactive_snapshot=[{"tag": "BUTTON", "text": "Add to Bet Slip"}],
      links=["https://betway.com/gb/en/sports"],
      inputs={"stake": ""},
      visible_actions=["Add to Bet Slip"],
      resource_hosts=["betway.com"],
      local_storage_keys=["theme"],
      notes=["free-bet"],
    ),
  )

  event = json.loads(bundle.events_path.read_text())

  assert event["source"] == "betway_uk"
  assert event["kind"] == "market_snapshot"
  assert event["page"] == "market"


def test_record_live_transport_appends_sanitized_event(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="smarkets_exchange",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 10, 15, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=True,
  )

  record_live_transport(
    bundle=bundle,
    payload={
      "type": "ws_handshake_request",
      "headers": {
        "Cookie": "abc=123",
        "Authorization": "Bearer secret-token",
      },
      "preview": '{"access_token":"abc","jwt":"eyJabc.def.ghi"}',
    },
  )

  event = json.loads(bundle.transport_path.read_text())

  assert event["headers"]["Cookie"] == "[REDACTED]"
  assert event["headers"]["Authorization"] == "[REDACTED]"
  assert "[REDACTED]" in event["preview"]


def test_record_watch_plan_appends_watch_plan_event(tmp_path: Path) -> None:
  bundle = initialize_run_bundle(
    source="smarkets_exchange",
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 20, 15, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  record_watch_plan(
    source="smarkets_exchange",
    bundle=bundle,
    payload={
      "captured_at": "2026-03-09T20:45:00Z",
      "page": "open_positions",
      "commission_rate": 0.0,
      "target_profit": 1.0,
      "stop_loss": 1.0,
      "position_count": 3,
      "watch_count": 2,
      "watches": [
        {
          "contract": "Draw",
          "market": "Full-time result",
          "profit_take_back_odds": 3.73,
          "stop_loss_back_odds": 3.04,
        },
      ],
    },
  )

  event = json.loads(bundle.events_path.read_text())
  assert event["kind"] == "watch_plan_snapshot"
  assert event["watch_count"] == 2


def test_capture_live_page_cli_command_uses_fixture_payload(tmp_path: Path) -> None:
  runner = CliRunner()

  init_result = runner.invoke(
    app,
    [
      "init-run",
      "--source",
      "betway_uk",
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

  payload_path = tmp_path / "live-page.json"
  payload_path.write_text(
    json.dumps(
      build_page_payload(
        captured_at=datetime(2026, 3, 9, 10, 16, tzinfo=UTC),
        page="confirmation",
        url="https://betway.com/gb/en/sports/event/16431700?marketGroup=SGP",
        document_title="Betway confirmation",
        body_text="Bet placed",
        interactive_snapshot=[{"tag": "BUTTON", "text": "Done"}],
        links=["https://betway.com/gb/en/sports"],
        inputs={"stake": "10"},
        visible_actions=["Done"],
        resource_hosts=["betway.com"],
        local_storage_keys=["theme"],
        notes=["free-bet"],
      ),
    ),
  )

  result = runner.invoke(
    app,
    [
      "capture-live-page",
      "--source",
      "betway_uk",
      "--run-dir",
      init_payload["run_dir"],
      "--payload-path",
      str(payload_path),
    ],
  )

  assert result.exit_code == 0

  event = json.loads(Path(init_payload["events_path"]).read_text())
  assert event["kind"] == "confirmation_snapshot"


@pytest.mark.parametrize(
  ("source", "page", "expected_kind"),
  [
    ("bet365", "my_bets", "positions_snapshot"),
    ("betuk", "market", "market_snapshot"),
    ("betfred", "my_bets", "positions_snapshot"),
    ("coral", "my_bets", "positions_snapshot"),
    ("ladbrokes", "my_bets", "positions_snapshot"),
    ("kwik", "my_bets", "positions_snapshot"),
    ("bet600", "my_bets", "positions_snapshot"),
    ("betdaq", "open_positions", "positions_snapshot"),
  ],
)
def test_record_live_page_accepts_generic_current_sources(
  tmp_path: Path,
  source: str,
  page: str,
  expected_kind: str,
) -> None:
  bundle = initialize_run_bundle(
    source=source,
    root_dir=tmp_path,
    started_at=datetime(2026, 3, 9, 10, 15, tzinfo=UTC),
    collector_version="test-v1",
    browser_profile_used="helium-copy",
    transport_capture_enabled=False,
  )

  payload = build_page_payload(
    captured_at=datetime(2026, 3, 9, 10, 16, tzinfo=UTC),
    page=page,
    url=f"https://{source}.example.test/{page}",
    document_title=f"{source} {page}",
    body_text="Example page",
    interactive_snapshot=[],
    links=[],
    inputs={},
    visible_actions=[],
    resource_hosts=[f"{source}.example.test"],
    local_storage_keys=["session"],
    notes=["fixture"],
  )

  record_live_page(source=source, bundle=bundle, payload=payload)

  event = json.loads(bundle.events_path.read_text())

  assert event["source"] == source
  assert event["kind"] == expected_kind
  assert event["screenshot_path"] == payload["screenshot_path"]
