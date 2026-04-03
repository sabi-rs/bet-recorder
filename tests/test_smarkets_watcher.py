from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
import json
import os
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.models import BrowserPageState  # noqa: E402
from bet_recorder import watcher_storage  # noqa: E402
from bet_recorder.watcher import (  # noqa: E402
    LIVE_EVENT_SUMMARY_REFRESH_SECONDS,
    LIVE_POLL_INTERVAL_SECONDS,
    PersistentSmarketsProfileClient,
    WatcherConfig,
    _capture_event_summaries,
    _next_poll_interval_seconds,
    _with_navigation_retry,
    acquire_watcher_process_slot,
    capture_current_smarkets_open_positions,
    bootstrap_smarkets_page,
    build_exchange_snapshot_from_payload,
    build_session_diagnostics,
    ensure_smarkets_authenticated,
    load_smarkets_credentials,
    release_watcher_process_slot,
    run_smarkets_watcher,
    validate_smarkets_open_positions_payload,
)


def test_run_smarkets_watcher_captures_and_writes_latest_state(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "smarkets-run"

    calls = {"capture": 0, "sleep": 0}

    def fake_capture(config: WatcherConfig, captured_at: datetime) -> dict:
        calls["capture"] += 1
        payload = {
            "captured_at": captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "smarkets_exchange",
            "kind": "positions_snapshot",
            "page": "open_positions",
            "url": "https://smarkets.com/open-positions",
            "document_title": "Open positions",
            "body_text": (
                "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
                "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
                "Lazio vs Sassuolo "
                "Sell 1 - 1 Correct score 7.2 £2.55 £15.81 £18.36 £2.46 £1.20 (3.53%) Order filled Trade out "
                "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£1.31 (3.13%) Order filled Trade out"
            ),
            "interactive_snapshot": [],
            "links": [],
            "inputs": {},
            "visible_actions": ["Trade out"],
            "resource_hosts": ["smarkets.com"],
            "local_storage_keys": [],
            "screenshot_path": None,
            "notes": ["watcher-loop"],
        }
        with (run_dir / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")
        return payload

    def fake_sleep(_: float) -> None:
        calls["sleep"] += 1

    state = run_smarkets_watcher(
        WatcherConfig(
            run_dir=run_dir,
            session="helium-copy",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
        capture_once=fake_capture,
        sleep=fake_sleep,
        now=lambda: datetime(2026, 3, 11, 12, 5, tzinfo=UTC),
        max_iterations=1,
    )

    assert calls["capture"] == 1
    assert calls["sleep"] == 0
    assert state["decision_count"] == 2
    assert state["decisions"][0]["status"] == "take_profit_ready"
    assert state["decisions"][1]["status"] == "stop_loss_ready"
    persisted = json.loads((run_dir / "watcher-state.json").read_text())
    assert persisted["run_dir"] == str(run_dir)
    assert persisted["decisions"][0]["contract"] == "1 - 1"
    assert (run_dir / "events.jsonl").exists()
    assert (run_dir / "metadata.json").exists()
    assert (run_dir / "screenshots").is_dir()


def test_run_smarkets_watcher_syncs_bookmaker_history_in_background(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    sync_calls: list[Path] = []

    def fake_capture(config: WatcherConfig, captured_at: datetime) -> dict:
        return {
            "captured_at": captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "smarkets_exchange",
            "kind": "positions_snapshot",
            "page": "open_positions",
            "url": "https://smarkets.com/open-positions",
            "document_title": "Open positions",
            "body_text": (
                "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
                "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
                "Lazio vs Sassuolo "
                "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£1.31 (3.13%) Order filled Trade out"
            ),
            "interactive_snapshot": [],
            "links": [],
            "inputs": {},
            "visible_actions": ["Trade out"],
            "resource_hosts": ["smarkets.com"],
            "local_storage_keys": [],
            "screenshot_path": None,
            "notes": ["watcher-loop"],
        }

    monkeypatch.setattr(
        "bet_recorder.watcher.sync_live_bookmaker_history_for_run_dir",
        lambda current_run_dir: sync_calls.append(current_run_dir),
    )

    run_smarkets_watcher(
        WatcherConfig(
            run_dir=run_dir,
            session="helium-copy",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
        capture_once=fake_capture,
        sleep=lambda _: None,
        now=lambda: datetime(2026, 3, 11, 12, 5, tzinfo=UTC),
        max_iterations=1,
    )

    assert sync_calls == [run_dir]


def test_run_smarkets_watcher_records_bookmaker_history_sync_warning(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    run_dir = tmp_path / "smarkets-run"

    def fake_capture(config: WatcherConfig, captured_at: datetime) -> dict:
        return {
            "captured_at": captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "smarkets_exchange",
            "kind": "positions_snapshot",
            "page": "open_positions",
            "url": "https://smarkets.com/open-positions",
            "document_title": "Open positions",
            "body_text": (
                "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
                "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
                "Lazio vs Sassuolo "
                "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£1.31 (3.13%) Order filled Trade out"
            ),
            "interactive_snapshot": [],
            "links": [],
            "inputs": {},
            "visible_actions": ["Trade out"],
            "resource_hosts": ["smarkets.com"],
            "local_storage_keys": [],
            "screenshot_path": None,
            "notes": ["watcher-loop"],
        }

    def failing_sync(_: Path) -> list[dict]:
        raise RuntimeError("history timeout")

    monkeypatch.setattr(
        "bet_recorder.watcher.sync_live_bookmaker_history_for_run_dir",
        failing_sync,
    )

    state = run_smarkets_watcher(
        WatcherConfig(
            run_dir=run_dir,
            session="helium-copy",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
        capture_once=fake_capture,
        sleep=lambda _: None,
        now=lambda: datetime(2026, 3, 11, 12, 5, tzinfo=UTC),
        max_iterations=1,
    )

    assert "bookmaker history sync failed: history timeout" in state["warnings"]


def test_build_exchange_snapshot_from_payload_surfaces_capture_warnings() -> None:
    payload = {
        "captured_at": "2026-03-11T12:05:00Z",
        "source": "smarkets_exchange",
        "kind": "positions_snapshot",
        "page": "open_positions",
        "url": "https://smarkets.com/open-positions",
        "document_title": "Open positions",
        "body_text": (
            "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
            "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
            "Lazio vs Sassuolo "
            "Sell 1 - 1 Correct score 7.2 £2.55 £15.81 £18.36 £2.46 £1.20 (3.53%) Order filled Trade out "
            "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£1.31 (3.13%) Order filled Trade out"
        ),
        "interactive_snapshot": [],
        "links": [],
        "inputs": {},
        "visible_actions": ["Trade out"],
        "resource_hosts": ["smarkets.com"],
        "local_storage_keys": [],
        "screenshot_path": None,
        "notes": ["watcher-loop"],
        "capture_warnings": [
            "event summary capture failed: timeout",
            "settled history capture failed: missing selector",
        ],
    }

    snapshot = build_exchange_snapshot_from_payload(
        payload,
        WatcherConfig(
            run_dir=Path("/tmp/smarkets-run"),
            session="helium-copy",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
    )

    assert snapshot["warnings"] == payload["capture_warnings"]
    assert "Warnings:" in snapshot["worker"]["detail"]
    assert "Warnings:" in snapshot["status_line"]


def test_run_smarkets_watcher_persists_explicit_error_for_blank_session(
    tmp_path: Path,
) -> None:
    run_dir = tmp_path / "smarkets-run"

    def blank_capture(config: WatcherConfig, captured_at: datetime) -> dict:
        payload = {
            "captured_at": captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "source": "smarkets_exchange",
            "kind": "positions_snapshot",
            "page": "open_positions",
            "url": "about:blank",
            "document_title": "",
            "body_text": "",
            "interactive_snapshot": [],
            "links": [],
            "inputs": {},
            "visible_actions": [],
            "resource_hosts": [],
            "local_storage_keys": [],
            "screenshot_path": None,
            "notes": ["watcher-loop"],
        }
        with (run_dir / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload) + "\n")
        return payload

    state = run_smarkets_watcher(
        WatcherConfig(
            run_dir=run_dir,
            session="helium-copy",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
        capture_once=blank_capture,
        sleep=lambda _: None,
        now=lambda: datetime(2026, 3, 11, 12, 10, tzinfo=UTC),
        max_iterations=1,
    )

    assert state["worker"]["status"] == "error"
    assert "about:blank" in state["worker"]["detail"]
    persisted = json.loads((run_dir / "watcher-state.json").read_text())
    assert persisted["worker"]["status"] == "error"


def test_validate_smarkets_open_positions_payload_includes_page_diagnostics() -> None:
    config = WatcherConfig(
        run_dir=Path("/tmp/smarkets-run"),
        session="helium-copy",
        interval_seconds=5.0,
        commission_rate=0.0,
        target_profit=1.0,
        stop_loss=1.0,
    )

    with pytest.raises(ValueError) as exc_info:
        validate_smarkets_open_positions_payload(
            {
                "url": "about:blank",
                "document_title": "Blank page",
                "body_text": "",
                "visible_actions": [],
                "interactive_snapshot": [],
            },
            config,
        )

    error = str(exc_info.value)
    assert 'Agent-browser session "helium-copy"' in error
    assert "url=about:blank" in error
    assert 'title="Blank page"' in error


def test_validate_smarkets_open_positions_payload_rejects_login_redirect() -> None:
    config = WatcherConfig(
        run_dir=Path("/tmp/smarkets-run"),
        session="helium-copy",
        interval_seconds=5.0,
        commission_rate=0.0,
        target_profit=1.0,
        stop_loss=1.0,
    )

    with pytest.raises(ValueError) as exc_info:
        validate_smarkets_open_positions_payload(
            {
                "url": "https://smarkets.com/?login=true&returnPath=%2Fportfolio%2F",
                "document_title": "Smarkets Predictions",
                "body_text": "Welcome back\nEmail\nPassword\nLog in\nCreate account",
                "visible_actions": ["Log in", "Create account"],
                "interactive_snapshot": [
                    {"ref": "e1", "role": "button", "name": "Log in"}
                ],
            },
            config,
        )

    error = str(exc_info.value)
    assert 'Agent-browser session "helium-copy"' in error
    assert "login=true" in error
    assert 'title="Smarkets Predictions"' in error


def test_bootstrap_smarkets_page_opens_portfolio_for_owned_profile() -> None:
    calls: list[tuple[str, str | int]] = []

    class FakeClient:
        def current_url(self) -> str:
            calls.append(("current_url", "about:blank"))
            return "about:blank"

        def open_url(self, url: str) -> None:
            calls.append(("open_url", url))

        def wait(self, milliseconds: int) -> None:
            calls.append(("wait", milliseconds))

    bootstrap_smarkets_page(
        client=FakeClient(),
        profile_path=Path("/tmp/owned-profile"),
    )

    assert calls == [
        ("current_url", "about:blank"),
        ("open_url", "https://smarkets.com/portfolio/?time=all&order-state=active"),
        ("wait", 1500),
    ]


def test_bootstrap_smarkets_page_opens_portfolio_for_blank_session_mode() -> None:
    calls: list[tuple[str, str | int]] = []

    class FakeClient:
        def current_url(self) -> str:
            calls.append(("current_url", "about:blank"))
            return "about:blank"

        def open_url(self, url: str) -> None:
            calls.append(("open_url", url))

        def wait(self, milliseconds: int) -> None:
            calls.append(("wait", milliseconds))

    bootstrap_smarkets_page(
        client=FakeClient(),
        profile_path=None,
    )

    assert calls == [
        ("current_url", "about:blank"),
        ("open_url", "https://smarkets.com/portfolio/?time=all&order-state=active"),
        ("wait", 1500),
    ]


def test_bootstrap_smarkets_page_does_not_navigate_ready_session() -> None:
    calls: list[tuple[str, str | int]] = []

    class FakeClient:
        def current_url(self) -> str:
            calls.append(
                (
                    "current_url",
                    "https://smarkets.com/portfolio/?time=all&order-state=active",
                )
            )
            return "https://smarkets.com/portfolio/?time=all&order-state=active"

        def open_url(self, url: str) -> None:
            calls.append(("open_url", url))

        def wait(self, milliseconds: int) -> None:
            calls.append(("wait", milliseconds))

    bootstrap_smarkets_page(
        client=FakeClient(),
        profile_path=None,
    )

    assert calls == [
        ("current_url", "https://smarkets.com/portfolio/?time=all&order-state=active")
    ]


def test_build_session_diagnostics_treats_portfolio_as_ready() -> None:
    diagnostics = build_session_diagnostics(
        {
            "url": "https://smarkets.com/portfolio/?time=all&order-state=active",
            "document_title": "Smarkets Predictions",
            "body_text": "Portfolio Trade out",
            "visible_actions": ["Trade out"],
            "interactive_snapshot": [],
        },
        WatcherConfig(
            run_dir=Path("/tmp/smarkets-run"),
            session="helium-copy",
            profile_path=Path("/tmp/owned-profile"),
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
    )

    assert diagnostics["page_hint"] == "open_positions"
    assert diagnostics["open_positions_ready"] is True


def test_build_session_diagnostics_flags_portfolio_error_overlay() -> None:
    diagnostics = build_session_diagnostics(
        {
            "url": "https://smarkets.com/portfolio/?order-state=active",
            "document_title": "Smarkets Predictions",
            "body_text": "Something went wrong\nTry Again\nWed",
            "visible_actions": ["Try Again"],
            "interactive_snapshot": [
                {"ref": "e1", "role": "button", "name": "Try Again"}
            ],
        },
        WatcherConfig(
            run_dir=Path("/tmp/smarkets-run"),
            session="helium-copy",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
    )

    assert diagnostics["page_hint"] == "error"
    assert diagnostics["open_positions_ready"] is False


def test_build_session_diagnostics_flags_login_redirect() -> None:
    diagnostics = build_session_diagnostics(
        {
            "url": "https://smarkets.com/?login=true&returnPath=%2Fportfolio%2F",
            "document_title": "Smarkets Predictions",
            "body_text": "Welcome back\nEmail\nPassword\nLog in\nCreate account",
            "visible_actions": ["Log in", "Create account"],
            "interactive_snapshot": [{"ref": "e1", "role": "button", "name": "Log in"}],
        },
        WatcherConfig(
            run_dir=Path("/tmp/smarkets-run"),
            session="helium-copy",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
    )

    assert diagnostics["page_hint"] == "login"
    assert diagnostics["open_positions_ready"] is False


def test_load_smarkets_credentials_reads_home_dotenv(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.delenv("SMARKETS_USERNAME", raising=False)
    monkeypatch.delenv("SMARKETS_PASSWORD", raising=False)
    (tmp_path / ".env").write_text(
        'SMARKETS_USERNAME="user@example.com"\nSMARKETS_PASSWORD="secret"\n'
    )

    credentials = load_smarkets_credentials(home=tmp_path)

    assert credentials == ("user@example.com", "secret")


def test_ensure_smarkets_authenticated_submits_login_when_credentials_exist(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SMARKETS_USERNAME", raising=False)
    monkeypatch.delenv("SMARKETS_PASSWORD", raising=False)
    (tmp_path / ".env").write_text(
        'SMARKETS_USERNAME="user@example.com"\nSMARKETS_PASSWORD="secret"\n'
    )
    calls: list[tuple[str, str | int]] = []

    class FakeClient:
        def current_url(self) -> str:
            calls.append(("current_url", "https://smarkets.com/?login=true"))
            return "https://smarkets.com/?login=true"

        def fill(self, selector: str, text: str) -> None:
            calls.append((f"fill:{selector}", text))

        def click(self, selector: str) -> None:
            calls.append(("click", selector))

        def wait(self, milliseconds: int) -> None:
            calls.append(("wait", milliseconds))

        def open_url(self, url: str) -> None:
            calls.append(("open_url", url))

    original_home = os.environ.get("HOME")
    os.environ["HOME"] = str(tmp_path)
    try:
        ensure_smarkets_authenticated(client=FakeClient())
    finally:
        if original_home is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = original_home

    assert calls[:5] == [
        ("current_url", "https://smarkets.com/?login=true"),
        ('fill:input[type="email"]', "user@example.com"),
        ('fill:input[type="password"]', "secret"),
        ("click", 'button[type="submit"]'),
        ("wait", 1500),
    ]
    assert (
        "open_url",
        "https://smarkets.com/portfolio/?time=all&order-state=active",
    ) in calls


def test_with_navigation_retry_retries_execution_context_destroyed() -> None:
    calls = {"count": 0}

    class FakeClient:
        def wait(self, milliseconds: int) -> None:
            calls["wait_ms"] = milliseconds

    def flaky_operation() -> str:
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError(
                '{"success":false,"data":null,"error":"page.evaluate: Execution context was destroyed, most likely because of a navigation"}'
            )
        return "ok"

    result = _with_navigation_retry(flaky_operation, client=FakeClient())

    assert result == "ok"
    assert calls["count"] == 2
    assert calls["wait_ms"] == 500


def test_acquire_watcher_process_slot_rejects_conflicting_process(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    pid_path = run_dir / "watcher.pid"
    pid_path.write_text("123\n")

    monkeypatch.setattr(
        watcher_storage,
        "_find_conflicting_watcher_pid",
        lambda run_dir_arg, pid_path_arg: 123,
    )

    with pytest.raises(RuntimeError, match="Watcher already running"):
        acquire_watcher_process_slot(run_dir)

    assert pid_path.read_text().strip() == "123"

    release_watcher_process_slot(run_dir)
    assert pid_path.exists()


def test_next_poll_interval_uses_live_cadence_for_in_play_positions() -> None:
    config = WatcherConfig(
        run_dir=Path("/tmp/smarkets-run"),
        session="helium-copy",
        interval_seconds=5.0,
        commission_rate=0.0,
        target_profit=1.0,
        stop_loss=1.0,
    )

    interval = _next_poll_interval_seconds(
        config,
        {
            "open_positions": [
                {"contract": "Man City", "is_in_play": True},
            ]
        },
    )

    assert interval == LIVE_POLL_INTERVAL_SECONDS
    assert interval == 0.1


def test_next_poll_interval_keeps_base_cadence_for_pre_match_positions() -> None:
    config = WatcherConfig(
        run_dir=Path("/tmp/smarkets-run"),
        session="helium-copy",
        interval_seconds=5.0,
        commission_rate=0.0,
        target_profit=1.0,
        stop_loss=1.0,
    )

    interval = _next_poll_interval_seconds(
        config,
        {
            "open_positions": [
                {"contract": "Man City", "is_in_play": False},
            ]
        },
    )

    assert interval == 5.0


def test_ensure_smarkets_authenticated_prefers_visible_login_button_with_eval_path(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("SMARKETS_USERNAME", raising=False)
    monkeypatch.delenv("SMARKETS_PASSWORD", raising=False)
    (tmp_path / ".env").write_text(
        'SMARKETS_USERNAME="user@example.com"\nSMARKETS_PASSWORD="secret"\n'
    )
    calls: list[tuple[str, str]] = []

    class FakeClient:
        def __init__(self) -> None:
            self.url_checks = 0

        def current_url(self) -> str:
            self.url_checks += 1
            if self.url_checks == 1:
                return "https://smarkets.com/?login=true"
            return "https://smarkets.com/portfolio/?order-state=active"

        def set_input_value(self, selector: str, text: str) -> None:
            calls.append((f"set:{selector}", text))

        def evaluate(self, script: str):
            calls.append(("eval", script))
            return {"submitted": True}

        def wait(self, milliseconds: int) -> None:
            calls.append(("wait", str(milliseconds)))

        def open_url(self, url: str) -> None:
            calls.append(("open_url", url))

    original_home = os.environ.get("HOME")
    os.environ["HOME"] = str(tmp_path)
    try:
        ensure_smarkets_authenticated(client=FakeClient())
    finally:
        if original_home is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = original_home

    assert (
        'set:input[autocomplete="email"], input[name="email"], input[type="email"]',
        "user@example.com",
    ) in calls
    assert (
        'set:input[autocomplete="current-password"], input[name="password"], input[type="password"]',
        "secret",
    ) in calls
    eval_calls = [value for kind, value in calls if kind == "eval"]
    assert eval_calls
    assert "text === 'Log in' && !element.disabled" in eval_calls[0]
    assert "form.requestSubmit(submit)" in eval_calls[0]
    assert ("open_url", "https://smarkets.com/portfolio/") not in calls


def test_run_smarkets_watcher_uses_persistent_profile_backend(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    calls: list[tuple[str, str | int]] = []

    class FakePageState:
        def to_payload(self) -> dict:
            return {
                "captured_at": "2026-03-11T12:05:00Z",
                "source": "smarkets_exchange",
                "kind": "positions_snapshot",
                "page": "open_positions",
                "url": "https://smarkets.com/portfolio/?time=all&order-state=active",
                "document_title": "Open positions",
                "body_text": (
                    "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
                    "Lazio vs Sassuolo Sell Draw Full-time result 3.35 £9.91 £23.29 "
                    "£33.20 £9.60 -£1.31 (3.13%) Order filled Trade out Back 5.00"
                ),
                "interactive_snapshot": [],
                "links": [],
                "inputs": {},
                "visible_actions": ["Trade out"],
                "resource_hosts": ["smarkets.com"],
                "local_storage_keys": [],
                "screenshot_path": None,
                "notes": ["watcher-loop"],
            }

    class FakePersistentClient:
        def __init__(
            self, profile_path: Path, *, executable_path: str = "/usr/bin/chromium"
        ) -> None:
            calls.append(("init", str(profile_path)))

        def __enter__(self):
            calls.append(("enter", ""))
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            calls.append(("exit", ""))

        def current_url(self) -> str:
            calls.append(("current_url", "about:blank"))
            return "about:blank"

        def open_url(self, url: str) -> None:
            calls.append(("open_url", url))

        def wait(self, milliseconds: int) -> None:
            calls.append(("wait", milliseconds))

        def capture_page_state(self, **_: object) -> FakePageState:
            calls.append(("capture_page_state", "open_positions"))
            return FakePageState()

    monkeypatch.setattr(
        "bet_recorder.watcher.PersistentSmarketsProfileClient",
        FakePersistentClient,
    )

    state = run_smarkets_watcher(
        WatcherConfig(
            run_dir=run_dir,
            session="helium-copy",
            profile_path=tmp_path / "owned-profile",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
        sleep=lambda _: None,
        now=lambda: datetime(2026, 3, 11, 12, 5, tzinfo=UTC),
        max_iterations=1,
    )

    assert state["worker"]["status"] == "ready"
    assert (
        "open_url",
        "https://smarkets.com/portfolio/?time=all&order-state=active",
    ) in calls
    assert ("capture_page_state", "open_positions") in calls


def test_capture_current_smarkets_open_positions_retries_try_again_overlay(
    tmp_path: Path,
    monkeypatch,
) -> None:
    calls: list[tuple[str, str | int]] = []
    payloads = [
        {
            "captured_at": "2026-03-18T06:36:33Z",
            "source": "smarkets_exchange",
            "kind": "positions_snapshot",
            "page": "open_positions",
            "url": "https://smarkets.com/portfolio/?order-state=active",
            "document_title": "Smarkets Predictions",
            "body_text": "Something went wrong\nTry Again\nWed",
            "interactive_snapshot": [
                {"ref": "e1", "role": "button", "name": "Try Again"}
            ],
            "links": [],
            "inputs": {},
            "visible_actions": ["Try Again"],
            "resource_hosts": ["smarkets.com"],
            "local_storage_keys": [],
            "screenshot_path": None,
            "notes": ["watcher-loop"],
        },
        {
            "captured_at": "2026-03-18T06:36:35Z",
            "source": "smarkets_exchange",
            "kind": "positions_snapshot",
            "page": "open_positions",
            "url": "https://smarkets.com/portfolio/?order-state=active",
            "document_title": "Smarkets Predictions",
            "body_text": (
                "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
                "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
                "Lazio vs Sassuolo "
                "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£1.31 (3.13%) "
                "Order filled Trade out"
            ),
            "interactive_snapshot": [],
            "links": [],
            "inputs": {},
            "visible_actions": ["Trade out"],
            "resource_hosts": ["smarkets.com"],
            "local_storage_keys": [],
            "screenshot_path": None,
            "notes": ["watcher-loop", "retry-after-error"],
        },
    ]

    class FakeClient:
        def current_url(self) -> str:
            calls.append(
                ("current_url", "https://smarkets.com/portfolio/?order-state=active")
            )
            return "https://smarkets.com/portfolio/?order-state=active"

        def open_url(self, url: str) -> None:
            calls.append(("open_url", url))

        def wait(self, milliseconds: int) -> None:
            calls.append(("wait", milliseconds))

        def evaluate(self, script: str):
            if "Try Again" in script:
                calls.append(("eval-try-again", "Try Again"))
                return True
            calls.append(("eval", "other"))
            return False

    def fake_capture_agent_browser_page_state(**_: object):
        return payloads.pop(0)

    monkeypatch.setattr(
        "bet_recorder.watcher.capture_agent_browser_page_state",
        fake_capture_agent_browser_page_state,
    )
    monkeypatch.setattr(
        "bet_recorder.watcher.accept_smarkets_cookies", lambda *, client: None
    )
    monkeypatch.setattr(
        "bet_recorder.watcher.ensure_smarkets_activity_filter", lambda **_: None
    )

    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()

    payload = capture_current_smarkets_open_positions(
        WatcherConfig(
            run_dir=run_dir,
            session="helium-copy",
            profile_path=tmp_path / "owned-profile",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
        datetime(2026, 3, 18, 6, 36, 33, tzinfo=UTC),
        client=FakeClient(),
    )

    assert payload["body_text"].startswith("Available balance")
    assert payload["notes"] == ["watcher-loop", "retry-after-error"]
    assert ("eval-try-again", "Try Again") in calls


def test_capture_current_smarkets_open_positions_merges_scroll_captured_rows(
    tmp_path: Path,
    monkeypatch,
) -> None:
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()

    first_state = BrowserPageState(
        captured_at=datetime(2026, 3, 19, 19, 13, tzinfo=UTC),
        page="open_positions",
        url="https://smarkets.com/portfolio/?order-state=active",
        document_title="Smarkets Predictions",
        body_text=(
            "Portfolio\n"
            "£72.44\n"
            "Your balance\n"
            "£21.75\n"
            "Exposure\n"
            "15:32 - Newbury\n"
            "Tomorrow At 3:32 PM|Newbury\n"
            "Contract\n"
            "Price\n"
            "Stake/\n"
            "Liability\n"
            "Return\n"
            "Current Value\n"
            "Status\n"
            "Sell J J Moon\n"
            "To win\n"
            "7.2\n"
            "£8.33\n"
            "£51.64\n"
            "£59.97\n"
            "£7.57\n"
            "-£0.76 (9.12%)\n"
            "Order filled\n"
            "Brumbies vs Chiefs\n"
            "Tomorrow At 8:35 AM|Super Rugby\n"
            "Contract\n"
            "Price\n"
            "Stake/\n"
            "Liability\n"
            "Return\n"
            "Current Value\n"
            "Status\n"
            "Sell Brumbies\n"
            "Winner (including overtime)\n"
            "3.0\n"
            "£10.40\n"
            "£20.80\n"
            "£31.20\n"
            "£7.23\n"
            "-£3.17 (30.48%)\n"
            "Order filled\n"
            "Trade out\n"
        ),
        interactive_snapshot=[],
        links=[],
        inputs={},
        visible_actions=["Trade out"],
        resource_hosts=["smarkets.com"],
        local_storage_keys=[],
        screenshot_path=None,
        notes=["watcher-loop"],
        metadata={
            "smarkets_portfolio": {
                "extractor": "dom-v1",
                "groups": [
                    {
                        "title": "15:32 - Newbury",
                        "event_status": "Tomorrow At 3:32 PM|Newbury",
                        "order_count": 1,
                        "positions": [
                            {
                                "side": "sell",
                                "contract": "J J Moon",
                                "market": "To win",
                                "price": 7.2,
                                "stake": 8.33,
                                "liability": 51.64,
                                "return_amount": 59.97,
                                "current_value": 7.57,
                                "pnl_amount": -0.76,
                                "pnl_percent": 9.12,
                                "status": "Order filled",
                            }
                        ],
                    }
                ],
                "positions": [
                    {
                        "side": "sell",
                        "contract": "J J Moon",
                        "market": "To win",
                        "event": "15:32 - Newbury",
                        "event_status": "Tomorrow At 3:32 PM|Newbury",
                        "price": 7.2,
                        "stake": 8.33,
                        "liability": 51.64,
                        "return_amount": 59.97,
                        "current_value": 7.57,
                        "pnl_amount": -0.76,
                        "pnl_percent": 9.12,
                        "status": "Order filled",
                    },
                    {
                        "side": "sell",
                        "contract": "Brumbies",
                        "market": "Winner (including overtime)",
                        "event": "Brumbies vs Chiefs",
                        "event_status": "Tomorrow At 8:35 AM|Super Rugby",
                        "price": 3.0,
                        "stake": 10.40,
                        "liability": 20.80,
                        "return_amount": 31.20,
                        "current_value": 7.23,
                        "pnl_amount": -3.17,
                        "pnl_percent": 30.48,
                        "status": "Order filled",
                    },
                ],
            }
        },
    )
    second_state = BrowserPageState(
        captured_at=datetime(2026, 3, 19, 19, 13, tzinfo=UTC),
        page="open_positions",
        url="https://smarkets.com/portfolio/?order-state=active",
        document_title="Smarkets Predictions",
        body_text=(
            "Brumbies vs Chiefs\n"
            "Tomorrow At 8:35 AM|Super Rugby\n"
            "Contract\n"
            "Price\n"
            "Stake/\n"
            "Liability\n"
            "Return\n"
            "Current Value\n"
            "Status\n"
            "Sell Brumbies\n"
            "Winner (including overtime)\n"
            "3.0\n"
            "£10.40\n"
            "£20.80\n"
            "£31.20\n"
            "£7.23\n"
            "-£3.17 (30.48%)\n"
            "Order filled\n"
            "Trade out\n"
            "Lazio vs Sassuolo\n"
            "Tomorrow At 10:00 PM|Serie A\n"
            "Contract\n"
            "Price\n"
            "Stake/\n"
            "Liability\n"
            "Return\n"
            "Current Value\n"
            "Status\n"
            "Sell Draw\n"
            "Full-time result\n"
            "3.35\n"
            "£9.91\n"
            "£23.29\n"
            "£33.20\n"
            "£9.60\n"
            "-£1.31 (3.13%)\n"
            "Order filled\n"
            "Trade out\n"
        ),
        interactive_snapshot=[],
        links=[],
        inputs={},
        visible_actions=["Trade out"],
        resource_hosts=["smarkets.com"],
        local_storage_keys=[],
        screenshot_path=None,
        notes=["watcher-loop"],
        metadata={
            "smarkets_portfolio": {
                "extractor": "dom-v1",
                "groups": [
                    {
                        "title": "Lazio vs Sassuolo",
                        "event_status": "Tomorrow At 10:00 PM|Serie A",
                        "order_count": 1,
                        "positions": [
                            {
                                "side": "sell",
                                "contract": "Draw",
                                "market": "Full-time result",
                                "price": 3.35,
                                "stake": 9.91,
                                "liability": 23.29,
                                "return_amount": 33.20,
                                "current_value": 9.60,
                                "pnl_amount": -1.31,
                                "pnl_percent": 3.13,
                                "status": "Order filled",
                            }
                        ],
                    }
                ],
                "positions": [
                    {
                        "side": "sell",
                        "contract": "Brumbies",
                        "market": "Winner (including overtime)",
                        "event": "Brumbies vs Chiefs",
                        "event_status": "Tomorrow At 8:35 AM|Super Rugby",
                        "price": 3.0,
                        "stake": 10.40,
                        "liability": 20.80,
                        "return_amount": 31.20,
                        "current_value": 7.23,
                        "pnl_amount": -3.17,
                        "pnl_percent": 30.48,
                        "status": "Order filled",
                    },
                    {
                        "side": "sell",
                        "contract": "Draw",
                        "market": "Full-time result",
                        "event": "Lazio vs Sassuolo",
                        "event_status": "Tomorrow At 10:00 PM|Serie A",
                        "price": 3.35,
                        "stake": 9.91,
                        "liability": 23.29,
                        "return_amount": 33.20,
                        "current_value": 9.60,
                        "pnl_amount": -1.31,
                        "pnl_percent": 3.13,
                        "status": "Order filled",
                    },
                ],
            }
        },
    )

    class FakeClient:
        def __init__(self) -> None:
            self.capture_calls = 0
            self.scroll_calls = 0

        def current_url(self) -> str:
            return "https://smarkets.com/portfolio/?time=all&order-state=active"

        def open_url(self, url: str) -> None:
            raise AssertionError(f"unexpected open_url({url})")

        def capture_page_state(self, **kwargs: object) -> BrowserPageState:
            page = str(kwargs.get("page", "open_positions"))
            self.capture_calls += 1
            state = first_state if self.capture_calls == 1 else second_state
            return replace(state, page=page)

        def evaluate(self, script: str):
            if "advanced" in script and "max_top" in script:
                self.scroll_calls += 1
                if self.scroll_calls == 1:
                    return {
                        "advanced": True,
                        "at_end": True,
                        "top": 720,
                        "max_top": 720,
                    }
                return {"advanced": False, "at_end": True, "top": 720, "max_top": 720}
            return True

        def wait(self, milliseconds: int) -> None:
            assert milliseconds == 200

    monkeypatch.setattr(
        "bet_recorder.watcher.accept_smarkets_cookies", lambda *, client: None
    )
    monkeypatch.setattr(
        "bet_recorder.watcher.ensure_smarkets_activity_filter", lambda **_: None
    )
    monkeypatch.setattr(
        "bet_recorder.watcher._capture_event_summaries",
        lambda **_: [],
    )

    payload = capture_current_smarkets_open_positions(
        WatcherConfig(
            run_dir=run_dir,
            session="helium-copy",
            profile_path=tmp_path / "owned-profile",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
        datetime(2026, 3, 19, 19, 13, tzinfo=UTC),
        client=FakeClient(),
    )

    assert payload["body_text"].count("Sell Brumbies") == 2
    assert payload["notes"] == ["watcher-loop", "scroll-capture"]

    events = [
        json.loads(line)
        for line in (run_dir / "events.jsonl").read_text().splitlines()
        if line.strip()
    ]
    assert len(events) == 2
    assert events[0]["body_text"] == payload["body_text"]
    assert events[0]["page"] == "open_positions"
    assert len(events[0]["metadata"]["smarkets_portfolio"]["positions"]) == 3
    assert events[1]["page"] == "history"
    assert events[1]["kind"] == "history_snapshot"

    positions = payload["event_summaries"]
    assert positions == []

    state = run_smarkets_watcher(
        WatcherConfig(
            run_dir=run_dir,
            session="helium-copy",
            interval_seconds=5.0,
            commission_rate=0.0,
            target_profit=1.0,
            stop_loss=1.0,
        ),
        capture_once=lambda *_: payload,
        sleep=lambda _: None,
        now=lambda: datetime(2026, 3, 19, 19, 13, tzinfo=UTC),
        max_iterations=1,
    )
    assert len(state["open_positions"]) == 3
    assert [position["contract"] for position in state["open_positions"]] == [
        "J J Moon",
        "Brumbies",
        "Draw",
    ]


def test_capture_event_summaries_uses_cache_between_fast_live_polls() -> None:
    calls: list[str] = []
    cache: dict[str, dict] = {}
    event_url = (
        "https://smarkets.com/football/england-premier-league/2026/03/14/20-00/"
        "west-ham-vs-manchester-city/44919693/"
    )

    class FakeClient:
        def evaluate(self, script: str):
            calls.append(script)
            return (
                '<html><body>{"scores":{"current":[1,0],"periods":[]},'
                '"match_period":"first_half"}</body></html>'
            )

        def wait(self, milliseconds: int) -> None:
            raise AssertionError(f"unexpected wait {milliseconds}")

    class FakeEventClient:
        def __init__(self, *, session: str | None = None, **_: object) -> None:
            assert session == "helium-copy-event-summary"

        def current_url(self) -> str:
            return event_url

        def open_url(self, url: str) -> None:
            raise AssertionError(f"unexpected open {url}")

        def wait(self, milliseconds: int) -> None:
            raise AssertionError(f"unexpected wait {milliseconds}")

        def evaluate(self, script: str):
            assert script == "document.body?.innerText ?? ''"
            calls.append("event-body")
            return "\n".join(
                [
                    "West Ham vs Man City",
                    "West Ham",
                    "Man City",
                    "1",
                    "0",
                    "33'",
                    "Winner",
                    "Order Book",
                    "BUY",
                    "SELL",
                    "Man City",
                    "81%",
                    "£5,416",
                    "80%",
                    "£646",
                ]
            )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr("bet_recorder.watcher.AgentBrowserClient", FakeEventClient)
    positions = [
        {
            "event": "West Ham vs Man City",
            "event_url": event_url,
            "is_in_play": True,
            "market": "Full-time result",
            "contract": "Man City",
        }
    ]

    try:
        first = _capture_event_summaries(
            client=FakeClient(),
            positions=positions,
            captured_at=datetime(2026, 3, 14, 15, 0, tzinfo=UTC),
            cache=cache,
            sidecar_session="helium-copy-event-summary",
        )
        second = _capture_event_summaries(
            client=FakeClient(),
            positions=positions,
            captured_at=datetime(2026, 3, 14, 15, 0, tzinfo=UTC)
            + timedelta(seconds=LIVE_EVENT_SUMMARY_REFRESH_SECONDS / 10),
            cache=cache,
            sidecar_session="helium-copy-event-summary",
        )
    finally:
        monkeypatch.undo()

    assert calls == ["event-body"]
    assert first == second
    assert first[0]["current_score"] == "1-0"
    assert first[0]["live_clock"] == "33'"
    assert first[0]["quotes"][0]["contract"] == "Man City"
