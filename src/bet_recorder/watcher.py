from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import os
from pathlib import Path
import json
import time

from bet_recorder.analysis.position_watch import build_smarkets_watch_plan
from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.capture.run_bundle import load_run_bundle
from bet_recorder.exchange_worker import (
    analyze_positions_payload,
    build_exchange_panel_snapshot,
)
from bet_recorder.live.agent_browser_capture import capture_agent_browser_page_state
from bet_recorder.live.smarkets_profile_capture import PersistentSmarketsProfileClient
from bet_recorder.watcher_state import (
    build_watcher_error_state,
    build_watcher_state,
    write_watcher_state,
)


@dataclass(frozen=True)
class WatcherConfig:
    run_dir: Path
    session: str
    interval_seconds: float
    commission_rate: float
    target_profit: float
    stop_loss: float
    profile_path: Path | None = None


CaptureOnce = Callable[[WatcherConfig, datetime], dict]
Sleep = Callable[[float], None]
Now = Callable[[], datetime]


def run_smarkets_watcher(
    config: WatcherConfig,
    *,
    capture_once: CaptureOnce | None = None,
    sleep: Sleep | None = None,
    now: Now | None = None,
    max_iterations: int | None = None,
) -> dict:
    effective_capture_once = capture_once or capture_current_smarkets_open_positions
    effective_sleep = sleep or time.sleep
    effective_now = now or _utc_now

    ensure_watcher_run_dir(config.run_dir)

    if capture_once is None and config.profile_path is not None:
        with PersistentSmarketsProfileClient(config.profile_path) as client:
            return _run_watcher_loop(
                config,
                capture_once=lambda cfg, captured_at: (
                    capture_current_smarkets_open_positions(
                        cfg,
                        captured_at,
                        client=client,
                    )
                ),
                sleep=effective_sleep,
                now=effective_now,
                max_iterations=max_iterations,
            )

    return _run_watcher_loop(
        config,
        capture_once=effective_capture_once,
        sleep=effective_sleep,
        now=effective_now,
        max_iterations=max_iterations,
    )


def _run_watcher_loop(
    config: WatcherConfig,
    *,
    capture_once: CaptureOnce,
    sleep: Sleep,
    now: Now,
    max_iterations: int | None,
) -> dict:

    latest_state: dict | None = None
    iteration = 0
    while max_iterations is None or iteration < max_iterations:
        iteration += 1
        captured_at = now()
        payload: dict | None = None
        try:
            payload = capture_once(config, captured_at)
            validate_smarkets_open_positions_payload(payload, config)
            snapshot = build_exchange_snapshot_from_payload(payload, config)
            latest_state = build_watcher_state(
                source="smarkets_exchange",
                run_dir=config.run_dir,
                interval_seconds=config.interval_seconds,
                iteration=iteration,
                snapshot=snapshot,
                captured_at=captured_at,
            )
            latest_state["session"] = build_session_diagnostics(payload, config)
        except Exception as error:
            latest_state = build_watcher_error_state(
                source="smarkets_exchange",
                run_dir=config.run_dir,
                interval_seconds=config.interval_seconds,
                iteration=iteration,
                captured_at=captured_at,
                error=str(error),
            )
            latest_state["session"] = build_session_diagnostics(
                payload,
                config,
                validation_error=str(error),
            )
        write_watcher_state(config.run_dir / "watcher-state.json", latest_state)
        if max_iterations is not None and iteration >= max_iterations:
            break
        sleep(config.interval_seconds)

    assert latest_state is not None
    return latest_state


def capture_current_smarkets_open_positions(
    config: WatcherConfig, captured_at: datetime, client=None
) -> dict:
    bundle = load_run_bundle(source="smarkets_exchange", run_dir=config.run_dir)
    client = client or AgentBrowserClient(session=config.session)
    bootstrap_smarkets_page(client=client, profile_path=config.profile_path)
    ensure_smarkets_authenticated(client=client)
    accept_smarkets_cookies(client=client)
    ensure_smarkets_activity_filter(client=client)
    return capture_agent_browser_page_state(
        source="smarkets_exchange",
        bundle=bundle,
        page="open_positions",
        captured_at=captured_at,
        client=client,
        notes=["watcher-loop"],
    )


def bootstrap_smarkets_page(*, client, profile_path: Path | None) -> None:
    current_url = client.current_url()
    if (
        current_url.startswith("https://smarkets.com/portfolio")
        or "open-positions" in current_url
    ):
        return
    client.open_url("https://smarkets.com/portfolio/")
    client.wait(1500)


def default_owned_smarkets_profile_path() -> Path:
    home = Path(os.environ.get("HOME", "/tmp")).expanduser()
    return home / ".config" / "smarkets-automation" / "profile"


def ensure_smarkets_authenticated(*, client) -> None:
    current_url = client.current_url()
    if "login=true" not in current_url:
        return

    credentials = load_smarkets_credentials()
    if credentials is None:
        return

    username, password = credentials
    client.set_input_value(
        'input[autocomplete="email"], input[name="email"]',
        username,
    )
    client.set_input_value(
        'input[autocomplete="current-password"], input[name="password"]',
        password,
    )
    client.evaluate(
        "() => {"
        "const remember = document.querySelector('input[type=\"checkbox\"]');"
        "if (remember instanceof HTMLInputElement && !remember.checked) {"
        "remember.click();"
        "}"
        "const submit = document.querySelector('button[type=\"submit\"]');"
        "if (!(submit instanceof HTMLButtonElement)) {"
        "throw new Error('Smarkets login submit button not found');"
        "}"
        "if (submit.disabled) {"
        "throw new Error('Smarkets login submit button is still disabled');"
        "}"
        "setTimeout(() => submit.click(), 0);"
        "return { submitted: true, rememberMeChecked: !!(remember && remember.checked) };"
        "}"
    )
    client.wait(3000)
    client.open_url("https://smarkets.com/portfolio/")
    client.wait(1500)


def accept_smarkets_cookies(*, client) -> None:
    accepted = client.evaluate(
        "(() => {"
        "const button = Array.from(document.querySelectorAll('button')).find((element) => {"
        "const text = (element.innerText || '').trim();"
        "return text === 'Accept all cookies' || text === 'Accept only essential cookies';"
        "});"
        "if (!(button instanceof HTMLButtonElement)) {"
        "return false;"
        "}"
        "button.click();"
        "return true;"
        "})()"
    )
    if accepted:
        client.wait(500)


def ensure_smarkets_activity_filter(*, client) -> None:
    current_url = client.current_url()
    if not current_url.startswith("https://smarkets.com/portfolio"):
        return

    current_filter = _current_smarkets_activity_filter(client=client)
    if current_filter == "All active" or current_filter is None:
        return

    client.evaluate(
        "(() => {"
        "const labels = new Set(["
        "'All orders',"
        "'All active',"
        "'Filled orders',"
        "'Unmatched orders',"
        "'Settled orders'"
        "]);"
        "const combobox = Array.from(document.querySelectorAll('[role=\"combobox\"]')).find("
        "(element) => labels.has((element.innerText || '').trim())"
        ");"
        "if (!(combobox instanceof HTMLElement)) {"
        "throw new Error('Smarkets activity filter not found');"
        "}"
        "combobox.click();"
        "const clickOption = () => {"
        "const option = Array.from(document.querySelectorAll('[role=\"option\"]')).find("
        "(element) => (element.innerText || '').trim() === 'All active'"
        ");"
        "if (!(option instanceof HTMLElement)) {"
        "setTimeout(clickOption, 50);"
        "return;"
        "}"
        "option.click();"
        "};"
        "setTimeout(clickOption, 0);"
        "return true;"
        "})()"
    )
    client.wait(750)
    current_filter = _current_smarkets_activity_filter(client=client)
    if current_filter != "All active":
        raise ValueError(
            f"Smarkets activity filter is not ready: expected 'All active', got {current_filter!r}"
        )


def _current_smarkets_activity_filter(*, client) -> str | None:
    return client.evaluate(
        "(() => {"
        "const labels = new Set(["
        "'All orders',"
        "'All active',"
        "'Filled orders',"
        "'Unmatched orders',"
        "'Settled orders'"
        "]);"
        "const combobox = Array.from(document.querySelectorAll('[role=\"combobox\"]')).find("
        "(element) => labels.has((element.innerText || '').trim())"
        ");"
        "return combobox ? (combobox.innerText || '').trim() : null;"
        "})()"
    )


def load_smarkets_credentials(home: Path | None = None) -> tuple[str, str] | None:
    username = os.environ.get("SMARKETS_USERNAME")
    password = os.environ.get("SMARKETS_PASSWORD")
    if username and password:
        return username, password

    dotenv_path = (home or Path.home()).expanduser() / ".env"
    if not dotenv_path.exists():
        return None

    values: dict[str, str] = {}
    for raw_line in dotenv_path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip().strip("'").strip('"')

    username = values.get("SMARKETS_USERNAME")
    password = values.get("SMARKETS_PASSWORD")
    if username and password:
        return username, password
    return None


def build_exchange_snapshot_from_payload(payload: dict, config: WatcherConfig) -> dict:
    positions_analysis = analyze_positions_payload(payload)
    watch = build_smarkets_watch_plan(
        positions=positions_analysis["positions"],
        commission_rate=config.commission_rate,
        target_profit=config.target_profit,
        stop_loss=config.stop_loss,
    )
    snapshot = build_exchange_panel_snapshot(watch)
    snapshot["account_stats"] = positions_analysis.get("account_stats")
    snapshot["open_positions"] = positions_analysis["positions"]
    snapshot["other_open_bets"] = positions_analysis.get("other_open_bets", [])
    snapshot["worker"]["detail"] = (
        f"Watcher iteration captured {watch['watch_count']} watch groups from "
        f"{watch['position_count']} positions."
    )
    snapshot["status_line"] = (
        f"Watcher updated {watch['watch_count']} Smarkets watch groups at "
        f"{payload['captured_at']}."
    )
    return snapshot


def validate_smarkets_open_positions_payload(
    payload: dict, config: WatcherConfig
) -> None:
    session = build_session_diagnostics(payload, config)
    if not bool(session["open_positions_ready"]):
        raise ValueError(
            _format_session_not_ready_error(session),
        )


def build_session_diagnostics(
    payload: dict | None,
    config: WatcherConfig,
    *,
    validation_error: str | None = None,
) -> dict:
    current_url = ""
    document_title = ""
    body_text = ""
    visible_actions: list[str] = []
    interactive_snapshot: list[dict] = []
    if payload is not None:
        current_url = str(payload.get("url", "") or "")
        document_title = str(payload.get("document_title", "") or "").strip()
        body_text = str(payload.get("body_text", "") or "").strip()
        visible_actions = payload.get("visible_actions", []) or []
        interactive_snapshot = payload.get("interactive_snapshot", []) or []
    visible_actions_lower = [str(action).strip().lower() for action in visible_actions]

    page_hint = "unknown"
    if current_url == "about:blank":
        page_hint = "blank"
    elif (
        "login=true" in current_url
        or "welcome back" in body_text.lower()
        or (
            "log in" in visible_actions_lower
            and ("email" in body_text.lower() or "password" in body_text.lower())
        )
    ):
        page_hint = "login"
    elif (
        "open-positions" in current_url
        or current_url.startswith("https://smarkets.com/portfolio")
        or "open positions" in document_title.lower()
        or ("portfolio" in body_text.lower() and "trade out" in body_text.lower())
    ):
        page_hint = "open_positions"
    elif (
        not body_text
        and not document_title
        and not visible_actions
        and not interactive_snapshot
    ):
        page_hint = "empty"

    open_positions_ready = page_hint == "open_positions"
    return {
        "name": config.session,
        "current_url": current_url,
        "document_title": document_title,
        "page_hint": page_hint,
        "open_positions_ready": open_positions_ready,
        "validation_error": validation_error,
    }


def _format_session_not_ready_error(session: dict) -> str:
    message = (
        f'Agent-browser session "{session["name"]}" is not ready: '
        f"url={session['current_url'] or '<unknown>'} "
        f'title="{session["document_title"]}".'
    )
    if session.get("page_hint") == "login":
        return (
            message
            + " agent-browser sessions are isolated and do not inherit cookies from "
            + "your main browser session."
        )
    return message


def _utc_now() -> datetime:
    return datetime.now(UTC)


def ensure_watcher_run_dir(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "screenshots").mkdir(parents=True, exist_ok=True)
    (run_dir / "events.jsonl").touch(exist_ok=True)
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        metadata_path.write_text(json.dumps({"source": "smarkets_exchange"}) + "\n")
