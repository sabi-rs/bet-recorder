from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import os
from pathlib import Path
import json
import signal
import time

from bet_recorder.analysis.smarkets_exchange import (
    parse_event_page_body_summary,
    parse_event_page_summary,
)
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
LIVE_POLL_INTERVAL_SECONDS = 0.1
LIVE_EVENT_SUMMARY_REFRESH_SECONDS = 2.0
IDLE_EVENT_SUMMARY_REFRESH_SECONDS = 30.0
EVENT_PAGE_NAVIGATION_WAIT_MS = 2500


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
    event_summary_cache: dict[str, dict] = {}

    ensure_watcher_run_dir(config.run_dir)
    acquire_watcher_process_slot(config.run_dir)
    try:
        if capture_once is None and config.profile_path is not None:
            with PersistentSmarketsProfileClient(config.profile_path) as client:
                return _run_watcher_loop(
                    config,
                    capture_once=lambda cfg, captured_at: (
                        capture_current_smarkets_open_positions(
                            cfg,
                            captured_at,
                            client=client,
                            event_summary_cache=event_summary_cache,
                        )
                    ),
                    sleep=effective_sleep,
                    now=effective_now,
                    max_iterations=max_iterations,
                )

        if capture_once is None:
            effective_capture_once = lambda cfg, captured_at: (
                capture_current_smarkets_open_positions(
                    cfg,
                    captured_at,
                    event_summary_cache=event_summary_cache,
                )
            )

        return _run_watcher_loop(
            config,
            capture_once=effective_capture_once,
            sleep=effective_sleep,
            now=effective_now,
            max_iterations=max_iterations,
        )
    finally:
        release_watcher_process_slot(config.run_dir)


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
        sleep(_next_poll_interval_seconds(config, latest_state))

    assert latest_state is not None
    return latest_state


def capture_current_smarkets_open_positions(
    config: WatcherConfig,
    captured_at: datetime,
    client=None,
    event_summary_cache: dict[str, dict] | None = None,
) -> dict:
    bundle = load_run_bundle(source="smarkets_exchange", run_dir=config.run_dir)
    client = client or AgentBrowserClient(session=config.session)
    bootstrap_smarkets_page(client=client, profile_path=config.profile_path)
    ensure_smarkets_authenticated(client=client)
    _with_navigation_retry(lambda: accept_smarkets_cookies(client=client), client=client)
    _with_navigation_retry(
        lambda: ensure_smarkets_activity_filter(client=client), client=client
    )
    payload = _with_navigation_retry(
        lambda: capture_agent_browser_page_state(
            source="smarkets_exchange",
            bundle=bundle,
            page="open_positions",
            captured_at=captured_at,
            client=client,
            notes=["watcher-loop"],
        ),
        client=client,
    )
    try:
        positions_analysis = analyze_positions_payload(payload)
    except Exception:
        return payload

    payload["event_summaries"] = _capture_event_summaries(
        client=client,
        positions=positions_analysis["positions"],
        captured_at=captured_at,
        cache=event_summary_cache,
        sidecar_session=_event_summary_session_name(config.session),
    )
    return payload


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
    set_input_value = getattr(client, "set_input_value", None)
    evaluate = getattr(client, "evaluate", None)
    email_selector = 'input[autocomplete="email"], input[name="email"], input[type="email"]'
    password_selector = (
        'input[autocomplete="current-password"], input[name="password"], input[type="password"]'
    )
    submit_selector = 'button[type="submit"]'

    submitted = False
    if callable(set_input_value) and callable(evaluate):
        try:
            set_input_value(email_selector, username)
            set_input_value(password_selector, password)
            evaluate(
                "() => {"
                "const remember = document.querySelector('input[type=\"checkbox\"]');"
                "if (remember instanceof HTMLInputElement && !remember.checked) {"
                "remember.click();"
                "}"
                "const submit = Array.from(document.querySelectorAll('button')).find((element) => {"
                "const text = (element.innerText || element.textContent || '').trim();"
                "return text === 'Log in' && !element.disabled;"
                "}) || Array.from(document.querySelectorAll('button[type=\"submit\"]')).find("
                "(element) => !element.disabled"
                ");"
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
            submitted = True
        except Exception:
            submitted = False

    if not submitted:
        client.fill('input[type="email"]', username)
        client.fill('input[type="password"]', password)
        if callable(evaluate):
            evaluate(
                "() => {"
                "const submit = Array.from(document.querySelectorAll('button')).find((element) => {"
                "const text = (element.innerText || element.textContent || '').trim();"
                "return text === 'Log in' && !element.disabled;"
                "}) || Array.from(document.querySelectorAll('button[type=\"submit\"]')).find("
                "(element) => !element.disabled"
                ");"
                "if (!(submit instanceof HTMLButtonElement)) {"
                "throw new Error('Smarkets login submit button not found');"
                "}"
                "submit.click();"
                "return true;"
                "}"
            )
        else:
            client.click(submit_selector)

    client.wait(1500)
    for _ in range(8):
        if "login=true" not in client.current_url():
            break
        client.wait(500)

    current_url = client.current_url()
    if "login=true" in current_url:
        client.open_url("https://smarkets.com/portfolio/")
        client.wait(1500)


def accept_smarkets_cookies(*, client) -> None:
    evaluate = getattr(client, "evaluate", None)
    if not callable(evaluate):
        return

    accepted = evaluate(
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
    if not callable(getattr(client, "evaluate", None)):
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
    evaluate = getattr(client, "evaluate", None)
    if not callable(evaluate):
        return None
    return evaluate(
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


def _with_navigation_retry(operation, *, client, attempts: int = 4, wait_ms: int = 500):
    last_error: Exception | None = None
    for attempt in range(attempts):
        try:
            return operation()
        except Exception as error:
            if not _is_transient_navigation_error(error) or attempt == attempts - 1:
                raise
            last_error = error
            client.wait(wait_ms)
    if last_error is not None:
        raise last_error
    raise RuntimeError("navigation retry exhausted without an operation result")


def _is_transient_navigation_error(error: Exception) -> bool:
    message = str(error)
    return (
        "Execution context was destroyed" in message
        or "Cannot find context with specified id" in message
        or "most likely because of a navigation" in message
    )


def _next_poll_interval_seconds(config: WatcherConfig, latest_state: dict) -> float:
    open_positions = latest_state.get("open_positions") or []
    if any(bool(position.get("is_in_play")) for position in open_positions):
        return min(float(config.interval_seconds), LIVE_POLL_INTERVAL_SECONDS)
    return float(config.interval_seconds)


def _capture_event_summaries(
    *,
    client,
    positions: list[dict],
    captured_at: datetime,
    cache: dict[str, dict] | None,
    sidecar_session: str,
) -> list[dict]:
    positions_by_url: dict[str, list[dict]] = {}
    for position in positions:
        event_url = position.get("event_url")
        if not event_url:
            continue
        positions_by_url.setdefault(str(event_url), []).append(position)

    if not positions_by_url:
        return []

    if cache is not None:
        stale_urls = set(cache) - set(positions_by_url)
        for stale_url in stale_urls:
            cache.pop(stale_url, None)

    summaries: list[dict] = []
    for event_url, event_positions in positions_by_url.items():
        refresh_seconds = _event_summary_refresh_seconds(event_positions)
        cached_entry = cache.get(event_url) if cache is not None else None
        if _event_summary_cache_is_fresh(
            cached_entry,
            captured_at=captured_at,
            refresh_seconds=refresh_seconds,
        ):
            summary = cached_entry.get("summary")
        else:
            summary = _fetch_event_summary(
                client=client,
                event_url=event_url,
                positions=event_positions,
                sidecar_session=sidecar_session,
            )
            if cache is not None:
                cache[event_url] = {
                    "captured_at": captured_at,
                    "summary": summary,
                }
        if summary is not None:
            summaries.append(summary)
    return summaries


def _event_summary_refresh_seconds(positions: list[dict]) -> float:
    if any(bool(position.get("is_in_play")) for position in positions):
        return LIVE_EVENT_SUMMARY_REFRESH_SECONDS
    return IDLE_EVENT_SUMMARY_REFRESH_SECONDS


def _event_summary_cache_is_fresh(
    cached_entry: dict | None,
    *,
    captured_at: datetime,
    refresh_seconds: float,
) -> bool:
    if not cached_entry:
        return False
    cached_at = cached_entry.get("captured_at")
    if not isinstance(cached_at, datetime):
        return False
    age_seconds = (captured_at - cached_at).total_seconds()
    return age_seconds < refresh_seconds


def _fetch_event_summary(
    *,
    client,
    event_url: str,
    positions: list[dict],
    sidecar_session: str,
) -> dict | None:
    summary = _fetch_event_summary_from_sidecar_session(
        event_url=event_url,
        positions=positions,
        sidecar_session=sidecar_session,
    )
    if summary is not None:
        return summary

    try:
        html = _with_navigation_retry(
            lambda: _fetch_event_page_html(client=client, event_url=event_url),
            client=client,
        )
    except Exception:
        return None
    return parse_event_page_summary(url=event_url, html=html)


def _fetch_event_summary_from_sidecar_session(
    *,
    event_url: str,
    positions: list[dict],
    sidecar_session: str,
) -> dict | None:
    event_client = AgentBrowserClient(session=sidecar_session)
    try:
        current_url = event_client.current_url()
    except Exception:
        current_url = ""

    try:
        if current_url != event_url:
            event_client.open_url(event_url)
            event_client.wait(EVENT_PAGE_NAVIGATION_WAIT_MS)
        body_text = str(event_client.evaluate("document.body?.innerText ?? ''") or "")
    except Exception:
        return None

    return parse_event_page_body_summary(
        url=event_url,
        body_text=body_text,
        positions=positions,
    )


def _fetch_event_page_html(*, client, event_url: str) -> str:
    return str(
        client.evaluate(
            "(() => {"
            f"const url = {json.dumps(event_url)};"
            "const request = new XMLHttpRequest();"
            "request.open('GET', url, false);"
            "request.send(null);"
            "if (request.status < 200 || request.status >= 300) {"
            "throw new Error(`Smarkets event page fetch failed: ${request.status}`);"
            "}"
            "return request.responseText;"
            "})()"
        )
        or ""
    )


def _event_summary_session_name(session: str) -> str:
    normalized = session.strip() or "smarkets"
    return f"{normalized}-event-summary"


def ensure_watcher_run_dir(run_dir: Path) -> None:
    run_dir.mkdir(parents=True, exist_ok=True)
    (run_dir / "screenshots").mkdir(parents=True, exist_ok=True)
    (run_dir / "events.jsonl").touch(exist_ok=True)
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        metadata_path.write_text(json.dumps({"source": "smarkets_exchange"}) + "\n")


def acquire_watcher_process_slot(run_dir: Path) -> None:
    pid_path = run_dir / "watcher.pid"
    conflicting_pid = _find_conflicting_watcher_pid(run_dir, pid_path)
    if conflicting_pid is not None:
        _terminate_process(conflicting_pid)
    pid_path.write_text(f"{os.getpid()}\n")


def release_watcher_process_slot(run_dir: Path) -> None:
    pid_path = run_dir / "watcher.pid"
    if not pid_path.exists():
        return
    try:
        recorded_pid = int(pid_path.read_text().strip())
    except ValueError:
        pid_path.unlink(missing_ok=True)
        return
    if recorded_pid == os.getpid():
        pid_path.unlink(missing_ok=True)


def _find_conflicting_watcher_pid(run_dir: Path, pid_path: Path) -> int | None:
    if not pid_path.exists():
        return None
    try:
        recorded_pid = int(pid_path.read_text().strip())
    except ValueError:
        return None
    if recorded_pid == os.getpid():
        return None
    if not _process_is_alive(recorded_pid):
        return None
    if not _process_matches_run_dir(recorded_pid, run_dir):
        return None
    return recorded_pid


def _process_matches_run_dir(pid: int, run_dir: Path) -> bool:
    cmdline_path = Path(f"/proc/{pid}/cmdline")
    if not cmdline_path.exists():
        return False
    try:
        command = cmdline_path.read_bytes().replace(b"\x00", b" ").decode("utf-8", "ignore")
    except Exception:
        return False
    return "watch-smarkets-session" in command and str(run_dir) in command


def _process_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _terminate_process(pid: int) -> None:
    try:
        os.kill(pid, signal.SIGTERM)
    except OSError:
        return
    for _ in range(20):
        if not _process_is_alive(pid):
            return
        time.sleep(0.1)
    try:
        os.kill(pid, signal.SIGKILL)
    except OSError:
        return
