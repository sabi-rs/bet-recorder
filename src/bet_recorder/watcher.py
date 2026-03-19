from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
import os
from pathlib import Path
import json
import signal
import time
from urllib.parse import parse_qs, urlparse

from bet_recorder.analysis.smarkets_exchange import (
    parse_event_page_body_summary,
    parse_event_page_summary,
)
from bet_recorder.analysis.position_watch import build_smarkets_watch_plan
from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.browser.models import BrowserPageState
from bet_recorder.capture.run_bundle import load_run_bundle
from bet_recorder.exchange_worker import (
    analyze_positions_payload,
    build_exchange_panel_snapshot,
)
from bet_recorder.live.agent_browser_capture import (
    capture_agent_browser_page_state,
    record_agent_browser_page_state,
)
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
PORTFOLIO_SCROLL_CAPTURE_WAIT_MS = 200
PORTFOLIO_SCROLL_CAPTURE_MAX_STEPS = 12
SMARKETS_PORTFOLIO_METADATA_KEY = "smarkets_portfolio"
SMARKETS_PORTFOLIO_EXTRACTOR_VERSION = "dom-v1"
SMARKETS_ACTIVE_PORTFOLIO_URL = (
    "https://smarkets.com/portfolio/?time=all&order-state=active"
)


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
        lambda: _capture_smarkets_open_positions_page_state(
            bundle=bundle,
            captured_at=captured_at,
            client=client,
            notes=["watcher-loop"],
        ),
        client=client,
    )
    if _payload_has_retryable_smarkets_error(payload):
        payload = _retry_smarkets_error_overlay_capture(
            client=client,
            bundle=bundle,
            captured_at=captured_at,
            original_payload=payload,
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


def _capture_smarkets_open_positions_page_state(
    *,
    bundle,
    captured_at: datetime,
    client,
    notes: list[str],
) -> dict:
    capture_page_state = getattr(client, "capture_page_state", None)
    evaluate = getattr(client, "evaluate", None)
    if not callable(capture_page_state) or not callable(evaluate):
        return capture_agent_browser_page_state(
            source="smarkets_exchange",
            bundle=bundle,
            page="open_positions",
            captured_at=captured_at,
            client=client,
            notes=notes,
        )

    states = [_capture_structured_smarkets_portfolio_state(
        capture_page_state=capture_page_state,
        client=client,
        captured_at=captured_at,
        notes=notes,
    )]
    try:
        for _ in range(PORTFOLIO_SCROLL_CAPTURE_MAX_STEPS):
            metrics = _scroll_smarkets_portfolio_page(client=client)
            if not metrics.get("advanced"):
                break
            client.wait(PORTFOLIO_SCROLL_CAPTURE_WAIT_MS)
            states.append(
                _capture_structured_smarkets_portfolio_state(
                    capture_page_state=capture_page_state,
                    client=client,
                    captured_at=captured_at,
                    notes=notes,
                )
            )
            if metrics.get("at_end"):
                break
    finally:
        _reset_smarkets_portfolio_scroll(client=client)

    merged_state = _merge_browser_page_states(states)
    return record_agent_browser_page_state(
        source="smarkets_exchange",
        bundle=bundle,
        page_state=merged_state,
    )


def _capture_structured_smarkets_portfolio_state(
    *,
    capture_page_state,
    client,
    captured_at: datetime,
    notes: list[str],
) -> BrowserPageState:
    page_state = capture_page_state(
        page="open_positions",
        captured_at=captured_at,
        screenshot_path=None,
        notes=list(notes),
    )
    metadata = dict(page_state.metadata)
    portfolio = _extract_smarkets_portfolio_structure(client=client)
    if portfolio.get("positions") or portfolio.get("groups"):
        metadata[SMARKETS_PORTFOLIO_METADATA_KEY] = portfolio
    return BrowserPageState(
        captured_at=page_state.captured_at,
        page=page_state.page,
        url=page_state.url,
        document_title=page_state.document_title,
        body_text=page_state.body_text,
        interactive_snapshot=page_state.interactive_snapshot,
        links=page_state.links,
        inputs=page_state.inputs,
        visible_actions=page_state.visible_actions,
        resource_hosts=page_state.resource_hosts,
        local_storage_keys=page_state.local_storage_keys,
        screenshot_path=page_state.screenshot_path,
        notes=page_state.notes,
        metadata=metadata,
    )


def _extract_smarkets_portfolio_structure(*, client) -> dict:
    result = client.evaluate(
        r"""(() => {
const normalizeText = (value) => (value || '').replace(/\s+/g, ' ').trim();
const textContent = (element) => normalizeText(element?.textContent || element?.innerText || '');
const bestText = (elements) => {
  const values = Array.from(elements || [])
    .map((element) => textContent(element))
    .filter(Boolean)
    .sort((left, right) => left.length - right.length);
  return values[0] || '';
};
const parseMoney = (value) => {
  const match = normalizeText(value).match(/^(-)?£([\d,.]+)$/);
  if (!match) return null;
  const amount = Number.parseFloat(match[2].replace(/,/g, ''));
  return match[1] ? -amount : amount;
};
const parseFloatValue = (value) => {
  const normalized = normalizeText(value).replace(/,/g, '');
  const parsed = Number.parseFloat(normalized);
  return Number.isFinite(parsed) ? parsed : null;
};
const parsePnl = (value) => {
  const match = normalizeText(value).match(/^([+-])£([\d,.]+)\s+\(([\d.]+)%\)$/);
  if (!match) return { pnl_amount: null, pnl_percent: null };
  const amount = Number.parseFloat(match[2].replace(/,/g, ''));
  return {
    pnl_amount: match[1] === '-' ? -amount : amount,
    pnl_percent: Number.parseFloat(match[3]),
  };
};
const cardContainers = Array.from(document.querySelectorAll('[class*="OrderListGroup_container"]'));
const groups = [];
for (const container of cardContainers) {
  const title = textContent(container.querySelector('[class*="OrderListGroup_title__"], [class*="OrderListGroup_title"]'));
  let eventTitle = bestText(container.querySelectorAll('[class*="OrderListGroup_title"]'));
  const orderCount = parseFloatValue(textContent(container.querySelector('[class*="OrderListGroup_orderCount"]')));
  if (eventTitle && orderCount !== null) {
    const orderCountLabel = String(orderCount);
    if (eventTitle.endsWith(orderCountLabel)) {
      eventTitle = normalizeText(eventTitle.slice(0, eventTitle.length - orderCountLabel.length));
    }
  }
  const labelNodes = Array.from(container.querySelectorAll('[class*="OrderListGroup_label"]')).map(textContent).filter(Boolean);
  const eventStatus = labelNodes.find((label) => label.includes('|')) || '';
  const worstOutcome = parseMoney(textContent(container.querySelector('[class*="OrderListGroup_headerWorstOutcome"] [class*="OrderListGroup_figure"], [class*="OrderListGroup_headerWorstOutcome"]')));
  const bestOutcome = parseMoney(textContent(container.querySelector('[class*="OrderListGroup_headerBestOutcome"] [class*="OrderListGroup_figure"], [class*="OrderListGroup_headerBestOutcome"]')));
  const eventUrl = (() => {
    const anchor = container.querySelector('a[href*="smarkets.com/"]');
    return anchor instanceof HTMLAnchorElement ? anchor.href : '';
  })();
  const itemContainers = Array.from(container.querySelectorAll('[class*="OrderListGroupItem_groupItemContainer"], [class*="OrderListGroupItem_container__"]'))
    .filter((element) => textContent(element).startsWith('Sell ') || textContent(element).startsWith('Buy '));
  const positions = itemContainers.map((item) => {
    const side = textContent(item.querySelector('[class*="OrderListGroupItem_orderSide"]')).toLowerCase();
    let contract = bestText(item.querySelectorAll('[class*="OrderListGroupItem_contract"]'));
    if (side && contract.toLowerCase().startsWith(`${side} `)) {
      contract = normalizeText(contract.slice(side.length + 1));
    }
    const market = textContent(item.querySelector('[class*="OrderListGroupItem_market"]'));
    const price = parseFloatValue(textContent(item.querySelector('[class*="OrderListGroupItem_colPrice"]')));
    const stakeValues = Array.from(item.querySelectorAll('[class*="OrderListGroupItem_colStake"] span, [class*="OrderListGroupItem_colStake"] div, [class*="OrderListGroupItem_colStake"]'))
      .map(textContent)
      .map(parseMoney)
      .filter((value) => value !== null);
    const stake = stakeValues[0] ?? null;
    const liability = (side === 'buy') ? stake : (stakeValues[1] ?? null);
    const returnAmount = parseMoney(textContent(item.querySelector('[class*="OrderListGroupItem_colReturn"]')));
    const currentValue = parseMoney(textContent(item.querySelector('[class*="OrderListGroupItem_colCurrentValue"] [class*="currentValueText"], [class*="OrderListGroupItem_colCurrentValue"] span:not([class*="Difference"]), [class*="OrderListGroupItem_colCurrentValue"]')));
    const pnl = parsePnl(textContent(item.querySelector('[class*="Difference"], [class*="currentValueDifference"], [class*="profitLoss"]')));
    const status = textContent(item.querySelector('[class*="OrderListGroupItem_colState"]'));
    const actionText = textContent(item.querySelector('[class*="OrderListGroupItem_colAction"]'));
    const currentBackOdds = (() => {
      const match = actionText.match(/Back\s+([\d.]+)/i);
      return match ? Number.parseFloat(match[1]) : null;
    })();
    return {
      side,
      contract,
      market,
      price,
      stake,
      liability,
      return_amount: returnAmount,
      current_value: currentValue,
      pnl_amount: pnl.pnl_amount,
      pnl_percent: pnl.pnl_percent,
      status,
      can_trade_out: /trade out/i.test(actionText),
      current_back_odds: currentBackOdds,
      event: eventTitle,
      event_status: eventStatus,
      event_url: eventUrl,
      order_count: orderCount,
      best_outcome: bestOutcome,
      worst_outcome: worstOutcome,
    };
  }).filter((position) => position.contract && position.market);
  if (!eventTitle && positions.length === 0) continue;
  groups.push({
    title: eventTitle,
    order_count: orderCount,
    event_status: eventStatus,
    event_url: eventUrl,
    best_outcome: bestOutcome,
    worst_outcome: worstOutcome,
    positions,
  });
}
return {
  extractor: 'dom-v1',
  groups,
  positions: groups.flatMap((group) => group.positions),
};
})()"""
    )
    if isinstance(result, dict):
        return result
    return {
        "extractor": SMARKETS_PORTFOLIO_EXTRACTOR_VERSION,
        "groups": [],
        "positions": [],
    }


def _scroll_smarkets_portfolio_page(*, client) -> dict:
    result = client.evaluate(
        "(() => {"
        "const candidates = [document.scrollingElement, ...document.querySelectorAll('*')].filter(Boolean);"
        "const target = candidates"
        ".filter((element) => {"
        "  const style = window.getComputedStyle(element);"
        "  const overflowY = style?.overflowY || '';"
        "  const canScroll = element.scrollHeight - element.clientHeight > 24;"
        "  const scrollable = ['auto', 'scroll', 'overlay'].includes(overflowY)"
        "    || element === document.scrollingElement;"
        "  return canScroll && scrollable && element.clientHeight >= 160;"
        "})"
        ".sort((left, right) => (right.scrollHeight - right.clientHeight) - (left.scrollHeight - left.clientHeight))[0]"
        " || document.scrollingElement || document.documentElement;"
        "if (!(target instanceof Element) && target !== document.scrollingElement && target !== document.documentElement) {"
        "  return { advanced: false, at_end: true, top: 0, max_top: 0 };"
        "}"
        "const viewport = Math.max(target.clientHeight || window.innerHeight || 0, 1);"
        "const maxTop = Math.max((target.scrollHeight || 0) - (target.clientHeight || 0), 0);"
        "const before = target === document.scrollingElement ? (window.scrollY || 0) : target.scrollTop;"
        "const next = Math.min(before + Math.max(Math.floor(viewport * 0.85), 200), maxTop);"
        "if (target === document.scrollingElement) {"
        "  window.scrollTo(0, next);"
        "} else {"
        "  target.scrollTop = next;"
        "}"
        "const after = target === document.scrollingElement ? (window.scrollY || 0) : target.scrollTop;"
        "return { advanced: after > before + 4, at_end: after >= maxTop - 4, top: after, max_top: maxTop };"
        "})()"
    )
    return result if isinstance(result, dict) else {"advanced": False, "at_end": True}


def _reset_smarkets_portfolio_scroll(*, client) -> None:
    evaluate = getattr(client, "evaluate", None)
    if not callable(evaluate):
        return
    try:
        evaluate(
            "(() => {"
            "const candidates = [document.scrollingElement, ...document.querySelectorAll('*')].filter(Boolean);"
            "const target = candidates"
            ".filter((element) => {"
            "  const style = window.getComputedStyle(element);"
            "  const overflowY = style?.overflowY || '';"
            "  const canScroll = element.scrollHeight - element.clientHeight > 24;"
            "  const scrollable = ['auto', 'scroll', 'overlay'].includes(overflowY)"
            "    || element === document.scrollingElement;"
            "  return canScroll && scrollable && element.clientHeight >= 160;"
            "})"
            ".sort((left, right) => (right.scrollHeight - right.clientHeight) - (left.scrollHeight - left.clientHeight))[0]"
            " || document.scrollingElement || document.documentElement;"
            "if (target === document.scrollingElement) {"
            "  window.scrollTo(0, 0);"
            "} else if (target) {"
            "  target.scrollTop = 0;"
            "}"
            "return true;"
            "})()"
        )
    except Exception:
        return


def _merge_browser_page_states(states: list[BrowserPageState]) -> BrowserPageState:
    if len(states) == 1:
        return states[0]

    base = states[0]
    interactive_snapshot = _dedupe_interactive_snapshots(states)
    body_parts: list[str] = []
    links: list[str] = []
    visible_actions: list[str] = []
    resource_hosts: list[str] = []
    local_storage_keys: list[str] = []
    inputs = dict(base.inputs)
    metadata = _merge_browser_page_state_metadata(states)

    for state in states:
        if state.body_text and state.body_text not in body_parts:
            body_parts.append(state.body_text)
        links.extend(state.links)
        visible_actions.extend(state.visible_actions)
        resource_hosts.extend(state.resource_hosts)
        local_storage_keys.extend(state.local_storage_keys)
        inputs.update(state.inputs)

    return BrowserPageState(
        captured_at=base.captured_at,
        page=base.page,
        url=states[-1].url,
        document_title=states[-1].document_title,
        body_text="\n".join(body_parts),
        interactive_snapshot=interactive_snapshot,
        links=list(dict.fromkeys(links)),
        inputs=inputs,
        visible_actions=list(dict.fromkeys(visible_actions)),
        resource_hosts=list(dict.fromkeys(resource_hosts)),
        local_storage_keys=list(dict.fromkeys(local_storage_keys)),
        screenshot_path=base.screenshot_path,
        notes=[*base.notes, "scroll-capture"],
        metadata=metadata,
    )


def _dedupe_interactive_snapshots(states: list[BrowserPageState]) -> list[dict]:
    deduped: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for state in states:
        for item in state.interactive_snapshot:
            key = (
                str(item.get("role", "") or ""),
                str(item.get("name", "") or ""),
            )
            if key in seen:
                continue
            seen.add(key)
            deduped.append(item)
    return deduped


def _merge_browser_page_state_metadata(states: list[BrowserPageState]) -> dict:
    merged: dict = {}
    portfolio_groups: list[dict] = []
    portfolio_positions: list[dict] = []
    seen_groups: set[tuple[str, str, int | None]] = set()
    seen_positions: set[tuple] = set()
    extractor = SMARKETS_PORTFOLIO_EXTRACTOR_VERSION

    for state in states:
        for key, value in state.metadata.items():
            if key != SMARKETS_PORTFOLIO_METADATA_KEY:
                merged[key] = value
        portfolio = state.metadata.get(SMARKETS_PORTFOLIO_METADATA_KEY)
        if not isinstance(portfolio, dict):
            continue
        extractor = str(portfolio.get("extractor") or extractor)
        for group in portfolio.get("groups") or []:
            if not isinstance(group, dict):
                continue
            group_key = (
                str(group.get("title", "") or ""),
                str(group.get("event_status", "") or ""),
                group.get("order_count"),
            )
            if group_key not in seen_groups:
                seen_groups.add(group_key)
                portfolio_groups.append(group)
        for position in portfolio.get("positions") or []:
            if not isinstance(position, dict):
                continue
            position_key = (
                str(position.get("side", "") or ""),
                str(position.get("contract", "") or ""),
                str(position.get("market", "") or ""),
                str(position.get("event", "") or ""),
                position.get("price"),
                position.get("stake"),
                position.get("liability"),
                position.get("return_amount"),
            )
            if position_key not in seen_positions:
                seen_positions.add(position_key)
                portfolio_positions.append(position)

    if portfolio_groups or portfolio_positions:
        merged[SMARKETS_PORTFOLIO_METADATA_KEY] = {
            "extractor": extractor,
            "groups": portfolio_groups,
            "positions": portfolio_positions,
        }
    return merged


def _retry_smarkets_error_overlay_capture(
    *, client, bundle, captured_at: datetime, original_payload: dict
) -> dict:
    payload: dict | None = None
    for _ in range(2):
        if not _click_smarkets_try_again(client=client):
            break
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
                notes=["watcher-loop", "retry-after-error"],
            ),
            client=client,
        )
        if not _payload_has_retryable_smarkets_error(payload):
            return payload
    return payload or original_payload


def _click_smarkets_try_again(*, client) -> bool:
    evaluate = getattr(client, "evaluate", None)
    if not callable(evaluate):
        return False

    clicked = evaluate(
        "(() => {"
        "const button = Array.from(document.querySelectorAll('button, [role=\"button\"]')).find((element) => {"
        "const text = (element.innerText || element.textContent || '').trim();"
        "return text === 'Try Again';"
        "});"
        "if (!(button instanceof HTMLElement)) {"
        "return false;"
        "}"
        "button.click();"
        "return true;"
        "})()"
    )
    if clicked:
        client.wait(1500)
    return bool(clicked)


def bootstrap_smarkets_page(*, client, profile_path: Path | None) -> None:
    current_url = client.current_url()
    if _is_smarkets_active_portfolio_url(current_url) or "open-positions" in current_url:
        return
    client.open_url(SMARKETS_ACTIVE_PORTFOLIO_URL)
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
                "const form = submit.form || submit.closest('form');"
                "setTimeout(() => {"
                "  if (form instanceof HTMLFormElement && typeof form.requestSubmit === 'function') {"
                "    form.requestSubmit(submit);"
                "    return;"
                "  }"
                "  submit.click();"
                "}, 0);"
                "return {"
                "  submitted: true,"
                "  method: (form instanceof HTMLFormElement && typeof form.requestSubmit === 'function') ? 'requestSubmit' : 'click',"
                "  rememberMeChecked: !!(remember && remember.checked)"
                "};"
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
                "const form = submit.form || submit.closest('form');"
                "if (form instanceof HTMLFormElement && typeof form.requestSubmit === 'function') {"
                "form.requestSubmit(submit);"
                "return { submitted: true, method: 'requestSubmit' };"
                "}"
                "submit.click();"
                "return { submitted: true, method: 'click' };"
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
        client.open_url(SMARKETS_ACTIVE_PORTFOLIO_URL)
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


def _is_smarkets_active_portfolio_url(url: str) -> bool:
    if not url.startswith("https://smarkets.com/portfolio"):
        return False
    parsed = urlparse(url)
    query = parse_qs(parsed.query)
    return (
        query.get("order-state", [None])[0] == "active"
        and query.get("time", [None])[0] == "all"
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
    elif _payload_has_retryable_smarkets_error(
        {"body_text": body_text, "visible_actions": visible_actions}
    ):
        page_hint = "error"
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
    if session.get("page_hint") == "error":
        return message + " Smarkets returned an in-app error state; retry the portfolio view."
    return message


def _payload_has_retryable_smarkets_error(payload: dict | None) -> bool:
    if payload is None:
        return False
    body_text = str(payload.get("body_text", "") or "").lower()
    visible_actions = [
        str(action).strip().lower()
        for action in (payload.get("visible_actions") or [])
    ]
    return "something went wrong" in body_text and "try again" in visible_actions


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
