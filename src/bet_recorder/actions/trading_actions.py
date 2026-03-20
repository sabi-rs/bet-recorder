from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlparse

from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.capture.operator_interaction import append_operator_interaction_event
from bet_recorder.capture.run_bundle import load_run_bundle
from bet_recorder.live.agent_browser_capture import capture_agent_browser_action
from bet_recorder.transport.writer import append_transport_marker

SMARKETS_HOSTS = {"smarkets.com", "www.smarkets.com"}
SMARKETS_PORTFOLIO_URL = "https://smarkets.com/portfolio/?time=all&order-state=active"
SMARKETS_PRIMARY_MARKET_SELECTOR = (
    "div[class*='CompetitorsEventPrimaryMarket_primaryContracts']"
)
SMARKETS_CONTRACT_ROW_SELECTOR = "div[class*='ContractRow_row']"
SMARKETS_SLIP_CONTRACT_SELECTOR = "[aria-label='Selected contract']"
SMARKETS_SLIP_SIDE_SELECTOR = "button[aria-label='Toggle side']"
SMARKETS_STAKE_INPUT_SELECTOR = "input[aria-label='Stake input']"
SMARKETS_PLACE_BET_LABEL = "Place bet"
SUBMISSION_POLL_INTERVAL_MS = 125
PORTFOLIO_POLL_INTERVAL_MS = 200


@dataclass(frozen=True)
class TradingExecutionPolicy:
    time_in_force: str
    cancel_unmatched_after_ms: int
    require_full_fill: bool
    max_price_drift: float

    @classmethod
    def from_dict(cls, payload: dict | None) -> TradingExecutionPolicy:
        normalized = payload or {}
        time_in_force = str(
            normalized.get("time_in_force") or "good_til_cancel"
        ).strip()
        if time_in_force not in {"good_til_cancel", "fill_or_kill"}:
            raise ValueError("Trading action time_in_force is invalid.")
        try:
            cancel_unmatched_after_ms = int(
                normalized.get("cancel_unmatched_after_ms") or 0
            )
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Trading action cancel_unmatched_after_ms must be numeric."
            ) from exc
        try:
            max_price_drift = float(normalized.get("max_price_drift") or 0.0)
        except (TypeError, ValueError) as exc:
            raise ValueError("Trading action max_price_drift must be numeric.") from exc
        return cls(
            time_in_force=time_in_force,
            cancel_unmatched_after_ms=max(cancel_unmatched_after_ms, 0),
            require_full_fill=bool(normalized.get("require_full_fill")),
            max_price_drift=max(max_price_drift, 0.0),
        )


@dataclass(frozen=True)
class TradingActionSourceContext:
    is_in_play: bool
    event_status: str
    market_status: str
    live_clock: str
    can_trade_out: bool
    current_pnl_amount: float | None
    baseline_stake: float | None
    baseline_liability: float | None
    baseline_price: float | None

    @classmethod
    def from_dict(cls, payload: dict | None) -> TradingActionSourceContext:
        normalized = payload or {}
        return cls(
            is_in_play=bool(normalized.get("is_in_play")),
            event_status=str(normalized.get("event_status") or "").strip(),
            market_status=str(normalized.get("market_status") or "").strip(),
            live_clock=str(normalized.get("live_clock") or "").strip(),
            can_trade_out=bool(normalized.get("can_trade_out")),
            current_pnl_amount=_optional_float(normalized.get("current_pnl_amount")),
            baseline_stake=_optional_float(normalized.get("baseline_stake")),
            baseline_liability=_optional_float(normalized.get("baseline_liability")),
            baseline_price=_optional_float(normalized.get("baseline_price")),
        )


@dataclass(frozen=True)
class TradingActionRiskReport:
    summary: str
    warning_count: int
    blocking_review_count: int
    blocking_submit_count: int
    reduce_only: bool
    checks: list[dict]

    @classmethod
    def from_dict(cls, payload: dict | None) -> TradingActionRiskReport:
        normalized = payload or {}
        checks = normalized.get("checks", [])
        return cls(
            summary=str(normalized.get("summary") or "").strip(),
            warning_count=int(normalized.get("warning_count") or 0),
            blocking_review_count=int(normalized.get("blocking_review_count") or 0),
            blocking_submit_count=int(normalized.get("blocking_submit_count") or 0),
            reduce_only=bool(normalized.get("reduce_only")),
            checks=[dict(item) for item in checks if isinstance(item, dict)],
        )


@dataclass(frozen=True)
class TradingActionIntent:
    action_kind: str
    source: str
    venue: str
    mode: str
    side: str
    request_id: str
    source_ref: str
    event_name: str
    market_name: str
    selection_name: str
    stake: float
    expected_price: float
    event_url: str | None
    deep_link_url: str | None
    betslip_market_id: str | None
    betslip_selection_id: str | None
    execution_policy: TradingExecutionPolicy
    risk_report: TradingActionRiskReport
    source_context: TradingActionSourceContext
    notes: list[str]

    @classmethod
    def from_dict(cls, payload: dict) -> TradingActionIntent:
        if payload.get("action_kind") != "place_bet":
            raise ValueError("Trading action must currently be place_bet.")
        venue = str(payload.get("venue") or "").strip()
        if not venue:
            raise ValueError("Trading action must include a venue.")
        mode = str(payload.get("mode") or "").strip().lower()
        if mode not in {"review", "confirm"}:
            raise ValueError("Trading action mode must be review or confirm.")
        side = str(payload.get("side") or "").strip().lower()
        if side not in {"buy", "sell"}:
            raise ValueError("Trading action side must be buy or sell.")
        request_id = str(payload.get("request_id") or "").strip()
        if not request_id:
            raise ValueError("Trading action request_id is required.")
        selection_name = str(payload.get("selection_name") or "").strip()
        if not selection_name:
            raise ValueError("Trading action selection_name is required.")
        market_name = str(payload.get("market_name") or "").strip()
        if not market_name:
            raise ValueError("Trading action market_name is required.")
        event_name = str(payload.get("event_name") or "").strip()
        if not event_name:
            raise ValueError("Trading action event_name is required.")
        try:
            stake = float(payload["stake"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Trading action stake must be numeric.") from exc
        if stake <= 0:
            raise ValueError("Trading action stake must be greater than zero.")
        try:
            expected_price = float(payload["expected_price"])
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError("Trading action expected_price must be numeric.") from exc
        if expected_price <= 1.0:
            raise ValueError("Trading action expected_price must be greater than 1.0.")

        event_url = _optional_string(payload.get("event_url"))
        deep_link_url = _optional_string(payload.get("deep_link_url"))
        if event_url is None and deep_link_url is None:
            raise ValueError("Trading action requires an event_url or deep_link_url.")

        return cls(
            action_kind="place_bet",
            source=str(payload.get("source") or "").strip(),
            venue=venue,
            mode=mode,
            side=side,
            request_id=request_id,
            source_ref=str(payload.get("source_ref") or "").strip(),
            event_name=event_name,
            market_name=market_name,
            selection_name=selection_name,
            stake=stake,
            expected_price=expected_price,
            event_url=event_url,
            deep_link_url=deep_link_url,
            betslip_market_id=_optional_string(payload.get("betslip_market_id")),
            betslip_selection_id=_optional_string(payload.get("betslip_selection_id")),
            execution_policy=TradingExecutionPolicy.from_dict(
                payload.get("execution_policy")
                if isinstance(payload.get("execution_policy"), dict)
                else None
            ),
            risk_report=TradingActionRiskReport.from_dict(
                payload.get("risk_report")
                if isinstance(payload.get("risk_report"), dict)
                else None
            ),
            source_context=TradingActionSourceContext.from_dict(
                payload.get("source_context")
                if isinstance(payload.get("source_context"), dict)
                else None
            ),
            notes=[str(note) for note in payload.get("notes", []) if str(note).strip()],
        )


@dataclass(frozen=True)
class TradingActionResult:
    detail: str
    action_status: str


def execute_trading_action(
    *,
    intent_payload: dict,
    agent_browser_session: str | None,
    run_dir: Path | None,
) -> TradingActionResult:
    intent = TradingActionIntent.from_dict(intent_payload)
    if intent.venue != "smarkets":
        raise ValueError(f"Unsupported trading action venue: {intent.venue}")
    if not agent_browser_session:
        raise ValueError(
            "Trading action execution requires agent_browser_session in worker config."
        )

    client = AgentBrowserClient(session=agent_browser_session)
    _record_trading_action_marker(
        run_dir=run_dir,
        intent=intent,
        phase="request",
        status="requested",
        detail=(
            f"{intent.mode} {intent.side} {intent.selection_name} "
            f"stake {intent.stake:.2f} at {intent.expected_price:.2f}"
        ),
    )

    try:
        target_url = intent.deep_link_url or intent.event_url
        assert_smarkets_target_url(target_url)
        client.open_url(target_url)
        client.wait(1200)

        if not _betslip_is_visible(client):
            if intent.event_url is None:
                raise ValueError(
                    "Trading action deep link did not expose a populated Smarkets bet slip."
                )
            _populate_smarkets_bet_slip(client=client, intent=intent)

        _set_smarkets_stake(client=client, stake=intent.stake)
        verification = _verify_smarkets_bet_slip(client=client)
        _assert_smarkets_bet_slip_matches_intent(intent=intent, verification=verification)

        if intent.mode == "review":
            result = TradingActionResult(
                detail=(
                    f"Smarkets {intent.side} {intent.selection_name} stake {intent.stake:.2f} "
                    f"loaded in review mode for {intent.event_name}."
                ),
                action_status="review_ready",
            )
        else:
            _click_smarkets_place_bet(client=client)
            client.wait(600)
            result = _resolve_post_submit_result(client=client, intent=intent)

        _record_trading_action(
            client=client,
            intent=intent,
            run_dir=run_dir,
            action_status=result.action_status,
        )
        _record_trading_action_marker(
            run_dir=run_dir,
            intent=intent,
            phase="response",
            status=result.action_status,
            detail=result.detail,
        )
        return result
    except Exception as exc:
        _record_trading_action_marker(
            run_dir=run_dir,
            intent=intent,
            phase="response",
            status="error",
            detail=str(exc),
        )
        raise


def assert_smarkets_target_url(url: str | None) -> None:
    if not url:
        raise ValueError("Trading action target URL is missing.")
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("Trading action target URL must be http or https.")
    if parsed.hostname not in SMARKETS_HOSTS:
        raise ValueError("Trading action target URL must point to smarkets.com.")


def _resolve_post_submit_result(
    *,
    client: AgentBrowserClient,
    intent: TradingActionIntent,
) -> TradingActionResult:
    if intent.execution_policy.time_in_force == "fill_or_kill":
        return _complete_smarkets_fill_or_kill(client=client, intent=intent)

    return TradingActionResult(
        detail=(
            f"Smarkets {intent.side} {intent.selection_name} stake {intent.stake:.2f} "
            f"submitted for {intent.event_name}."
        ),
        action_status="submitted",
    )


def _complete_smarkets_fill_or_kill(
    *,
    client: AgentBrowserClient,
    intent: TradingActionIntent,
) -> TradingActionResult:
    submission = _poll_smarkets_submission_state(
        client=client,
        timeout_ms=max(intent.execution_policy.cancel_unmatched_after_ms, 750),
    )
    if submission["state"] == "filled":
        return TradingActionResult(
            detail=(
                f"Smarkets {intent.side} {intent.selection_name} stake {intent.stake:.2f} "
                f"fully matched under synthetic fill-or-kill for {intent.event_name}."
            ),
            action_status="filled",
        )

    _open_smarkets_portfolio(client=client)
    _set_smarkets_activity_filter(client=client, label="All active")
    deadline = datetime.now(UTC).timestamp() + (
        max(intent.execution_policy.cancel_unmatched_after_ms, 750) / 1000.0
    )
    latest_state: dict[str, object] | None = None
    while datetime.now(UTC).timestamp() <= deadline:
        latest_state = _inspect_smarkets_portfolio_order_state(client=client, intent=intent)
        if latest_state["unmatched_present"]:
            _cancel_smarkets_portfolio_order(client=client, intent=intent)
            client.wait(750)
            latest_state = _inspect_smarkets_portfolio_order_state(
                client=client,
                intent=intent,
            )
            if latest_state["unmatched_present"]:
                raise ValueError(
                    "Synthetic fill-or-kill cancel left unmatched residue on the portfolio page."
                )
            if submission["state"] == "partial" or latest_state["filled_present"]:
                return TradingActionResult(
                    detail=(
                        f"Smarkets {intent.side} {intent.selection_name} stake {intent.stake:.2f} "
                        f"was partially matched and the unmatched residue was cancelled."
                    ),
                    action_status="partially_filled_killed",
                )
            return TradingActionResult(
                detail=(
                    f"Smarkets {intent.side} {intent.selection_name} stake {intent.stake:.2f} "
                    f"did not fill and was cancelled under synthetic fill-or-kill."
                ),
                action_status="killed_unmatched",
            )
        if latest_state["filled_present"]:
            return TradingActionResult(
                detail=(
                    f"Smarkets {intent.side} {intent.selection_name} stake {intent.stake:.2f} "
                    f"fully matched under synthetic fill-or-kill for {intent.event_name}."
                ),
                action_status="filled",
            )
        client.wait(PORTFOLIO_POLL_INTERVAL_MS)

    latest_state = latest_state or _inspect_smarkets_portfolio_order_state(
        client=client,
        intent=intent,
    )
    if latest_state["unmatched_present"]:
        _cancel_smarkets_portfolio_order(client=client, intent=intent)
        client.wait(750)
        latest_state = _inspect_smarkets_portfolio_order_state(client=client, intent=intent)
        if latest_state["unmatched_present"]:
            raise ValueError(
                "Synthetic fill-or-kill timed out and the unmatched residue could not be cancelled."
            )
        return TradingActionResult(
            detail=(
                f"Smarkets {intent.side} {intent.selection_name} stake {intent.stake:.2f} "
                f"timed out and the unmatched residue was cancelled."
            ),
            action_status="killed_unmatched",
        )
    if latest_state["filled_present"] or submission["state"] == "filled":
        return TradingActionResult(
            detail=(
                f"Smarkets {intent.side} {intent.selection_name} stake {intent.stake:.2f} "
                f"fully matched under synthetic fill-or-kill for {intent.event_name}."
            ),
            action_status="filled",
        )
    raise ValueError(
        "Synthetic fill-or-kill could not prove whether the order filled or was cancelled."
    )


def _poll_smarkets_submission_state(
    *,
    client: AgentBrowserClient,
    timeout_ms: int,
) -> dict[str, str]:
    deadline = datetime.now(UTC).timestamp() + (timeout_ms / 1000.0)
    latest_state = {"state": "unknown", "message": ""}
    while datetime.now(UTC).timestamp() <= deadline:
        latest_state = _read_smarkets_submission_state(client=client)
        if latest_state["state"] != "unknown":
            return latest_state
        client.wait(SUBMISSION_POLL_INTERVAL_MS)
    return latest_state


def _read_smarkets_submission_state(*, client: AgentBrowserClient) -> dict[str, str]:
    result = client.evaluate(
        "(() => {"
        "const text = document.body ? (document.body.innerText || '') : '';"
        "if (/Bet Fully Matched \\(Pending\\)/i.test(text)) {"
        "  return { state: 'filled', message: 'Bet Fully Matched (Pending)' };"
        "}"
        "if (/Bet Fully Matched/i.test(text) || /Bet Match Confirmed/i.test(text)) {"
        "  return { state: 'filled', message: 'Bet Fully Matched' };"
        "}"
        "if (/Bet Partially Matched/i.test(text)) {"
        "  return { state: 'partial', message: 'Bet Partially Matched' };"
        "}"
        "if (/unmatched/i.test(text) && /bet/i.test(text)) {"
        "  return { state: 'working', message: 'Unmatched' };"
        "}"
        "return { state: 'unknown', message: '' };"
        "})()"
    )
    if not isinstance(result, dict):
        raise ValueError("Smarkets submission status probe did not return a payload.")
    return {
        "state": str(result.get("state") or "unknown"),
        "message": str(result.get("message") or ""),
    }


def _open_smarkets_portfolio(*, client: AgentBrowserClient) -> None:
    current_url = client.current_url()
    if not current_url.startswith("https://smarkets.com/portfolio"):
        client.open_url(SMARKETS_PORTFOLIO_URL)
        client.wait(1200)


def _set_smarkets_activity_filter(*, client: AgentBrowserClient, label: str) -> None:
    current_filter = _current_smarkets_activity_filter(client=client)
    if current_filter == label or current_filter is None:
        return
    result = client.evaluate(
        "(() => {"
        f"const target = {_js_string(label)};"
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
        "  throw new Error('Smarkets activity filter not found');"
        "}"
        "combobox.click();"
        "const clickOption = () => {"
        "  const option = Array.from(document.querySelectorAll('[role=\"option\"]')).find("
        "    (element) => (element.innerText || '').trim() === target"
        "  );"
        "  if (!(option instanceof HTMLElement)) {"
        "    setTimeout(clickOption, 50);"
        "    return;"
        "  }"
        "  option.click();"
        "};"
        "setTimeout(clickOption, 0);"
        "return true;"
        "})()"
    )
    if result is not True:
        raise ValueError("Smarkets activity filter could not be updated.")
    client.wait(750)
    if _current_smarkets_activity_filter(client=client) != label:
        raise ValueError(
            f"Smarkets activity filter is not ready: expected {label!r}."
        )


def _current_smarkets_activity_filter(*, client: AgentBrowserClient) -> str | None:
    result = client.evaluate(
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
    return _optional_string(result)


def _inspect_smarkets_portfolio_order_state(
    *,
    client: AgentBrowserClient,
    intent: TradingActionIntent,
) -> dict[str, object]:
    result = client.evaluate(_smarkets_portfolio_match_script(intent=intent, click_cancel=False))
    if not isinstance(result, dict):
        raise ValueError("Smarkets portfolio probe did not return a payload.")
    return {
        "unmatched_present": bool(result.get("unmatchedPresent")),
        "filled_present": bool(result.get("filledPresent")),
        "cancel_text": str(result.get("cancelText") or ""),
        "trade_out_text": str(result.get("tradeOutText") or ""),
        "body_text": str(result.get("bodyText") or ""),
    }


def _cancel_smarkets_portfolio_order(
    *,
    client: AgentBrowserClient,
    intent: TradingActionIntent,
) -> None:
    result = client.evaluate(_smarkets_portfolio_match_script(intent=intent, click_cancel=True))
    if not isinstance(result, dict) or not result.get("cancelClicked"):
        raise ValueError("Smarkets unmatched order cancel button was not found.")


def _smarkets_portfolio_match_script(
    *,
    intent: TradingActionIntent,
    click_cancel: bool,
) -> str:
    return (
        "(() => {"
        f"const selection = {_js_string(intent.selection_name.lower())};"
        f"const market = {_js_string(intent.market_name.lower())};"
        f"const side = {_js_string(intent.side.lower())};"
        f"const stake = {_js_string(f'£{intent.stake:.2f}'.lower())};"
        f"const price = {_js_string(f'{intent.expected_price:.2f}'.lower())};"
        f"const clickCancel = {str(click_cancel).lower()};"
        "const normalize = (value) => String(value || '').toLowerCase().replace(/\\s+/g, ' ').trim();"
        "const signals = [selection, market];"
        "const hasRequiredTerms = (text) => signals.every((term) => !term || text.includes(term));"
        "const hasPricingSignal = (text) => text.includes(stake) || text.includes(price) || text.includes(side);"
        "const containerTextFor = (element) => {"
        "  let node = element;"
        "  for (let depth = 0; node && depth < 8; depth += 1, node = node.parentElement) {"
        "    const text = normalize(node.innerText || '');"
        "    if (!text || text.length > 900) {"
        "      continue;"
        "    }"
        "    if (hasRequiredTerms(text) && hasPricingSignal(text)) {"
        "      return { node, text };"
        "    }"
        "  }"
        "  return null;"
        "};"
        "const bodyText = document.body ? (document.body.innerText || '') : '';"
        "const actionButtons = Array.from(document.querySelectorAll('button, [role=\"button\"]'));"
        "let cancelMatch = null;"
        "let tradeOutMatch = null;"
        "for (const button of actionButtons) {"
        "  const label = normalize(button.innerText || button.textContent || '');"
        "  if (label !== 'cancel' && label !== 'trade out') {"
        "    continue;"
        "  }"
        "  const match = containerTextFor(button);"
        "  if (!match) {"
        "    continue;"
        "  }"
        "  if (label === 'cancel' && cancelMatch === null) {"
        "    cancelMatch = { button, text: match.text };"
        "  }"
        "  if (label === 'trade out' && tradeOutMatch === null) {"
        "    tradeOutMatch = { button, text: match.text };"
        "  }"
        "}"
        "if (clickCancel && cancelMatch && cancelMatch.button instanceof HTMLElement) {"
        "  cancelMatch.button.click();"
        "}"
        "return {"
        "  cancelClicked: !!(clickCancel && cancelMatch),"
        "  unmatchedPresent: !!cancelMatch,"
        "  filledPresent: !!tradeOutMatch,"
        "  cancelText: cancelMatch ? cancelMatch.text : '',"
        "  tradeOutText: tradeOutMatch ? tradeOutMatch.text : '',"
        "  bodyText"
        "};"
        "})()"
    )


def _populate_smarkets_bet_slip(
    *,
    client: AgentBrowserClient,
    intent: TradingActionIntent,
) -> None:
    _wait_for(
        client=client,
        predicate_js=(
            "(() => document.querySelectorAll("
            f"{_js_string(SMARKETS_CONTRACT_ROW_SELECTOR)}"
            ").length > 0)()"
        ),
        timeout_ms=6000,
        error_message="Smarkets contract rows did not load in time.",
    )

    result = client.evaluate(
        "(() => {"
        f"const selector = {_js_string(intent.selection_name)};"
        f"const side = {_js_string(intent.side)};"
        f"const primarySelector = {_js_string(SMARKETS_PRIMARY_MARKET_SELECTOR)};"
        f"const rowSelector = {_js_string(SMARKETS_CONTRACT_ROW_SELECTOR)};"
        "const primary = document.querySelector(primarySelector);"
        "const containers = primary ? [primary] : [document];"
        "for (const container of containers) {"
        "  const rows = Array.from(container.querySelectorAll(rowSelector));"
        "  const row = rows.find((candidate) => "
        "    (candidate.innerText || '').split('\\n').map((value) => value.trim()).includes(selector)"
        "  );"
        "  if (!row) {"
        "    continue;"
        "  }"
        "  const button = row.querySelector(`button[class*='BetButton_${side}']`);"
        "  if (!(button instanceof HTMLElement)) {"
        "    throw new Error(`Requested ${side} button was not found for ${selector}`);"
        "  }"
        "  button.click();"
        "  return { rowText: row.innerText || '' };"
        "}"
        "throw new Error(`Smarkets contract row was not found for ${selector}`);"
        "})()"
    )
    if not isinstance(result, dict):
        raise ValueError("Smarkets market interaction did not return a contract row.")

    _wait_for(
        client=client,
        predicate_js=(
            "(() => {"
            f"const contract = document.querySelector({_js_string(SMARKETS_SLIP_CONTRACT_SELECTOR)});"
            "return contract && (contract.innerText || '').trim().length > 0;"
            "})()"
        ),
        timeout_ms=5000,
        error_message="Smarkets bet slip did not populate after selecting the contract.",
    )


def _set_smarkets_stake(*, client: AgentBrowserClient, stake: float) -> None:
    _wait_for(
        client=client,
        predicate_js=(
            "(() => {"
            f"const input = document.querySelector({_js_string(SMARKETS_STAKE_INPUT_SELECTOR)});"
            "return input instanceof HTMLInputElement;"
            "})()"
        ),
        timeout_ms=5000,
        error_message="Smarkets stake input was not found.",
    )
    client.set_input_value(SMARKETS_STAKE_INPUT_SELECTOR, f"{stake:.2f}")


def _verify_smarkets_bet_slip(*, client: AgentBrowserClient) -> dict:
    result = client.evaluate(
        "(() => {"
        f"const contract = document.querySelector({_js_string(SMARKETS_SLIP_CONTRACT_SELECTOR)});"
        f"const side = document.querySelector({_js_string(SMARKETS_SLIP_SIDE_SELECTOR)});"
        f"const stakeInput = document.querySelector({_js_string(SMARKETS_STAKE_INPUT_SELECTOR)});"
        "const bodyText = document.body ? document.body.innerText || '' : '';"
        "const priceMatch = bodyText.match(/Current price\\s*:?\\s*(\\d+(?:\\.\\d+)?)/i);"
        "return {"
        "  contractText: contract ? (contract.innerText || '').trim() : '',"
        "  sideText: side ? (side.innerText || '').trim() : '',"
        "  stakeValue: stakeInput instanceof HTMLInputElement ? stakeInput.value.trim() : '',"
        "  priceValue: priceMatch ? priceMatch[1] : '',"
        "  bodyText"
        "};"
        "})()"
    )
    if not isinstance(result, dict):
        raise ValueError("Smarkets bet slip verification did not return a payload.")
    return result


def _assert_smarkets_bet_slip_matches_intent(
    *,
    intent: TradingActionIntent,
    verification: dict,
) -> None:
    contract_text = str(verification.get("contractText") or "").strip()
    side_text = str(verification.get("sideText") or "").strip().lower()
    stake_value = str(verification.get("stakeValue") or "").strip()
    price_value = str(verification.get("priceValue") or "").strip()

    if intent.selection_name not in contract_text:
        raise ValueError(
            f"Smarkets bet slip contract mismatch: expected {intent.selection_name!r}, "
            f"got {contract_text!r}."
        )
    if intent.side not in side_text:
        raise ValueError(
            f"Smarkets bet slip side mismatch: expected {intent.side!r}, got {side_text!r}."
        )
    if stake_value != f"{intent.stake:.2f}":
        raise ValueError(
            f"Smarkets bet slip stake mismatch: expected {intent.stake:.2f}, got {stake_value!r}."
        )
    if not price_value:
        raise ValueError("Smarkets bet slip did not expose a current price.")
    try:
        observed_price = float(price_value)
    except ValueError as exc:
        raise ValueError(
            f"Smarkets bet slip current price was not numeric: {price_value!r}."
        ) from exc
    if abs(observed_price - intent.expected_price) > intent.execution_policy.max_price_drift:
        raise ValueError(
            "Smarkets bet slip quote drift exceeded the Rust-authored execution policy."
        )


def _click_smarkets_place_bet(*, client: AgentBrowserClient) -> None:
    result = client.evaluate(
        "(() => {"
        "const button = Array.from(document.querySelectorAll('button')).find("
        f"  (candidate) => ((candidate.innerText || '').trim() === {_js_string(SMARKETS_PLACE_BET_LABEL)})"
        ");"
        "if (!(button instanceof HTMLElement)) {"
        "  throw new Error('Smarkets Place bet button was not found');"
        "}"
        "button.click();"
        "return true;"
        "})()"
    )
    if result is not True:
        raise ValueError("Smarkets Place bet action did not complete.")


def _record_trading_action(
    *,
    client: AgentBrowserClient,
    intent: TradingActionIntent,
    run_dir: Path | None,
    action_status: str,
) -> None:
    if run_dir is None:
        return
    bundle = load_run_bundle(source="smarkets_exchange", run_dir=run_dir)
    bundle.screenshots_dir.mkdir(parents=True, exist_ok=True)
    bundle.events_path.parent.mkdir(parents=True, exist_ok=True)
    bundle.events_path.touch(exist_ok=True)
    if not bundle.metadata_path.exists():
        bundle.metadata_path.touch()

    capture_agent_browser_action(
        source="smarkets_exchange",
        bundle=bundle,
        page="confirmation" if intent.mode == "confirm" else "betslip",
        action="place_bet",
        target="smarkets_place_bet",
        status=action_status,
        captured_at=datetime.now(UTC),
        client=client,
        notes=[*intent.notes, f"request:{intent.request_id}"],
        metadata={
            "request_id": intent.request_id,
            "source": intent.source,
            "source_ref": intent.source_ref,
            "venue": intent.venue,
            "mode": intent.mode,
            "side": intent.side,
            "stake": intent.stake,
            "expected_price": intent.expected_price,
            "event_name": intent.event_name,
            "market_name": intent.market_name,
            "selection_name": intent.selection_name,
            "time_in_force": intent.execution_policy.time_in_force,
            "cancel_unmatched_after_ms": intent.execution_policy.cancel_unmatched_after_ms,
            "risk_summary": intent.risk_report.summary,
            "risk_warning_count": intent.risk_report.warning_count,
            "risk_blocking_submit_count": intent.risk_report.blocking_submit_count,
            "reduce_only": intent.risk_report.reduce_only,
        },
    )


def _record_trading_action_marker(
    *,
    run_dir: Path | None,
    intent: TradingActionIntent,
    phase: str,
    status: str,
    detail: str,
) -> None:
    if run_dir is None:
        return
    bundle = load_run_bundle(source="smarkets_exchange", run_dir=run_dir)
    bundle.events_path.parent.mkdir(parents=True, exist_ok=True)
    bundle.events_path.touch(exist_ok=True)
    append_operator_interaction_event(
        bundle.events_path,
        action="place_bet",
        status=f"{phase}:{status}",
        detail=detail,
        request_id=intent.request_id,
        reference_id=intent.source_ref or None,
        metadata={
            "venue": intent.venue,
            "mode": intent.mode,
            "side": intent.side,
            "stake": intent.stake,
            "expected_price": intent.expected_price,
            "event_name": intent.event_name,
            "market_name": intent.market_name,
            "selection_name": intent.selection_name,
        },
    )
    if bundle.transport_path is not None:
        append_transport_marker(
            bundle.transport_path,
            action="place_bet",
            phase=phase,
            detail=detail,
            request_id=intent.request_id,
            reference_id=intent.source_ref or None,
            metadata={
                "venue": intent.venue,
                "mode": intent.mode,
                "side": intent.side,
                "stake": intent.stake,
                "selection_name": intent.selection_name,
                "event_name": intent.event_name,
                "status": status,
            },
        )


def _betslip_is_visible(client: AgentBrowserClient) -> bool:
    result = client.evaluate(
        "(() => {"
        f"const contract = document.querySelector({_js_string(SMARKETS_SLIP_CONTRACT_SELECTOR)});"
        f"const side = document.querySelector({_js_string(SMARKETS_SLIP_SIDE_SELECTOR)});"
        f"const stake = document.querySelector({_js_string(SMARKETS_STAKE_INPUT_SELECTOR)});"
        "return !!contract && !!side && !!stake;"
        "})()"
    )
    return bool(result)


def _wait_for(
    *,
    client: AgentBrowserClient,
    predicate_js: str,
    timeout_ms: int,
    error_message: str,
) -> None:
    deadline = datetime.now(UTC).timestamp() + (timeout_ms / 1000.0)
    while datetime.now(UTC).timestamp() <= deadline:
        if bool(client.evaluate(predicate_js)):
            return
        client.wait(125)
    raise ValueError(error_message)


def _optional_string(value: object) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _js_string(value: str) -> str:
    escaped = (
        value.replace("\\", "\\\\")
        .replace("'", "\\'")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )
    return f"'{escaped}'"
