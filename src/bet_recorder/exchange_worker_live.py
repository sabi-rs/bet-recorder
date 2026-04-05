from __future__ import annotations

import json
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from copy import deepcopy
from pathlib import Path

from bet_recorder.analysis.bet365 import analyze_bet365_page
from bet_recorder.analysis.betuk import analyze_betuk_page
from bet_recorder.analysis.betway_uk import analyze_betway_page
from bet_recorder.analysis.generic_sportsbooks import (
    analyze_bet600_page,
    analyze_bet10_page,
    analyze_betano_page,
    analyze_betfred_page,
    analyze_betmgm_page,
    analyze_betvictor_page,
    analyze_boylesports_page,
    analyze_coral_page,
    analyze_fanteam_page,
    analyze_kwik_page,
    analyze_ladbrokes_page,
    analyze_leovegas_page,
    analyze_midnite_page,
    analyze_paddypower_page,
    analyze_skybet_page,
    analyze_sportingindex_page,
    analyze_talksportbet_page,
    analyze_williamhill_page,
)
from bet_recorder.analysis.racing_markets import analyze_racing_market_page
from bet_recorder.browser.cdp import (
    capture_debug_target_page_state,
    click_debug_target_by_labels,
    evaluate_debug_target_value,
    fetch_debug_target_json,
    list_debug_targets,
    navigate_debug_target,
    select_debug_target_by_fragments,
)
from bet_recorder.bookmaker_history_runtime import extract_live_bookmaker_ledger_entries
from bet_recorder.sources.history_adapters import get_live_venue_history_adapter
from bet_recorder.sources.profiles import get_source_profile

LIVE_VENUE_ORDER = (
    "betfair",
    "bet365",
    "matchbook",
    "betdaq",
    "betfred",
    "betway",
    "betuk",
    "coral",
    "ladbrokes",
    "kwik",
    "bet600",
    "betano",
    "betmgm",
    "betvictor",
    "skybet",
    "talksportbet",
    "paddypower",
    "boylesports",
    "williamhill",
    "sportingindex",
    "leovegas",
    "fanteam",
    "midnite",
    "bet10",
)
HISTORY_EXPAND_LABELS = ("View More", "Show More")
MAX_HISTORY_EXPAND_STEPS = 4
HORSE_MARKET_VENUES = (
    "bet365",
    "betuk",
    "betway",
    "betfred",
    "coral",
    "ladbrokes",
    "kwik",
    "bet600",
    "smarkets",
    "betdaq",
)


@dataclass(frozen=True)
class LiveVenueDefinition:
    venue: str
    label: str
    source: str
    page: str
    url_fragments: tuple[str, ...]
    my_bets_urls: tuple[str, ...] = ()
    history_urls: tuple[str, ...] = ()
    my_bets_labels: tuple[str, ...] = ()
    history_labels: tuple[str, ...] = ()


def _generic_sportsbook_live_venue_definition(
    *,
    venue: str,
    label: str,
    source: str,
    url_fragments: tuple[str, ...],
) -> LiveVenueDefinition:
    return LiveVenueDefinition(
        venue=venue,
        label=label,
        source=source,
        page="my_bets",
        url_fragments=url_fragments,
        my_bets_labels=("My Bets", "Open Bets"),
        history_labels=("Settled", "History", "Closed"),
    )


LIVE_VENUE_DEFINITIONS = {
    "betfair": LiveVenueDefinition(
        venue="betfair",
        label="Betfair",
        source="betfair",
        page="open_positions",
        url_fragments=("betfair.com",),
        my_bets_labels=("My Bets", "Open Bets"),
        history_labels=("History", "Settled", "Closed"),
    ),
    "bet365": LiveVenueDefinition(
        venue="bet365",
        label="bet365",
        source="bet365",
        page="my_bets",
        url_fragments=("bet365.com/#/MB/UB", "bet365.com"),
        my_bets_urls=("https://www.bet365.com/#/MB/", "https://www.bet365.com/#/MB/UB"),
        my_bets_labels=("My Bets",),
        history_labels=("Settled", "Settled Bets", "History"),
    ),
    "betuk": LiveVenueDefinition(
        venue="betuk",
        label="BetUK",
        source="betuk",
        page="my_bets",
        url_fragments=("betuk.com",),
        my_bets_urls=("https://www.betuk.com/betting#mybets",),
        history_urls=("https://www.betuk.com/betting#bethistory",),
        my_bets_labels=("My Bets",),
        history_labels=("History", "Settled"),
    ),
    "betway": LiveVenueDefinition(
        venue="betway",
        label="Betway",
        source="betway_uk",
        page="my_bets",
        url_fragments=("betway.com/gb/en/sports/my-bets", "betway.com"),
        my_bets_urls=("https://betway.com/gb/en/sports/my-bets",),
        my_bets_labels=("My Bets",),
        history_labels=("Settled", "History", "Closed"),
    ),
    "betfred": LiveVenueDefinition(
        venue="betfred",
        label="Betfred",
        source="betfred",
        page="my_bets",
        url_fragments=("betfred.com",),
        my_bets_urls=("https://www.betfred.com/sport/my-bets",),
        my_bets_labels=("My Bets",),
        history_labels=("Settled", "History"),
    ),
    "coral": LiveVenueDefinition(
        venue="coral",
        label="Coral",
        source="coral",
        page="my_bets",
        url_fragments=("coral.co.uk", "coral"),
        my_bets_urls=("https://sports.coral.co.uk/my-bets",),
        my_bets_labels=("My Bets",),
        history_labels=("Settled", "History"),
    ),
    "ladbrokes": LiveVenueDefinition(
        venue="ladbrokes",
        label="Ladbrokes",
        source="ladbrokes",
        page="my_bets",
        url_fragments=("ladbrokes.com", "ladbrokes"),
        my_bets_urls=("https://sports.ladbrokes.com/my-bets",),
        my_bets_labels=("My Bets",),
        history_labels=("Settled", "History"),
    ),
    "kwik": LiveVenueDefinition(
        venue="kwik",
        label="Kwik",
        source="kwik",
        page="my_bets",
        url_fragments=("kwiff.com", "kwik"),
        my_bets_urls=("https://sports.kwiff.com/my-bets",),
        my_bets_labels=("My Bets",),
        history_labels=("Settled", "History"),
    ),
    "bet600": LiveVenueDefinition(
        venue="bet600",
        label="Bet600",
        source="bet600",
        page="my_bets",
        url_fragments=("bet600.co.uk", "bet600.com", "bet600"),
        my_bets_urls=("https://www.bet600.co.uk/my-bets",),
        my_bets_labels=("My Bets",),
        history_labels=("Settled", "History"),
    ),
    "betano": LiveVenueDefinition(
        venue="betano",
        label="Betano",
        source="betano",
        page="my_bets",
        url_fragments=("betano.co.uk", "betano"),
        my_bets_labels=("My Bets",),
        history_labels=("Bet History", "Settled", "History"),
    ),
    "betmgm": _generic_sportsbook_live_venue_definition(
        venue="betmgm",
        label="BetMGM",
        source="betmgm",
        url_fragments=("betmgm",),
    ),
    "betvictor": _generic_sportsbook_live_venue_definition(
        venue="betvictor",
        label="BetVictor",
        source="betvictor",
        url_fragments=("betvictor",),
    ),
    "skybet": _generic_sportsbook_live_venue_definition(
        venue="skybet",
        label="Sky Bet",
        source="skybet",
        url_fragments=("skybet",),
    ),
    "talksportbet": _generic_sportsbook_live_venue_definition(
        venue="talksportbet",
        label="talkSPORT BET",
        source="talksportbet",
        url_fragments=("talksportbet",),
    ),
    "paddypower": _generic_sportsbook_live_venue_definition(
        venue="paddypower",
        label="Paddy Power",
        source="paddypower",
        url_fragments=("paddypower",),
    ),
    "boylesports": _generic_sportsbook_live_venue_definition(
        venue="boylesports",
        label="BoyleSports",
        source="boylesports",
        url_fragments=("boylesports",),
    ),
    "williamhill": _generic_sportsbook_live_venue_definition(
        venue="williamhill",
        label="William Hill",
        source="williamhill",
        url_fragments=("williamhill",),
    ),
    "sportingindex": _generic_sportsbook_live_venue_definition(
        venue="sportingindex",
        label="Sporting Index",
        source="sportingindex",
        url_fragments=("sportingindex",),
    ),
    "leovegas": _generic_sportsbook_live_venue_definition(
        venue="leovegas",
        label="LeoVegas",
        source="leovegas",
        url_fragments=("leovegas",),
    ),
    "fanteam": _generic_sportsbook_live_venue_definition(
        venue="fanteam",
        label="FanTeam",
        source="fanteam",
        url_fragments=("fanteam",),
    ),
    "midnite": _generic_sportsbook_live_venue_definition(
        venue="midnite",
        label="Midnite",
        source="midnite",
        url_fragments=("midnite",),
    ),
    "bet10": _generic_sportsbook_live_venue_definition(
        venue="bet10",
        label="Bet10",
        source="bet10",
        url_fragments=("bet10", "10bet"),
    ),
    "matchbook": LiveVenueDefinition(
        venue="matchbook",
        label="Matchbook",
        source="matchbook",
        page="open_positions",
        url_fragments=("matchbook.com",),
        my_bets_labels=("My Bets", "Open Bets"),
        history_labels=("History", "Settled", "Closed"),
    ),
    "betdaq": LiveVenueDefinition(
        venue="betdaq",
        label="Betdaq",
        source="betdaq",
        page="open_positions",
        url_fragments=("betdaq.com",),
    ),
}
HORSE_MARKET_DEFINITIONS = {
    **LIVE_VENUE_DEFINITIONS,
    "smarkets": LiveVenueDefinition(
        venue="smarkets",
        label="Smarkets",
        source="smarkets_exchange",
        page="market",
        url_fragments=("smarkets.com",),
    ),
    "betway": LIVE_VENUE_DEFINITIONS["betway"],
}


def capture_current_live_venue_payload(venue: str) -> dict:
    definition = LIVE_VENUE_DEFINITIONS.get(venue)
    if definition is None:
        raise ValueError(f"Unsupported live venue: {venue}")
    return _capture_live_venue_payload(definition=definition, purpose="open_bets")


def capture_live_venue_history_payload(venue: str) -> dict:
    definition = LIVE_VENUE_DEFINITIONS.get(venue)
    if definition is None:
        raise ValueError(f"Unsupported live venue: {venue}")
    return _capture_live_venue_payload(definition=definition, purpose="history")


def capture_live_venue_history_sync_report(venue: str) -> dict:
    definition = LIVE_VENUE_DEFINITIONS.get(venue)
    if definition is None:
        raise ValueError(f"Unsupported live venue: {venue}")
    started_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    try:
        profile = get_source_profile(definition.source)
    except ValueError as exc:
        return _build_history_sync_report(
            venue=venue,
            status="unsupported",
            started_at=started_at,
            finished_at=started_at,
            detail=str(exc),
            trace=[],
        )
    adapter = get_live_venue_history_adapter(venue)
    if adapter is None or not adapter.historical_reconciliation_supported:
        missing = adapter.missing_capabilities() if adapter is not None else ("history adapter",)
        return _build_history_sync_report(
            venue=venue,
            status="unsupported",
            started_at=started_at,
            finished_at=started_at,
            detail=f"Vendor-specific historical reconciliation is not implemented for this venue: {', '.join(missing)}.",
            trace=[],
        )

    try:
        payload = capture_live_venue_history_payload(venue)
        analysis = analyze_live_venue_payload(venue, payload)
        entries = extract_live_bookmaker_ledger_entries(
            venue=venue,
            payload=payload,
            analysis=analysis,
        )
    except Exception as exc:
        finished_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
        return _build_history_sync_report(
            venue=venue,
            status="error",
            started_at=started_at,
            finished_at=finished_at,
            detail=str(exc),
            trace=[],
        )

    finished_at = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    status = "success" if entries else "no_rows"
    detail = (
        f"Extracted {len(entries)} settled row(s) from live history."
        if entries
        else "No settled history rows were extracted from the captured venue surface."
    )
    return _build_history_sync_report(
        venue=venue,
        status=status,
        started_at=started_at,
        finished_at=finished_at,
        detail=detail,
        payload=payload,
        trace=payload.get("history_capture_trace", []),
        entries=entries,
    )


def _capture_live_venue_payload(
  *,
  definition: LiveVenueDefinition,
  purpose: str,
) -> dict:
    adapter = get_live_venue_history_adapter(definition.venue)
    preferred_target_fragments: tuple[str, ...] = ()
    if purpose == "history":
        history_targets = (
            adapter.history_urls
            if adapter is not None and adapter.history_urls
            else definition.history_urls
        )
        if history_targets:
            preferred_target_fragments = history_targets
    targets = list_debug_targets()
    try:
        target = select_debug_target_by_fragments(
            targets=targets,
            url_fragments=preferred_target_fragments or definition.url_fragments,
        )
    except ValueError:
        if not preferred_target_fragments:
            raise
        target = select_debug_target_by_fragments(
            targets=targets,
            url_fragments=definition.url_fragments,
        )
    payload = _capture_live_venue_page_state(
        definition=definition,
        target=target,
        note_suffix=purpose,
        frame_url_fragments=_history_frame_fragments(definition.venue, purpose=purpose),
    )
    _append_history_trace(
        payload,
        action="capture",
        detail=f"Captured initial {purpose} surface.",
        page=str(payload.get("page", "") or definition.page),
        url=str(payload.get("url", "") or ""),
    )
    if purpose == "open_bets":
        return _ensure_live_venue_open_bets_view(
            definition=definition,
            target=target,
            payload=payload,
        )
    if purpose == "history":
        return _ensure_live_venue_history_view(
            definition=definition,
            target=target,
            payload=payload,
        )
    raise ValueError(f"Unsupported live venue capture purpose: {purpose}")


def _capture_live_venue_page_state(
  *,
  definition: LiveVenueDefinition,
  target,
  note_suffix: str,
  frame_url_fragments: tuple[str, ...] = (),
) -> dict:
    kwargs = {
        "websocket_debugger_url": target.websocket_debugger_url,
        "page": definition.page,
        "captured_at": datetime.now(UTC),
        "notes": [f"exchange-worker-live:{definition.venue}", f"view:{note_suffix}"],
    }
    if frame_url_fragments:
        try:
            return capture_debug_target_page_state(
                **kwargs,
                frame_url_fragments=frame_url_fragments,
            )
        except ValueError:
            pass
    return capture_debug_target_page_state(**kwargs)


def _ensure_live_venue_open_bets_view(
    *,
    definition: LiveVenueDefinition,
    target,
    payload: dict,
) -> dict:
    analysis = analyze_live_venue_payload(definition.venue, payload)
    if _analysis_is_live_venue_readable(analysis):
        return payload
    return _navigate_live_venue_to_my_bets(
        definition=definition,
        target=target,
        payload=payload,
    )


def _ensure_live_venue_history_view(
    *,
    definition: LiveVenueDefinition,
    target,
    payload: dict,
) -> dict:
    adapter = get_live_venue_history_adapter(definition.venue)
    frame_url_fragments = _history_frame_fragments(definition.venue, purpose="history")
    if _payload_has_live_bookmaker_history_rows(definition.venue, payload):
        _append_history_trace(
            payload,
            action="probe",
            detail="Initial surface already contains parseable settled history rows.",
            page=str(payload.get("page", "") or definition.page),
            url=str(payload.get("url", "") or ""),
        )
        return _enrich_live_venue_history_payload(
            definition=definition,
            target=target,
            payload=_expand_live_venue_history_payload(
                definition=definition,
                target=target,
                payload=payload,
            ),
        )

    latest_payload = payload
    for url in adapter.history_urls if adapter is not None else definition.history_urls:
        _append_history_trace(
            latest_payload,
            action="navigate",
            detail=f"Navigating directly to history URL {url}.",
            page=str(latest_payload.get("page", "") or definition.page),
            url=url,
        )
        navigate_debug_target(
            websocket_debugger_url=target.websocket_debugger_url,
            url=url,
        )
        latest_payload = _capture_live_venue_page_state(
            definition=definition,
            target=target,
            note_suffix="history",
            frame_url_fragments=frame_url_fragments,
        )
        _append_history_trace(
            latest_payload,
            action="capture",
            detail="Captured venue after direct history navigation.",
            page=str(latest_payload.get("page", "") or definition.page),
            url=str(latest_payload.get("url", "") or ""),
        )
        latest_payload = _expand_live_venue_history_payload(
            definition=definition,
            target=target,
            payload=latest_payload,
        )
        latest_payload = _enrich_live_venue_history_payload(
            definition=definition,
            target=target,
            payload=latest_payload,
        )
        latest_payload = _apply_live_venue_history_filters(
            definition=definition,
            target=target,
            payload=latest_payload,
        )
        if _payload_has_live_bookmaker_history_rows(definition.venue, latest_payload):
            return latest_payload
        if adapter is not None and adapter.parser_name == "fanteam_history":
            return latest_payload

    latest_payload = _navigate_live_venue_to_my_bets(
        definition=definition,
        target=target,
        payload=latest_payload,
    )
    latest_payload = _enrich_live_venue_history_payload(
        definition=definition,
        target=target,
        payload=latest_payload,
    )
    latest_payload = _apply_live_venue_history_filters(
        definition=definition,
        target=target,
        payload=latest_payload,
    )
    if _payload_has_live_bookmaker_history_rows(definition.venue, latest_payload):
        return latest_payload

    history_labels = adapter.history_labels if adapter is not None else definition.history_labels
    if history_labels:
        _append_history_trace(
            latest_payload,
            action="click",
            detail="Attempting history tab click.",
            page=str(latest_payload.get("page", "") or definition.page),
            url=str(latest_payload.get("url", "") or ""),
            labels=list(history_labels),
        )
        click_debug_target_by_labels(
            websocket_debugger_url=target.websocket_debugger_url,
            labels=history_labels,
            wait_ms=2500,
            frame_url_fragments=frame_url_fragments,
        )
        latest_payload = _capture_live_venue_page_state(
            definition=definition,
            target=target,
            note_suffix="history",
            frame_url_fragments=frame_url_fragments,
        )
        _append_history_trace(
            latest_payload,
            action="capture",
            detail="Captured venue after history tab click.",
            page=str(latest_payload.get("page", "") or definition.page),
            url=str(latest_payload.get("url", "") or ""),
        )
        latest_payload = _expand_live_venue_history_payload(
            definition=definition,
            target=target,
            payload=latest_payload,
        )
        latest_payload = _enrich_live_venue_history_payload(
            definition=definition,
            target=target,
            payload=latest_payload,
        )
        latest_payload = _apply_live_venue_history_filters(
            definition=definition,
            target=target,
            payload=latest_payload,
        )
        if _payload_has_live_bookmaker_history_rows(definition.venue, latest_payload):
            return latest_payload
    return _enrich_live_venue_history_payload(
        definition=definition,
        target=target,
        payload=_expand_live_venue_history_payload(
            definition=definition,
            target=target,
            payload=latest_payload,
        ),
    )


def _navigate_live_venue_to_my_bets(
    *,
    definition: LiveVenueDefinition,
    target,
    payload: dict,
) -> dict:
    latest_payload = payload
    current_url = str(latest_payload.get("url", "") or "")
    if current_url in definition.my_bets_urls:
        analysis = analyze_live_venue_payload(definition.venue, latest_payload)
        if _analysis_is_live_venue_readable(analysis):
            return latest_payload
    else:
        current_url = ""
    for url in definition.my_bets_urls:
        if current_url == url:
            continue
        _append_history_trace(
            latest_payload,
            action="navigate",
            detail=f"Navigating to my bets URL {url}.",
            page=str(latest_payload.get("page", "") or definition.page),
            url=url,
        )
        navigate_debug_target(
            websocket_debugger_url=target.websocket_debugger_url,
            url=url,
        )
        latest_payload = _capture_live_venue_page_state(
            definition=definition,
            target=target,
            note_suffix="open_bets",
        )
        _append_history_trace(
            latest_payload,
            action="capture",
            detail="Captured venue after my bets navigation.",
            page=str(latest_payload.get("page", "") or definition.page),
            url=str(latest_payload.get("url", "") or ""),
        )
        analysis = analyze_live_venue_payload(definition.venue, latest_payload)
        if _analysis_is_live_venue_readable(analysis):
            return latest_payload

    if definition.my_bets_labels:
        _append_history_trace(
            latest_payload,
            action="click",
            detail="Attempting my bets tab click.",
            page=str(latest_payload.get("page", "") or definition.page),
            url=str(latest_payload.get("url", "") or ""),
            labels=list(definition.my_bets_labels),
        )
        click_debug_target_by_labels(
            websocket_debugger_url=target.websocket_debugger_url,
            labels=definition.my_bets_labels,
        )
        latest_payload = _capture_live_venue_page_state(
            definition=definition,
            target=target,
            note_suffix="open_bets",
        )
        _append_history_trace(
            latest_payload,
            action="capture",
            detail="Captured venue after my bets tab click.",
            page=str(latest_payload.get("page", "") or definition.page),
            url=str(latest_payload.get("url", "") or ""),
        )
    return latest_payload


def _analysis_is_live_venue_readable(analysis: dict) -> bool:
    return str(analysis.get("status", "")) in {"ready", "no_open_bets", "login_required"}


def _history_frame_fragments(venue: str, *, purpose: str) -> tuple[str, ...]:
    if purpose != "history":
        return ()
    adapter = get_live_venue_history_adapter(venue)
    if adapter is None or not adapter.frame_url_fragments:
        return ()
    return tuple(adapter.frame_url_fragments)


def _payload_has_live_bookmaker_history_rows(venue: str, payload: dict) -> bool:
    analysis = analyze_live_venue_payload(venue, payload)
    return bool(
        extract_live_bookmaker_ledger_entries(
            venue=venue,
            payload=payload,
            analysis=analysis,
        )
    )


def _expand_live_venue_history_payload(
    *,
    definition: LiveVenueDefinition,
    target,
    payload: dict,
) -> dict:
    adapter = get_live_venue_history_adapter(definition.venue)
    expand_labels = (
        adapter.pagination_labels
        if adapter is not None and adapter.pagination_labels
        else HISTORY_EXPAND_LABELS
    )
    latest_payload = payload
    frame_url_fragments = _history_frame_fragments(definition.venue, purpose="history")
    for _ in range(MAX_HISTORY_EXPAND_STEPS):
        if not _payload_supports_history_expand(latest_payload, labels=expand_labels):
            break
        before_body = str(latest_payload.get("body_text", "") or "")
        _append_history_trace(
            latest_payload,
            action="click",
            detail="Attempting history expansion.",
            page=str(latest_payload.get("page", "") or definition.page),
            url=str(latest_payload.get("url", "") or ""),
            labels=list(expand_labels),
        )
        try:
            click_debug_target_by_labels(
                websocket_debugger_url=target.websocket_debugger_url,
                labels=expand_labels,
                wait_ms=2000,
                frame_url_fragments=frame_url_fragments,
            )
        except Exception as exc:
            _append_history_trace(
                latest_payload,
                action="expand_error",
                detail=f"History expansion failed: {exc}",
                page=str(latest_payload.get('page', '') or definition.page),
                url=str(latest_payload.get('url', '') or ''),
                labels=list(expand_labels),
            )
            break
        expanded_payload = _capture_live_venue_page_state(
            definition=definition,
            target=target,
            note_suffix="history",
            frame_url_fragments=frame_url_fragments,
        )
        if str(expanded_payload.get("body_text", "") or "") == before_body:
            _append_history_trace(
                expanded_payload,
                action="expand_no_change",
                detail="History expansion click did not change the captured body.",
                page=str(expanded_payload.get("page", "") or definition.page),
                url=str(expanded_payload.get("url", "") or ""),
            )
            break
        _append_history_trace(
            expanded_payload,
            action="capture",
            detail="Captured venue after history expansion.",
            page=str(expanded_payload.get("page", "") or definition.page),
            url=str(expanded_payload.get("url", "") or ""),
        )
        latest_payload = expanded_payload
    return latest_payload


def _apply_live_venue_history_filters(
    *,
    definition: LiveVenueDefinition,
    target,
    payload: dict,
) -> dict:
    adapter = get_live_venue_history_adapter(definition.venue)
    if adapter is None:
        return payload
    frame_url_fragments = _history_frame_fragments(definition.venue, purpose="history")
    latest_payload = payload
    for labels, detail in (
        (adapter.date_range_labels, "Attempting history date-range filter."),
        (adapter.history_submit_labels, "Attempting history filter submission."),
    ):
        if not labels or _payload_has_live_bookmaker_history_rows(definition.venue, latest_payload):
            continue
        try:
            click_debug_target_by_labels(
                websocket_debugger_url=target.websocket_debugger_url,
                labels=labels,
                wait_ms=2000,
                frame_url_fragments=frame_url_fragments,
            )
        except Exception:
            continue
        _append_history_trace(
            latest_payload,
            action="click",
            detail=detail,
            page=str(latest_payload.get("page", "") or definition.page),
            url=str(latest_payload.get("url", "") or ""),
            labels=list(labels),
        )
        latest_payload = _capture_live_venue_page_state(
            definition=definition,
            target=target,
            note_suffix="history",
            frame_url_fragments=frame_url_fragments,
        )
        latest_payload = _enrich_live_venue_history_payload(
            definition=definition,
            target=target,
            payload=latest_payload,
        )
    return latest_payload


def _enrich_live_venue_history_payload(
    *,
    definition: LiveVenueDefinition,
    target,
    payload: dict,
) -> dict:
    adapter = get_live_venue_history_adapter(definition.venue)
    frame_url_fragments = _history_frame_fragments(definition.venue, purpose="history")
    if adapter is not None and adapter.parser_name == "boylesports_history":
        try:
            payload["body_html"] = str(
                evaluate_debug_target_value(
                    websocket_debugger_url=target.websocket_debugger_url,
                    expression='document.body ? document.body.innerHTML : ""',
                    frame_url_fragments=frame_url_fragments,
                )
                or ""
            )
        except Exception as exc:
            _append_history_trace(
                payload,
                action="capture_html_error",
                detail=f"Failed to capture history HTML: {exc}",
                page=str(payload.get("page", "") or definition.page),
                url=str(payload.get("url", "") or ""),
            )
    if adapter is None or not adapter.history_api_endpoints:
        if adapter is not None and adapter.parser_name == "fanteam_history":
            try:
                api_responses = _fetch_fanteam_history_api_responses(
                    websocket_debugger_url=target.websocket_debugger_url,
                )
            except Exception as exc:
                _append_history_trace(
                    payload,
                    action="fetch_api_error",
                    detail=f"Failed to fetch Fanteam history API: {exc}",
                    page=str(payload.get("page", "") or definition.page),
                    url=str(payload.get("url", "") or ""),
                )
                return payload
            if api_responses:
                payload["history_api_responses"] = api_responses
                for endpoint_name, response in api_responses.items():
                    status = int(response.get("status", 0) or 0)
                    detail = f"Fetched Fanteam history API {endpoint_name} with HTTP {status}"
                    body = response.get("body")
                    if endpoint_name == "count" and isinstance(body, dict):
                        open_count = int(body.get("open", 0) or 0)
                        detail += f" and {open_count} open bet(s)."
                    elif endpoint_name == "history":
                        row_count = _count_fanteam_history_rows(body)
                        if row_count:
                            detail += f" and {row_count} history row(s)."
                        else:
                            detail += "."
                    else:
                        detail += "."
                    _append_history_trace(
                        payload,
                        action="fetch_api",
                        detail=detail,
                        page=str(payload.get("page", "") or definition.page),
                        url=str(response.get("url", "") or payload.get("url", "") or ""),
                    )
        return payload

    api_responses = dict(payload.get("history_api_responses", {}))
    for endpoint_name, url in adapter.history_api_endpoints:
        try:
            response = fetch_debug_target_json(
                websocket_debugger_url=target.websocket_debugger_url,
                url=url,
                frame_url_fragments=frame_url_fragments,
            )
        except Exception as exc:
            _append_history_trace(
                payload,
                action="fetch_api_error",
                detail=f"Failed to fetch history API {endpoint_name}: {exc}",
                page=str(payload.get("page", "") or definition.page),
                url=url,
            )
            continue
        api_responses[endpoint_name] = response
        body = response.get("body")
        row_count = 0
        if isinstance(body, dict):
            groups = body.get("groups")
            if isinstance(groups, list):
                row_count = sum(
                    len(group.get("bets", []))
                    for group in groups
                    if isinstance(group, dict) and isinstance(group.get("bets"), list)
                )
        _append_history_trace(
            payload,
            action="fetch_api",
            detail=(
                f"Fetched history API {endpoint_name} with HTTP {response.get('status', 0)}"
                + (f" and {row_count} bet row(s)." if row_count else ".")
            ),
            page=str(payload.get("page", "") or definition.page),
            url=url,
        )
    if api_responses:
        payload["history_api_responses"] = api_responses
    return payload


def _load_fanteam_credentials(home: Path | None = None) -> tuple[str, str] | None:
    username = os.environ.get("FANTEAM_USERNAME")
    password = os.environ.get("FANTEAM_PASSWORD")
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

    username = values.get("FANTEAM_USERNAME")
    password = values.get("FANTEAM_PASSWORD")
    if username and password:
        return username, password
    return None


def _fetch_fanteam_history_api_responses(*, websocket_debugger_url: str) -> dict[str, dict]:
    credentials = _load_fanteam_credentials()
    username = credentials[0] if credentials is not None else None
    password = credentials[1] if credentials is not None else None
    script = f"""
      (async () => {{
        const username = {json.dumps(username)};
        const password = {json.dumps(password)};

        const normalizeResponse = async (response, url) => {{
          const text = await response.text();
          let body = null;
          try {{
            body = JSON.parse(text);
          }} catch (error) {{
            body = text;
          }}
          return {{
            url,
            status: response.status,
            ok: response.ok,
            body,
            body_text: text,
          }};
        }};

        const request = async (url, opts = {{}}) => {{
          const response = await fetch(url, {{
            ...opts,
            headers: {{
              "Content-Type": "application/json",
              "Accept": "application/json",
              ...(opts.headers || {{}}),
            }},
          }});
          return normalizeResponse(response, url);
        }};

        const decodeClaims = (token) => {{
          if (!token || typeof token !== "string" || token.split(".").length < 2) {{
            return {{}};
          }}
          try {{
            const encoded = token.split(".")[1];
            const normalized = encoded.replace(/-/g, "+").replace(/_/g, "/");
            return JSON.parse(atob(normalized));
          }} catch (error) {{
            return {{}};
          }}
        }};

        const isExpired = (token) => {{
          const claims = decodeClaims(token);
          const exp = Number(claims.exp || 0);
          if (!exp) {{
            return false;
          }}
          return exp <= Math.floor(Date.now() / 1000) + 60;
        }};

        let ftToken = localStorage.getItem("ftToken");
        let refreshToken = localStorage.getItem("refreshToken");
        const responses = {{}};

        if ((!ftToken || isExpired(ftToken)) && refreshToken) {{
          const refreshed = await request(
            "https://fanteam-scott.api.scoutgg.net/api/users/refresh",
            {{
              method: "POST",
              body: JSON.stringify({{ token: refreshToken }}),
            }},
          );
          responses.refresh = refreshed;
          if (refreshed.ok && refreshed.body && typeof refreshed.body === "object") {{
            if (refreshed.body.token) {{
              ftToken = String(refreshed.body.token);
              localStorage.setItem("ftToken", ftToken);
            }}
            if (refreshed.body.refreshToken) {{
              refreshToken = String(refreshed.body.refreshToken);
              localStorage.setItem("refreshToken", refreshToken);
            }}
          }}
        }}

        if ((!ftToken || isExpired(ftToken)) && username && password) {{
          const login = await request(
            "https://fanteam-scott.api.scoutgg.net/api/users/login",
            {{
              method: "POST",
              body: JSON.stringify({{ username, password }}),
            }},
          );
          responses.login = login;
          if (login.ok && login.body && typeof login.body === "object") {{
            if (login.body.token) {{
              ftToken = String(login.body.token);
              localStorage.setItem("ftToken", ftToken);
            }}
            if (login.body.refreshToken) {{
              refreshToken = String(login.body.refreshToken);
              localStorage.setItem("refreshToken", String(login.body.refreshToken));
            }}
          }}
        }}

        if (!ftToken) {{
          return responses;
        }}

        const claims = decodeClaims(ftToken);
        const sportsbookSession = await request(
          "https://sportsbook-hub.api.scoutgg.net/session?",
          {{
            method: "POST",
            headers: {{
              Authorization: `Bearer fanteam ${{ftToken}}`,
            }},
            body: JSON.stringify({{
              uid: String(claims.id || claims.uid || claims.sub || ""),
              username: String(claims.username || ""),
              whitelabelName: "fanteam",
            }}),
          }},
        );
        responses.sportsbook_session = sportsbookSession;
        if (!sportsbookSession.ok || !sportsbookSession.body || !sportsbookSession.body.token) {{
          return responses;
        }}

        const signin = await request(
          "https://sb2auth-altenar2.biahosted.com/api/WidgetAuth/SignIn",
          {{
            method: "POST",
            body: JSON.stringify({{
              culture: "en-GB",
              timezoneOffset: 0,
              integration: "fanteam",
              deviceType: 1,
              numFormat: "en-GB",
              countryCode: "GB",
              token: sportsbookSession.body.token,
            }}),
          }},
        );
        responses.signin = signin;
        if (!signin.ok || !signin.body || !signin.body.accessToken) {{
          return responses;
        }}

        const historyHeaders = {{
          Authorization: `Bearer ${{signin.body.accessToken}}`,
        }};
        const query =
          "culture=en-GB&timezoneOffset=0&integration=fanteam&deviceType=1&numFormat=en-GB&countryCode=GB";
        responses.count = await request(
          `https://sb2bethistory-gateway-altenar2.biahosted.com/api/WidgetReports/GetBetsCountWithEvents?${{query}}`,
          {{ headers: historyHeaders }},
        );
        responses.history = await request(
          `https://sb2bethistory-gateway-altenar2.biahosted.com/api/WidgetReports/widgetBetHistory?${{query}}`,
          {{ headers: historyHeaders }},
        );
        return responses;
      }})()
    """
    payload = evaluate_debug_target_value(
        websocket_debugger_url=websocket_debugger_url,
        expression=script,
        await_promise=True,
    )
    if not isinstance(payload, dict):
        raise ValueError("Fanteam history fetch did not return an object payload.")
    return {
        str(key): dict(value)
        for key, value in payload.items()
        if str(key).strip() and isinstance(value, dict)
    }


def _count_fanteam_history_rows(body: object) -> int:
    if isinstance(body, list):
        return len([item for item in body if isinstance(item, dict)])
    if isinstance(body, dict):
        for key in ("bets", "items", "rows", "results", "data"):
            value = body.get(key)
            if isinstance(value, list):
                return len([item for item in value if isinstance(item, dict)])
        nested = body.get("history")
        if isinstance(nested, (dict, list)):
            return _count_fanteam_history_rows(nested)
    return 0


def _append_history_trace(
    payload: dict,
    *,
    action: str,
    detail: str,
    page: str,
    url: str,
    labels: list[str] | None = None,
) -> None:
    trace = payload.setdefault("history_capture_trace", [])
    if not isinstance(trace, list):
        return
    trace.append(
        {
            "action": action,
            "detail": detail,
            "page": page,
            "url": url,
            "labels": list(labels or []),
        }
    )


def _build_history_sync_report(
    *,
    venue: str,
    status: str,
    started_at: str,
    finished_at: str,
    detail: str,
    trace: list[dict],
    payload: dict | None = None,
    entries: list[dict] | None = None,
) -> dict:
    rows_extracted = len(entries or [])
    summary = f"{venue} history sync {status}"
    if rows_extracted:
        summary = f"{summary} ({rows_extracted} row(s))"
    report = {
        "venue": venue,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "summary": summary,
        "detail": detail,
        "rows_extracted": rows_extracted,
        "page": str((payload or {}).get("page", "") or ""),
        "url": str((payload or {}).get("url", "") or ""),
        "document_title": str((payload or {}).get("document_title", "") or ""),
        "trace": [deepcopy(step) for step in trace if isinstance(step, dict)],
        "entries": [deepcopy(entry) for entry in (entries or []) if isinstance(entry, dict)],
    }
    return report


def _payload_supports_history_expand(payload: dict, *, labels: tuple[str, ...] = HISTORY_EXPAND_LABELS) -> bool:
    body_text = str(payload.get("body_text", "") or "").lower()
    visible_actions = [str(action).strip().lower() for action in payload.get("visible_actions", [])]
    return any(label.lower() in body_text for label in labels) or any(
        label.lower() in visible_actions for label in labels
    )


def analyze_live_venue_payload(venue: str, payload: dict) -> dict:
    if venue == "betfair":
        return analyze_generic_live_venue_page(
            venue=venue,
            page=payload["page"],
            body_text=payload["body_text"],
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "bet365":
        return analyze_bet365_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "matchbook":
        return analyze_generic_live_venue_page(
            venue=venue,
            page=payload["page"],
            body_text=payload["body_text"],
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "betuk":
        return analyze_betuk_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "betway":
        return analyze_betway_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "betfred":
        return analyze_betfred_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "coral":
        return analyze_coral_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "ladbrokes":
        return analyze_ladbrokes_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "kwik":
        return analyze_kwik_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "bet600":
        return analyze_bet600_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "betano":
        return analyze_betano_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "betmgm":
        return analyze_betmgm_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "betvictor":
        return analyze_betvictor_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "skybet":
        return analyze_skybet_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "talksportbet":
        return analyze_talksportbet_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "paddypower":
        return analyze_paddypower_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "boylesports":
        return analyze_boylesports_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "williamhill":
        return analyze_williamhill_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "sportingindex":
        return analyze_sportingindex_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "leovegas":
        return analyze_leovegas_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "fanteam":
        return analyze_fanteam_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "midnite":
        return analyze_midnite_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "bet10":
        return analyze_bet10_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    if venue == "betdaq":
        return analyze_generic_live_venue_page(
            venue=venue,
            page=payload["page"],
            body_text=payload["body_text"],
            visible_actions=payload.get("visible_actions", []),
        )
    raise ValueError(f"Unsupported live venue analysis: {venue}")


def analyze_generic_live_venue_page(
    *,
    venue: str,
    page: str,
    body_text: str,
    visible_actions: list[str],
) -> dict:
    lower_body = body_text.lower()
    if "log in" in lower_body or "login" in lower_body:
        status = "login_required"
    elif "my bets is empty" in lower_body or "no bets" in lower_body:
        status = "no_open_bets"
    else:
        status = "unknown"
    return {
        "page": page,
        "venue": venue,
        "status": status,
        "open_bets": [],
        "open_bet_count": 0,
        "supports_cash_out": any(action.lower() == "cash out" for action in visible_actions),
    }


def build_live_venue_summaries(
    *,
    selected_venue: str | None,
    selected_status: str | None = None,
) -> list[dict]:
    summaries = [
        {
            "id": "smarkets",
            "label": "Smarkets",
            "status": "ready" if selected_venue == "smarkets" else "connected",
            "detail": (
                "Recorder-backed exchange monitoring"
                if selected_venue == "smarkets"
                else "Available via watcher state"
            ),
            "event_count": 0,
            "market_count": 0,
        }
    ]
    try:
        targets = list_debug_targets()
    except Exception:
        targets = []
    for venue in LIVE_VENUE_ORDER:
        definition = LIVE_VENUE_DEFINITIONS[venue]
        target = _find_live_venue_target(targets=targets, venue=venue)
        if target is None:
            if venue == selected_venue and selected_status is not None:
                summaries.append(
                    {
                        "id": venue,
                        "label": definition.label,
                        "status": (
                            "ready"
                            if selected_status in {"ready", "no_open_bets"}
                            else "connected"
                            if selected_status == "awaiting_capture"
                            else "error"
                        ),
                        "detail": _with_support_note(
                            definition,
                            _selected_live_venue_detail(
                                definition.label,
                                selected_status,
                            ),
                        ),
                        "event_count": 0,
                        "market_count": 0,
                    }
                )
                continue
            summaries.append(
                {
                    "id": venue,
                    "label": definition.label,
                    "status": "planned",
                    "detail": _with_support_note(
                        definition,
                        "No live browser tab detected",
                    ),
                    "event_count": 0,
                    "market_count": 0,
                }
            )
            continue
        status = "connected"
        detail = _with_support_note(
            definition,
            f"Live browser tab detected: {target.title or target.url}",
        )
        if venue == selected_venue:
            status = (
                "ready"
                if selected_status in {"ready", "no_open_bets"}
                else "connected"
                if selected_status == "awaiting_capture"
                else "error"
            )
            detail = _with_support_note(
                definition,
                _selected_live_venue_detail(definition.label, selected_status),
            )
        summaries.append(
            {
                "id": venue,
                "label": definition.label,
                "status": status,
                "detail": detail,
                "event_count": 0,
                "market_count": 0,
            }
        )
    return summaries


def live_venue_status_line(
    *,
    definition: LiveVenueDefinition,
    status: str,
    open_bets: list[dict],
    count_hint: int | None,
) -> str:
    if status == "ready":
        return _with_support_note(
            definition,
            f"Loaded {len(open_bets)} {definition.label} open bets from the live browser tab.",
        )
    if status == "no_open_bets":
        return _with_support_note(
            definition,
            f"{definition.label} is connected and currently has no open bets.",
        )
    if status == "navigation_required":
        return _with_support_note(
            definition,
            f"{definition.label} is connected but not currently on a bet history view.",
        )
    if status == "login_required":
        return _with_support_note(
            definition,
            f"{definition.label} requires login before live bets can be loaded.",
        )
    if count_hint is not None:
        return _with_support_note(
            definition,
            f"{definition.label} is connected but only exposed a count hint of {count_hint}.",
        )
    return _with_support_note(
        definition,
        f"{definition.label} is connected but did not expose readable open bets.",
    )


def selected_live_venue_detail(label: str, status: str | None) -> str:
    if status == "ready":
        return f"{label} open bets loaded from live browser tab"
    if status == "no_open_bets":
        return f"{label} live browser tab is connected with no open bets"
    if status == "awaiting_capture":
        return f"{label} is selected; run a live refresh to capture its current bets"
    if status == "navigation_required":
        return f"{label} live browser tab is connected but not on a bet history view"
    if status == "login_required":
        return f"{label} live browser tab requires login before bets can be read"
    if status == "unavailable":
        return f"{label} does not currently have a live browser tab available"
    return f"{label} live browser tab is connected but not yet readable"


def _support_note(definition: LiveVenueDefinition) -> str:
    adapter = get_live_venue_history_adapter(definition.venue)
    if adapter is None:
        return ""
    missing = adapter.missing_capabilities()
    if not missing:
        return ""
    return f"Live-only venue; {', '.join(missing)} not yet supported."


def _with_support_note(definition: LiveVenueDefinition, detail: str) -> str:
    note = _support_note(definition)
    if not note:
        return detail
    return f"{detail} {note}"


def find_live_venue_targets(
    *,
    targets: list,
    venue: str,
) -> list:
    definition = HORSE_MARKET_DEFINITIONS.get(venue)
    if definition is None:
        return []
    lowered_fragments = [fragment.lower() for fragment in definition.url_fragments if fragment]
    matched = []
    for target in targets:
        lowered_url = str(target.url).lower()
        if any(fragment in lowered_url for fragment in lowered_fragments):
            matched.append(target)
    return matched


def prioritize_horse_market_targets(*, targets: list, query: dict) -> list:
    prioritized = [
        target
        for target in targets
        if _target_looks_like_horse_market_candidate(target=target, query=query)
    ]
    return prioritized or targets


def matches_horse_search_terms(*, query: dict, event_name: str, selection_names: list[str]) -> bool:
    terms = [
        str(term).strip().lower()
        for term in query.get("search", [])
        if str(term).strip()
    ]
    if not terms:
        return True
    haystack = " ".join([event_name, *selection_names]).lower()
    return any(term in haystack for term in terms)


def capture_live_horse_market_snapshot(query: dict) -> dict:
    bookmakers = [
        str(venue).strip().lower()
        for venue in query.get("bookmakers", [])
        if str(venue).strip()
    ]
    exchanges = [
        str(venue).strip().lower()
        for venue in query.get("exchanges", [])
        if str(venue).strip()
    ]
    requested_venues = list(dict.fromkeys([*bookmakers, *exchanges]))
    if not requested_venues:
        requested_venues = ["betfred", "coral", "smarkets", "betdaq"]

    unsupported = [venue for venue in requested_venues if venue not in HORSE_MARKET_DEFINITIONS]
    if unsupported:
        raise ValueError(
            "Unsupported horse matcher venues: " + ", ".join(sorted(set(unsupported)))
        )

    try:
        targets = list_debug_targets()
    except Exception as exc:
        raise ValueError(f"Failed to inspect browser targets for horse matcher: {exc}") from exc

    captured_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    sources: list[dict] = []

    for venue in requested_venues:
        definition = HORSE_MARKET_DEFINITIONS[venue]
        venue_targets = find_live_venue_targets(targets=targets, venue=venue)
        if not venue_targets:
            sources.append(
                {
                    "venue": venue,
                    "venue_label": definition.label,
                    "kind": "exchange" if venue in {"smarkets", "betdaq"} else "sportsbook",
                    "status": "missing",
                    "detail": "No live browser tab detected for venue.",
                    "page_url": "",
                    "page_title": "",
                    "event_name": "",
                    "market_name": "",
                    "start_hint": "",
                    "captured_at": captured_at,
                    "quotes": [],
                }
            )
            continue

        for target in prioritize_horse_market_targets(targets=venue_targets, query=query):
            try:
                payload = capture_debug_target_page_state(
                    websocket_debugger_url=target.websocket_debugger_url,
                    page="market",
                    captured_at=datetime.now(UTC),
                    notes=[f"horse-matcher:{venue}"],
                )
                analysis = analyze_racing_market_page(
                    venue=venue,
                    page=payload["page"],
                    url=str(payload.get("url", "")),
                    document_title=str(payload.get("document_title", "")),
                    body_text=str(payload.get("body_text", "")),
                    interactive_snapshot=list(payload.get("interactive_snapshot", [])),
                )
                selection_names = [
                    str(quote.get("selection_name", ""))
                    for quote in analysis.get("quotes", [])
                    if str(quote.get("selection_name", ""))
                ]
                status = str(analysis.get("status", "ignored"))
                detail = str(analysis.get("detail", ""))
                if status == "ready" and not matches_horse_search_terms(
                    query=query,
                    event_name=str(analysis.get("event_name", "")),
                    selection_names=selection_names,
                ):
                    status = "ignored"
                    detail = "Market captured but filtered out by search terms."

                sources.append(
                    {
                        "venue": venue,
                        "venue_label": definition.label,
                        "kind": "exchange" if venue in {"smarkets", "betdaq"} else "sportsbook",
                        "status": status,
                        "detail": detail,
                        "page_url": str(payload.get("url", "")),
                        "page_title": str(payload.get("document_title", "")),
                        "event_name": str(analysis.get("event_name", "")),
                        "market_name": str(analysis.get("market_name", "")),
                        "start_hint": str(analysis.get("start_hint", "")),
                        "captured_at": str(payload.get("captured_at", captured_at)),
                        "quotes": list(analysis.get("quotes", [])),
                    }
                )
            except Exception as exc:
                sources.append(
                    {
                        "venue": venue,
                        "venue_label": definition.label,
                        "kind": "exchange" if venue in {"smarkets", "betdaq"} else "sportsbook",
                        "status": "error",
                        "detail": f"Failed to inspect live horse market target: {exc}",
                        "page_url": str(target.url),
                        "page_title": str(target.title),
                        "event_name": "",
                        "market_name": "",
                        "start_hint": "",
                        "captured_at": captured_at,
                        "quotes": [],
                    }
                )

    ready_count = sum(1 for source in sources if source["status"] == "ready")
    return {
        "captured_at": captured_at,
        "source_count": len(sources),
        "ready_source_count": ready_count,
        "sources": sources,
    }


def _find_live_venue_target(
    *,
    targets: list,
    venue: str,
):
    definition = LIVE_VENUE_DEFINITIONS.get(venue)
    if definition is None:
        return None
    try:
        return select_debug_target_by_fragments(
            targets=targets,
            url_fragments=definition.url_fragments,
        )
    except ValueError:
        return None


def _selected_live_venue_detail(label: str, status: str | None) -> str:
    return selected_live_venue_detail(label, status)


def _target_looks_like_horse_market_candidate(*, target, query: dict) -> bool:
    lowered_haystack = f"{target.title} {target.url}".lower()
    search_terms = [
        str(term).strip().lower()
        for term in query.get("search", [])
        if str(term).strip()
    ]
    if search_terms and any(term in lowered_haystack for term in search_terms):
        return True
    return any(
        marker in lowered_haystack
        for marker in (
            "/horse-racing/",
            "horse-racing",
            "horse racing",
            "racing",
            "racecard",
            "runner",
            "each-way",
        )
    )
