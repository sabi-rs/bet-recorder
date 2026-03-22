from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from bet_recorder.analysis.bet365 import analyze_bet365_page
from bet_recorder.analysis.betuk import analyze_betuk_page
from bet_recorder.analysis.betway_uk import analyze_betway_page
from bet_recorder.analysis.generic_sportsbooks import (
    analyze_bet600_page,
    analyze_betfred_page,
    analyze_coral_page,
    analyze_kwik_page,
    analyze_ladbrokes_page,
)
from bet_recorder.analysis.racing_markets import analyze_racing_market_page
from bet_recorder.browser.cdp import (
    capture_debug_target_page_state,
    list_debug_targets,
    select_debug_target_by_fragments,
)

LIVE_VENUE_ORDER = (
    "bet365",
    "betdaq",
    "betfred",
    "betway",
    "betuk",
    "coral",
    "ladbrokes",
    "kwik",
    "bet600",
)
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


LIVE_VENUE_DEFINITIONS = {
    "bet365": LiveVenueDefinition(
        venue="bet365",
        label="bet365",
        source="bet365",
        page="my_bets",
        url_fragments=("bet365.com/#/MB/UB", "bet365.com"),
    ),
    "betuk": LiveVenueDefinition(
        venue="betuk",
        label="BetUK",
        source="betuk",
        page="my_bets",
        url_fragments=("betuk.com",),
    ),
    "betway": LiveVenueDefinition(
        venue="betway",
        label="Betway",
        source="betway_uk",
        page="my_bets",
        url_fragments=("betway.com/gb/en/sports/my-bets", "betway.com"),
    ),
    "betfred": LiveVenueDefinition(
        venue="betfred",
        label="Betfred",
        source="betfred",
        page="my_bets",
        url_fragments=("betfred.com",),
    ),
    "coral": LiveVenueDefinition(
        venue="coral",
        label="Coral",
        source="coral",
        page="my_bets",
        url_fragments=("coral.co.uk", "coral"),
    ),
    "ladbrokes": LiveVenueDefinition(
        venue="ladbrokes",
        label="Ladbrokes",
        source="ladbrokes",
        page="my_bets",
        url_fragments=("ladbrokes.com", "ladbrokes"),
    ),
    "kwik": LiveVenueDefinition(
        venue="kwik",
        label="Kwik",
        source="kwik",
        page="my_bets",
        url_fragments=("kwiff.com", "kwik"),
    ),
    "bet600": LiveVenueDefinition(
        venue="bet600",
        label="Bet600",
        source="bet600",
        page="my_bets",
        url_fragments=("bet600.co.uk", "bet600.com", "bet600"),
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
    targets = list_debug_targets()
    target = select_debug_target_by_fragments(
        targets=targets,
        url_fragments=definition.url_fragments,
    )
    return capture_debug_target_page_state(
        websocket_debugger_url=target.websocket_debugger_url,
        page=definition.page,
        captured_at=datetime.now(UTC),
        notes=[f"exchange-worker-live:{venue}"],
    )


def analyze_live_venue_payload(venue: str, payload: dict) -> dict:
    if venue == "bet365":
        return analyze_bet365_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
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
                            else "error"
                        ),
                        "detail": _selected_live_venue_detail(
                            definition.label,
                            selected_status,
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
                    "detail": "No live browser tab detected",
                    "event_count": 0,
                    "market_count": 0,
                }
            )
            continue
        status = "connected"
        detail = f"Live browser tab detected: {target.title or target.url}"
        if venue == selected_venue:
            status = "ready" if selected_status in {"ready", "no_open_bets"} else "error"
            detail = _selected_live_venue_detail(definition.label, selected_status)
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
        return f"Loaded {len(open_bets)} {definition.label} open bets from the live browser tab."
    if status == "no_open_bets":
        return f"{definition.label} is connected and currently has no open bets."
    if status == "navigation_required":
        return f"{definition.label} is connected but not currently on a bet history view."
    if status == "login_required":
        return f"{definition.label} requires login before live bets can be loaded."
    if count_hint is not None:
        return f"{definition.label} is connected but only exposed a count hint of {count_hint}."
    return f"{definition.label} is connected but did not expose readable open bets."


def selected_live_venue_detail(label: str, status: str | None) -> str:
    if status == "ready":
        return f"{label} open bets loaded from live browser tab"
    if status == "no_open_bets":
        return f"{label} live browser tab is connected with no open bets"
    if status == "navigation_required":
        return f"{label} live browser tab is connected but not on a bet history view"
    if status == "login_required":
        return f"{label} live browser tab requires login before bets can be read"
    if status == "unavailable":
        return f"{label} does not currently have a live browser tab available"
    return f"{label} live browser tab is connected but not yet readable"


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
