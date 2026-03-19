from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json

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
from bet_recorder.analysis.position_watch import build_smarkets_watch_plan
from bet_recorder.actions.smarkets_cashout import handle_cash_out_tracked_bet
from bet_recorder.actions.trading_actions import execute_trading_action
from bet_recorder.analysis.smarkets_exchange import analyze_smarkets_page
from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.browser.cdp import (
    capture_debug_target_page_state,
    list_debug_targets,
    select_debug_target_by_fragments,
)
from bet_recorder.ledger.loader import load_tracked_bets
from bet_recorder.ledger.policy import build_exit_recommendations

DEFAULT_LEDGER_HISTORY_PATH = (
    Path.home() / "Documents" / "Bets" / "output" / "ledger" / "statement-history.json"
)
HISTORICAL_POSITION_ACTIVITY_TYPES = {
    "bet_settled",
    "exchange_settlement",
    "market_settled",
    "cash_out",
}
DEFAULT_SELECTED_VENUE = "smarkets"
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


@dataclass(frozen=True)
class WorkerConfig:
    positions_payload_path: Path | None
    run_dir: Path | None
    account_payload_path: Path | None
    open_bets_payload_path: Path | None
    companion_legs_path: Path | None
    agent_browser_session: str | None
    commission_rate: float
    target_profit: float
    stop_loss: float
    hard_margin_call_profit_floor: float | None
    warn_only_default: bool

    @classmethod
    def from_dict(cls, payload: dict) -> WorkerConfig:
        if (
            payload.get("positions_payload_path") is None
            and payload.get("run_dir") is None
        ):
            raise ValueError(
                "Worker config requires positions_payload_path or run_dir."
            )
        try:
            return cls(
                positions_payload_path=(
                    Path(payload["positions_payload_path"])
                    if payload.get("positions_payload_path") is not None
                    else None
                ),
                run_dir=Path(payload["run_dir"])
                if payload.get("run_dir") is not None
                else None,
                account_payload_path=(
                    Path(payload["account_payload_path"])
                    if payload.get("account_payload_path") is not None
                    else None
                ),
                open_bets_payload_path=(
                    Path(payload["open_bets_payload_path"])
                    if payload.get("open_bets_payload_path") is not None
                    else None
                ),
                companion_legs_path=(
                    Path(payload["companion_legs_path"])
                    if payload.get("companion_legs_path") is not None
                    else None
                ),
                agent_browser_session=(
                    str(payload["agent_browser_session"])
                    if payload.get("agent_browser_session") is not None
                    else None
                ),
                commission_rate=float(payload["commission_rate"]),
                target_profit=float(payload["target_profit"]),
                stop_loss=float(payload["stop_loss"]),
                hard_margin_call_profit_floor=(
                    float(payload["hard_margin_call_profit_floor"])
                    if payload.get("hard_margin_call_profit_floor") is not None
                    else None
                ),
                warn_only_default=bool(payload.get("warn_only_default", True)),
            )
        except KeyError as exc:
            raise ValueError(f"Missing worker config field: {exc.args[0]}") from exc
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Worker config fields must be valid JSON scalars."
            ) from exc


def load_watch_snapshot(
    *,
    payload_path: Path,
    commission_rate: float,
    target_profit: float,
    stop_loss: float,
) -> dict:
    payload = json.loads(payload_path.read_text())
    analysis = analyze_smarkets_page(
        page=payload["page"],
        body_text=payload["body_text"],
        inputs=payload.get("inputs", {}),
        visible_actions=payload.get("visible_actions", []),
        links=payload.get("links", []),
        event_summaries=payload.get("event_summaries", []),
        metadata=payload.get("metadata"),
    )
    if analysis["page"] != "open_positions":
        raise ValueError("watch-open-positions requires an open_positions payload.")

    return build_smarkets_watch_plan(
        positions=analysis["positions"],
        commission_rate=commission_rate,
        target_profit=target_profit,
        stop_loss=stop_loss,
    )


def load_positions_analysis(payload_path: Path) -> dict:
    payload = json.loads(payload_path.read_text())
    return analyze_positions_payload(payload)


def analyze_positions_payload(payload: dict) -> dict:
    analysis = analyze_smarkets_page(
        page=payload["page"],
        body_text=payload["body_text"],
        inputs=payload.get("inputs", {}),
        visible_actions=payload.get("visible_actions", []),
        links=payload.get("links", []),
        event_summaries=payload.get("event_summaries", []),
        metadata=payload.get("metadata"),
    )
    if analysis["page"] != "open_positions":
        raise ValueError("positions payload must be an open_positions capture.")
    return analysis


def load_latest_positions_payload_from_run_dir(run_dir: Path) -> dict:
    events_path = run_dir / "events.jsonl"
    if not events_path.exists():
        raise ValueError(f"Run bundle does not contain events.jsonl: {events_path}")

    latest_payload: dict | None = None
    for line in events_path.read_text().splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        if event.get("kind") != "positions_snapshot":
            continue
        if event.get("page") != "open_positions":
            continue
        latest_payload = event

    if latest_payload is None:
        raise ValueError(f"No positions_snapshot event found in run bundle: {run_dir}")
    return latest_payload


def load_positions_analysis_for_config(config: WorkerConfig) -> dict:
    if config.positions_payload_path is not None:
        return load_positions_analysis(config.positions_payload_path)
    if config.run_dir is not None:
        return analyze_positions_payload(
            load_latest_positions_payload_from_run_dir(config.run_dir)
        )
    raise ValueError("Worker config requires positions_payload_path or run_dir.")


def capture_current_smarkets_open_positions(config: WorkerConfig) -> None:
    if config.run_dir is None or config.agent_browser_session is None:
        return

    from bet_recorder.watcher import (
        WatcherConfig,
        capture_current_smarkets_open_positions as capture_open_positions_for_watcher,
    )

    client = AgentBrowserClient(session=config.agent_browser_session)
    capture_open_positions_for_watcher(
        WatcherConfig(
            run_dir=config.run_dir,
            session=config.agent_browser_session,
            interval_seconds=0.0,
            commission_rate=config.commission_rate,
            target_profit=config.target_profit,
            stop_loss=config.stop_loss,
        ),
        datetime.now(UTC),
        client=client,
    )


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


def build_exchange_panel_snapshot(watch: dict) -> dict:
    unique_market_count = len({row["market"] for row in watch["watches"]})
    venues = build_live_venue_summaries(
        selected_venue="smarkets",
        selected_status="ready",
    )
    if venues:
        venues[0]["event_count"] = watch["watch_count"]
        venues[0]["market_count"] = unique_market_count
        venues[0]["detail"] = (
            f"{watch['watch_count']} grouped watches across {unique_market_count} markets"
        )
    return {
        "worker": {
            "name": "bet-recorder",
            "status": "ready",
            "detail": (
                f"Loaded {watch['watch_count']} watch groups from {watch['position_count']} positions."
            ),
        },
        "venues": venues,
        "selected_venue": "smarkets",
        "events": [
            {
                "id": f"{row['market']}::{row['contract']}",
                "label": row["contract"],
                "competition": row["market"],
                "start_time": (
                    f"profit {row['profit_take_back_odds']:.2f} stop {row['stop_loss_back_odds']:.2f}"
                ),
                "url": "",
            }
            for row in watch["watches"]
        ],
        "markets": [
            {
                "name": row["market"],
                "contract_count": row["position_count"],
            }
            for row in watch["watches"]
        ],
        "preflight": None,
        "status_line": f"Loaded {watch['watch_count']} Smarkets watch groups from bet-recorder.",
        "runtime": None,
        "account_stats": None,
        "open_positions": [],
        "historical_positions": [],
        "ledger_pnl_summary": load_ledger_pnl_summary(),
        "other_open_bets": [],
        "decisions": [],
        "watch": watch,
        "tracked_bets": [],
        "exit_policy": build_exit_policy_summary(
            watch,
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
        ),
        "exit_recommendations": [],
    }


def build_live_venue_snapshot(
    *,
    venue: str,
    payload: dict,
    analysis: dict,
    config: WorkerConfig,
) -> dict:
    definition = LIVE_VENUE_DEFINITIONS[venue]
    open_bets = analysis.get("open_bets") or []
    status = str(analysis.get("status") or "unknown")
    venue_status = "ready" if status in {"ready", "no_open_bets"} else "error"
    count_hint = analysis.get("count_hint")
    event_count = len(open_bets)
    if event_count == 0 and isinstance(count_hint, int):
        event_count = count_hint
    markets = _build_live_venue_markets(open_bets)
    snapshot = {
        "worker": {
            "name": "bet-recorder",
            "status": "ready" if venue_status == "ready" else "error",
            "detail": _live_venue_status_line(
                definition=definition,
                status=status,
                open_bets=open_bets,
                count_hint=count_hint,
            ),
        },
        "venues": build_live_venue_summaries(selected_venue=venue, selected_status=status),
        "selected_venue": venue,
        "events": [
            {
                "id": f"{venue}::{index}",
                "label": str(row.get("label", "")),
                "competition": str(row.get("event", "") or row.get("market", "")),
                "start_time": str(row.get("status", "")),
                "url": str(payload.get("url", "")),
            }
            for index, row in enumerate(open_bets)
        ],
        "markets": markets,
        "preflight": None,
        "status_line": _live_venue_status_line(
            definition=definition,
            status=status,
            open_bets=open_bets,
            count_hint=count_hint,
        ),
        "runtime": {
            "updated_at": str(payload.get("captured_at", "")),
            "source": f"{definition.source}:{definition.page}",
            "decision_count": 0,
            "watcher_iteration": None,
            "stale": False,
            "session": {
                "name": None,
                "current_url": str(payload.get("url", "")),
                "document_title": str(payload.get("document_title", "")),
                "page_hint": definition.page,
                "open_positions_ready": status in {"ready", "no_open_bets"},
                "validation_error": None
                if status in {"ready", "no_open_bets"}
                else _live_venue_status_line(
                    definition=definition,
                    status=status,
                    open_bets=open_bets,
                    count_hint=count_hint,
                ),
            },
        },
        "account_stats": None,
        "open_positions": [],
        "historical_positions": load_historical_positions(),
        "ledger_pnl_summary": load_ledger_pnl_summary(),
        "other_open_bets": [
            {
                "venue": venue,
                "event": str(row.get("event", "")),
                "label": str(row.get("label", "")),
                "market": str(row.get("market", "")),
                "side": str(row.get("side", "back")),
                "odds": float(row.get("odds", 0.0)),
                "stake": float(row.get("stake", 0.0)),
                "status": str(row.get("status", "open")),
            }
            for row in open_bets
        ],
        "decisions": [],
        "watch": None,
        "tracked_bets": load_tracked_bets(config.companion_legs_path),
        "exit_policy": build_exit_policy_summary(
            {"target_profit": 0.0, "stop_loss": 0.0},
            hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
            warn_only_default=config.warn_only_default,
        ),
        "exit_recommendations": [],
    }
    return snapshot


def build_unavailable_live_venue_snapshot(
    *,
    venue: str,
    detail: str,
    config: WorkerConfig,
) -> dict:
    definition = LIVE_VENUE_DEFINITIONS[venue]
    return {
        "worker": {
            "name": "bet-recorder",
            "status": "error",
            "detail": detail,
        },
        "venues": build_live_venue_summaries(
            selected_venue=venue,
            selected_status="unavailable",
        ),
        "selected_venue": venue,
        "events": [],
        "markets": [],
        "preflight": None,
        "status_line": detail,
        "runtime": {
            "updated_at": "",
            "source": f"{definition.source}:{definition.page}",
            "decision_count": 0,
            "watcher_iteration": None,
            "stale": False,
            "session": {
                "name": None,
                "current_url": "",
                "document_title": "",
                "page_hint": definition.page,
                "open_positions_ready": False,
                "validation_error": detail,
            },
        },
        "account_stats": None,
        "open_positions": [],
        "historical_positions": load_historical_positions(),
        "ledger_pnl_summary": load_ledger_pnl_summary(),
        "other_open_bets": [],
        "decisions": [],
        "watch": None,
        "tracked_bets": load_tracked_bets(config.companion_legs_path),
        "exit_policy": build_exit_policy_summary(
            {"target_profit": 0.0, "stop_loss": 0.0},
            hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
            warn_only_default=config.warn_only_default,
        ),
        "exit_recommendations": [],
    }


def build_waiting_for_watcher_snapshot(*, config: WorkerConfig, detail: str) -> dict:
    return {
        "worker": {
            "name": "bet-recorder",
            "status": "busy",
            "detail": detail,
        },
        "venues": build_live_venue_summaries(
            selected_venue="smarkets",
            selected_status="connected",
        ),
        "selected_venue": "smarkets",
        "events": [],
        "markets": [],
        "preflight": None,
        "status_line": detail,
        "runtime": {
            "updated_at": "",
            "source": "watcher-state",
            "decision_count": 0,
            "watcher_iteration": None,
            "stale": False,
            "session": {
                "name": config.agent_browser_session,
                "current_url": "",
                "document_title": "",
                "page_hint": "waiting",
                "open_positions_ready": False,
                "validation_error": detail,
            },
        },
        "account_stats": None,
        "open_positions": [],
        "historical_positions": load_historical_positions(),
        "ledger_pnl_summary": load_ledger_pnl_summary(),
        "other_open_bets": [],
        "decisions": [],
        "watch": None,
        "tracked_bets": load_tracked_bets(config.companion_legs_path),
        "exit_policy": build_exit_policy_summary(
            {"target_profit": 0.0, "stop_loss": 0.0},
            hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
            warn_only_default=config.warn_only_default,
        ),
        "exit_recommendations": [],
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


def _should_return_waiting_snapshot(config: WorkerConfig, error: ValueError) -> bool:
    if config.run_dir is None or config.agent_browser_session is None:
        return False
    message = str(error)
    return (
        "Run bundle does not contain events.jsonl" in message
        or "No positions_snapshot event found in run bundle" in message
    )


def _selected_live_venue_detail(label: str, status: str | None) -> str:
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


def _live_venue_status_line(
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


def _build_live_venue_markets(open_bets: list[dict]) -> list[dict]:
    counts: dict[str, int] = {}
    for row in open_bets:
        market = str(row.get("market", "") or "Unknown")
        counts[market] = counts.get(market, 0) + 1
    return [
        {
            "name": market,
            "contract_count": contract_count,
        }
        for market, contract_count in counts.items()
    ]


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


def _find_live_venue_targets(
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


def _matches_horse_search_terms(*, query: dict, event_name: str, selection_names: list[str]) -> bool:
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
        venue_targets = _find_live_venue_targets(targets=targets, venue=venue)
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

        for target in venue_targets:
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
            if status == "ready" and not _matches_horse_search_terms(
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

    ready_count = sum(1 for source in sources if source["status"] == "ready")
    return {
        "captured_at": captured_at,
        "source_count": len(sources),
        "ready_source_count": ready_count,
        "sources": sources,
    }


def build_horse_matcher_snapshot_response(
    *,
    market_snapshot: dict,
    selected_venue: str,
) -> dict:
    ready_source_count = int(market_snapshot.get("ready_source_count", 0))
    source_count = int(market_snapshot.get("source_count", 0))
    status = "ready" if ready_source_count > 0 else "error"
    detail = (
        f"Captured {ready_source_count} readable horse-racing market source(s) from {source_count} tab(s)."
    )
    return {
        "worker": {
            "name": "bet-recorder",
            "status": status,
            "detail": detail,
        },
        "venues": build_live_venue_summaries(selected_venue=selected_venue),
        "selected_venue": selected_venue,
        "events": [],
        "markets": [],
        "preflight": None,
        "status_line": detail,
        "runtime": {
            "updated_at": str(market_snapshot.get("captured_at", "")),
            "source": "horse_market_capture",
            "decision_count": 0,
            "watcher_iteration": None,
            "stale": False,
        },
        "account_stats": None,
        "open_positions": [],
        "historical_positions": [],
        "ledger_pnl_summary": load_ledger_pnl_summary(),
        "other_open_bets": [],
        "decisions": [],
        "watch": None,
        "tracked_bets": [],
        "exit_policy": build_exit_policy_summary(
            {"target_profit": 0.0, "stop_loss": 0.0},
            hard_margin_call_profit_floor=None,
            warn_only_default=True,
        ),
        "exit_recommendations": [],
        "horse_matcher": market_snapshot,
    }


def build_exit_policy_summary(
    watch: dict,
    *,
    hard_margin_call_profit_floor: float | None,
    warn_only_default: bool,
) -> dict:
    return {
        "target_profit": float(watch.get("target_profit", 0.0)),
        "stop_loss": float(watch.get("stop_loss", 0.0)),
        "hard_margin_call_profit_floor": hard_margin_call_profit_floor,
        "warn_only_default": warn_only_default,
    }


def load_watch_snapshot_for_config(config: WorkerConfig) -> dict:
    positions_analysis = load_positions_analysis_for_config(config)
    return build_smarkets_watch_plan(
        positions=positions_analysis["positions"],
        commission_rate=config.commission_rate,
        target_profit=config.target_profit,
        stop_loss=config.stop_loss,
    )


def load_open_positions(payload_path: Path) -> list[dict]:
    return load_positions_analysis(payload_path)["positions"]


def load_account_stats(payload_path: Path | None) -> dict | None:
    if payload_path is None:
        return None
    payload = json.loads(payload_path.read_text())
    required_fields = ("available_balance", "exposure", "unrealized_pnl", "currency")
    missing_fields = [field for field in required_fields if field not in payload]
    if missing_fields:
        raise ValueError(
            f"Account stats payload is missing fields: {', '.join(missing_fields)}",
        )
    return {
        "available_balance": float(payload["available_balance"]),
        "exposure": float(payload["exposure"]),
        "unrealized_pnl": float(payload["unrealized_pnl"]),
        "cumulative_pnl": (
            float(payload["cumulative_pnl"])
            if payload.get("cumulative_pnl") is not None
            else None
        ),
        "cumulative_pnl_label": str(payload.get("cumulative_pnl_label", "")),
        "currency": str(payload["currency"]),
    }


def load_other_open_bets(payload_path: Path | None) -> list[dict]:
    if payload_path is None:
        return []
    payload = json.loads(payload_path.read_text())
    bets = payload.get("bets")
    if not isinstance(bets, list):
        raise ValueError("Open bets payload must contain a bets list.")
    return [
        {
            "venue": str(bet.get("venue", "")),
            "event": str(bet.get("event", "")),
            "label": str(bet["label"]),
            "market": str(bet["market"]),
            "side": str(bet["side"]),
            "odds": float(bet["odds"]),
            "stake": float(bet["stake"]),
            "status": str(bet["status"]),
            "current_cashout_value": _optional_float(bet.get("current_cashout_value")),
            "supports_cash_out": bool(bet.get("supports_cash_out", False)),
        }
        for bet in bets
    ]


def capture_live_other_open_bets() -> list[dict]:
    collected: list[dict] = []
    for venue in LIVE_VENUE_ORDER:
        try:
            payload = capture_current_live_venue_payload(venue)
            analysis = analyze_live_venue_payload(venue, payload)
        except Exception:
            continue

        for row in analysis.get("open_bets", []):
            collected.append(
                {
                    "venue": venue,
                    "event": str(row.get("event", "")),
                    "label": str(row.get("label", "")),
                    "market": str(row.get("market", "")),
                    "side": str(row.get("side", "back")),
                    "odds": float(row.get("odds", 0.0)),
                    "stake": float(row.get("stake", 0.0)),
                    "status": str(row.get("status", "open")),
                    "current_cashout_value": _optional_float(row.get("current_cashout_value")),
                    "supports_cash_out": bool(row.get("supports_cash_out", False)),
                }
            )
    return collected


def merge_other_open_bets(primary: list[dict], secondary: list[dict]) -> list[dict]:
    merged: list[dict] = []
    seen: set[tuple[str, str, str, str, float, float, str]] = set()

    for row in [*(primary or []), *(secondary or [])]:
        normalized = {
            "venue": str(row.get("venue", "")),
            "event": str(row.get("event", "")),
            "label": str(row.get("label", "")),
            "market": str(row.get("market", "")),
            "side": str(row.get("side", "back")),
            "odds": float(row.get("odds", 0.0)),
            "stake": float(row.get("stake", 0.0)),
            "status": str(row.get("status", "open")),
            "current_cashout_value": _optional_float(row.get("current_cashout_value")),
            "supports_cash_out": bool(row.get("supports_cash_out", False)),
        }
        dedupe_key = (
            normalized["venue"],
            normalized["event"],
            normalized["label"],
            normalized["market"],
            normalized["odds"],
            normalized["stake"],
            normalized["status"],
            normalized["current_cashout_value"],
        )
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        merged.append(normalized)

    return merged


def load_historical_positions(payload_path: Path | None = None) -> list[dict]:
    candidate_path = payload_path or DEFAULT_LEDGER_HISTORY_PATH
    if candidate_path is None or not candidate_path.exists():
        return []

    payload = json.loads(candidate_path.read_text())
    entries = payload.get("ledger_entries")
    if not isinstance(entries, list):
        return []

    grouped_entries: dict[tuple[str, str, str], list[dict]] = {}
    for entry in entries:
        if not _is_historical_position_group_member(entry):
            continue
        group_key = _historical_position_group_key(entry)
        grouped_entries.setdefault(group_key, []).append(entry)

    historical_positions = [
        _build_historical_position_row(group)
        for group in grouped_entries.values()
        if any(_is_historical_position_entry(entry) for entry in group)
    ]

    historical_positions.sort(
        key=lambda row: (str(row.get("live_clock", "")), str(row.get("event", ""))),
        reverse=True,
    )
    return historical_positions


def load_ledger_pnl_summary(payload_path: Path | None = None) -> dict:
    candidate_path = payload_path or DEFAULT_LEDGER_HISTORY_PATH
    if candidate_path is None or not candidate_path.exists():
        return _empty_ledger_pnl_summary()

    payload = json.loads(candidate_path.read_text())
    entries = payload.get("ledger_entries")
    if not isinstance(entries, list):
        return _empty_ledger_pnl_summary()

    rows: list[tuple[str, str, float, str, str, str]] = []
    exchange_total = 0.0
    sportsbook_total = 0.0
    promo_total = 0.0
    standard_count = 0
    promo_count = 0
    unknown_count = 0

    for entry in entries:
        platform_kind = str(entry.get("platform_kind", "") or "")
        if platform_kind not in {"exchange", "sportsbook"}:
            continue

        pnl_value = entry.get("realised_pnl_gbp")
        if pnl_value in (None, ""):
            continue

        pnl_amount = _optional_float(pnl_value)
        if pnl_amount is None:
            continue

        funding_kind = _classify_ledger_funding_kind(entry)
        occurred_at = str(entry.get("occurred_at", "") or "")
        entry_id = str(entry.get("entry_id", "") or "")
        platform = str(entry.get("platform", "") or "")

        if platform_kind == "exchange":
            exchange_total += pnl_amount
        else:
            sportsbook_total += pnl_amount

        if funding_kind == "promo":
            promo_count += 1
            promo_total += pnl_amount
        elif funding_kind == "standard":
            standard_count += 1
        else:
            unknown_count += 1

        rows.append((occurred_at, entry_id, pnl_amount, platform, platform_kind, funding_kind))

    rows.sort(key=lambda row: (row[0], row[1]))

    cumulative_total = 0.0
    cumulative_promo = 0.0
    points = []
    for occurred_at, entry_id, pnl_amount, platform, platform_kind, funding_kind in rows:
        cumulative_total += pnl_amount
        if funding_kind == "promo":
            cumulative_promo += pnl_amount
        points.append(
            {
                "occurred_at": occurred_at,
                "entry_id": entry_id,
                "platform": platform,
                "platform_kind": platform_kind,
                "delta": pnl_amount,
                "total": cumulative_total,
                "promo_total": cumulative_promo,
                "funding_kind": funding_kind,
            }
        )

    return {
        "realised_total": cumulative_total,
        "exchange_total": exchange_total,
        "sportsbook_total": sportsbook_total,
        "promo_total": promo_total,
        "settled_count": len(rows),
        "standard_count": standard_count,
        "promo_count": promo_count,
        "unknown_count": unknown_count,
        "points": points,
    }


def _empty_ledger_pnl_summary() -> dict:
    return {
        "realised_total": 0.0,
        "exchange_total": 0.0,
        "sportsbook_total": 0.0,
        "promo_total": 0.0,
        "settled_count": 0,
        "standard_count": 0,
        "promo_count": 0,
        "unknown_count": 0,
        "points": [],
    }


def _classify_ledger_funding_kind(entry: dict) -> str:
    activity_type = str(entry.get("activity_type", "") or "").strip().lower()
    text_parts = [
        activity_type,
        str(entry.get("description", "") or ""),
        str(entry.get("bet_type", "") or ""),
        str(entry.get("market", "") or ""),
        str(entry.get("selection", "") or ""),
        str(entry.get("source_kind", "") or ""),
    ]
    raw_fields = entry.get("raw_fields") or {}
    if isinstance(raw_fields, dict):
        text_parts.extend(str(value) for value in raw_fields.values() if value not in (None, ""))
    haystack = " ".join(text_parts).lower()

    if any(
        keyword in haystack
        for keyword in (
            "free bet",
            "freebet",
            "snr",
            "stake returned",
            "risk free",
            "refund",
            "promo",
            "promotion",
            "bonus",
            "boost",
        )
    ):
        return "promo"

    if activity_type in {
        "bet_settled",
        "market_settled",
        "bet_won",
        "bet_lost",
        "exchange_settlement",
        "cash_out",
    }:
        return "standard"

    if any(keyword in haystack for keyword in ("qualifying", "cash", "normal")):
        return "standard"

    return "unknown"


def _historical_position_group_key(entry: dict) -> tuple[str, str, str]:
    for key_name, prefix in (
        ("group_id", "group"),
        ("bet_id", "bet"),
        ("reference", "reference"),
        ("entry_id", "entry"),
    ):
        value = str(entry.get(key_name, "") or "").strip()
        if value:
            return (
                prefix,
                value,
                str(entry.get("selection", "") or "").strip().lower(),
            )

    return (
        str(entry.get("occurred_at", "") or "").strip(),
        str(entry.get("event", "") or "").strip().lower(),
        str(entry.get("market", "") or "").strip().lower(),
    )


def load_watcher_state(*, run_dir: Path | None) -> dict | None:
    if run_dir is None:
        return None

    watcher_state_path = run_dir / "watcher-state.json"
    if not watcher_state_path.exists():
        return None

    return json.loads(watcher_state_path.read_text())


def _positions_missing_event_context(positions: list[dict]) -> bool:
    for position in positions:
        if not isinstance(position, dict):
            continue
        if (
            not str(position.get("event", "") or "").strip()
            or not str(position.get("event_status", "") or "").strip()
            or not str(position.get("event_url", "") or "").strip()
        ):
            return True
    return False


def _load_latest_positions_payload_for_config(config: WorkerConfig) -> dict | None:
    try:
        if config.positions_payload_path is not None:
            return json.loads(config.positions_payload_path.read_text())
        if config.run_dir is not None:
            return load_latest_positions_payload_from_run_dir(config.run_dir)
    except Exception:
        return None
    return None


def _should_prefer_fresh_positions(
    *,
    watcher_state: dict,
    watcher_open_positions: list[dict],
    fresh_positions_analysis: dict | None,
    fresh_positions_payload: dict | None,
) -> bool:
    if fresh_positions_analysis is None:
        return False
    if _positions_missing_event_context(watcher_open_positions):
        return True

    fresh_positions = fresh_positions_analysis.get("positions") or []
    if len(fresh_positions) != len(watcher_open_positions):
        return True

    watcher_updated_at = str(watcher_state.get("updated_at", "") or "")
    fresh_captured_at = (
        str(fresh_positions_payload.get("captured_at", "") or "")
        if isinstance(fresh_positions_payload, dict)
        else ""
    )
    if not watcher_updated_at or not fresh_captured_at:
        return False

    try:
        watcher_updated = datetime.fromisoformat(
            watcher_updated_at.replace("Z", "+00:00")
        )
        fresh_captured = datetime.fromisoformat(
            fresh_captured_at.replace("Z", "+00:00")
        )
    except ValueError:
        return False
    return fresh_captured > watcher_updated


def load_exchange_snapshot_for_config(
    config: WorkerConfig,
    *,
    selected_venue: str = DEFAULT_SELECTED_VENUE,
    capture_live: bool = True,
) -> dict:
    if selected_venue != "smarkets":
        try:
            payload = capture_current_live_venue_payload(selected_venue)
        except ValueError as exc:
            return build_unavailable_live_venue_snapshot(
                venue=selected_venue,
                detail=str(exc),
                config=config,
            )
        analysis = analyze_live_venue_payload(selected_venue, payload)
        return build_live_venue_snapshot(
            venue=selected_venue,
            payload=payload,
            analysis=analysis,
            config=config,
        )

    if capture_live and not _can_use_fresh_watcher_state_without_live_capture(config):
        capture_current_smarkets_open_positions(config)
    watcher_state = load_watcher_state(run_dir=config.run_dir)
    if watcher_state is not None:
        worker = watcher_state.get("worker") or {}
        if worker.get("status") == "error":
            return {
                "worker": worker,
                "venues": [
                    {
                        **build_live_venue_summaries(
                            selected_venue="smarkets",
                            selected_status="error",
                        )[0],
                        "status": "error",
                        "detail": str(worker.get("detail", "watcher error")),
                    },
                    *build_live_venue_summaries(
                        selected_venue="smarkets",
                        selected_status="error",
                    )[1:],
                ],
                "selected_venue": "smarkets",
                "events": [],
                "markets": [],
                "preflight": None,
                "status_line": str(worker.get("detail", "watcher error")),
                "runtime": build_runtime_summary(
                    run_dir=config.run_dir,
                    positions_payload={
                        "captured_at": watcher_state.get("updated_at", "")
                    },
                    watcher_state=watcher_state,
                ),
                "account_stats": None,
                "open_positions": [],
                "historical_positions": load_historical_positions(),
                "other_open_bets": [],
                "decisions": [],
                "watch": watcher_state.get("watch"),
                "tracked_bets": [],
                "exit_policy": build_exit_policy_summary(
                    watcher_state.get("watch") or {},
                    hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
                    warn_only_default=config.warn_only_default,
                ),
                "exit_recommendations": [],
            }
        if worker:
            watch = watcher_state.get("watch") or {
                "position_count": 0,
                "watch_count": 0,
                "commission_rate": config.commission_rate,
                "target_profit": config.target_profit,
                "stop_loss": config.stop_loss,
                "watches": [],
            }
            watcher_open_positions = watcher_state.get("open_positions", [])
            fresh_positions_payload = _load_latest_positions_payload_for_config(config)
            fresh_positions_analysis = (
                analyze_positions_payload(fresh_positions_payload)
                if fresh_positions_payload is not None
                else None
            )
            prefer_fresh_positions = _should_prefer_fresh_positions(
                watcher_state=watcher_state,
                watcher_open_positions=watcher_open_positions,
                fresh_positions_analysis=fresh_positions_analysis,
                fresh_positions_payload=fresh_positions_payload,
            )

            snapshot = build_exchange_panel_snapshot(watch)
            snapshot["worker"] = worker
            snapshot["account_stats"] = watcher_state.get("account_stats")
            snapshot["open_positions"] = (
                fresh_positions_analysis["positions"]
                if prefer_fresh_positions
                else watcher_open_positions
            )
            snapshot["historical_positions"] = load_historical_positions()
            snapshot["ledger_pnl_summary"] = load_ledger_pnl_summary()
            snapshot["other_open_bets"] = merge_other_open_bets(
                watcher_state.get("other_open_bets", []),
                capture_live_other_open_bets(),
            )
            snapshot["decisions"] = watcher_state.get("decisions", [])
            snapshot["watch"] = watcher_state.get("watch")
            snapshot["tracked_bets"] = load_tracked_bets(config.companion_legs_path)
            snapshot["exit_recommendations"] = build_exit_recommendations(
                tracked_bets=snapshot["tracked_bets"],
                open_positions=snapshot["open_positions"],
                commission_rate=config.commission_rate,
                target_profit=config.target_profit,
                stop_loss=config.stop_loss,
                hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
                warn_only_default=config.warn_only_default,
            )
            snapshot["exit_policy"] = build_exit_policy_summary(
                watch,
                hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
                warn_only_default=config.warn_only_default,
            )
            snapshot["runtime"] = build_runtime_summary(
                run_dir=config.run_dir,
                positions_payload=(
                    fresh_positions_payload
                    if prefer_fresh_positions and fresh_positions_payload is not None
                    else {"captured_at": watcher_state.get("updated_at", "")}
                ),
                watcher_state=watcher_state,
            )
            snapshot["status_line"] = worker.get("detail") or snapshot["status_line"]
            return snapshot

    if config.run_dir is not None and config.positions_payload_path is None:
        positions_payload = load_latest_positions_payload_from_run_dir(config.run_dir)
    elif config.positions_payload_path is not None:
        positions_payload = json.loads(config.positions_payload_path.read_text())
    else:
        raise ValueError("Worker config requires positions_payload_path or run_dir.")

    positions_analysis = analyze_positions_payload(positions_payload)
    watch = build_smarkets_watch_plan(
        positions=positions_analysis["positions"],
        commission_rate=config.commission_rate,
        target_profit=config.target_profit,
        stop_loss=config.stop_loss,
    )
    snapshot = build_exchange_panel_snapshot(watch)
    snapshot["account_stats"] = positions_analysis.get(
        "account_stats"
    ) or load_account_stats(config.account_payload_path)
    snapshot["open_positions"] = positions_analysis["positions"]
    snapshot["historical_positions"] = load_historical_positions()
    snapshot["ledger_pnl_summary"] = load_ledger_pnl_summary()
    snapshot["other_open_bets"] = (
        positions_analysis.get("other_open_bets")
        or load_other_open_bets(config.open_bets_payload_path)
    )
    snapshot["decisions"] = load_decisions(run_dir=config.run_dir)
    snapshot["tracked_bets"] = load_tracked_bets(config.companion_legs_path)
    snapshot["exit_recommendations"] = build_exit_recommendations(
        tracked_bets=snapshot["tracked_bets"],
        open_positions=snapshot["open_positions"],
        commission_rate=config.commission_rate,
        target_profit=config.target_profit,
        stop_loss=config.stop_loss,
        hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
        warn_only_default=config.warn_only_default,
    )
    snapshot["exit_policy"] = build_exit_policy_summary(
        watch,
        hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
        warn_only_default=config.warn_only_default,
    )
    snapshot["runtime"] = build_runtime_summary(
        run_dir=config.run_dir,
        positions_payload=positions_payload,
        watcher_state=watcher_state,
    )
    if snapshot["account_stats"] is not None:
        snapshot["worker"]["detail"] = (
            f"Loaded {watch['watch_count']} watch groups and richer account state from "
            f"{watch['position_count']} positions."
        )
    return snapshot


def _can_use_fresh_watcher_state_without_live_capture(config: WorkerConfig) -> bool:
    if config.run_dir is None:
        return False

    watcher_state = load_watcher_state(run_dir=config.run_dir)
    if watcher_state is None:
        return False

    worker = watcher_state.get("worker") or {}
    if worker.get("status") != "ready":
        return False

    updated_at = str(watcher_state.get("updated_at", "") or "")
    if not updated_at:
        return False

    interval_seconds = float(watcher_state.get("interval_seconds", 0) or 0)
    try:
        updated = datetime.fromisoformat(updated_at.replace("Z", "+00:00"))
    except ValueError:
        return False

    stale_after_seconds = max(interval_seconds * 3, interval_seconds + 5, 10)
    is_stale = (datetime.now(UTC) - updated).total_seconds() > stale_after_seconds
    return not is_stale


def _is_historical_position_entry(entry: dict) -> bool:
    if not isinstance(entry, dict):
        return False
    if str(entry.get("activity_type", "")) not in HISTORICAL_POSITION_ACTIVITY_TYPES:
        return False
    if str(entry.get("platform_kind", "")) not in {"sportsbook", "exchange"}:
        return False
    return any(str(entry.get(field, "")).strip() for field in ("event", "market", "selection"))


def _is_historical_position_group_member(entry: dict) -> bool:
    if not isinstance(entry, dict):
        return False
    if str(entry.get("platform_kind", "")) not in {"sportsbook", "exchange"}:
        return False
    return any(str(entry.get(field, "")).strip() for field in ("event", "market", "selection"))


def _build_historical_position_row(entries: list[dict]) -> dict:
    primary_entry = next(
        (entry for entry in entries if _is_historical_position_entry(entry)),
        entries[0],
    )
    odds_candidates = [
        _optional_float(entry.get("odds_decimal")) or _optional_float(entry.get("back_price"))
        for entry in entries
    ]
    odds = next((value for value in odds_candidates if value is not None), None)
    payout = sum(_optional_float(entry.get("payout_gbp")) or 0.0 for entry in entries)
    stake = sum(_optional_float(entry.get("stake_gbp")) or 0.0 for entry in entries)
    liability = sum(
        abs(_optional_float(entry.get("exposure_gbp")) or _optional_float(entry.get("stake_gbp")) or 0.0)
        for entry in entries
    )
    pnl_amount = sum(
        (
            _optional_float(entry.get("realised_pnl_gbp"))
            if entry.get("realised_pnl_gbp") not in (None, "")
            else _optional_float(entry.get("amount_gbp"))
        )
        or 0.0
        for entry in entries
        if _is_historical_position_entry(entry)
    )
    occurred_at = str(primary_entry.get("occurred_at", ""))
    selections = [
        str(entry.get("selection", "") or "").strip()
        for entry in entries
        if str(entry.get("selection", "") or "").strip()
    ]
    unique_selections = list(dict.fromkeys(selections))
    if len(unique_selections) == 1:
        contract = unique_selections[0]
    elif len(unique_selections) > 1:
        contract = "Multiple"
    else:
        contract = str(primary_entry.get("activity_type", "") or "settled")

    return {
        "event": str(primary_entry.get("event", "") or ""),
        "event_status": "Settled",
        "event_url": "",
        "contract": contract,
        "market": str(primary_entry.get("market", "") or primary_entry.get("bet_type", "") or ""),
        "status": "settled",
        "market_status": "settled",
        "is_in_play": False,
        "price": odds or 0.0,
        "stake": stake,
        "liability": liability,
        "current_value": payout,
        "pnl_amount": pnl_amount or 0.0,
        "current_back_odds": odds,
        "current_implied_probability": (1.0 / odds) if odds else None,
        "current_implied_percentage": (100.0 / odds) if odds else None,
        "current_buy_odds": odds,
        "current_buy_implied_probability": (1.0 / odds) if odds else None,
        "current_sell_odds": None,
        "current_sell_implied_probability": None,
        "current_score": "",
        "current_score_home": None,
        "current_score_away": None,
        "live_clock": occurred_at,
        "can_trade_out": False,
    }


def build_runtime_summary(
    *,
    run_dir: Path | None,
    positions_payload: dict,
    watcher_state: dict | None = None,
) -> dict:
    runtime = {
        "updated_at": str(positions_payload.get("captured_at", "")),
        "source": "positions_snapshot",
        "decision_count": 0,
        "watcher_iteration": None,
        "stale": False,
        "session": build_runtime_session_summary(
            positions_payload=positions_payload,
            watcher_state=watcher_state,
        ),
    }
    if run_dir is None:
        return runtime

    watcher_state_path = run_dir / "watcher-state.json"
    if watcher_state is None and not watcher_state_path.exists():
        return runtime

    effective_watcher_state = watcher_state
    if effective_watcher_state is None:
        effective_watcher_state = json.loads(watcher_state_path.read_text())

    runtime["updated_at"] = str(
        effective_watcher_state.get("updated_at") or runtime["updated_at"]
    )
    runtime["source"] = "watcher-state"
    runtime["decision_count"] = int(
        effective_watcher_state.get(
            "decision_count",
            len(effective_watcher_state.get("decisions", [])),
        ),
    )
    runtime["watcher_iteration"] = effective_watcher_state.get("iteration")
    runtime["session"] = build_runtime_session_summary(
        positions_payload=positions_payload,
        watcher_state=effective_watcher_state,
    )

    interval_seconds = float(effective_watcher_state.get("interval_seconds", 0) or 0)
    if interval_seconds > 0 and runtime["updated_at"]:
        updated_at = datetime.fromisoformat(
            runtime["updated_at"].replace("Z", "+00:00")
        )
        runtime["stale"] = (datetime.now(UTC) - updated_at).total_seconds() > max(
            interval_seconds * 3,
            interval_seconds + 5,
        )
    return runtime


def build_runtime_session_summary(
    *, positions_payload: dict, watcher_state: dict | None
) -> dict | None:
    if watcher_state is not None and isinstance(watcher_state.get("session"), dict):
        return watcher_state["session"]

    current_url = str(positions_payload.get("url", "") or "")
    document_title = str(positions_payload.get("document_title", "") or "").strip()
    if not current_url and not document_title:
        return None

    return {
        "name": None,
        "current_url": current_url,
        "document_title": document_title,
        "page_hint": (
            "open_positions"
            if "open-positions" in current_url
            or "open positions" in document_title.lower()
            else "unknown"
        ),
        "open_positions_ready": (
            "open-positions" in current_url
            or "open positions" in document_title.lower()
        ),
        "validation_error": None,
    }


def load_decisions(*, run_dir: Path | None) -> list[dict]:
    if run_dir is None:
        return []

    watcher_state_path = run_dir / "watcher-state.json"
    if not watcher_state_path.exists():
        return []

    watcher_state = json.loads(watcher_state_path.read_text())
    decisions = watcher_state.get("decisions", [])
    if not isinstance(decisions, list):
        return []

    return [
        {
            "contract": str(decision.get("contract", "")),
            "market": str(decision.get("market", "")),
            "status": str(decision.get("status", "hold")),
            "reason": str(decision.get("reason", "unknown")),
            "current_pnl_amount": float(decision.get("current_pnl_amount", 0.0)),
            "current_back_odds": (
                float(decision["current_back_odds"])
                if decision.get("current_back_odds") is not None
                else None
            ),
            "profit_take_back_odds": float(decision.get("profit_take_back_odds", 0.0)),
            "stop_loss_back_odds": float(decision.get("stop_loss_back_odds", 0.0)),
        }
        for decision in decisions
    ]


def parse_worker_request(request: str | dict) -> tuple[str, dict | None]:
    if isinstance(request, str) and request in {"LoadDashboard", "Refresh"}:
        return request, None

    if isinstance(request, dict) and len(request) == 1:
        request_name, request_payload = next(iter(request.items()))
        if request_name in {"LoadDashboard", "Refresh"}:
            if request_payload is None:
                return request_name, None
            if not isinstance(request_payload, dict):
                raise ValueError(
                    f"{request_name} payload must be an object when provided."
                )
            return request_name, request_payload

        if request_name == "SelectVenue":
            if not isinstance(request_payload, dict):
                raise ValueError("SelectVenue payload must be an object.")
            return request_name, request_payload

        if request_name == "CashOutTrackedBet":
            if not isinstance(request_payload, dict):
                raise ValueError("CashOutTrackedBet payload must be an object.")
            return request_name, request_payload

        if request_name == "ExecuteTradingAction":
            if not isinstance(request_payload, dict):
                raise ValueError("ExecuteTradingAction payload must be an object.")
            return request_name, request_payload

        if request_name == "LoadHorseMatcher":
            if not isinstance(request_payload, dict):
                raise ValueError("LoadHorseMatcher payload must be an object.")
            return request_name, request_payload

    raise ValueError(f"Unsupported worker request: {request}")


def require_worker_config(
    config: WorkerConfig | None, request_name: str
) -> WorkerConfig:
    if config is None:
        raise ValueError(
            f"{request_name} requires worker config. Send LoadDashboard with config first.",
        )
    return config


def resolve_load_dashboard_config(
    request_payload: dict | None,
    *,
    config: WorkerConfig | None,
) -> WorkerConfig:
    next_config = config
    if request_payload is not None:
        request_config = request_payload.get("config")
        if request_config is not None:
            if not isinstance(request_config, dict):
                raise ValueError("LoadDashboard config must be an object.")
            next_config = WorkerConfig.from_dict(request_config)
    return require_worker_config(next_config, "LoadDashboard")


def build_worker_request_error_response(detail: str) -> dict:
    return {
        "snapshot": {
            "worker": {
                "name": "bet-recorder",
                "status": "error",
                "detail": detail,
            },
            "venues": [],
            "selected_venue": None,
            "events": [],
            "markets": [],
            "preflight": None,
            "status_line": detail,
            "watch": None,
        },
        "request_error": detail,
    }


def handle_worker_request(
    *,
    request: str | dict,
    config: WorkerConfig | None,
    selected_venue: str | None = None,
):
    effective_selected_venue = selected_venue or DEFAULT_SELECTED_VENUE
    request_name, request_payload = parse_worker_request(request)

    if request_name == "LoadDashboard":
        resolved_config = resolve_load_dashboard_config(
            request_payload,
            config=config,
        )
        try:
            snapshot = load_exchange_snapshot_for_config(
                resolved_config,
                selected_venue=effective_selected_venue,
                capture_live=False,
            )
        except ValueError as exc:
            if _should_return_waiting_snapshot(resolved_config, exc):
                snapshot = build_waiting_for_watcher_snapshot(
                    config=resolved_config,
                    detail="Recorder started; waiting for first snapshot.",
                )
            else:
                raise
        response = {"snapshot": snapshot}
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, effective_selected_venue

    resolved_config = require_worker_config(config, request_name)

    if request_name == "Refresh":
        response = {
            "snapshot": load_exchange_snapshot_for_config(
                resolved_config,
                selected_venue=effective_selected_venue,
                capture_live=(effective_selected_venue != "smarkets"),
            )
        }
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, effective_selected_venue

    if request_name == "SelectVenue":
        assert request_payload is not None
        try:
            venue = request_payload["venue"]
        except KeyError as exc:
            raise ValueError("SelectVenue payload must include venue.") from exc
        if venue not in {"smarkets", *LIVE_VENUE_DEFINITIONS.keys()}:
            raise ValueError(f"Unsupported venue for recorder worker: {venue}")
        response = {
            "snapshot": load_exchange_snapshot_for_config(
                resolved_config,
                selected_venue=venue,
                capture_live=(venue != "smarkets"),
            )
        }
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, venue

    if request_name == "CashOutTrackedBet":
        assert request_payload is not None
        try:
            bet_id = str(request_payload["bet_id"])
        except KeyError as exc:
            raise ValueError("CashOutTrackedBet payload must include bet_id.") from exc
        snapshot = load_exchange_snapshot_for_config(
            resolved_config,
            selected_venue=effective_selected_venue,
            capture_live=False,
        )
        response = {
            "snapshot": handle_cash_out_tracked_bet(snapshot=snapshot, bet_id=bet_id)
        }
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, effective_selected_venue

    if request_name == "ExecuteTradingAction":
        assert request_payload is not None
        try:
            intent_payload = request_payload["intent"]
        except KeyError as exc:
            raise ValueError("ExecuteTradingAction payload must include intent.") from exc
        if not isinstance(intent_payload, dict):
            raise ValueError("ExecuteTradingAction intent must be an object.")
        result = execute_trading_action(
            intent_payload=intent_payload,
            agent_browser_session=resolved_config.agent_browser_session,
            run_dir=resolved_config.run_dir,
        )
        snapshot = load_exchange_snapshot_for_config(
            resolved_config,
            selected_venue="smarkets",
            capture_live=True,
        )
        snapshot["worker"] = {
            **dict(snapshot.get("worker") or {}),
            "status": "ready",
            "detail": result.detail,
        }
        snapshot["status_line"] = result.detail
        response = {"snapshot": snapshot}
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, "smarkets"

    if request_name == "LoadHorseMatcher":
        assert request_payload is not None
        query = request_payload.get("query")
        if not isinstance(query, dict):
            raise ValueError("LoadHorseMatcher payload must include query.")
        market_snapshot = capture_live_horse_market_snapshot(query)
        response = {
            "snapshot": build_horse_matcher_snapshot_response(
                market_snapshot=market_snapshot,
                selected_venue=effective_selected_venue,
            )
        }
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, effective_selected_venue

    raise AssertionError(f"Unhandled worker request type: {request_name}")


def handle_worker_request_line(
    *,
    request_line: str,
    config: WorkerConfig | None,
    selected_venue: str | None = None,
):
    effective_selected_venue = selected_venue or DEFAULT_SELECTED_VENUE
    next_config = config
    next_selected_venue = effective_selected_venue
    try:
        request = json.loads(request_line)
        request_name, request_payload = parse_worker_request(request)
        if request_name == "LoadDashboard":
            next_config = resolve_load_dashboard_config(
                request_payload,
                config=config,
            )
        if request_name == "SelectVenue" and request_payload is not None:
            next_selected_venue = str(request_payload.get("venue") or next_selected_venue)
        if selected_venue is None:
            return handle_worker_request(
                request=request,
                config=config,
            )
        return handle_worker_request(
            request=request,
            config=config,
            selected_venue=effective_selected_venue,
        )
    except ValueError as exc:
        if selected_venue is None:
            return build_worker_request_error_response(str(exc)), next_config
        return build_worker_request_error_response(str(exc)), next_config, next_selected_venue


def iter_worker_session_responses(
    *,
    request_lines: Iterable[str],
) -> Iterator[dict]:
    config: WorkerConfig | None = None
    selected_venue = DEFAULT_SELECTED_VENUE
    for request_line in request_lines:
        normalized_request_line = request_line.strip()
        if not normalized_request_line:
            continue
        response, config, selected_venue = handle_worker_request_line(
            request_line=normalized_request_line,
            config=config,
            selected_venue=selected_venue,
        )
        yield response


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)
