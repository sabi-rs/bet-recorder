from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import json

from bet_recorder.analysis.bet365 import analyze_bet365_page
from bet_recorder.analysis.betuk import analyze_betuk_page
from bet_recorder.analysis.betway_uk import analyze_betway_page
from bet_recorder.analysis.position_watch import build_smarkets_watch_plan
from bet_recorder.actions.smarkets_cashout import handle_cash_out_tracked_bet
from bet_recorder.analysis.smarkets_exchange import analyze_smarkets_page
from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.browser.cdp import (
    capture_debug_target_page_state,
    list_debug_targets,
    select_debug_target_by_fragments,
)
from bet_recorder.capture.run_bundle import load_run_bundle
from bet_recorder.ledger.loader import load_tracked_bets
from bet_recorder.ledger.policy import build_exit_recommendations
from bet_recorder.live.agent_browser_capture import capture_agent_browser_page

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
    "betdaq": LiveVenueDefinition(
        venue="betdaq",
        label="Betdaq",
        source="betdaq",
        page="open_positions",
        url_fragments=("betdaq.com",),
    ),
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

    bundle = load_run_bundle(source="smarkets_exchange", run_dir=config.run_dir)
    client = AgentBrowserClient(session=config.agent_browser_session)
    capture_agent_browser_page(
        source="smarkets_exchange",
        bundle=bundle,
        page="open_positions",
        captured_at=datetime.now(UTC),
        client=client,
        notes=["exchange-worker-refresh"],
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
    if venue in {"betfred", "betdaq"}:
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
        "other_open_bets": [
            {
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
    for venue in ("bet365", "betdaq", "betfred", "betway", "betuk"):
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
            "label": str(bet["label"]),
            "market": str(bet["market"]),
            "side": str(bet["side"]),
            "odds": float(bet["odds"]),
            "stake": float(bet["stake"]),
            "status": str(bet["status"]),
        }
        for bet in bets
    ]


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
        occurred_day = str(entry.get("occurred_at", ""))[:10]
        group_key = (
            occurred_day,
            str(entry.get("event", "") or "").strip().lower(),
            str(entry.get("market", "") or "").strip().lower(),
        )
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


def load_watcher_state(*, run_dir: Path | None) -> dict | None:
    if run_dir is None:
        return None

    watcher_state_path = run_dir / "watcher-state.json"
    if not watcher_state_path.exists():
        return None

    return json.loads(watcher_state_path.read_text())


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

    if capture_live:
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
            snapshot = build_exchange_panel_snapshot(watch)
            snapshot["worker"] = worker
            snapshot["account_stats"] = watcher_state.get("account_stats")
            snapshot["open_positions"] = watcher_state.get("open_positions", [])
            snapshot["historical_positions"] = load_historical_positions()
            snapshot["other_open_bets"] = watcher_state.get("other_open_bets", [])
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
                positions_payload={"captured_at": watcher_state.get("updated_at", "")},
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
    snapshot["other_open_bets"] = positions_analysis.get(
        "other_open_bets"
    ) or load_other_open_bets(config.open_bets_payload_path)
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
        response = {
            "snapshot": load_exchange_snapshot_for_config(
                resolved_config,
                selected_venue=effective_selected_venue,
                capture_live=True,
            )
        }
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
