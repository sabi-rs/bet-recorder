from __future__ import annotations

from collections.abc import Iterable, Iterator
from copy import deepcopy
from datetime import UTC, datetime
from pathlib import Path
import json

from bet_recorder import exchange_worker_live as exchange_worker_live_module
from bet_recorder.analysis.position_watch import build_smarkets_watch_plan
from bet_recorder.analysis.racing_markets import analyze_racing_market_page
from bet_recorder.actions.smarkets_cashout import handle_cash_out_tracked_bet
from bet_recorder.actions.trading_actions import execute_trading_action
from bet_recorder.analysis.smarkets_exchange import analyze_smarkets_page
from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.browser.cdp import (
    capture_debug_target_page_state,
    click_debug_target_by_labels,
    list_debug_targets,
    navigate_debug_target,
    select_debug_target_by_fragments,
)
from bet_recorder.bookmaker_history_runtime import (
    extract_live_bookmaker_ledger_entries,
    load_runtime_bookmaker_history_entries,
    load_runtime_bookmaker_history_payload,
    merge_runtime_bookmaker_history_entries,
    runtime_bookmaker_history_path,
    write_runtime_bookmaker_history_entries,
)
from bet_recorder.capture.bookmaker_history_sync import append_bookmaker_history_sync_event
from bet_recorder.exchange_worker_evidence import (
    build_recorder_bundle_summary,
    build_transport_marker_summary,
    load_recent_recorder_events,
    load_recent_transport_markers,
)
from bet_recorder.exchange_worker_live import (
    LIVE_VENUE_DEFINITIONS,
    LIVE_VENUE_ORDER,
    capture_live_venue_history_sync_report,
    live_venue_status_line,
)
from bet_recorder.exchange_worker_runtime import (
    build_runtime_summary,
    load_decisions,
)
from bet_recorder.ledger.loader import load_tracked_bets, load_tracked_bets_payload
from bet_recorder.ledger.policy import build_exit_recommendations
from bet_recorder.ledger.tracked_bets_backfill import (
    build_backfilled_tracked_bets,
    load_statement_history_payload,
)
from bet_recorder.ledger.taxonomy import (
    funding_kind_is_cash,
    funding_kind_is_promo,
    infer_funding_kind,
    infer_market_family,
    normalize_funding_kind,
)
from bet_recorder.tracked_bets_runtime import (
    auto_tracked_bets_path,
    event_matches,
    load_runtime_tracked_bets,
    market_matches,
    normalize_market,
    normalize_key,
    reconcile_runtime_tracked_bets,
    text_matches,
    write_runtime_tracked_bets,
)
from bet_recorder.worker_protocol import (
    WorkerConfig,
    build_worker_request_error_response,
    parse_worker_request,
    require_worker_config,
    resolve_load_dashboard_config,
)

DEFAULT_LEDGER_HISTORY_PATH = (
    Path.home() / "Documents" / "Bets" / "output" / "ledger" / "statement-history.json"
)
BOOKMAKER_HISTORY_SYNC_INTERVAL_SECONDS = 300
BOOKMAKER_HISTORY_SYNC_BATCH_SIZE = 1
HISTORICAL_POSITION_ACTIVITY_TYPES = {
    "bet_settled",
    "exchange_settlement",
    "market_settled",
    "cash_out",
}
DEFAULT_SELECTED_VENUE = "smarkets"


def _sync_live_module_test_seams() -> None:
    exchange_worker_live_module.list_debug_targets = list_debug_targets
    exchange_worker_live_module.capture_debug_target_page_state = (
        capture_debug_target_page_state
    )
    exchange_worker_live_module.navigate_debug_target = navigate_debug_target
    exchange_worker_live_module.click_debug_target_by_labels = (
        click_debug_target_by_labels
    )
    exchange_worker_live_module.select_debug_target_by_fragments = (
        select_debug_target_by_fragments
    )
    exchange_worker_live_module.analyze_racing_market_page = analyze_racing_market_page


def capture_current_live_venue_payload(venue: str) -> dict:
    _sync_live_module_test_seams()
    return exchange_worker_live_module.capture_current_live_venue_payload(venue)


def capture_live_venue_history_payload(venue: str) -> dict:
    _sync_live_module_test_seams()
    return exchange_worker_live_module.capture_live_venue_history_payload(venue)


def analyze_live_venue_payload(venue: str, payload: dict) -> dict:
    return exchange_worker_live_module.analyze_live_venue_payload(venue, payload)


def build_live_venue_summaries(
    *,
    selected_venue: str | None,
    selected_status: str | None = None,
) -> list[dict]:
    _sync_live_module_test_seams()
    return exchange_worker_live_module.build_live_venue_summaries(
        selected_venue=selected_venue,
        selected_status=selected_status,
    )


def capture_live_horse_market_snapshot(query: dict) -> dict:
    _sync_live_module_test_seams()
    return exchange_worker_live_module.capture_live_horse_market_snapshot(query)


def _load_existing_tracked_bets(config: WorkerConfig) -> list[dict]:
    if config.companion_legs_path is not None:
        return load_tracked_bets(config.companion_legs_path)

    tracked_bets: list[dict] = []
    tracked_bets_path = auto_tracked_bets_path(config.run_dir)
    if tracked_bets_path is not None and tracked_bets_path.exists():
        tracked_bets = load_tracked_bets(tracked_bets_path)
    return _merge_existing_and_backfilled_tracked_bets(config, tracked_bets)


def _load_cached_other_open_bets(cached_snapshot: dict | None) -> list[dict]:
    if not isinstance(cached_snapshot, dict):
        return []
    rows = cached_snapshot.get("other_open_bets")
    if not isinstance(rows, list):
        return []
    return [dict(row) for row in rows if isinstance(row, dict)]


def _sync_tracked_bets_for_snapshot(
    *,
    config: WorkerConfig,
    open_positions: list[dict],
    other_open_bets: list[dict],
    historical_positions: list[dict],
    captured_at: str,
    commission_rate: float,
) -> list[dict]:
    if config.companion_legs_path is not None:
        return load_tracked_bets(config.companion_legs_path)

    tracked_bets_path = auto_tracked_bets_path(config.run_dir)
    if tracked_bets_path is None:
        return _load_existing_tracked_bets(config)

    existing_tracked_bets = load_runtime_tracked_bets(tracked_bets_path)
    reconciled_tracked_bets = reconcile_runtime_tracked_bets(
        existing_tracked_bets=existing_tracked_bets,
        open_positions=open_positions,
        other_open_bets=other_open_bets,
        historical_positions=historical_positions,
        captured_at=captured_at,
        commission_rate=commission_rate,
    )
    if reconciled_tracked_bets or tracked_bets_path.exists():
        write_runtime_tracked_bets(
            tracked_bets_path,
            reconciled_tracked_bets,
            updated_at=captured_at or datetime.now(UTC).isoformat(),
        )
    return _load_existing_tracked_bets(config)


def _merge_existing_and_backfilled_tracked_bets(
    config: WorkerConfig,
    existing_tracked_bets: list[dict],
) -> list[dict]:
    merged = [deepcopy(tracked_bet) for tracked_bet in existing_tracked_bets]
    for backfilled_tracked_bet in _load_auto_backfilled_tracked_bets(config):
        existing_index = _find_matching_tracked_bet_index(
            merged,
            candidate=backfilled_tracked_bet,
        )
        if existing_index is None:
            merged.append(backfilled_tracked_bet)
            continue
        merged[existing_index] = _merge_tracked_bet_rows(
            existing=merged[existing_index],
            incoming=backfilled_tracked_bet,
        )
    merged.sort(
        key=lambda tracked_bet: (
            _tracked_bet_sort_rank(tracked_bet),
            str(tracked_bet.get("placed_at", "") or ""),
            str(tracked_bet.get("bet_id", "") or ""),
        )
    )
    return merged


def _load_auto_backfilled_tracked_bets(config: WorkerConfig) -> list[dict]:
    payload = _load_combined_statement_history_payload(config.run_dir)
    entries = payload.get("ledger_entries") if isinstance(payload, dict) else None
    if not isinstance(entries, list) or not entries:
        return []
    backfilled_payload = build_backfilled_tracked_bets(payload)
    if not backfilled_payload.get("tracked_bets"):
        return []
    return load_tracked_bets_payload(backfilled_payload)


def _load_combined_statement_history_payload(run_dir: Path | None) -> dict:
    ledger_entries: list[dict] = []
    seen_entry_ids: set[str] = set()

    if DEFAULT_LEDGER_HISTORY_PATH.exists():
        payload = load_statement_history_payload(DEFAULT_LEDGER_HISTORY_PATH)
        for entry in payload.get("ledger_entries", []):
            if not isinstance(entry, dict):
                continue
            entry_id = str(entry.get("entry_id", "") or "")
            if entry_id and entry_id in seen_entry_ids:
                continue
            if entry_id:
                seen_entry_ids.add(entry_id)
            ledger_entries.append(dict(entry))

    runtime_history_path = runtime_bookmaker_history_path(run_dir)
    for entry in load_runtime_bookmaker_history_entries(runtime_history_path):
        entry_id = str(entry.get("entry_id", "") or "")
        if entry_id and entry_id in seen_entry_ids:
            continue
        if entry_id:
            seen_entry_ids.add(entry_id)
        ledger_entries.append(dict(entry))

    for entry in _load_synthetic_smarkets_history_entries(run_dir):
        entry_id = str(entry.get("entry_id", "") or "")
        if entry_id and entry_id in seen_entry_ids:
            continue
        if entry_id:
            seen_entry_ids.add(entry_id)
        ledger_entries.append(dict(entry))

    return {"ledger_entries": ledger_entries}


def _load_historical_positions_for_run_dir(run_dir: Path | None) -> list[dict]:
    payload = _load_combined_statement_history_payload(run_dir)
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


def _load_synthetic_smarkets_history_entries(run_dir: Path | None) -> list[dict]:
    if run_dir is None:
        return []
    try:
        payload = load_latest_page_payload_from_run_dir(
            run_dir,
            kind="history_snapshot",
            page="history",
        )
    except ValueError:
        return []

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return []
    portfolio = metadata.get("smarkets_portfolio")
    if not isinstance(portfolio, dict):
        return []

    captured_at = str(payload.get("captured_at", "") or "")
    entries: list[dict] = []
    for index, position in enumerate(portfolio.get("positions") or []):
        if not isinstance(position, dict):
            continue
        entry = _build_synthetic_smarkets_history_entry(
            position=position,
            captured_at=captured_at,
            index=index,
        )
        if entry is not None:
            entries.append(entry)
    return entries


def _build_synthetic_smarkets_history_entry(
    *,
    position: dict,
    captured_at: str,
    index: int,
) -> dict | None:
    contract = str(position.get("contract", "") or "").strip()
    market = str(position.get("market", "") or "").strip()
    event = str(position.get("event", "") or "").strip()
    if not contract or not market or not event:
        return None

    side = str(position.get("side", "") or "").strip().lower()
    price = _optional_float(position.get("price")) or 0.0
    raw_stake = _optional_float(position.get("stake"))
    raw_liability = _optional_float(position.get("liability"))
    return_amount = _optional_float(position.get("return_amount"))
    current_value = (
        _optional_float(position.get("current_value"))
        if position.get("current_value") not in (None, "")
        else return_amount
    ) or 0.0
    stake = raw_stake
    if stake is None:
        stake = _derive_smarkets_history_stake(
            side=side,
            price=price,
            liability=raw_liability,
            return_amount=return_amount,
            current_value=current_value,
        )
    stake = stake or 0.0
    liability = raw_liability
    if liability is None:
        liability = _derive_smarkets_history_liability(
            side=side,
            price=price,
            stake=stake,
            current_value=current_value,
        )
    liability = liability or 0.0
    pnl_amount = _optional_float(position.get("pnl_amount"))
    if pnl_amount is None:
        pnl_amount = _derive_smarkets_history_pnl_amount(
            side=side,
            price=price,
            stake=stake,
            liability=liability,
            return_amount=return_amount,
            current_value=current_value,
        )
    status = str(position.get("status", "") or "Settled").strip().lower()
    return {
        "entry_id": f"run_history:{captured_at}:{index}",
        "occurred_at": captured_at,
        "platform": "smarkets",
        "activity_type": "bet_settled",
        "status": status,
        "platform_kind": "exchange",
        "exchange": "smarkets",
        "event": event,
        "market": market,
        "selection": contract,
        "side": "lay" if side == "sell" else "back",
        "bet_type": "single",
        "market_family": infer_market_family(
            explicit_market_family=None,
            market=market,
        ),
        "currency": "GBP",
        "stake_gbp": stake,
        "odds_decimal": price,
        "exposure_gbp": (-abs(liability) if side == "sell" else abs(stake)),
        "payout_gbp": None,
        "realised_pnl_gbp": pnl_amount,
        "reference": f"run_history:{index}",
        "source_file": "events.jsonl",
        "source_kind": "smarkets_run_history",
        "description": str(position.get("status", "") or "Settled"),
    }


def sync_live_bookmaker_history(config: WorkerConfig) -> list[dict]:
    return sync_live_bookmaker_history_for_run_dir(config.run_dir)


def sync_live_bookmaker_history_for_run_dir(run_dir: Path | None) -> list[dict]:
    history_path = runtime_bookmaker_history_path(run_dir)
    if history_path is None:
        return []
    existing_payload = load_runtime_bookmaker_history_payload(history_path)
    existing_entries = list(existing_payload.get("ledger_entries", []))
    venue_updated_at = dict(existing_payload.get("venue_updated_at", {}))
    sync_reports = dict(existing_payload.get("sync_reports", {}))
    available_venues = _list_available_live_bookmaker_history_venues()
    venues_to_capture = _select_due_live_bookmaker_history_venues(
        available_venues=available_venues,
        venue_updated_at=venue_updated_at,
    )
    if not venues_to_capture:
        return existing_entries
    sync_result = capture_live_bookmaker_history_entries(venues=venues_to_capture)
    incoming_entries = list(sync_result.get("entries", []))
    reports = list(sync_result.get("reports", []))
    merged_entries = merge_runtime_bookmaker_history_entries(
        existing_entries,
        incoming_entries,
    )
    sync_timestamp = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    for venue in venues_to_capture:
        venue_updated_at[venue] = sync_timestamp
    for report in reports:
        venue = str(report.get("venue", "") or "").strip()
        if venue:
            sync_reports[venue] = dict(report)
        if run_dir is not None:
            append_bookmaker_history_sync_event(run_dir / "events.jsonl", report)
    if merged_entries or history_path.exists():
        write_runtime_bookmaker_history_entries(
            history_path,
            merged_entries,
            updated_at=sync_timestamp,
            venue_updated_at=venue_updated_at,
            sync_reports=sync_reports,
        )
    return merged_entries


def _list_available_live_bookmaker_history_venues() -> list[str]:
    try:
        targets = list_debug_targets()
    except Exception:
        return []
    available: list[str] = []
    for venue in LIVE_VENUE_ORDER:
        definition = LIVE_VENUE_DEFINITIONS.get(venue)
        if definition is None:
            continue
        try:
            select_debug_target_by_fragments(
                targets=targets,
                url_fragments=definition.url_fragments,
            )
        except ValueError:
            continue
        available.append(venue)
    return available


def _select_due_live_bookmaker_history_venues(
    *,
    available_venues: list[str],
    venue_updated_at: dict[str, str],
) -> list[str]:
    due: list[str] = []
    now = datetime.now(UTC)
    for venue in available_venues:
        updated_at = _parse_runtime_timestamp(venue_updated_at.get(venue))
        if updated_at is None:
            due.append(venue)
            continue
        age_seconds = (now - updated_at).total_seconds()
        if age_seconds >= BOOKMAKER_HISTORY_SYNC_INTERVAL_SECONDS:
            due.append(venue)
    return due[:BOOKMAKER_HISTORY_SYNC_BATCH_SIZE]


def _parse_runtime_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(UTC)
    except ValueError:
        return None


def _find_matching_tracked_bet_index(
    tracked_bets: list[dict],
    *,
    candidate: dict,
) -> int | None:
    for index, existing in enumerate(tracked_bets):
        if _tracked_bets_match(existing, candidate):
            return index
    return None


def _tracked_bets_match(left: dict, right: dict) -> bool:
    left_platform = normalize_key(str(left.get("platform", "") or ""))
    right_platform = normalize_key(str(right.get("platform", "") or ""))
    if left_platform and right_platform and left_platform != right_platform:
        return False
    return (
        event_matches(str(left.get("event", "") or ""), str(right.get("event", "") or ""))
        and market_matches(str(left.get("market", "") or ""), str(right.get("market", "") or ""))
        and text_matches(str(left.get("selection", "") or ""), str(right.get("selection", "") or ""))
        and _tracked_bet_time_matches(left, right)
    )


def _tracked_bet_time_matches(left: dict, right: dict) -> bool:
    left_settled = str(left.get("settled_at", "") or "").strip()
    right_settled = str(right.get("settled_at", "") or "").strip()
    if left_settled and right_settled:
        return left_settled == right_settled
    left_placed = str(left.get("placed_at", "") or "").strip()
    right_placed = str(right.get("placed_at", "") or "").strip()
    if left_placed and right_placed:
        return left_placed == right_placed
    return True


def _merge_tracked_bet_rows(*, existing: dict, incoming: dict) -> dict:
    merged = deepcopy(existing)
    for field in (
        "settled_at",
        "realised_pnl_gbp",
        "payout_gbp",
        "funding_kind",
        "bet_type",
        "market_family",
        "platform",
        "platform_kind",
        "exchange",
        "sport_name",
        "stake_gbp",
        "potential_returns_gbp",
        "back_price",
        "lay_price",
    ):
        if merged.get(field) in (None, "", {}, []) and incoming.get(field) not in (None, "", {}, []):
            merged[field] = incoming[field]

    incoming_status = str(incoming.get("status", "") or "").strip()
    existing_status = str(merged.get("status", "") or "").strip()
    if _tracked_bet_sort_rank(incoming) > _tracked_bet_sort_rank(merged):
        merged["status"] = incoming_status or existing_status
    elif not existing_status and incoming_status:
        merged["status"] = incoming_status

    if incoming.get("expected_ev", {}).get("status") == "calculated" and merged.get("expected_ev", {}).get("status") != "calculated":
        merged["expected_ev"] = incoming["expected_ev"]
    if incoming.get("realised_ev", {}).get("status") == "calculated" and merged.get("realised_ev", {}).get("status") != "calculated":
        merged["realised_ev"] = incoming["realised_ev"]

    merged["legs"] = _merge_tracked_bet_legs(
        merged.get("legs", []),
        incoming.get("legs", []),
    )
    merged["activities"] = _merge_tracked_bet_activities(
        merged.get("activities", []),
        incoming.get("activities", []),
    )
    return merged


def _merge_tracked_bet_legs(existing_legs: list[dict], incoming_legs: list[dict]) -> list[dict]:
    merged = [deepcopy(leg) for leg in existing_legs if isinstance(leg, dict)]
    for incoming_leg in incoming_legs:
        if not isinstance(incoming_leg, dict):
            continue
        incoming_venue = normalize_key(str(incoming_leg.get("venue", "") or ""))
        for index, existing_leg in enumerate(merged):
            if normalize_key(str(existing_leg.get("venue", "") or "")) == incoming_venue:
                merged[index] = deepcopy(existing_leg)
                for field in (
                    "status",
                    "settled_at",
                    "placed_at",
                    "liability",
                    "commission_rate",
                    "market",
                    "exchange",
                ):
                    if merged[index].get(field) in (None, "", {}, []) and incoming_leg.get(field) not in (None, "", {}, []):
                        merged[index][field] = incoming_leg[field]
                if _leg_status_rank(incoming_leg) > _leg_status_rank(existing_leg):
                    merged[index]["status"] = incoming_leg.get("status", existing_leg.get("status"))
                    if incoming_leg.get("settled_at") not in (None, ""):
                        merged[index]["settled_at"] = incoming_leg["settled_at"]
                break
        else:
            merged.append(deepcopy(incoming_leg))
    return merged


def _merge_tracked_bet_activities(existing_activities: list[dict], incoming_activities: list[dict]) -> list[dict]:
    merged = [deepcopy(activity) for activity in existing_activities if isinstance(activity, dict)]
    seen = {
        (
            str(activity.get("occurred_at", "") or ""),
            str(activity.get("activity_type", "") or ""),
            str(activity.get("source_file", "") or ""),
            str(activity.get("raw_text", "") or ""),
        )
        for activity in merged
    }
    for activity in incoming_activities:
        if not isinstance(activity, dict):
            continue
        key = (
            str(activity.get("occurred_at", "") or ""),
            str(activity.get("activity_type", "") or ""),
            str(activity.get("source_file", "") or ""),
            str(activity.get("raw_text", "") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        merged.append(deepcopy(activity))
    return merged


def _tracked_bet_sort_rank(tracked_bet: dict) -> int:
    status = normalize_key(str(tracked_bet.get("status", "") or ""))
    if status in {"won", "lost", "settled"}:
        return 3
    if str(tracked_bet.get("settled_at", "") or "").strip():
        return 3
    if status in {"open", "matched"}:
        return 2
    return 1


def _leg_status_rank(leg: dict) -> int:
    status = normalize_key(str(leg.get("status", "") or ""))
    if status in {"won", "lost", "settled"}:
        return 3
    if status in {"open", "matched"}:
        return 2
    return 1

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
    return load_latest_page_payload_from_run_dir(
        run_dir,
        kind="positions_snapshot",
        page="open_positions",
    )


def load_latest_page_payload_from_run_dir(
    run_dir: Path,
    *,
    kind: str,
    page: str,
) -> dict:
    events_path = run_dir / "events.jsonl"
    if not events_path.exists():
        raise ValueError(f"Run bundle does not contain events.jsonl: {events_path}")

    latest_payload: dict | None = None
    for line in events_path.read_text().splitlines():
        if not line.strip():
            continue
        event = json.loads(line)
        if event.get("kind") != kind:
            continue
        if event.get("page") != page:
            continue
        latest_payload = event

    if latest_payload is None:
        raise ValueError(f"No {kind} event found in run bundle: {run_dir}")
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
        "ledger_pnl_summary": load_ledger_pnl_summary(run_dir=None),
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
            "detail": live_venue_status_line(
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
        "status_line": live_venue_status_line(
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
                else live_venue_status_line(
                    definition=definition,
                    status=status,
                    open_bets=open_bets,
                    count_hint=count_hint,
                ),
            },
        },
        "account_stats": None,
        "open_positions": [],
        "historical_positions": merge_historical_position_sources(
            load_smarkets_history_positions(config.run_dir),
            _load_historical_positions_for_run_dir(config.run_dir),
        ),
        "ledger_pnl_summary": load_ledger_pnl_summary(run_dir=config.run_dir),
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
                "bet_type": str(row.get("bet_type", "")),
                "funding_kind": infer_funding_kind(
                    explicit_funding_kind=row.get("funding_kind"),
                    notes=row.get("notes"),
                    bet_type=row.get("bet_type"),
                    status=row.get("status"),
                    free_bet=row.get("free_bet"),
                ),
                "current_cashout_value": _optional_float(row.get("current_cashout_value")),
                "supports_cash_out": bool(row.get("supports_cash_out", False)),
            }
            for row in open_bets
        ],
        "decisions": [],
        "watch": None,
        "tracked_bets": _load_existing_tracked_bets(config),
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
        "historical_positions": merge_historical_position_sources(
            load_smarkets_history_positions(config.run_dir),
            _load_historical_positions_for_run_dir(config.run_dir),
        ),
        "ledger_pnl_summary": load_ledger_pnl_summary(run_dir=config.run_dir),
        "other_open_bets": [],
        "decisions": [],
        "watch": None,
        "tracked_bets": _load_existing_tracked_bets(config),
        "exit_policy": build_exit_policy_summary(
            {"target_profit": 0.0, "stop_loss": 0.0},
            hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
            warn_only_default=config.warn_only_default,
        ),
        "exit_recommendations": [],
    }


def build_pending_live_venue_snapshot(
    *,
    venue: str,
    config: WorkerConfig,
) -> dict:
    definition = LIVE_VENUE_DEFINITIONS[venue]
    detail = (
        f"Selected {definition.label}. No cached live snapshot is available yet. "
        "Run a live refresh to capture the current bets surface."
    )
    return {
        "worker": {
            "name": "bet-recorder",
            "status": "idle",
            "detail": detail,
        },
        "venues": build_live_venue_summaries(
            selected_venue=venue,
            selected_status="awaiting_capture",
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
                "validation_error": None,
            },
        },
        "account_stats": None,
        "open_positions": [],
        "historical_positions": merge_historical_position_sources(
            load_smarkets_history_positions(config.run_dir),
            _load_historical_positions_for_run_dir(config.run_dir),
        ),
        "ledger_pnl_summary": load_ledger_pnl_summary(run_dir=config.run_dir),
        "other_open_bets": [],
        "decisions": [],
        "watch": None,
        "tracked_bets": _load_existing_tracked_bets(config),
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
        "historical_positions": merge_historical_position_sources(
            load_smarkets_history_positions(config.run_dir),
            _load_historical_positions_for_run_dir(config.run_dir),
        ),
        "ledger_pnl_summary": load_ledger_pnl_summary(run_dir=config.run_dir),
        "other_open_bets": [],
        "decisions": [],
        "watch": None,
        "tracked_bets": _load_existing_tracked_bets(config),
        "exit_policy": build_exit_policy_summary(
            {"target_profit": 0.0, "stop_loss": 0.0},
            hard_margin_call_profit_floor=config.hard_margin_call_profit_floor,
            warn_only_default=config.warn_only_default,
        ),
        "exit_recommendations": [],
    }


def _should_return_waiting_snapshot(config: WorkerConfig, error: ValueError) -> bool:
    if config.run_dir is None or config.agent_browser_session is None:
        return False
    message = str(error)
    return (
        "Run bundle does not contain events.jsonl" in message
        or "No positions_snapshot event found in run bundle" in message
    )


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
        "ledger_pnl_summary": load_ledger_pnl_summary(run_dir=None),
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
            "bet_type": str(bet.get("bet_type", "")),
            "funding_kind": infer_funding_kind(
                explicit_funding_kind=bet.get("funding_kind"),
                notes=bet.get("notes"),
                bet_type=bet.get("bet_type"),
                status=bet.get("status"),
                free_bet=bet.get("free_bet"),
            ),
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
                    "bet_type": str(row.get("bet_type", "")),
                    "funding_kind": infer_funding_kind(
                        explicit_funding_kind=row.get("funding_kind"),
                        notes=row.get("notes"),
                        bet_type=row.get("bet_type"),
                        status=row.get("status"),
                        free_bet=row.get("free_bet"),
                    ),
                    "current_cashout_value": _optional_float(row.get("current_cashout_value")),
                    "supports_cash_out": bool(row.get("supports_cash_out", False)),
                }
            )
    return collected


def capture_live_bookmaker_history_entries(venues: list[str] | None = None) -> dict:
    collected: list[dict] = []
    reports: list[dict] = []
    effective_venues = [
        venue for venue in (venues or list(LIVE_VENUE_ORDER)) if venue in LIVE_VENUE_ORDER
    ]
    for venue in effective_venues:
        report = capture_live_venue_history_sync_report(venue)
        reports.append({key: value for key, value in report.items() if key != "entries"})
        collected.extend(
            [dict(entry) for entry in report.get("entries", []) if isinstance(entry, dict)]
        )
    return {"entries": collected, "reports": reports}


def merge_other_open_bets(primary: list[dict], secondary: list[dict]) -> list[dict]:
    merged: list[dict] = []
    seen: dict[tuple[str, str, str, str, float, float, str, float | None], dict] = {}

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
            "bet_type": str(row.get("bet_type", "")),
            "funding_kind": infer_funding_kind(
                explicit_funding_kind=row.get("funding_kind"),
                notes=row.get("notes"),
                bet_type=row.get("bet_type"),
                status=row.get("status"),
                free_bet=row.get("free_bet"),
            ),
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
        existing = seen.get(dedupe_key)
        if existing is not None:
            if (
                normalize_funding_kind(existing.get("funding_kind"))
                in {"", "unknown"}
                and normalize_funding_kind(normalized.get("funding_kind")) not in {"", "unknown"}
            ):
                existing["funding_kind"] = normalized["funding_kind"]
            if not str(existing.get("bet_type", "")).strip() and str(
                normalized.get("bet_type", "")
            ).strip():
                existing["bet_type"] = normalized["bet_type"]
            if existing.get("current_cashout_value") is None and normalized.get(
                "current_cashout_value"
            ) is not None:
                existing["current_cashout_value"] = normalized["current_cashout_value"]
            if not existing.get("supports_cash_out") and normalized.get("supports_cash_out"):
                existing["supports_cash_out"] = True
            continue
        seen[dedupe_key] = normalized
        merged.append(normalized)

    return merged


def load_historical_positions(
    payload_path: Path | None = None,
    *,
    run_dir: Path | None = None,
) -> list[dict]:
    if payload_path is None:
        payload = _load_combined_statement_history_payload(run_dir)
    else:
        if not payload_path.exists():
            return []
        payload = json.loads(payload_path.read_text())
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


def load_smarkets_history_positions(run_dir: Path | None) -> list[dict]:
    if run_dir is None:
        return []

    try:
        payload = load_latest_page_payload_from_run_dir(
            run_dir,
            kind="history_snapshot",
            page="history",
        )
    except ValueError:
        return []

    metadata = payload.get("metadata")
    if not isinstance(metadata, dict):
        return []
    portfolio = metadata.get("smarkets_portfolio")
    if not isinstance(portfolio, dict):
        return []

    captured_at = str(payload.get("captured_at", "") or "")
    rows = [
        _build_smarkets_history_position_row(position, captured_at=captured_at)
        for position in portfolio.get("positions") or []
        if isinstance(position, dict)
    ]
    rows = [row for row in rows if row is not None]
    rows.sort(
        key=lambda row: (str(row.get("live_clock", "")), str(row.get("event", ""))),
        reverse=True,
    )
    return rows


def merge_historical_position_sources(*sources: list[dict]) -> list[dict]:
    merged: list[dict] = []
    seen: set[tuple] = set()
    for source in sources:
        for row in source:
            key = (
                str(row.get("event", "") or "").strip().lower(),
                str(row.get("market", "") or "").strip().lower(),
                str(row.get("contract", "") or "").strip().lower(),
                str(row.get("live_clock", "") or "").strip(),
                _optional_float(row.get("stake")) or 0.0,
                _optional_float(row.get("price")) or 0.0,
                _optional_float(row.get("pnl_amount")) or 0.0,
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)
    merged.sort(
        key=lambda row: (str(row.get("live_clock", "")), str(row.get("event", ""))),
        reverse=True,
    )
    return merged


def load_ledger_pnl_summary(
    payload_path: Path | None = None,
    *,
    run_dir: Path | None = None,
) -> dict:
    if payload_path is None:
        payload = _load_combined_statement_history_payload(run_dir)
    else:
        if not payload_path.exists():
            return _empty_ledger_pnl_summary()
        payload = json.loads(payload_path.read_text())
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
    explicit_funding_kind = normalize_funding_kind(entry.get("funding_kind"))
    if funding_kind_is_promo(explicit_funding_kind):
        return "promo"
    if funding_kind_is_cash(explicit_funding_kind):
        return "standard"

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
    inferred_funding_kind = infer_funding_kind(
        explicit_funding_kind="",
        notes=" ".join(text_parts),
        bet_type=entry.get("bet_type"),
        status=entry.get("status"),
    )

    if funding_kind_is_promo(inferred_funding_kind):
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

    if funding_kind_is_cash(inferred_funding_kind):
        return "standard"

    return "unknown"


def _build_smarkets_history_position_row(
    position: dict,
    *,
    captured_at: str,
) -> dict | None:
    contract = str(position.get("contract", "") or "").strip()
    market = str(position.get("market", "") or "").strip()
    if not contract or not market:
        return None

    side = str(position.get("side", "") or "").strip().lower()
    price = _optional_float(position.get("price")) or 0.0
    raw_stake = _optional_float(position.get("stake"))
    raw_liability = _optional_float(position.get("liability"))
    return_amount = _optional_float(position.get("return_amount"))
    current_value = (
        _optional_float(position.get("current_value"))
        if position.get("current_value") not in (None, "")
        else return_amount
    ) or 0.0
    stake = raw_stake
    if stake is None:
        stake = _derive_smarkets_history_stake(
            side=side,
            price=price,
            liability=raw_liability,
            return_amount=return_amount,
            current_value=current_value,
        )
    stake = stake or 0.0
    liability = raw_liability
    if liability is None:
        liability = _derive_smarkets_history_liability(
            side=side,
            price=price,
            stake=stake,
            current_value=current_value,
        )
    liability = liability or (stake if side == "buy" else 0.0) or stake
    pnl_amount = _optional_float(position.get("pnl_amount"))
    if pnl_amount is None:
        pnl_amount = _derive_smarkets_history_pnl_amount(
            side=side,
            price=price,
            stake=stake,
            liability=liability,
            return_amount=return_amount,
            current_value=current_value,
        )
    pnl_amount = pnl_amount or 0.0
    event_status = str(position.get("event_status", "") or "").strip()
    status = str(position.get("status", "") or "").strip() or "Settled"
    event = str(position.get("event", "") or "").strip()
    event_url = str(position.get("event_url", "") or "").strip()

    return {
        "event": event,
        "event_status": event_status or "Settled",
        "event_url": event_url,
        "contract": contract,
        "market": market,
        "status": status,
        "market_status": "settled",
        "is_in_play": False,
        "price": price,
        "stake": stake,
        "liability": liability,
        "current_value": current_value,
        "pnl_amount": pnl_amount,
        "overall_pnl_known": False,
        "current_back_odds": price or None,
        "current_implied_probability": (1.0 / price) if price else None,
        "current_implied_percentage": (100.0 / price) if price else None,
        "current_buy_odds": price or None,
        "current_buy_implied_probability": (1.0 / price) if price else None,
        "current_sell_odds": None,
        "current_sell_implied_probability": None,
        "current_score": "",
        "current_score_home": None,
        "current_score_away": None,
        "live_clock": captured_at,
        "can_trade_out": False,
    }


def _derive_smarkets_history_stake(
    *,
    side: str,
    price: float,
    liability: float | None,
    return_amount: float | None,
    current_value: float,
) -> float | None:
    if side == "buy":
        if liability is not None:
            return liability
        if current_value < 0:
            return round(abs(current_value), 2)
        if price > 0:
            candidate = return_amount if return_amount not in (None, 0.0) else current_value
            if candidate not in (None, 0.0):
                return round(abs(candidate) / price, 2)
        return None

    if side == "sell":
        if liability is not None and price > 1.0:
            return round(liability / (price - 1.0), 2)
        if price > 0:
            candidate = return_amount if return_amount not in (None, 0.0) else current_value
            if candidate is not None and candidate > 0:
                return round(candidate / price, 2)
        return None

    return None


def _derive_smarkets_history_liability(
    *,
    side: str,
    price: float,
    stake: float,
    current_value: float,
) -> float | None:
    if side == "buy":
        return round(stake, 2) if stake > 0 else None

    if side == "sell":
        if current_value < 0:
            return round(abs(current_value), 2)
        if stake > 0 and price > 1.0:
            return round(stake * (price - 1.0), 2)
        return None

    return None


def _derive_smarkets_history_pnl_amount(
    *,
    side: str,
    price: float,
    stake: float,
    liability: float,
    return_amount: float | None,
    current_value: float,
) -> float | None:
    if current_value < 0:
        return round(current_value, 2)

    if side == "buy":
        baseline = stake if stake > 0 else None
        if baseline is None and price > 0:
            candidate = return_amount if return_amount not in (None, 0.0) else current_value
            if candidate not in (None, 0.0):
                baseline = abs(candidate) / price
        if baseline is not None:
            return round(current_value - baseline, 2)
        return None

    if side == "sell":
        if liability > 0:
            return round(current_value - liability, 2)
        if stake > 0:
            return round(stake, 2)
        if price > 0:
            candidate = return_amount if return_amount not in (None, 0.0) else current_value
            if candidate is not None and candidate > 0:
                return round(candidate / price, 2)
        return None

    return None


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
    cached_snapshot: dict | None = None,
) -> dict:
    if selected_venue != "smarkets":
        if not capture_live:
            if (
                isinstance(cached_snapshot, dict)
                and cached_snapshot.get("selected_venue") == selected_venue
            ):
                return deepcopy(cached_snapshot)
            return build_pending_live_venue_snapshot(
                venue=selected_venue,
                config=config,
            )
        try:
            payload = capture_current_live_venue_payload(selected_venue)
        except (ValueError, OSError) as exc:
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
                "historical_positions": merge_historical_position_sources(
                    load_smarkets_history_positions(config.run_dir),
                    _load_historical_positions_for_run_dir(config.run_dir),
                ),
                "other_open_bets": [],
                "decisions": [],
                "watch": watcher_state.get("watch"),
                "warnings": watcher_state.get("warnings", []),
                "tracked_bets": _load_existing_tracked_bets(config),
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
            if capture_live:
                sync_live_bookmaker_history(config)
            snapshot["open_positions"] = (
                fresh_positions_analysis["positions"]
                if prefer_fresh_positions
                else watcher_open_positions
            )
            snapshot["historical_positions"] = merge_historical_position_sources(
                load_smarkets_history_positions(config.run_dir),
                _load_historical_positions_for_run_dir(config.run_dir),
            )
            snapshot["ledger_pnl_summary"] = load_ledger_pnl_summary(run_dir=config.run_dir)
            snapshot["other_open_bets"] = merge_other_open_bets(
                watcher_state.get("other_open_bets", []),
                (
                    capture_live_other_open_bets()
                    if capture_live
                    else _load_cached_other_open_bets(cached_snapshot)
                ),
            )
            snapshot["decisions"] = watcher_state.get("decisions", [])
            snapshot["watch"] = watcher_state.get("watch")
            snapshot["warnings"] = watcher_state.get("warnings", [])
            captured_at = str(
                (
                    fresh_positions_payload.get("captured_at", "")
                    if prefer_fresh_positions and fresh_positions_payload is not None
                    else watcher_state.get("updated_at", "")
                )
                or ""
            )
            snapshot["tracked_bets"] = _sync_tracked_bets_for_snapshot(
                config=config,
                open_positions=snapshot["open_positions"],
                other_open_bets=snapshot["other_open_bets"],
                historical_positions=snapshot["historical_positions"],
                captured_at=captured_at,
                commission_rate=config.commission_rate,
            )
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
    if capture_live:
        sync_live_bookmaker_history(config)
    snapshot["open_positions"] = positions_analysis["positions"]
    snapshot["historical_positions"] = merge_historical_position_sources(
        load_smarkets_history_positions(config.run_dir),
        _load_historical_positions_for_run_dir(config.run_dir),
    )
    snapshot["ledger_pnl_summary"] = load_ledger_pnl_summary(run_dir=config.run_dir)
    snapshot["other_open_bets"] = merge_other_open_bets(
        positions_analysis.get("other_open_bets")
        or load_other_open_bets(config.open_bets_payload_path),
        capture_live_other_open_bets() if capture_live else _load_cached_other_open_bets(cached_snapshot),
    )
    snapshot["decisions"] = load_decisions(run_dir=config.run_dir)
    snapshot["tracked_bets"] = _sync_tracked_bets_for_snapshot(
        config=config,
        open_positions=snapshot["open_positions"],
        other_open_bets=snapshot["other_open_bets"],
        historical_positions=snapshot["historical_positions"],
        captured_at=str(positions_payload.get("captured_at", "") or ""),
        commission_rate=config.commission_rate,
    )
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


def annotate_snapshot_refresh_kind(snapshot: dict, refresh_kind: str) -> dict:
    runtime = snapshot.get("runtime")
    if not isinstance(runtime, dict):
        runtime = {}
        snapshot["runtime"] = runtime
    runtime["refresh_kind"] = refresh_kind
    return snapshot


def attach_snapshot_bundle_evidence(snapshot: dict, run_dir: Path | None) -> dict:
    snapshot["recorder_bundle"] = build_recorder_bundle_summary(run_dir)
    snapshot["recorder_events"] = load_recent_recorder_events(run_dir)
    snapshot["transport_summary"] = build_transport_marker_summary(run_dir)
    snapshot["transport_events"] = load_recent_transport_markers(run_dir)
    return snapshot


def build_worker_snapshot_response(
    snapshot: dict,
    *,
    refresh_kind: str,
    run_dir: Path | None,
) -> dict:
    return {
        "snapshot": attach_snapshot_bundle_evidence(
            annotate_snapshot_refresh_kind(snapshot, refresh_kind),
            run_dir,
        )
    }


def handle_worker_request(
    *,
    request: str | dict,
    config: WorkerConfig | None,
    selected_venue: str | None = None,
    cached_snapshot: dict | None = None,
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
                cached_snapshot=cached_snapshot,
            )
        except ValueError as exc:
            if _should_return_waiting_snapshot(resolved_config, exc):
                snapshot = build_waiting_for_watcher_snapshot(
                    config=resolved_config,
                    detail="Recorder started; waiting for first snapshot.",
                )
            else:
                raise
        response = build_worker_snapshot_response(
            snapshot,
            refresh_kind="bootstrap",
            run_dir=resolved_config.run_dir,
        )
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, effective_selected_venue

    resolved_config = require_worker_config(config, request_name)

    if request_name in {"Refresh", "RefreshLive"}:
        response = build_worker_snapshot_response(
            load_exchange_snapshot_for_config(
                resolved_config,
                selected_venue=effective_selected_venue,
                capture_live=True,
                cached_snapshot=cached_snapshot,
            ),
            refresh_kind="live_capture",
            run_dir=resolved_config.run_dir,
        )
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, effective_selected_venue

    if request_name == "RefreshCached":
        try:
            snapshot = load_exchange_snapshot_for_config(
                resolved_config,
                selected_venue=effective_selected_venue,
                capture_live=False,
                cached_snapshot=cached_snapshot,
            )
        except ValueError as exc:
            if _should_return_waiting_snapshot(resolved_config, exc):
                snapshot = build_waiting_for_watcher_snapshot(
                    config=resolved_config,
                    detail="Recorder started; waiting for first snapshot.",
                )
            else:
                raise
        response = build_worker_snapshot_response(
            snapshot,
            refresh_kind="cached",
            run_dir=resolved_config.run_dir,
        )
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
        response = build_worker_snapshot_response(
            load_exchange_snapshot_for_config(
                resolved_config,
                selected_venue=venue,
                capture_live=False,
                cached_snapshot=cached_snapshot,
            ),
            refresh_kind="cached",
            run_dir=resolved_config.run_dir,
        )
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
            cached_snapshot=cached_snapshot,
        )
        response = build_worker_snapshot_response(
            handle_cash_out_tracked_bet(
                snapshot=snapshot,
                bet_id=bet_id,
                run_dir=resolved_config.run_dir,
            ),
            refresh_kind="cached",
            run_dir=resolved_config.run_dir,
        )
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
            cached_snapshot=cached_snapshot,
        )
        snapshot["worker"] = {
            **dict(snapshot.get("worker") or {}),
            "status": "ready",
            "detail": result.detail,
        }
        snapshot["status_line"] = result.detail
        response = build_worker_snapshot_response(
            snapshot,
            refresh_kind="live_capture",
            run_dir=resolved_config.run_dir,
        )
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, "smarkets"

    if request_name == "LoadHorseMatcher":
        assert request_payload is not None
        query = request_payload.get("query")
        if not isinstance(query, dict):
            raise ValueError("LoadHorseMatcher payload must include query.")
        market_snapshot = capture_live_horse_market_snapshot(query)
        response = build_worker_snapshot_response(
            build_horse_matcher_snapshot_response(
                market_snapshot=market_snapshot,
                selected_venue=effective_selected_venue,
            ),
            refresh_kind="live_capture",
            run_dir=resolved_config.run_dir,
        )
        if selected_venue is None:
            return response, resolved_config
        return response, resolved_config, effective_selected_venue

    raise AssertionError(f"Unhandled worker request type: {request_name}")


def handle_worker_request_line(
    *,
    request_line: str,
    config: WorkerConfig | None,
    selected_venue: str | None = None,
    cached_snapshot: dict | None = None,
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
        if selected_venue is None:
            return handle_worker_request(
                request=request,
                config=config,
                cached_snapshot=cached_snapshot,
            )
        return handle_worker_request(
            request=request,
            config=config,
            selected_venue=effective_selected_venue,
            cached_snapshot=cached_snapshot,
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
    cached_snapshot: dict | None = None
    for request_line in request_lines:
        normalized_request_line = request_line.strip()
        if not normalized_request_line:
            continue
        response, config, selected_venue = handle_worker_request_line(
            request_line=normalized_request_line,
            config=config,
            selected_venue=selected_venue,
            cached_snapshot=cached_snapshot,
        )
        if "request_error" not in response:
            cached_snapshot = deepcopy(response.get("snapshot"))
        yield response


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)
