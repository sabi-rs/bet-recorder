from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
import hashlib
import json

from bet_recorder.ledger.expected_value import settled_leg_pnl
from bet_recorder.ledger.taxonomy import infer_funding_kind, normalize_vendor
from bet_recorder.tracked_bets_runtime import event_matches, market_matches, normalize_key, text_matches


MATCHABLE_SPORTSBOOK_ACTIVITY_TYPES = {"bet settled", "bet won", "bet lost"}
MATCHABLE_SMARKETS_ACTIVITY_TYPES = {
    "bet placed",
    "bet fully matched",
    "bet partially matched",
    "bet match confirmed",
    "bet settled",
    "bet won",
    "bet lost",
}


def load_statement_history_payload(path: Path) -> dict:
    payload = json.loads(path.read_text())
    ledger_entries = payload.get("ledger_entries")
    if not isinstance(ledger_entries, list):
        raise ValueError("Statement history payload must contain a ledger_entries list.")
    return payload


def build_backfilled_tracked_bets(
    statement_history_payload: dict,
    *,
    commission_rate: float = 0.0,
    max_match_hours: float = 168.0,
    generated_at: str | None = None,
) -> dict:
    ledger_entries = statement_history_payload.get("ledger_entries")
    if not isinstance(ledger_entries, list):
        raise ValueError("Statement history payload must contain a ledger_entries list.")
    if max_match_hours <= 0:
        raise ValueError("max_match_hours must be greater than zero.")

    sportsbook_entries = [
        entry
        for entry in ledger_entries
        if isinstance(entry, dict) and _is_matchable_sportsbook_entry(entry)
    ]
    smarkets_entries = [
        entry
        for entry in ledger_entries
        if isinstance(entry, dict) and _is_matchable_smarkets_entry(entry)
    ]
    used_smarkets_entry_ids: set[str] = set()
    tracked_bets: list[dict] = []
    unmatched_entries: list[dict] = []

    for sportsbook_entry in sorted(
        sportsbook_entries,
        key=lambda entry: (
            str(entry.get("occurred_at", "") or ""),
            str(entry.get("entry_id", "") or ""),
        ),
    ):
        selected_smarkets_entry = _select_best_smarkets_entry(
            sportsbook_entry=sportsbook_entry,
            smarkets_entries=smarkets_entries,
            used_smarkets_entry_ids=used_smarkets_entry_ids,
            max_match_hours=max_match_hours,
        )
        if selected_smarkets_entry is None:
            unmatched_entries.append(
                {
                    "entry_id": str(sportsbook_entry.get("entry_id", "") or ""),
                    "platform": normalize_vendor(sportsbook_entry.get("platform")),
                    "occurred_at": str(sportsbook_entry.get("occurred_at", "") or ""),
                    "event": str(sportsbook_entry.get("event", "") or ""),
                    "market": str(sportsbook_entry.get("market", "") or ""),
                    "selection": str(sportsbook_entry.get("selection", "") or ""),
                    "reason": "no_matching_smarkets_leg",
                }
            )
            continue

        used_smarkets_entry_ids.add(str(selected_smarkets_entry.get("entry_id", "") or ""))
        tracked_bets.append(
            _build_backfilled_tracked_bet(
                sportsbook_entry=sportsbook_entry,
                smarkets_entry=selected_smarkets_entry,
                commission_rate=commission_rate,
            )
        )

    tracked_bets.sort(
        key=lambda tracked_bet: (
            str(tracked_bet.get("placed_at", "") or ""),
            str(tracked_bet.get("bet_id", "") or ""),
        )
    )

    generated_timestamp = generated_at or _utc_now_z()
    matched_platforms = sorted(
        {
            normalize_vendor(tracked_bet.get("platform"))
            for tracked_bet in tracked_bets
            if str(tracked_bet.get("platform", "") or "").strip()
        }
    )
    return {
        "source": "statement_history_backfill",
        "generated_at": generated_timestamp,
        "summary": {
            "ledger_entry_count": len(ledger_entries),
            "sportsbook_candidate_count": len(sportsbook_entries),
            "smarkets_candidate_count": len(smarkets_entries),
            "matched_tracked_bet_count": len(tracked_bets),
            "unmatched_sportsbook_entry_count": len(unmatched_entries),
            "matched_platforms": matched_platforms,
            "max_match_hours": max_match_hours,
        },
        "tracked_bets": tracked_bets,
        "unmatched_entries": unmatched_entries,
    }


def _is_matchable_sportsbook_entry(entry: dict) -> bool:
    if normalize_vendor(entry.get("platform")) == "smarkets":
        return False
    if str(entry.get("platform_kind", "") or "").strip().lower() != "sportsbook":
        return False
    if normalize_key(str(entry.get("activity_type", "") or "")) not in MATCHABLE_SPORTSBOOK_ACTIVITY_TYPES:
        return False
    if _selection_wins_from_sportsbook_entry(entry) is None:
        return False
    return _has_match_fields(entry)


def _is_matchable_smarkets_entry(entry: dict) -> bool:
    if normalize_vendor(entry.get("platform")) != "smarkets":
        return False
    if str(entry.get("platform_kind", "") or "").strip().lower() != "exchange":
        return False
    if normalize_key(str(entry.get("activity_type", "") or "")) not in MATCHABLE_SMARKETS_ACTIVITY_TYPES:
        return False
    if normalize_key(str(entry.get("side", "") or "")) not in {"lay", "back"}:
        return False
    return _has_match_fields(entry)


def _has_match_fields(entry: dict) -> bool:
    return bool(
        str(entry.get("event", "") or "").strip()
        and str(entry.get("market", "") or "").strip()
        and str(entry.get("selection", "") or "").strip()
        and entry.get("stake_gbp") not in (None, "")
        and entry.get("odds_decimal") not in (None, "")
        and str(entry.get("occurred_at", "") or "").strip()
    )


def _select_best_smarkets_entry(
    *,
    sportsbook_entry: dict,
    smarkets_entries: list[dict],
    used_smarkets_entry_ids: set[str],
    max_match_hours: float,
) -> dict | None:
    best_entry: dict | None = None
    best_score: tuple[float, float, str] | None = None
    sportsbook_time = _parse_iso_datetime(sportsbook_entry.get("occurred_at"))
    if sportsbook_time is None:
        return None

    for smarkets_entry in smarkets_entries:
        entry_id = str(smarkets_entry.get("entry_id", "") or "")
        if entry_id in used_smarkets_entry_ids:
            continue
        if not event_matches(
            str(sportsbook_entry.get("event", "") or ""),
            str(smarkets_entry.get("event", "") or ""),
        ):
            continue
        if not market_matches(
            str(sportsbook_entry.get("market", "") or ""),
            str(smarkets_entry.get("market", "") or ""),
        ):
            continue
        if not text_matches(
            str(sportsbook_entry.get("selection", "") or ""),
            str(smarkets_entry.get("selection", "") or ""),
        ):
            continue

        smarkets_time = _parse_iso_datetime(smarkets_entry.get("occurred_at"))
        if smarkets_time is None:
            continue
        time_delta_hours = abs((sportsbook_time - smarkets_time).total_seconds()) / 3600.0
        if time_delta_hours > max_match_hours:
            continue

        placement_bias = 0.0 if smarkets_time <= sportsbook_time else 24.0
        score = (
            placement_bias + time_delta_hours,
            abs(float(sportsbook_entry.get("odds_decimal")) - float(smarkets_entry.get("odds_decimal"))),
            entry_id,
        )
        if best_score is None or score < best_score:
            best_score = score
            best_entry = smarkets_entry
    return best_entry


def _build_backfilled_tracked_bet(
    *,
    sportsbook_entry: dict,
    smarkets_entry: dict,
    commission_rate: float,
) -> dict:
    selection_wins = _selection_wins_from_sportsbook_entry(sportsbook_entry)
    if selection_wins is None:
        raise ValueError("Cannot backfill tracked bet without a settled sportsbook result.")

    funding_kind = infer_funding_kind(
        explicit_funding_kind=sportsbook_entry.get("funding_kind"),
        notes=sportsbook_entry.get("description"),
        bet_type=sportsbook_entry.get("bet_type"),
        status=sportsbook_entry.get("status"),
    )
    bookmaker_leg = _build_sportsbook_leg(
        sportsbook_entry=sportsbook_entry,
        settled_at=str(sportsbook_entry.get("occurred_at", "") or ""),
    )
    exchange_leg = _build_smarkets_leg(
        smarkets_entry=smarkets_entry,
        commission_rate=commission_rate,
        settled_at=str(sportsbook_entry.get("occurred_at", "") or ""),
        selection_wins=selection_wins,
    )
    total_realised_pnl = round(
        settled_leg_pnl(leg=bookmaker_leg, selection_wins=selection_wins)
        + settled_leg_pnl(leg=exchange_leg, selection_wins=selection_wins),
        2,
    )
    stake_gbp = float(sportsbook_entry.get("stake_gbp"))
    placed_at = str(smarkets_entry.get("occurred_at", "") or sportsbook_entry.get("occurred_at", "") or "")
    settled_at = str(sportsbook_entry.get("occurred_at", "") or "")
    payout_gbp = round(stake_gbp + total_realised_pnl, 2)
    digest = hashlib.sha1(
        "|".join(
            [
                normalize_vendor(sportsbook_entry.get("platform")),
                normalize_key(str(sportsbook_entry.get("event", "") or "")),
                normalize_key(str(sportsbook_entry.get("market", "") or "")),
                normalize_key(str(sportsbook_entry.get("selection", "") or "")),
                settled_at,
            ]
        ).encode("utf-8")
    ).hexdigest()[:12]
    return {
        "bet_id": f"history-{digest}",
        "group_id": f"history-{digest}",
        "placed_at": placed_at,
        "settled_at": settled_at,
        "platform": normalize_vendor(sportsbook_entry.get("platform")),
        "platform_kind": "sportsbook",
        "exchange": "smarkets",
        "event": str(sportsbook_entry.get("event", "") or ""),
        "market": str(sportsbook_entry.get("market", "") or ""),
        "selection": str(sportsbook_entry.get("selection", "") or ""),
        "status": _overall_tracked_bet_status(total_realised_pnl),
        "sport_name": str(sportsbook_entry.get("sport_name", "") or ""),
        "bet_type": str(sportsbook_entry.get("bet_type", "") or "single"),
        "market_family": str(
            sportsbook_entry.get("market_family")
            or smarkets_entry.get("market_family")
            or ""
        ),
        "funding_kind": funding_kind,
        "stake_gbp": stake_gbp,
        "potential_returns_gbp": _optional_float(sportsbook_entry.get("payout_gbp"))
        or round(stake_gbp * float(sportsbook_entry.get("odds_decimal")), 2),
        "payout_gbp": payout_gbp,
        "realised_pnl_gbp": total_realised_pnl,
        "back_price": float(sportsbook_entry.get("odds_decimal")),
        "lay_price": float(smarkets_entry.get("odds_decimal")),
        "parse_confidence": "medium",
        "notes": "statement_history_backfill",
        "activities": [
            {
                "occurred_at": str(smarkets_entry.get("occurred_at", "") or ""),
                "activity_type": "exchange_leg_imported",
                "amount_gbp": _optional_float(smarkets_entry.get("amount_gbp")),
                "source_file": str(smarkets_entry.get("source_file", "") or ""),
                "raw_text": str(smarkets_entry.get("description", "") or ""),
            },
            {
                "occurred_at": settled_at,
                "activity_type": "sportsbook_leg_imported",
                "amount_gbp": _optional_float(sportsbook_entry.get("realised_pnl_gbp")),
                "source_file": str(sportsbook_entry.get("source_file", "") or ""),
                "raw_text": str(sportsbook_entry.get("description", "") or ""),
            },
        ],
        "legs": [exchange_leg, bookmaker_leg],
    }


def _build_smarkets_leg(
    *,
    smarkets_entry: dict,
    commission_rate: float,
    settled_at: str,
    selection_wins: bool,
) -> dict:
    side = _normalize_leg_side(smarkets_entry.get("side"))
    liability = _optional_float(smarkets_entry.get("exposure_gbp"))
    effective_liability = abs(liability) if liability is not None else None
    leg = {
        "venue": "smarkets",
        "outcome": str(smarkets_entry.get("selection", "") or ""),
        "side": side,
        "odds": float(smarkets_entry.get("odds_decimal")),
        "stake": float(smarkets_entry.get("stake_gbp")),
        "status": "settled",
        "market": str(smarkets_entry.get("market", "") or ""),
        "liability": effective_liability,
        "commission_rate": commission_rate,
        "exchange": "smarkets",
        "placed_at": str(smarkets_entry.get("occurred_at", "") or ""),
        "settled_at": settled_at,
    }
    leg["status"] = _settled_leg_status(
        settled_leg_pnl(leg=leg, selection_wins=selection_wins)
    )
    return leg


def _build_sportsbook_leg(*, sportsbook_entry: dict, settled_at: str) -> dict:
    leg = {
        "venue": normalize_vendor(sportsbook_entry.get("platform")),
        "outcome": str(sportsbook_entry.get("selection", "") or ""),
        "side": "back",
        "odds": float(sportsbook_entry.get("odds_decimal")),
        "stake": float(sportsbook_entry.get("stake_gbp")),
        "status": "settled",
        "market": str(sportsbook_entry.get("market", "") or ""),
        "placed_at": str(sportsbook_entry.get("occurred_at", "") or ""),
        "settled_at": settled_at,
    }
    selection_wins = _selection_wins_from_sportsbook_entry(sportsbook_entry)
    if selection_wins is not None:
        leg["status"] = _settled_leg_status(
            settled_leg_pnl(leg=leg, selection_wins=selection_wins)
        )
    return leg


def _selection_wins_from_sportsbook_entry(entry: dict) -> bool | None:
    status = normalize_key(str(entry.get("status", "") or ""))
    if status in {"won", "win"}:
        return True
    if status in {"lost", "lose"}:
        return False

    realised_pnl = _optional_float(entry.get("realised_pnl_gbp"))
    if realised_pnl is not None:
        if realised_pnl > 0:
            return True
        if realised_pnl < 0:
            return False

    payout = _optional_float(entry.get("payout_gbp"))
    stake = _optional_float(entry.get("stake_gbp"))
    if payout is not None and payout > 0 and stake is not None:
        if payout > stake:
            return True
        if payout == 0:
            return False
    return None


def _overall_tracked_bet_status(realised_pnl_gbp: float) -> str:
    if realised_pnl_gbp > 0:
        return "won"
    if realised_pnl_gbp < 0:
        return "lost"
    return "settled"


def _settled_leg_status(pnl_amount: float) -> str:
    if pnl_amount > 0:
        return "won"
    if pnl_amount < 0:
        return "lost"
    return "settled"


def _normalize_leg_side(value: object) -> str:
    normalized = normalize_key(str(value or ""))
    if normalized == "buy":
        return "back"
    if normalized == "sell":
        return "lay"
    return normalized or "lay"


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def _parse_iso_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(UTC)


def _utc_now_z() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
