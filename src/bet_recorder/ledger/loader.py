from __future__ import annotations

from pathlib import Path
import json

from bet_recorder.ledger.expected_value import (
    calculate_expected_value,
    calculate_realised_value,
)
from bet_recorder.ledger.models import BetActivity, TrackedBet, TrackedLeg, ValueMetric
from bet_recorder.ledger.taxonomy import (
    infer_bet_type,
    infer_exchange,
    infer_market_family,
    infer_platform_kind,
    infer_primary_price,
    infer_spread,
    normalize_vendor,
)


def load_tracked_bets(path: Path | None) -> list[dict]:
    if path is None:
        return []

    payload = json.loads(path.read_text())
    tracked_bets = payload.get("tracked_bets")
    if not isinstance(tracked_bets, list):
        raise ValueError("Companion legs payload must contain a tracked_bets list.")

    return [_normalize_tracked_bet_payload(tracked_bet) for tracked_bet in tracked_bets]


def _normalize_tracked_bet_payload(tracked_bet: dict) -> dict:
    legs_payload = tracked_bet.get("legs")
    if not isinstance(legs_payload, list) or not legs_payload:
        raise ValueError("Tracked bets must contain a non-empty legs list.")

    legs = [_build_leg(leg, default_market=str(tracked_bet.get("market", ""))) for leg in legs_payload]
    leg_payloads = [leg.to_payload() for leg in legs]

    market = str(tracked_bet.get("market", "")).strip()
    selection_line = _optional_float(tracked_bet.get("selection_line"))
    back_price = _optional_float(tracked_bet.get("back_price"))
    lay_price = _optional_float(tracked_bet.get("lay_price"))
    stake_gbp = _optional_float(tracked_bet.get("stake_gbp"))

    normalized = TrackedBet(
        bet_id=str(tracked_bet["bet_id"]),
        group_id=str(tracked_bet.get("group_id", tracked_bet["bet_id"])),
        event=str(tracked_bet.get("event", "")),
        market=market,
        selection=str(tracked_bet.get("selection", "")),
        status=str(tracked_bet.get("status", "")),
        legs=legs,
        placed_at=str(tracked_bet.get("placed_at", "")),
        settled_at=str(tracked_bet.get("settled_at", "")),
        platform=normalize_vendor(
            tracked_bet.get("platform") or _infer_platform_from_legs(leg_payloads)
        ),
        platform_kind=str(
            tracked_bet.get("platform_kind")
            or infer_platform_kind(
                platform=str(
                    tracked_bet.get("platform") or _infer_platform_from_legs(leg_payloads)
                ),
                legs=leg_payloads,
            )
        ),
        exchange=infer_exchange(
            explicit_exchange=tracked_bet.get("exchange"),
            legs=leg_payloads,
        ),
        sport_key=str(tracked_bet.get("sport_key", "")),
        sport_name=str(tracked_bet.get("sport_name", "")),
        bet_type=str(
            tracked_bet.get("bet_type")
            or infer_bet_type(
                explicit_bet_type=tracked_bet.get("bet_type"),
                legs=leg_payloads,
                market=market,
            )
        ),
        market_family=str(
            tracked_bet.get("market_family")
            or infer_market_family(
                explicit_market_family=tracked_bet.get("market_family"),
                market=market,
            )
        ),
        selection_line=selection_line,
        currency=str(tracked_bet.get("currency", "GBP")),
        stake_gbp=stake_gbp if stake_gbp is not None else _infer_stake_gbp(leg_payloads),
        potential_returns_gbp=_optional_float(tracked_bet.get("potential_returns_gbp")),
        payout_gbp=_optional_float(tracked_bet.get("payout_gbp")),
        realised_pnl_gbp=_optional_float(tracked_bet.get("realised_pnl_gbp")),
        back_price=back_price if back_price is not None else infer_primary_price(side="back", legs=leg_payloads),
        lay_price=lay_price if lay_price is not None else infer_primary_price(side="lay", legs=leg_payloads),
        spread=infer_spread(
            explicit_spread=tracked_bet.get("spread"),
            selection_line=selection_line,
            legs=leg_payloads,
        ),
        expected_ev=_build_value_metric(tracked_bet.get("expected_ev")),
        realised_ev=_build_value_metric(tracked_bet.get("realised_ev")),
        activities=[_build_activity(activity) for activity in tracked_bet.get("activities", [])],
        odds_reference=dict(tracked_bet.get("odds_reference") or {}),
        parse_confidence=str(tracked_bet.get("parse_confidence", "high")),
        notes=str(tracked_bet.get("notes", "")),
    )
    normalized_payload = normalized.to_payload()
    expected_ev = calculate_expected_value(normalized_payload)
    normalized_payload["expected_ev"] = expected_ev
    normalized_payload["realised_ev"] = calculate_realised_value(
        normalized_payload,
        expected_ev,
    )
    return normalized_payload


def _build_leg(leg: dict, *, default_market: str) -> TrackedLeg:
    return TrackedLeg(
        venue=normalize_vendor(leg["venue"]),
        outcome=str(leg["outcome"]),
        side=str(leg["side"]),
        odds=float(leg["odds"]),
        stake=float(leg["stake"]),
        status=str(leg["status"]),
        market=str(leg.get("market", default_market)),
        market_family=str(
            leg.get("market_family")
            or infer_market_family(
                explicit_market_family=leg.get("market_family"),
                market=str(leg.get("market", default_market)),
            )
        ),
        line=_optional_float(leg.get("line")),
        liability=_optional_float(leg.get("liability")),
        commission_rate=_optional_float(leg.get("commission_rate")),
        exchange=(
            normalize_vendor(leg.get("exchange"))
            if leg.get("exchange") not in (None, "")
            else None
        ),
        placed_at=str(leg.get("placed_at", "")),
        settled_at=str(leg.get("settled_at", "")),
    )


def _build_activity(payload: dict) -> BetActivity:
    return BetActivity(
        occurred_at=str(payload.get("occurred_at", "")),
        activity_type=str(payload.get("activity_type", "")),
        amount_gbp=_optional_float(payload.get("amount_gbp")),
        balance_after_gbp=_optional_float(payload.get("balance_after_gbp")),
        source_file=str(payload.get("source_file", "")),
        raw_text=str(payload.get("raw_text", "")),
    )


def _build_value_metric(payload: dict | None) -> ValueMetric:
    payload = payload or {}
    return ValueMetric(
        gbp=_optional_float(payload.get("gbp")),
        pct=_optional_float(payload.get("pct")),
        method=str(payload.get("method", "")),
        source=str(payload.get("source", "")),
        status=str(payload.get("status", "unavailable")),
        inputs=dict(payload.get("inputs") or {}),
    )


def _infer_platform_from_legs(legs: list[dict]) -> str:
    for leg in legs:
        venue = normalize_vendor(leg.get("venue"))
        if venue != "smarkets" and str(leg.get("side", "")).lower() == "back":
            return venue
    return normalize_vendor(legs[0].get("venue"))


def _infer_stake_gbp(legs: list[dict]) -> float | None:
    for leg in legs:
        if str(leg.get("side", "")).lower() == "back":
            return float(leg["stake"])
    if legs:
        return float(legs[0]["stake"])
    return None


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)
