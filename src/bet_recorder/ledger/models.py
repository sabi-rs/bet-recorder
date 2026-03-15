from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class ValueMetric:
    gbp: float | None = None
    pct: float | None = None
    method: str = ""
    source: str = ""
    status: str = "unavailable"
    inputs: dict[str, float | str | bool | None] = field(default_factory=dict)

    def to_payload(self) -> dict:
        return {
            "gbp": self.gbp,
            "pct": self.pct,
            "method": self.method,
            "source": self.source,
            "status": self.status,
            "inputs": dict(self.inputs),
        }


@dataclass(frozen=True)
class BetActivity:
    occurred_at: str
    activity_type: str
    amount_gbp: float | None = None
    balance_after_gbp: float | None = None
    source_file: str = ""
    raw_text: str = ""

    def to_payload(self) -> dict:
        return {
            "occurred_at": self.occurred_at,
            "activity_type": self.activity_type,
            "amount_gbp": self.amount_gbp,
            "balance_after_gbp": self.balance_after_gbp,
            "source_file": self.source_file,
            "raw_text": self.raw_text,
        }


@dataclass(frozen=True)
class AccountActivity:
    occurred_at: str
    platform: str
    activity_type: str
    amount_gbp: float | None = None
    balance_after_gbp: float | None = None
    currency: str = "GBP"
    reference: str = ""
    source_file: str = ""
    source_kind: str = ""
    description: str = ""
    raw_fields: dict[str, str | float | bool | None] = field(default_factory=dict)

    def to_payload(self) -> dict:
        return {
            "occurred_at": self.occurred_at,
            "platform": self.platform,
            "activity_type": self.activity_type,
            "amount_gbp": self.amount_gbp,
            "balance_after_gbp": self.balance_after_gbp,
            "currency": self.currency,
            "reference": self.reference,
            "source_file": self.source_file,
            "source_kind": self.source_kind,
            "description": self.description,
            "raw_fields": dict(self.raw_fields),
        }


@dataclass(frozen=True)
class LedgerEntry:
    entry_id: str
    occurred_at: str
    platform: str
    activity_type: str
    status: str
    platform_kind: str = ""
    exchange: str | None = None
    event: str = ""
    market: str = ""
    selection: str = ""
    side: str = ""
    bet_type: str = ""
    market_family: str = ""
    sport_name: str = ""
    currency: str = "GBP"
    amount_gbp: float | None = None
    balance_after_gbp: float | None = None
    stake_gbp: float | None = None
    odds_decimal: float | None = None
    exposure_gbp: float | None = None
    payout_gbp: float | None = None
    realised_pnl_gbp: float | None = None
    reference: str = ""
    source_file: str = ""
    source_kind: str = ""
    description: str = ""
    raw_fields: dict[str, str | float | bool | None] = field(default_factory=dict)

    def to_payload(self) -> dict:
        return {
            "entry_id": self.entry_id,
            "occurred_at": self.occurred_at,
            "platform": self.platform,
            "activity_type": self.activity_type,
            "status": self.status,
            "platform_kind": self.platform_kind,
            "exchange": self.exchange,
            "event": self.event,
            "market": self.market,
            "selection": self.selection,
            "side": self.side,
            "bet_type": self.bet_type,
            "market_family": self.market_family,
            "sport_name": self.sport_name,
            "currency": self.currency,
            "amount_gbp": self.amount_gbp,
            "balance_after_gbp": self.balance_after_gbp,
            "stake_gbp": self.stake_gbp,
            "odds_decimal": self.odds_decimal,
            "exposure_gbp": self.exposure_gbp,
            "payout_gbp": self.payout_gbp,
            "realised_pnl_gbp": self.realised_pnl_gbp,
            "reference": self.reference,
            "source_file": self.source_file,
            "source_kind": self.source_kind,
            "description": self.description,
            "raw_fields": dict(self.raw_fields),
        }


@dataclass(frozen=True)
class TrackedLeg:
    venue: str
    outcome: str
    side: str
    odds: float
    stake: float
    status: str
    market: str = ""
    market_family: str = ""
    line: float | None = None
    liability: float | None = None
    commission_rate: float | None = None
    exchange: str | None = None
    placed_at: str = ""
    settled_at: str = ""

    def to_payload(self) -> dict:
        return {
            "venue": self.venue,
            "outcome": self.outcome,
            "side": self.side,
            "odds": self.odds,
            "stake": self.stake,
            "status": self.status,
            "market": self.market,
            "market_family": self.market_family,
            "line": self.line,
            "liability": self.liability,
            "commission_rate": self.commission_rate,
            "exchange": self.exchange,
            "placed_at": self.placed_at,
            "settled_at": self.settled_at,
        }


@dataclass(frozen=True)
class TrackedBet:
    bet_id: str
    group_id: str
    event: str
    market: str
    selection: str
    status: str
    legs: list[TrackedLeg]
    placed_at: str = ""
    settled_at: str = ""
    platform: str = ""
    platform_kind: str = ""
    exchange: str | None = None
    sport_key: str = ""
    sport_name: str = ""
    bet_type: str = ""
    market_family: str = ""
    selection_line: float | None = None
    currency: str = "GBP"
    stake_gbp: float | None = None
    potential_returns_gbp: float | None = None
    payout_gbp: float | None = None
    realised_pnl_gbp: float | None = None
    back_price: float | None = None
    lay_price: float | None = None
    spread: float | None = None
    expected_ev: ValueMetric = field(default_factory=ValueMetric)
    realised_ev: ValueMetric = field(default_factory=ValueMetric)
    activities: list[BetActivity] = field(default_factory=list)
    odds_reference: dict[str, float | str | bool | None] = field(default_factory=dict)
    parse_confidence: str = "high"
    notes: str = ""

    def to_payload(self) -> dict:
        return {
            "bet_id": self.bet_id,
            "group_id": self.group_id,
            "event": self.event,
            "market": self.market,
            "selection": self.selection,
            "status": self.status,
            "placed_at": self.placed_at,
            "settled_at": self.settled_at,
            "platform": self.platform,
            "platform_kind": self.platform_kind,
            "exchange": self.exchange,
            "sport_key": self.sport_key,
            "sport_name": self.sport_name,
            "bet_type": self.bet_type,
            "market_family": self.market_family,
            "selection_line": self.selection_line,
            "currency": self.currency,
            "stake_gbp": self.stake_gbp,
            "potential_returns_gbp": self.potential_returns_gbp,
            "payout_gbp": self.payout_gbp,
            "realised_pnl_gbp": self.realised_pnl_gbp,
            "back_price": self.back_price,
            "lay_price": self.lay_price,
            "spread": self.spread,
            "expected_ev": self.expected_ev.to_payload(),
            "realised_ev": self.realised_ev.to_payload(),
            "activities": [activity.to_payload() for activity in self.activities],
            "odds_reference": dict(self.odds_reference),
            "parse_confidence": self.parse_confidence,
            "notes": self.notes,
            "legs": [leg.to_payload() for leg in self.legs],
        }
