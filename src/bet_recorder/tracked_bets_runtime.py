from __future__ import annotations

from pathlib import Path
import json
import hashlib
import os

from bet_recorder.ledger.expected_value import settled_leg_pnl
from bet_recorder.ledger.taxonomy import infer_funding_kind

AUTO_TRACKED_BETS_FILENAME = "tracked-bets.json"


def auto_tracked_bets_path(run_dir: Path | None) -> Path | None:
    if run_dir is None:
        return None
    return run_dir / AUTO_TRACKED_BETS_FILENAME


def load_runtime_tracked_bets(path: Path | None) -> list[dict]:
    if path is None or not path.exists():
        return []
    payload = json.loads(path.read_text())
    tracked_bets = payload.get("tracked_bets")
    if not isinstance(tracked_bets, list):
        return []
    return [dict(tracked_bet) for tracked_bet in tracked_bets if isinstance(tracked_bet, dict)]


def write_runtime_tracked_bets(path: Path, tracked_bets: list[dict], *, updated_at: str) -> None:
    if path.parent:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            path.parent.chmod(0o700)
        except OSError:
            pass
    payload = {
        "source": "auto_runtime_matcher",
        "updated_at": updated_at,
        "tracked_bets": tracked_bets,
    }
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2) + "\n")
    try:
        os.chmod(temp_path, 0o600)
    except OSError:
        pass
    temp_path.replace(path)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def reconcile_runtime_tracked_bets(
    *,
    existing_tracked_bets: list[dict],
    open_positions: list[dict],
    other_open_bets: list[dict],
    historical_positions: list[dict],
    captured_at: str,
    commission_rate: float,
) -> list[dict]:
    tracked_bets = [json.loads(json.dumps(tracked_bet)) for tracked_bet in existing_tracked_bets]
    used_open_indexes: set[int] = set()
    used_bet_indexes: set[int] = set()
    used_history_indexes: set[int] = set()

    for tracked_bet in tracked_bets:
        if _tracked_bet_is_closed(tracked_bet):
            continue

        open_index = _first_matching_index(
            rows=open_positions,
            used_indexes=used_open_indexes,
            predicate=lambda row: tracked_bet_matches_open_position(tracked_bet, row),
        )
        if open_index is not None:
            used_open_indexes.add(open_index)
            open_position = open_positions[open_index]
            _update_tracked_bet_from_open_position(
                tracked_bet,
                open_position=open_position,
                captured_at=captured_at,
                commission_rate=commission_rate,
            )
            sportsbook_index = _first_matching_index(
                rows=other_open_bets,
                used_indexes=used_bet_indexes,
                predicate=lambda row: sportsbook_bet_matches_tracked_bet(row, tracked_bet),
            )
            if sportsbook_index is not None:
                used_bet_indexes.add(sportsbook_index)
                _update_tracked_bet_from_sportsbook_bet(
                    tracked_bet,
                    sportsbook_bet=other_open_bets[sportsbook_index],
                    captured_at=captured_at,
                )
            continue

        history_index = _first_matching_index(
            rows=historical_positions,
            used_indexes=used_history_indexes,
            predicate=lambda row: tracked_bet_matches_history_position(tracked_bet, row),
        )
        if history_index is not None:
            used_history_indexes.add(history_index)
            _settle_tracked_bet_from_history_position(
                tracked_bet,
                history_position=historical_positions[history_index],
            )

    for open_index, open_position in enumerate(open_positions):
        if open_index in used_open_indexes:
            continue
        sportsbook_index = _first_matching_index(
            rows=other_open_bets,
            used_indexes=used_bet_indexes,
            predicate=lambda row: sportsbook_bet_matches_open_position(row, open_position),
        )
        if sportsbook_index is None:
            continue
        used_open_indexes.add(open_index)
        used_bet_indexes.add(sportsbook_index)
        tracked_bets.append(
            _build_runtime_tracked_bet(
                open_position=open_position,
                sportsbook_bet=other_open_bets[sportsbook_index],
                captured_at=captured_at,
                commission_rate=commission_rate,
            )
        )

    tracked_bets.sort(
        key=lambda tracked_bet: (
            _tracked_bet_is_closed(tracked_bet),
            str(tracked_bet.get("placed_at", "") or ""),
            str(tracked_bet.get("bet_id", "") or ""),
        )
    )
    return tracked_bets


def _first_matching_index(*, rows: list[dict], used_indexes: set[int], predicate) -> int | None:
    for index, row in enumerate(rows):
        if index in used_indexes:
            continue
        if predicate(row):
            return index
    return None


def _build_runtime_tracked_bet(
    *,
    open_position: dict,
    sportsbook_bet: dict,
    captured_at: str,
    commission_rate: float,
) -> dict:
    event = str(open_position.get("event", "") or sportsbook_bet.get("event", "") or "").strip()
    market = str(open_position.get("market", "") or sportsbook_bet.get("market", "") or "").strip()
    selection = str(
        open_position.get("contract", "") or sportsbook_bet.get("label", "") or ""
    ).strip()
    fingerprint = "|".join(
        [
            normalize_key(str(sportsbook_bet.get("venue", ""))),
            normalize_key(event),
            normalize_market(market),
            normalize_key(selection),
            f"{float(sportsbook_bet.get('stake', 0.0) or 0.0):.2f}",
            f"{float(sportsbook_bet.get('odds', 0.0) or 0.0):.4f}",
            f"{float(open_position.get('stake', 0.0) or 0.0):.2f}",
            f"{float(open_position.get('price', 0.0) or 0.0):.4f}",
            captured_at,
        ]
    )
    digest = hashlib.sha1(fingerprint.encode("utf-8")).hexdigest()[:12]
    return {
        "bet_id": f"auto-{digest}",
        "group_id": f"auto-{digest}",
        "placed_at": captured_at,
        "platform": str(sportsbook_bet.get("venue", "") or "").strip(),
        "platform_kind": "sportsbook",
        "exchange": "smarkets",
        "event": event,
        "market": market,
        "selection": selection,
        "status": "open",
        "funding_kind": infer_funding_kind(
            explicit_funding_kind=sportsbook_bet.get("funding_kind"),
            notes=sportsbook_bet.get("notes"),
            bet_type=sportsbook_bet.get("bet_type"),
            status=sportsbook_bet.get("status"),
            free_bet=sportsbook_bet.get("free_bet"),
        ),
        "stake_gbp": _optional_float(sportsbook_bet.get("stake")),
        "potential_returns_gbp": _sportsbook_potential_returns(sportsbook_bet),
        "notes": "auto_runtime_pair",
        "legs": [
            _build_smarkets_leg(
                open_position=open_position,
                captured_at=captured_at,
                commission_rate=commission_rate,
            ),
            _build_sportsbook_leg(
                sportsbook_bet=sportsbook_bet,
                captured_at=captured_at,
            ),
        ],
    }


def _update_tracked_bet_from_open_position(
    tracked_bet: dict,
    *,
    open_position: dict,
    captured_at: str,
    commission_rate: float,
) -> None:
    tracked_bet["event"] = str(open_position.get("event", "") or tracked_bet.get("event", "") or "")
    tracked_bet["market"] = str(open_position.get("market", "") or tracked_bet.get("market", "") or "")
    tracked_bet["selection"] = str(
        open_position.get("contract", "") or tracked_bet.get("selection", "") or ""
    )
    tracked_bet["status"] = "open"
    tracked_bet.setdefault("exchange", "smarkets")
    tracked_bet["legs"] = _upsert_leg(
        tracked_bet.get("legs", []),
        leg=_build_smarkets_leg(
            open_position=open_position,
            captured_at=captured_at,
            commission_rate=commission_rate,
        ),
        venue="smarkets",
    )


def _update_tracked_bet_from_sportsbook_bet(
    tracked_bet: dict,
    *,
    sportsbook_bet: dict,
    captured_at: str,
) -> None:
    tracked_bet["platform"] = str(sportsbook_bet.get("venue", "") or tracked_bet.get("platform", "") or "")
    tracked_bet.setdefault("platform_kind", "sportsbook")
    tracked_bet["funding_kind"] = infer_funding_kind(
        explicit_funding_kind=sportsbook_bet.get("funding_kind") or tracked_bet.get("funding_kind"),
        notes=sportsbook_bet.get("notes") or tracked_bet.get("notes"),
        bet_type=sportsbook_bet.get("bet_type") or tracked_bet.get("bet_type"),
        status=sportsbook_bet.get("status") or tracked_bet.get("status"),
        free_bet=sportsbook_bet.get("free_bet"),
    )
    tracked_bet["stake_gbp"] = (
        _optional_float(sportsbook_bet.get("stake"))
        if sportsbook_bet.get("stake") not in (None, "")
        else tracked_bet.get("stake_gbp")
    )
    tracked_bet["potential_returns_gbp"] = _sportsbook_potential_returns(sportsbook_bet)
    tracked_bet["legs"] = _upsert_leg(
        tracked_bet.get("legs", []),
        leg=_build_sportsbook_leg(
            sportsbook_bet=sportsbook_bet,
            captured_at=captured_at,
        ),
        venue=str(sportsbook_bet.get("venue", "") or "").strip(),
    )


def _settle_tracked_bet_from_history_position(
    tracked_bet: dict,
    *,
    history_position: dict,
) -> None:
    smarkets_leg = next(
        (
            leg
            for leg in tracked_bet.get("legs", [])
            if normalize_key(str(leg.get("venue", ""))) == "smarkets"
        ),
        None,
    )
    if smarkets_leg is None:
        return

    selection_wins = _selection_wins_from_history_position(
        history_position=history_position,
        smarkets_leg=smarkets_leg,
    )
    if selection_wins is None:
        return

    total_pnl = 0.0
    settled_at = str(history_position.get("live_clock", "") or "").strip()
    updated_legs = []
    for leg in tracked_bet.get("legs", []):
        pnl_amount = settled_leg_pnl(leg=leg, selection_wins=selection_wins)
        total_pnl += pnl_amount
        updated_leg = dict(leg)
        updated_leg["status"] = _settled_leg_status(pnl_amount)
        updated_leg["settled_at"] = settled_at
        updated_legs.append(updated_leg)

    tracked_bet["legs"] = updated_legs
    tracked_bet["status"] = str(history_position.get("status", "") or "settled").strip().lower()
    tracked_bet["settled_at"] = settled_at
    tracked_bet["realised_pnl_gbp"] = round(total_pnl, 2)
    stake_gbp = _optional_float(tracked_bet.get("stake_gbp"))
    if stake_gbp is not None:
        tracked_bet["payout_gbp"] = round(stake_gbp + total_pnl, 2)


def _selection_wins_from_history_position(
    *,
    history_position: dict,
    smarkets_leg: dict,
) -> bool | None:
    pnl_amount = _optional_float(history_position.get("pnl_amount"))
    if pnl_amount is None:
        return None
    side = normalize_key(str(smarkets_leg.get("side", "")))
    if side == "lay":
        return pnl_amount < 0
    if side == "back":
        return pnl_amount > 0
    return None


def _settled_leg_status(pnl_amount: float) -> str:
    if pnl_amount > 0:
        return "won"
    if pnl_amount < 0:
        return "lost"
    return "settled"


def _upsert_leg(existing_legs: list[dict], *, leg: dict, venue: str) -> list[dict]:
    normalized_venue = normalize_key(venue)
    legs = [dict(existing_leg) for existing_leg in existing_legs if isinstance(existing_leg, dict)]
    for index, existing_leg in enumerate(legs):
        if normalize_key(str(existing_leg.get("venue", ""))) == normalized_venue:
            legs[index] = leg
            return legs
    legs.append(leg)
    return legs


def _build_smarkets_leg(
    *,
    open_position: dict,
    captured_at: str,
    commission_rate: float,
) -> dict:
    side = _normalize_exchange_leg_side(open_position.get("side"))
    return {
        "venue": "smarkets",
        "outcome": str(open_position.get("contract", "") or "").strip(),
        "side": side,
        "odds": float(open_position.get("price", 0.0) or 0.0),
        "stake": float(open_position.get("stake", 0.0) or 0.0),
        "status": "open",
        "market": str(open_position.get("market", "") or "").strip(),
        "liability": _optional_float(open_position.get("liability")),
        "commission_rate": commission_rate,
        "exchange": "smarkets",
        "placed_at": captured_at,
    }


def _build_sportsbook_leg(*, sportsbook_bet: dict, captured_at: str) -> dict:
    return {
        "venue": str(sportsbook_bet.get("venue", "") or "").strip(),
        "outcome": str(sportsbook_bet.get("label", "") or "").strip(),
        "side": str(sportsbook_bet.get("side", "") or "back").strip().lower(),
        "odds": float(sportsbook_bet.get("odds", 0.0) or 0.0),
        "stake": float(sportsbook_bet.get("stake", 0.0) or 0.0),
        "status": str(sportsbook_bet.get("status", "") or "open").strip().lower(),
        "market": str(sportsbook_bet.get("market", "") or "").strip(),
        "placed_at": captured_at,
    }


def _normalize_exchange_leg_side(value: object) -> str:
    normalized = normalize_key(str(value or ""))
    if normalized == "buy":
        return "back"
    if normalized == "sell":
        return "lay"
    return "lay"


def _sportsbook_potential_returns(sportsbook_bet: dict) -> float | None:
    stake = _optional_float(sportsbook_bet.get("stake"))
    odds = _optional_float(sportsbook_bet.get("odds"))
    if stake is None or odds is None:
        return None
    return round(stake * odds, 2)


def _tracked_bet_is_closed(tracked_bet: dict) -> bool:
    settled_at = str(tracked_bet.get("settled_at", "") or "").strip()
    status = normalize_key(str(tracked_bet.get("status", "") or ""))
    return bool(settled_at) or status in {"settled", "closed", "cashedout", "void", "lost", "won"}


def tracked_bet_matches_open_position(tracked_bet: dict, open_position: dict) -> bool:
    return (
        text_matches(str(tracked_bet.get("selection", "")), str(open_position.get("contract", "")))
        and market_matches(str(tracked_bet.get("market", "")), str(open_position.get("market", "")))
        and event_matches(str(tracked_bet.get("event", "")), str(open_position.get("event", "")))
    )


def sportsbook_bet_matches_open_position(sportsbook_bet: dict, open_position: dict) -> bool:
    return (
        text_matches(str(sportsbook_bet.get("label", "")), str(open_position.get("contract", "")))
        and market_matches(str(sportsbook_bet.get("market", "")), str(open_position.get("market", "")))
        and event_matches(str(sportsbook_bet.get("event", "")), str(open_position.get("event", "")))
    )


def sportsbook_bet_matches_tracked_bet(sportsbook_bet: dict, tracked_bet: dict) -> bool:
    return (
        text_matches(str(sportsbook_bet.get("label", "")), str(tracked_bet.get("selection", "")))
        and market_matches(str(sportsbook_bet.get("market", "")), str(tracked_bet.get("market", "")))
        and event_matches(str(sportsbook_bet.get("event", "")), str(tracked_bet.get("event", "")))
    )


def tracked_bet_matches_history_position(tracked_bet: dict, history_position: dict) -> bool:
    return (
        text_matches(str(tracked_bet.get("selection", "")), str(history_position.get("contract", "")))
        and market_matches(str(tracked_bet.get("market", "")), str(history_position.get("market", "")))
        and event_matches(str(tracked_bet.get("event", "")), str(history_position.get("event", "")))
    )


def normalize_key(value: str) -> str:
    return " ".join(
        value.lower()
        .replace("vs", "v")
        .encode("ascii", "ignore")
        .decode("ascii")
        .translate({ord(character): " " for character in r"""!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~"""})
        .split()
    )


def text_matches(left: str, right: str) -> bool:
    left_key = normalize_key(left)
    right_key = normalize_key(right)
    return bool(left_key and right_key and (left_key == right_key or left_key in right_key or right_key in left_key))


def normalize_market(value: str) -> str:
    normalized = normalize_key(value)
    if normalized in {
        "full time",
        "full time result",
        "match odds",
        "to win",
        "winner",
    }:
        return "match odds"
    return normalized


def market_matches(left: str, right: str) -> bool:
    left_key = normalize_market(left)
    right_key = normalize_market(right)
    return bool(left_key and right_key and left_key == right_key)


def event_matches(left: str, right: str) -> bool:
    if not left or not right:
        return True
    left_key = normalize_key(left)
    right_key = normalize_key(right)
    if left_key == right_key:
        return True
    left_sides = _split_event_sides(left)
    right_sides = _split_event_sides(right)
    if left_sides is None or right_sides is None:
        return False
    return (
        text_matches(left_sides[0], right_sides[0])
        and text_matches(left_sides[1], right_sides[1])
    ) or (
        text_matches(left_sides[0], right_sides[1])
        and text_matches(left_sides[1], right_sides[0])
    )


def _split_event_sides(value: str) -> tuple[str, str] | None:
    lowered = (
        value.lower()
        .encode("ascii", "ignore")
        .decode("ascii")
        .replace(" vs ", " v ")
        .replace(" - ", " v ")
        .replace(" – ", " v ")
        .replace(" — ", " v ")
    )
    if " v " not in lowered:
        return None
    left, right = lowered.split(" v ", 1)
    left_key = normalize_key(left)
    right_key = normalize_key(right)
    if not left_key or not right_key:
        return None
    return left_key, right_key


def _optional_float(value: object) -> float | None:
    if value in (None, ""):
        return None
    return float(value)
