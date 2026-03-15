from __future__ import annotations

from bet_recorder.ledger.taxonomy import infer_primary_price


def normalize_commission_rate(value: float | None) -> float:
    if value is None:
        return 0.0
    return value / 100.0 if value > 1.0 else value


def probability_from_price(price: float | None) -> float | None:
    if price in (None, 0):
        return None
    return 1.0 / float(price)


def back_ev(*, odds: float, stake: float, probability: float) -> float:
    return (probability * (stake * (odds - 1.0))) + ((1.0 - probability) * (-stake))


def lay_ev(*, odds: float, stake: float, probability: float, commission_rate: float) -> float:
    liability = stake * (odds - 1.0)
    win_profit = stake * (1.0 - normalize_commission_rate(commission_rate))
    return ((1.0 - probability) * win_profit) + (probability * (-liability))


def settled_leg_pnl(*, leg: dict, selection_wins: bool) -> float:
    stake = float(leg["stake"])
    odds = float(leg["odds"])
    side = str(leg["side"]).lower()
    commission_rate = normalize_commission_rate(
        float(leg["commission_rate"]) if leg.get("commission_rate") is not None else None
    )
    if side == "back":
        return stake * (odds - 1.0) if selection_wins else -stake
    if side == "lay":
        liability = leg.get("liability")
        effective_liability = float(liability) if liability is not None else stake * (odds - 1.0)
        return -effective_liability if selection_wins else stake * (1.0 - commission_rate)
    raise ValueError(f"Unsupported leg side: {leg['side']}")


def locked_profit(tracked_bet: dict) -> float | None:
    legs = tracked_bet.get("legs", [])
    if not legs:
        return None
    selection_wins = sum(settled_leg_pnl(leg=leg, selection_wins=True) for leg in legs)
    selection_loses = sum(settled_leg_pnl(leg=leg, selection_wins=False) for leg in legs)
    if abs(selection_wins - selection_loses) > 1e-6:
        return None
    return selection_wins


def infer_probability(tracked_bet: dict) -> tuple[float | None, str, dict[str, float | str | bool | None]]:
    expected_ev = tracked_bet.get("expected_ev") or {}
    odds_reference = tracked_bet.get("odds_reference") or {}
    inputs = expected_ev.get("inputs") or {}

    if inputs.get("win_probability") is not None:
        probability = float(inputs["win_probability"])
        return probability, "provided_probability", {"win_probability": probability}

    for key in ("fair_price", "reference_price"):
        candidate = inputs.get(key, odds_reference.get(key))
        if candidate not in (None, ""):
            fair_price = float(candidate)
            probability = probability_from_price(fair_price)
            return probability, "fair_price", {"fair_price": fair_price}

    reference_probability = odds_reference.get("win_probability")
    if reference_probability not in (None, ""):
        probability = float(reference_probability)
        return probability, "odds_reference_probability", {"win_probability": probability}

    back_price = tracked_bet.get("back_price")
    if back_price not in (None, ""):
        price = float(back_price)
        probability = probability_from_price(price)
        return probability, "implied_from_back_price", {"back_price": price}

    lay_price = tracked_bet.get("lay_price")
    if lay_price not in (None, ""):
        price = float(lay_price)
        probability = probability_from_price(price)
        return probability, "implied_from_lay_price", {"lay_price": price}

    inferred_price = infer_primary_price(side="back", legs=tracked_bet.get("legs", []))
    if inferred_price is not None:
        probability = probability_from_price(inferred_price)
        return probability, "implied_from_leg_back_price", {"back_price": inferred_price}
    return None, "", {}


def calculate_expected_value(tracked_bet: dict) -> dict:
    existing = tracked_bet.get("expected_ev")
    if isinstance(existing, dict) and existing.get("gbp") is not None:
        payload = {
            "gbp": float(existing.get("gbp")),
            "pct": (
                float(existing["pct"])
                if existing.get("pct") is not None
                else _pct(float(existing.get("gbp")), tracked_bet.get("stake_gbp"))
            ),
            "method": str(existing.get("method", "provided")),
            "source": str(existing.get("source", "input")),
            "status": str(existing.get("status", "provided")),
            "inputs": dict(existing.get("inputs") or {}),
        }
        return payload

    locked = locked_profit(tracked_bet)
    if locked is not None:
        return {
            "gbp": locked,
            "pct": _pct(locked, tracked_bet.get("stake_gbp")),
            "method": "matched_pair_locked_profit",
            "source": "local_formula",
            "status": "calculated",
            "inputs": {"locked_profit": locked},
        }

    probability, method, inputs = infer_probability(tracked_bet)
    if probability is None:
        return {
            "gbp": None,
            "pct": None,
            "method": "",
            "source": "local_formula",
            "status": "unavailable",
            "inputs": {},
        }

    total = 0.0
    for leg in tracked_bet.get("legs", []):
        odds = float(leg["odds"])
        stake = float(leg["stake"])
        side = str(leg["side"]).lower()
        if side == "back":
            total += back_ev(odds=odds, stake=stake, probability=probability)
        elif side == "lay":
            total += lay_ev(
                odds=odds,
                stake=stake,
                probability=probability,
                commission_rate=(
                    float(leg["commission_rate"])
                    if leg.get("commission_rate") is not None
                    else 0.0
                ),
            )

    return {
        "gbp": total,
        "pct": _pct(total, tracked_bet.get("stake_gbp")),
        "method": method,
        "source": "local_formula",
        "status": "calculated",
        "inputs": inputs,
    }


def calculate_realised_value(tracked_bet: dict, expected_ev: dict) -> dict:
    existing = tracked_bet.get("realised_ev")
    if isinstance(existing, dict) and existing.get("gbp") is not None:
        return {
            "gbp": float(existing.get("gbp")),
            "pct": (
                float(existing["pct"])
                if existing.get("pct") is not None
                else _pct(float(existing.get("gbp")), tracked_bet.get("stake_gbp"))
            ),
            "method": str(existing.get("method", "provided")),
            "source": str(existing.get("source", "input")),
            "status": str(existing.get("status", "provided")),
            "inputs": dict(existing.get("inputs") or {}),
        }

    realised_pnl = tracked_bet.get("realised_pnl_gbp")
    if realised_pnl is None or expected_ev.get("gbp") is None:
        return {
            "gbp": None,
            "pct": None,
            "method": "",
            "source": "local_formula",
            "status": "unavailable",
            "inputs": {},
        }

    realised_value = float(realised_pnl) - float(expected_ev["gbp"])
    return {
        "gbp": realised_value,
        "pct": _pct(realised_value, tracked_bet.get("stake_gbp")),
        "method": "realised_minus_expected",
        "source": "local_formula",
        "status": "calculated",
        "inputs": {
            "realised_pnl_gbp": float(realised_pnl),
            "expected_ev_gbp": float(expected_ev["gbp"]),
        },
    }


def _pct(value: float | None, stake: object) -> float | None:
    if value is None or stake in (None, "", 0, 0.0):
        return None
    return float(value) / float(stake)
