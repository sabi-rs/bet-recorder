from __future__ import annotations


EXCHANGE_VENUES = {"smarkets", "betfair_exchange", "betfair", "matchbook", "betdaq"}


def normalize_vendor(value: object) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    aliases = {
        "betfair_exchange": "betfair_exchange",
        "betfair": "betfair",
        "betmgm": "betmgm",
        "bet_mgm": "betmgm",
        "bet365": "bet365",
        "smarkets": "smarkets",
        "matchbook": "matchbook",
        "betdaq": "betdaq",
    }
    return aliases.get(normalized, normalized)


def infer_platform_kind(*, platform: str, legs: list[dict]) -> str:
    normalized_platform = normalize_vendor(platform)
    if normalized_platform in EXCHANGE_VENUES:
        return "exchange"
    if any(normalize_vendor(leg.get("venue")) in EXCHANGE_VENUES for leg in legs):
        return "exchange_hedged"
    return "sportsbook"


def infer_exchange(*, explicit_exchange: object, legs: list[dict]) -> str | None:
    if explicit_exchange not in (None, ""):
        return normalize_vendor(explicit_exchange)
    for leg in legs:
        venue = normalize_vendor(leg.get("venue"))
        if venue in EXCHANGE_VENUES and str(leg.get("side", "")).lower() == "lay":
            return venue
    return None


def infer_bet_type(*, explicit_bet_type: object, legs: list[dict], market: str) -> str:
    if str(explicit_bet_type or "").strip():
        return str(explicit_bet_type)
    if len(legs) > 1:
        if "bet builder" in market.lower():
            return "bet_builder"
        return "multiple" if len({leg.get("outcome") for leg in legs}) > 1 else "single"
    if "each way" in market.lower() or "e/w" in market.lower():
        return "each_way"
    return "single"


def infer_market_family(*, explicit_market_family: object, market: str) -> str:
    if str(explicit_market_family or "").strip():
        return str(explicit_market_family)

    lowered = market.lower()
    if "match odds" in lowered or "full-time result" in lowered or "full time result" in lowered:
        return "match_odds"
    if "handicap" in lowered:
        return "handicap"
    if "over/under" in lowered or "total" in lowered or "totals" in lowered:
        return "totals"
    if "card" in lowered:
        return "player_cards"
    if "shot" in lowered:
        return "player_shots"
    if "each way" in lowered or "e/w" in lowered:
        return "horse_racing_each_way"
    if "win" in lowered and ("race" in lowered or "hcap" in lowered):
        return "horse_racing_win"
    if "bet builder" in lowered:
        return "bet_builder"
    return "custom"


def infer_primary_price(*, side: str, legs: list[dict]) -> float | None:
    for leg in legs:
        if str(leg.get("side", "")).lower() == side:
            return float(leg["odds"])
    return None


def infer_spread(*, explicit_spread: object, selection_line: object, legs: list[dict]) -> float | None:
    for candidate in (explicit_spread, selection_line):
        if candidate not in (None, ""):
            return float(candidate)
    for leg in legs:
        line = leg.get("line")
        if line not in (None, ""):
            return float(line)
    return None
