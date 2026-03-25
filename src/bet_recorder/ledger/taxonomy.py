from __future__ import annotations


EXCHANGE_VENUES = {"smarkets", "betfair_exchange", "betfair", "matchbook", "betdaq"}


def normalize_vendor(value: object) -> str:
    normalized = str(value or "").strip().lower().replace(" ", "_")
    aliases = {
        "10bet": "bet10",
        "betfair_exchange": "betfair_exchange",
        "betfair": "betfair",
        "betano": "betano",
        "betmgm": "betmgm",
        "bet_mgm": "betmgm",
        "betvictor": "betvictor",
        "bet_victor": "betvictor",
        "bet365": "bet365",
        "bet10": "bet10",
        "boylesports": "boylesports",
        "boyle_sports": "boylesports",
        "fanteam": "fanteam",
        "fan_team": "fanteam",
        "leovegas": "leovegas",
        "leo_vegas": "leovegas",
        "midnite": "midnite",
        "paddypower": "paddypower",
        "paddy_power": "paddypower",
        "skybet": "skybet",
        "sky_bet": "skybet",
        "smarkets": "smarkets",
        "sportingindex": "sportingindex",
        "sporting_index": "sportingindex",
        "talksportbet": "talksportbet",
        "talksport_bet": "talksportbet",
        "matchbook": "matchbook",
        "williamhill": "williamhill",
        "william_hill": "williamhill",
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


def normalize_funding_kind(value: object) -> str:
    normalized = str(value or "").strip().lower().replace("-", "_").replace(" ", "_")
    aliases = {
        "cash": "cash",
        "standard": "cash",
        "normal": "cash",
        "qualifying": "cash",
        "freebet": "free_bet",
        "free_bet": "free_bet",
        "snr": "free_bet",
        "stake_returned": "free_bet",
        "stake_not_returned": "free_bet",
        "risk_free": "risk_free",
        "refund": "risk_free",
        "bonus": "bonus",
        "promo": "bonus",
        "promotion": "bonus",
        "boost": "bonus",
        "unknown": "unknown",
    }
    return aliases.get(normalized, normalized)


def infer_funding_kind(
    *,
    explicit_funding_kind: object,
    notes: object = None,
    bet_type: object = None,
    status: object = None,
    free_bet: object = None,
) -> str:
    normalized_explicit = normalize_funding_kind(explicit_funding_kind)
    if normalized_explicit and normalized_explicit != "unknown":
        return normalized_explicit

    if bool(free_bet):
        return "free_bet"

    haystack = " ".join(
        str(candidate or "").strip().lower()
        for candidate in (notes, bet_type, status)
        if str(candidate or "").strip()
    )
    if not haystack:
        return "unknown"

    if any(keyword in haystack for keyword in ("free bet", "freebet", "snr", "stake returned")):
        return "free_bet"
    if any(keyword in haystack for keyword in ("risk free", "refund")):
        return "risk_free"
    if any(keyword in haystack for keyword in ("bonus", "promo", "promotion", "boost")):
        return "bonus"
    if any(keyword in haystack for keyword in ("qualifying", "cash", "normal")):
        return "cash"
    return "unknown"


def funding_kind_is_promo(value: object) -> bool:
    return normalize_funding_kind(value) in {"free_bet", "risk_free", "bonus"}


def funding_kind_is_cash(value: object) -> bool:
    return normalize_funding_kind(value) == "cash"
