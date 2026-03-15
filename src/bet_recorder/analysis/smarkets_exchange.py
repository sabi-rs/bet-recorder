from __future__ import annotations

import re
from urllib.parse import unquote, urlparse

POSITION_RE = re.compile(
    r"Sell\s+(?P<contract>.+?)\s+"
    r"(?P<market>Correct score|Full-time result|To win)\s+"
    r"(?P<price>\d+(?:\.\d+)?)\s+"
    r"£(?P<stake>\d+(?:\.\d+)?)\s+"
    r"£(?P<liability>\d+(?:\.\d+)?)\s+"
    r"£(?P<return_amount>\d+(?:\.\d+)?)\s+"
    r"£(?P<current_value>\d+(?:\.\d+)?)\s+"
    r"(?P<pnl_sign>-?)£(?P<pnl_amount>\d+(?:\.\d+)?)\s+"
    r"\((?P<pnl_percent>\d+(?:\.\d+)?)%\)\s+"
    r"(?P<status>Order filled)"
    r"(?:\s+(?P<trade_out_label>Trade out)"
    r"(?:\s+Back\s+(?P<current_back_odds>\d+(?:\.\d+)?))?)?",
    re.I,
)
AVAILABLE_BALANCE_RE = re.compile(
    r"Available balance\s+£(?P<amount>\d+(?:\.\d+)?)", re.I
)
EXPOSURE_RE = re.compile(r"Exposure\s+£(?P<amount>\d+(?:\.\d+)?)", re.I)
UNREALIZED_PNL_RE = re.compile(
    r"Unrealized P/L\s+(?P<sign>-?)£(?P<amount>\d+(?:\.\d+)?)",
    re.I,
)
OPEN_BET_RE = re.compile(
    r"Back\s+(?P<label>.+?)\s+"
    r"(?P<market>Bet Builder|Correct score|Full-time result)\s+"
    r"(?P<odds>\d+(?:\.\d+)?)\s+"
    r"£(?P<stake>\d+(?:\.\d+)?)\s+"
    r"(?P<status>Open)",
    re.I,
)
EVENT_NAME_LINE_RE = re.compile(r"^(?:.+\s+vs\s+.+|\d{1,2}:\d{2}\s+-\s+.+)$")
LIVE_EVENT_STATUS_RE = re.compile(
    r"^(?:\d{1,3}(?:\+\d{1,2})?'|HT|FT|ET|Pens)(?=$|\||\s)",
    re.I,
)
PENDING_EVENT_STATUS_RE = re.compile(r"^(?:Today|Tomorrow)\s+At\s+\d{1,2}:\d{2}\b", re.I)
ENDED_EVENT_STATUS_RE = re.compile(r"^Event ended\b", re.I)
MONEY_LINE_RE = re.compile(r"^£(?P<amount>\d+(?:\.\d+)?)$")
PNL_LINE_RE = re.compile(
    r"^(?P<sign>[+-])£(?P<amount>\d+(?:\.\d+)?)\s+\((?P<percent>\d+(?:\.\d+)?)%\)$"
)
BACK_ODDS_LINE_RE = re.compile(r"^Back\s+(?P<odds>\d+(?:\.\d+)?)$", re.I)
PERCENTAGE_LINE_RE = re.compile(r"^(?P<value><)?(?P<number>\d+(?:\.\d+)?)%$")
LIVE_CLOCK_LINE_RE = re.compile(r"^\d{1,3}(?:\+\d{1,2})?'$")
TABLE_HEADER = (
    "Contract",
    "Price",
    "Stake/",
    "Liability",
    "Return",
    "Current Value",
    "Status",
)


def analyze_smarkets_page(
    *,
    page: str,
    body_text: str,
    inputs: dict[str, str],
    visible_actions: list[str],
    links: list[str] | None = None,
    event_summaries: list[dict] | None = None,
) -> dict:
    if page == "open_positions":
        positions = _extract_structured_positions(body_text)
        if not positions:
            positions = [
                _build_position(match, body_text=body_text)
                for match in POSITION_RE.finditer(body_text)
            ]
        positions = enrich_open_positions(
            positions,
            links=links or [],
            event_summaries=event_summaries or [],
        )
        return {
            "page": page,
            "position_count": len(positions),
            "positions": positions,
            "account_stats": _extract_account_stats(body_text),
            "other_open_bets": [
                _build_open_bet(match) for match in OPEN_BET_RE.finditer(body_text)
            ],
            "can_trade_out": any(
                action.lower() == "trade out" for action in visible_actions
            ),
            "inputs": inputs,
        }

    return {
        "page": page,
        "body_text_length": len(body_text),
        "visible_action_count": len(visible_actions),
    }


def _build_position(match: re.Match[str], *, body_text: str) -> dict:
    pnl_amount = float(match.group("pnl_amount"))
    if match.group("pnl_sign") == "-":
        pnl_amount = -pnl_amount

    event = None
    event_status = None
    event, event_status = _extract_event_context(body_text, match.start())
    can_trade_out = match.group("trade_out_label") is not None

    return {
        "contract": match.group("contract").strip(),
        "market": match.group("market").strip(),
        "event": event,
        "event_status": event_status,
        "is_in_play": _event_status_is_live(event_status),
        "market_status": _market_status(
            can_trade_out=can_trade_out,
            event_status=event_status,
        ),
        "price": float(match.group("price")),
        "stake": float(match.group("stake")),
        "liability": float(match.group("liability")),
        "return_amount": float(match.group("return_amount")),
        "current_value": float(match.group("current_value")),
        "pnl_amount": pnl_amount,
        "pnl_percent": float(match.group("pnl_percent")),
        "status": match.group("status"),
        "current_back_odds": (
            float(match.group("current_back_odds"))
            if match.group("current_back_odds") is not None
            else None
        ),
        "can_trade_out": can_trade_out,
    }


def _extract_structured_positions(body_text: str) -> list[dict]:
    if "\n" not in body_text:
        return []

    lines = [line.strip() for line in body_text.splitlines() if line.strip()]
    positions: list[dict] = []
    for index, line in enumerate(lines):
        if line != TABLE_HEADER[0]:
            continue
        if tuple(lines[index : index + len(TABLE_HEADER)]) != TABLE_HEADER:
            continue
        row = _parse_structured_position(lines, header_index=index)
        if row is not None:
            positions.append(row)
    return positions


def _parse_structured_position(lines: list[str], *, header_index: int) -> dict | None:
    row_index = header_index + len(TABLE_HEADER)
    required_index = row_index + 8
    if required_index >= len(lines):
        return None
    if not lines[row_index].startswith("Sell "):
        return None

    try:
        price = float(lines[row_index + 2])
        stake = _parse_money_line(lines[row_index + 3])
        liability = _parse_money_line(lines[row_index + 4])
        return_amount = _parse_money_line(lines[row_index + 5])
        current_value = _parse_money_line(lines[row_index + 6])
        pnl_amount, pnl_percent = _parse_pnl_line(lines[row_index + 7])
    except ValueError:
        return None

    status = lines[row_index + 8]
    if status != "Order filled":
        return None

    trade_out_label = None
    current_back_odds = None
    next_index = row_index + 9
    if next_index < len(lines) and lines[next_index] == "Trade out":
        trade_out_label = "Trade out"
        if next_index + 1 < len(lines):
            back_odds_match = BACK_ODDS_LINE_RE.fullmatch(lines[next_index + 1])
            if back_odds_match is not None:
                current_back_odds = float(back_odds_match.group("odds"))

    event, event_status = _extract_event_context_from_lines(lines, header_index=header_index)
    can_trade_out = trade_out_label is not None

    return {
        "contract": lines[row_index].removeprefix("Sell ").strip(),
        "market": lines[row_index + 1],
        "event": event,
        "event_status": event_status,
        "is_in_play": _event_status_is_live(event_status),
        "market_status": _market_status(
            can_trade_out=can_trade_out,
            event_status=event_status,
        ),
        "price": price,
        "stake": stake,
        "liability": liability,
        "return_amount": return_amount,
        "current_value": current_value,
        "pnl_amount": pnl_amount,
        "pnl_percent": pnl_percent,
        "status": status,
        "current_back_odds": current_back_odds,
        "can_trade_out": can_trade_out,
    }


def _extract_event_context_from_lines(
    lines: list[str], *, header_index: int
) -> tuple[str | None, str | None]:
    for index in range(header_index - 1, -1, -1):
        if not _looks_like_event_status(lines[index]):
            continue
        event = None
        for event_index in range(index - 1, -1, -1):
            if _looks_like_event_name(lines[event_index]):
                event = lines[event_index]
                break
        return event, lines[index]
    return None, None


def _extract_event_context(body_text: str, position_start: int) -> tuple[str | None, str | None]:
    prefix = body_text[:position_start]
    lines = [line.strip() for line in prefix.splitlines() if line.strip()]
    if lines:
        last_line = lines[-1]
        if _looks_like_event_status(last_line):
            event = lines[-2] if len(lines) >= 2 and _looks_like_event_name(lines[-2]) else None
            return event, last_line
        if _looks_like_event_name(last_line):
            return last_line, None

    fallback_lines = [line.strip() for line in prefix.split("Sell")[-1].splitlines() if line.strip()]
    if fallback_lines:
        last_line = fallback_lines[-1]
        if _looks_like_event_name(last_line):
            return last_line, None

    return None, None


def _looks_like_event_name(value: str) -> bool:
    return bool(EVENT_NAME_LINE_RE.fullmatch(value.strip()))


def _looks_like_event_status(value: str) -> bool:
    normalized = value.strip()
    return (
        "|" in normalized
        and (
            LIVE_EVENT_STATUS_RE.match(normalized) is not None
            or PENDING_EVENT_STATUS_RE.match(normalized) is not None
            or ENDED_EVENT_STATUS_RE.match(normalized) is not None
        )
    )


def _event_status_is_live(event_status: str | None) -> bool:
    if event_status is None:
        return False
    return LIVE_EVENT_STATUS_RE.match(event_status.strip()) is not None


def _market_status(*, can_trade_out: bool, event_status: str | None) -> str:
    if can_trade_out:
        return "tradable"
    if event_status is None:
        return "unavailable"

    normalized = event_status.strip()
    if _event_status_is_live(normalized):
        return "suspended"
    if PENDING_EVENT_STATUS_RE.match(normalized) is not None:
        return "pre_event"
    if ENDED_EVENT_STATUS_RE.match(normalized) is not None:
        return "settled"
    return "unavailable"


def _parse_money_line(value: str) -> float:
    match = MONEY_LINE_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"Not a money line: {value!r}")
    return float(match.group("amount"))


def _parse_pnl_line(value: str) -> tuple[float, float]:
    match = PNL_LINE_RE.fullmatch(value.strip())
    if match is None:
        raise ValueError(f"Not a pnl line: {value!r}")
    amount = float(match.group("amount"))
    if match.group("sign") == "-":
        amount = -amount
    return amount, float(match.group("percent"))


def _extract_account_stats(body_text: str) -> dict | None:
    available_balance_match = AVAILABLE_BALANCE_RE.search(body_text)
    exposure_match = EXPOSURE_RE.search(body_text)
    unrealized_pnl_match = UNREALIZED_PNL_RE.search(body_text)
    if (
        available_balance_match is None
        or exposure_match is None
        or unrealized_pnl_match is None
    ):
        return None

    unrealized_pnl = float(unrealized_pnl_match.group("amount"))
    if unrealized_pnl_match.group("sign") == "-":
        unrealized_pnl = -unrealized_pnl

    return {
        "available_balance": float(available_balance_match.group("amount")),
        "exposure": float(exposure_match.group("amount")),
        "unrealized_pnl": unrealized_pnl,
        "currency": "GBP",
    }


def _build_open_bet(match: re.Match[str]) -> dict:
    return {
        "label": match.group("label").strip(),
        "market": match.group("market").strip(),
        "side": "back",
        "odds": float(match.group("odds")),
        "stake": float(match.group("stake")),
        "status": match.group("status").strip(),
    }


def enrich_open_positions(
    positions: list[dict],
    *,
    links: list[str],
    event_summaries: list[dict],
) -> list[dict]:
    event_urls = event_urls_for_positions(positions=positions, links=links)
    summaries_by_url = {
        str(summary.get("url", "")).strip(): summary
        for summary in event_summaries
        if str(summary.get("url", "")).strip()
    }

    enriched_positions: list[dict] = []
    for position in positions:
        enriched = dict(position)
        event = str(position.get("event", "") or "")
        event_url = event_urls.get(event)
        current_back_odds = position.get("current_back_odds")
        enriched["event"] = event
        enriched["event_status"] = str(position.get("event_status", "") or "")

        enriched["event_url"] = event_url or ""
        enriched["current_implied_probability"] = _implied_probability(current_back_odds)
        enriched["current_implied_percentage"] = _implied_percentage(current_back_odds)

        summary = summaries_by_url.get(event_url or "")
        enriched["current_score"] = (
            str(summary.get("current_score", "")).strip() if summary else ""
        )
        enriched["current_score_home"] = (
            int(summary["current_score_home"])
            if summary and summary.get("current_score_home") is not None
            else None
        )
        enriched["current_score_away"] = (
            int(summary["current_score_away"])
            if summary and summary.get("current_score_away") is not None
            else None
        )
        enriched["live_clock"] = (
            str(summary.get("live_clock", "")).strip() if summary else ""
        )

        quote = _summary_quote_for_position(summary, enriched) if summary else None
        if quote is None and current_back_odds is not None:
            enriched["current_buy_odds"] = float(current_back_odds)
            enriched["current_buy_implied_probability"] = _implied_probability(
                current_back_odds
            )
            enriched["current_sell_odds"] = None
            enriched["current_sell_implied_probability"] = None
        else:
            enriched["current_buy_odds"] = (
                float(quote["buy_odds"])
                if quote and quote.get("buy_odds") is not None
                else None
            )
            enriched["current_buy_implied_probability"] = (
                float(quote["buy_implied_probability"])
                if quote and quote.get("buy_implied_probability") is not None
                else None
            )
            enriched["current_sell_odds"] = (
                float(quote["sell_odds"])
                if quote and quote.get("sell_odds") is not None
                else None
            )
            enriched["current_sell_implied_probability"] = (
                float(quote["sell_implied_probability"])
                if quote and quote.get("sell_implied_probability") is not None
                else None
            )
        enriched_positions.append(enriched)

    return enriched_positions


def event_urls_for_positions(*, positions: list[dict], links: list[str]) -> dict[str, str]:
    candidate_links = list(dict.fromkeys(_event_candidate_links(links)))
    event_urls: dict[str, str] = {}
    for position in positions:
        event = str(position.get("event", "") or "").strip()
        if not event or event in event_urls:
            continue
        matched_url = _match_event_url(event=event, candidate_links=candidate_links)
        if matched_url is not None:
            event_urls[event] = matched_url
    return event_urls


def parse_event_page_summary(*, url: str, html: str) -> dict | None:
    score_match = re.search(
        r'"scores"\s*:\s*\{\s*"current"\s*:\s*\[\s*(?P<home>\d+)\s*,\s*(?P<away>\d+)\s*\]',
        html,
    )
    event_match = re.search(
        r'"name"\s*:\s*"(?P<event_name>[^"]+)"[^}]*?"scores"\s*:\s*\{\s*"current"\s*:\s*\[\s*(?P<home>\d+)\s*,\s*(?P<away>\d+)\s*\][^}]*?\}[^}]*?"match_time"\s*:\s*"(?P<match_time>[^"]+)"',
        html,
        re.S,
    )
    live_clock = (
        _match_time_to_live_clock(event_match.group("match_time"))
        if event_match is not None
        else ""
    )
    if score_match is None:
        if event_match is None:
            return None
        home = int(event_match.group("home"))
        away = int(event_match.group("away"))
        return {
            "url": url,
            "current_score": f"{home}-{away}",
            "current_score_home": home,
            "current_score_away": away,
            "live_clock": live_clock,
            "quotes": [],
        }

    home = int(score_match.group("home"))
    away = int(score_match.group("away"))
    return {
        "url": url,
        "current_score": f"{home}-{away}",
        "current_score_home": home,
        "current_score_away": away,
        "live_clock": live_clock,
        "quotes": [],
    }


def parse_event_page_body_summary(
    *,
    url: str,
    body_text: str,
    positions: list[dict],
) -> dict | None:
    lines = [line.strip() for line in body_text.splitlines() if line.strip()]
    if not lines:
        return None

    event_name = _best_event_name(positions)
    header = _parse_event_page_header(lines, event_name=event_name)
    if header is None:
        return None

    quotes = []
    for position in positions:
        quote = _parse_market_quote(
            lines,
            market=str(position.get("market", "") or ""),
            contract=str(position.get("contract", "") or ""),
        )
        if quote is not None:
            quotes.append(quote)

    return {
        "url": url,
        "current_score": header["current_score"],
        "current_score_home": header["current_score_home"],
        "current_score_away": header["current_score_away"],
        "live_clock": header["live_clock"],
        "quotes": quotes,
    }


def _event_candidate_links(links: list[str]) -> list[str]:
    return [
        link
        for link in links
        if link.startswith("https://smarkets.com/")
        and link.rstrip("/").split("/")[-1].isdigit()
    ]


def _match_event_url(*, event: str, candidate_links: list[str]) -> str | None:
    if not candidate_links:
        return None

    event_segments = _event_name_segments(event)
    best_link: str | None = None
    best_score = 0
    for link in candidate_links:
        link_tokens = _url_event_tokens(link)
        if not link_tokens:
            continue
        if not all(_segment_matches_tokens(segment, link_tokens) for segment in event_segments):
            continue

        score = sum(_segment_match_score(segment, link_tokens) for segment in event_segments)
        if score > best_score:
            best_link = link
            best_score = score
    return best_link


def _event_name_segments(event: str) -> list[str]:
    normalized_event = event.strip()
    if " vs " in normalized_event.lower():
        return [segment.strip() for segment in re.split(r"\s+vs\s+", normalized_event, flags=re.I)]
    return [normalized_event]


def _segment_matches_tokens(segment: str, link_tokens: list[str]) -> bool:
    segment_tokens = _normalized_tokens(segment)
    if not segment_tokens:
        return False

    match_count = sum(
        1
        for token in segment_tokens
        if any(
            candidate.startswith(token) or token.startswith(candidate)
            for candidate in link_tokens
        )
    )
    required_matches = min(2, len(segment_tokens))
    return match_count >= required_matches


def _segment_match_score(segment: str, link_tokens: list[str]) -> int:
    return sum(
        1
        for token in _normalized_tokens(segment)
        if any(
            candidate.startswith(token) or token.startswith(candidate)
            for candidate in link_tokens
        )
    )


def _url_event_tokens(link: str) -> list[str]:
    path_segments = [
        segment
        for segment in urlparse(link).path.split("/")
        if segment and not segment.isdigit()
    ]
    if not path_segments:
        return []
    event_slug = unquote(path_segments[-1])
    return _normalized_tokens(event_slug)


def _normalized_tokens(value: str) -> list[str]:
    return [
        token
        for token in re.split(r"[^a-z0-9]+", value.lower())
        if token and token not in {"vs", "v", "at"}
    ]


def _implied_probability(odds: float | None) -> float | None:
    if odds is None or odds <= 0:
        return None
    return 1 / float(odds)


def _implied_percentage(odds: float | None) -> float | None:
    probability = _implied_probability(odds)
    if probability is None:
        return None
    return probability * 100


def _best_event_name(positions: list[dict]) -> str:
    for position in positions:
        event_name = str(position.get("event", "") or "").strip()
        if event_name:
            return event_name
    return ""


def _parse_event_page_header(
    lines: list[str], *, event_name: str
) -> dict[str, int | str] | None:
    if not event_name:
        return None

    try:
        event_index = lines.index(event_name)
    except ValueError:
        return None

    required_index = event_index + 5
    if required_index >= len(lines):
        return None

    home = lines[event_index + 1]
    away = lines[event_index + 2]
    if not home or not away:
        return None

    try:
        home_score = int(lines[event_index + 3])
        away_score = int(lines[event_index + 4])
    except ValueError:
        return None

    live_clock = lines[event_index + 5] if LIVE_CLOCK_LINE_RE.fullmatch(lines[event_index + 5]) else ""
    return {
        "home": home,
        "away": away,
        "current_score": f"{home_score}-{away_score}",
        "current_score_home": home_score,
        "current_score_away": away_score,
        "live_clock": live_clock,
    }


def _parse_market_quote(lines: list[str], *, market: str, contract: str) -> dict | None:
    if not market or not contract:
        return None

    for alias in _market_section_aliases(market):
        for section_index, value in enumerate(lines):
            if value != alias:
                continue
            quote = _find_contract_quote_in_section(
                lines,
                section_index=section_index,
                contract=contract,
                market=market,
            )
            if quote is not None:
                return quote
    return None


def _market_section_aliases(market: str) -> list[str]:
    normalized = market.strip().lower()
    aliases = [market.strip()]
    aliases.extend(
        {
            "full-time result": ["Graph", "Winner"],
            "correct score": ["Correct Score"],
            "to win": ["Graph", "Winner"],
        }.get(normalized, [])
    )
    return list(dict.fromkeys(alias for alias in aliases if alias))


def _find_contract_quote_in_section(
    lines: list[str],
    *,
    section_index: int,
    contract: str,
    market: str,
) -> dict | None:
    search_end = min(len(lines), section_index + 120)
    for index in range(section_index + 1, search_end):
        if lines[index] != contract:
            continue
        probabilities = [
            value
            for value in (
                _parse_probability_line(token)
                for token in lines[index + 1 : min(search_end, index + 9)]
            )
            if value is not None
        ]
        if not probabilities:
            return None

        buy_probability = probabilities[0]
        sell_probability = probabilities[1] if len(probabilities) > 1 else None
        return {
            "market": market,
            "contract": contract,
            "buy_implied_probability": buy_probability,
            "sell_implied_probability": sell_probability,
            "buy_odds": _probability_to_odds(buy_probability),
            "sell_odds": _probability_to_odds(sell_probability),
        }
    return None


def _parse_probability_line(value: str) -> float | None:
    match = PERCENTAGE_LINE_RE.fullmatch(value.strip())
    if match is None:
        return None
    probability = float(match.group("number")) / 100.0
    if match.group("value") == "<":
        return probability
    return probability


def _probability_to_odds(probability: float | None) -> float | None:
    if probability is None or probability <= 0:
        return None
    return 1 / probability


def _summary_quote_for_position(summary: dict, position: dict) -> dict | None:
    quotes = summary.get("quotes")
    if not isinstance(quotes, list):
        return None

    market = str(position.get("market", "") or "").strip().lower()
    contract = str(position.get("contract", "") or "").strip().lower()
    for quote in quotes:
        if not isinstance(quote, dict):
            continue
        if (
            str(quote.get("market", "")).strip().lower() == market
            and str(quote.get("contract", "")).strip().lower() == contract
        ):
            return quote
    return None


def _match_time_to_live_clock(match_time: str | None) -> str:
    if not match_time:
        return ""
    parts = str(match_time).split(":")
    if len(parts) != 3:
        return ""
    try:
        hours = int(parts[0])
        minutes = int(parts[1])
    except ValueError:
        return ""
    return f"{hours * 60 + minutes}'"
