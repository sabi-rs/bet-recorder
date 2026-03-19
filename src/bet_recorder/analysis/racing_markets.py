from __future__ import annotations

from dataclasses import dataclass
import re
from urllib.parse import unquote, urlparse

from bet_recorder.analysis.open_bets_common import clean_lines, parse_money, parse_odds

HORSE_EVENT_RE = re.compile(
  r"\b(?P<time>\d{1,2}:\d{2})\b(?:\s*[-–]?\s*)(?P<course>[A-Za-z][A-Za-z '&.-]+)",
  re.I,
)
LOGIN_MARKERS = ("log in", "login", "sign in")
SPORTSBOOK_MARKET_RE = re.compile(
  r"^(?P<selection>.+?)\s+(?P<odds>\d+(?:\.\d+)?|\d+/\d+)$"
)
EXCHANGE_MARKET_RE = re.compile(
  r"^(?P<selection>.+?)\s+(?P<first>\d+(?:\.\d+)?)\s+(?P<second>\d+(?:\.\d+)?)(?:\s+£?(?P<liquidity>\d+(?:\.\d+)?))?$",
  re.I,
)
EXCHANGE_LABELLED_RE = re.compile(
  r"^(?P<selection>.+?)\s+(?:buy|back)\s+(?P<buy>\d+(?:\.\d+)?)\s+(?:sell|lay)\s+(?P<sell>\d+(?:\.\d+)?)(?:\s+£?(?P<liquidity>\d+(?:\.\d+)?))?$",
  re.I,
)
GENERIC_IGNORE_LINES = {
  "racecard",
  "results",
  "forecast",
  "tricast",
  "extra places",
  "place terms",
  "show all runners",
  "sort by",
  "price boosts",
  "cash out",
  "suspended",
  "market suspended",
  "starting price",
  "runners",
  "runner",
  "bet slip",
  "betslip",
  "my bets",
  "open bets",
  "single",
  "multiples",
  "ew",
  "each way",
}
SPORTSBOOK_VENUES = {
  "bet365",
  "betuk",
  "betway",
  "betfred",
  "coral",
  "ladbrokes",
  "kwik",
  "bet600",
}
EXCHANGE_VENUES = {"smarkets", "betdaq"}


@dataclass(frozen=True)
class ParsedHorseQuote:
  selection_name: str
  side: str
  odds: float
  liquidity: float | None = None


def analyze_racing_market_page(
  *,
  venue: str,
  page: str,
  url: str,
  document_title: str,
  body_text: str,
  interactive_snapshot: list[dict],
) -> dict:
  if page != "market":
    return _ignored_result(detail=f"{venue} target is not a market page.")

  lower_body = body_text.lower()
  if any(marker in lower_body for marker in LOGIN_MARKERS):
    return _ignored_result(status="login_required", detail=f"{venue} market tab requires login.")

  lines = clean_lines(body_text)
  interactive_lines = _interactive_lines(interactive_snapshot)
  merged_lines = _merge_lines(lines=lines, interactive_lines=interactive_lines)
  event_name = _extract_event_name(
    document_title=document_title,
    url=url,
    lines=merged_lines,
  )
  market_name = _extract_market_name(document_title=document_title, lines=merged_lines)
  start_hint = _extract_start_hint(event_name=event_name, document_title=document_title)

  if venue in SPORTSBOOK_VENUES:
    quotes = _extract_sportsbook_quotes(merged_lines)
  elif venue in EXCHANGE_VENUES:
    quotes = _extract_exchange_quotes(merged_lines)
  else:
    return _ignored_result(detail=f"{venue} does not have a racing market parser.")

  if not _looks_like_racing_context(
    event_name=event_name,
    document_title=document_title,
    url=url,
    market_name=market_name,
    quote_count=len(quotes),
  ):
    return _ignored_result(detail=f"{venue} target does not look like a horse-racing market.")

  if not quotes:
    return _ignored_result(status="unreadable", detail=f"{venue} horse-racing market exposed no readable quotes.")

  return {
    "status": "ready",
    "detail": f"Captured {len(quotes)} {venue} racing quote(s).",
    "event_name": event_name,
    "market_name": market_name,
    "start_hint": start_hint,
    "quotes": [
      {
        "selection_name": quote.selection_name,
        "side": quote.side,
        "odds": quote.odds,
        "liquidity": quote.liquidity,
      }
      for quote in quotes
    ],
  }


def _ignored_result(
  *,
  status: str = "ignored",
  detail: str,
) -> dict:
  return {
    "status": status,
    "detail": detail,
    "event_name": "",
    "market_name": "",
    "start_hint": "",
    "quotes": [],
  }


def _interactive_lines(interactive_snapshot: list[dict]) -> list[str]:
  lines: list[str] = []
  for item in interactive_snapshot:
    name = str(item.get("name", "") or "").strip()
    if not name:
      continue
    lines.extend(clean_lines(name))
  return lines


def _merge_lines(*, lines: list[str], interactive_lines: list[str]) -> list[str]:
  merged: list[str] = []
  seen: set[str] = set()
  for line in [*lines, *interactive_lines]:
    normalized = _normalize_space(line)
    if not normalized:
      continue
    lowered = normalized.lower()
    if lowered in seen:
      continue
    seen.add(lowered)
    merged.append(normalized)
  return merged


def _extract_event_name(*, document_title: str, url: str, lines: list[str]) -> str:
  for candidate in [document_title, unquote(urlparse(url).path).replace("-", " "), *lines[:40]]:
    match = HORSE_EVENT_RE.search(candidate)
    if match is not None:
      event_name = _normalize_space(f"{match.group('time')} {match.group('course')}")
      event_name = re.sub(
        r"\s*[-–]\s*(?:win market|to win|winner|each way).*$",
        "",
        event_name,
        flags=re.I,
      )
      return _normalize_space(event_name)
  return ""


def _extract_market_name(*, document_title: str, lines: list[str]) -> str:
  haystacks = [document_title, *lines[:40]]
  for value in haystacks:
    lowered = value.lower()
    if "to win" in lowered or "win market" in lowered or "winner" in lowered:
      return "Win"
    if "each way" in lowered:
      return "Each Way"
  return "Win"


def _extract_start_hint(*, event_name: str, document_title: str) -> str:
  for candidate in [event_name, document_title]:
    match = HORSE_EVENT_RE.search(candidate)
    if match is not None:
      return match.group("time")
  return ""


def _looks_like_racing_context(
  *,
  event_name: str,
  document_title: str,
  url: str,
  market_name: str,
  quote_count: int,
) -> bool:
  if quote_count >= 4 and event_name:
    return True
  lowered = " ".join((event_name, document_title, url, market_name)).lower()
  if "horse" in lowered or "racing" in lowered:
    return True
  return bool(HORSE_EVENT_RE.search(lowered))


def _extract_sportsbook_quotes(lines: list[str]) -> list[ParsedHorseQuote]:
  quotes: list[ParsedHorseQuote] = []
  seen: set[tuple[str, float]] = set()

  for index, line in enumerate(lines):
    combined_match = SPORTSBOOK_MARKET_RE.fullmatch(line)
    if combined_match is not None:
      selection = _clean_selection(combined_match.group("selection"))
      odds = parse_odds(combined_match.group("odds"))
      if selection and odds is not None and _selection_is_viable(selection):
        key = (selection.lower(), odds)
        if key not in seen:
          seen.add(key)
          quotes.append(ParsedHorseQuote(selection_name=selection, side="back", odds=odds))
      continue

    odds = parse_odds(line)
    if odds is None or index == 0:
      continue
    selection = _clean_selection(lines[index - 1])
    if not selection or not _selection_is_viable(selection):
      continue
    key = (selection.lower(), odds)
    if key in seen:
      continue
    seen.add(key)
    quotes.append(ParsedHorseQuote(selection_name=selection, side="back", odds=odds))

  return quotes


def _extract_exchange_quotes(lines: list[str]) -> list[ParsedHorseQuote]:
  quotes: list[ParsedHorseQuote] = []
  seen: set[tuple[str, float]] = set()

  for index, line in enumerate(lines):
    labelled_match = EXCHANGE_LABELLED_RE.fullmatch(line)
    if labelled_match is not None:
      selection = _clean_selection(labelled_match.group("selection"))
      sell_odds = float(labelled_match.group("sell"))
      liquidity = _optional_float(labelled_match.group("liquidity"))
      if selection and _selection_is_viable(selection):
        key = (selection.lower(), sell_odds)
        if key not in seen:
          seen.add(key)
          quotes.append(
            ParsedHorseQuote(
              selection_name=selection,
              side="lay",
              odds=sell_odds,
              liquidity=liquidity,
            )
          )
      continue

    combined_match = EXCHANGE_MARKET_RE.fullmatch(line)
    if combined_match is not None:
      selection = _clean_selection(combined_match.group("selection"))
      if not selection or not _selection_is_viable(selection):
        continue
      first = float(combined_match.group("first"))
      second = float(combined_match.group("second"))
      sell_odds = max(first, second)
      liquidity = _optional_float(combined_match.group("liquidity"))
      key = (selection.lower(), sell_odds)
      if key in seen:
        continue
      seen.add(key)
      quotes.append(
        ParsedHorseQuote(
          selection_name=selection,
          side="lay",
          odds=sell_odds,
          liquidity=liquidity,
        )
      )
      continue

    if index < 2:
      continue
    first = parse_odds(lines[index - 1])
    second = parse_odds(line)
    if first is None or second is None:
      continue
    selection = _clean_selection(lines[index - 2])
    if not selection or not _selection_is_viable(selection):
      continue
    sell_odds = max(first, second)
    key = (selection.lower(), sell_odds)
    if key in seen:
      continue
    seen.add(key)
    quotes.append(
      ParsedHorseQuote(
        selection_name=selection,
        side="lay",
        odds=sell_odds,
      )
    )

  return quotes


def _selection_is_viable(value: str) -> bool:
  lowered = value.lower()
  if lowered in GENERIC_IGNORE_LINES:
    return False
  if parse_money(value) is not None or parse_odds(value) is not None:
    return False
  if HORSE_EVENT_RE.search(value):
    return False
  if any(token in lowered for token in ("boost", "cash out", "bet slip", "suspended")):
    return False
  return len(value) >= 2


def _clean_selection(value: str) -> str:
  return _normalize_space(value.strip(" -|"))


def _normalize_space(value: str) -> str:
  return re.sub(r"\s+", " ", value or "").strip()


def _optional_float(value: str | None) -> float | None:
  if value in (None, ""):
    return None
  return float(value)
