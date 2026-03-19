from __future__ import annotations

from dataclasses import dataclass
import re

from bet_recorder.analysis.open_bets_common import (
  clean_lines,
  looks_like_date_fragment,
  parse_money,
  parse_odds,
)

COUNT_HINT_RE = re.compile(
  r"(?:\+(?P<prefix_count>\d+)|(?:open|my)\s+bets?(?:\s*\(|\s+)(?P<count>\d+))",
  re.I,
)
MONEY_PREFIX_RE = re.compile(r"^(?:stake|returns?|cash out)\b", re.I)
IGNORE_MARKERS = {
  "my bets",
  "open",
  "open bets",
  "settled",
  "cash out",
  "reuse selections",
  "stake",
  "net return",
  "returns",
  "single",
  "singles",
  "double",
  "doubles",
  "treble",
  "acca",
  "acca(s)",
}


@dataclass(frozen=True)
class GenericSportsbookProfile:
  venue: str
  login_phrases: tuple[str, ...]
  no_open_bets_phrases: tuple[str, ...]
  navigation_markers: tuple[str, ...] = ("my bets", "open bets")


BETFRED_PROFILE = GenericSportsbookProfile(
  venue="betfred",
  login_phrases=("log in", "login", "sign in"),
  no_open_bets_phrases=("my bets is empty", "you have no open bets", "no open bets"),
)
CORAL_PROFILE = GenericSportsbookProfile(
  venue="coral",
  login_phrases=("log in", "login", "sign in"),
  no_open_bets_phrases=("you currently have no bets", "no open bets", "my bets is empty"),
)
LADBROKES_PROFILE = GenericSportsbookProfile(
  venue="ladbrokes",
  login_phrases=("log in", "login", "sign in"),
  no_open_bets_phrases=("you currently have no bets", "no open bets", "my bets is empty"),
)
KWIK_PROFILE = GenericSportsbookProfile(
  venue="kwik",
  login_phrases=("log in", "login", "sign in"),
  no_open_bets_phrases=("you have no bets", "no open bets", "my bets is empty"),
)
BET600_PROFILE = GenericSportsbookProfile(
  venue="bet600",
  login_phrases=("log in", "login", "sign in"),
  no_open_bets_phrases=("you have no bets", "no open bets", "my bets is empty"),
)


def analyze_betfred_page(
  *,
  page: str,
  body_text: str,
  inputs: dict[str, str],
  visible_actions: list[str],
) -> dict:
  return analyze_generic_sportsbook_page(
    page=page,
    body_text=body_text,
    visible_actions=visible_actions,
    profile=BETFRED_PROFILE,
  )


def analyze_coral_page(
  *,
  page: str,
  body_text: str,
  inputs: dict[str, str],
  visible_actions: list[str],
) -> dict:
  return analyze_generic_sportsbook_page(
    page=page,
    body_text=body_text,
    visible_actions=visible_actions,
    profile=CORAL_PROFILE,
  )


def analyze_ladbrokes_page(
  *,
  page: str,
  body_text: str,
  inputs: dict[str, str],
  visible_actions: list[str],
) -> dict:
  return analyze_generic_sportsbook_page(
    page=page,
    body_text=body_text,
    visible_actions=visible_actions,
    profile=LADBROKES_PROFILE,
  )


def analyze_kwik_page(
  *,
  page: str,
  body_text: str,
  inputs: dict[str, str],
  visible_actions: list[str],
) -> dict:
  return analyze_generic_sportsbook_page(
    page=page,
    body_text=body_text,
    visible_actions=visible_actions,
    profile=KWIK_PROFILE,
  )


def analyze_bet600_page(
  *,
  page: str,
  body_text: str,
  inputs: dict[str, str],
  visible_actions: list[str],
) -> dict:
  return analyze_generic_sportsbook_page(
    page=page,
    body_text=body_text,
    visible_actions=visible_actions,
    profile=BET600_PROFILE,
  )


def analyze_generic_sportsbook_page(
  *,
  page: str,
  body_text: str,
  visible_actions: list[str],
  profile: GenericSportsbookProfile,
) -> dict:
  if page != "my_bets":
    return {
      "page": page,
      "body_text_length": len(body_text),
      "visible_action_count": len(visible_actions),
    }

  lines = clean_lines(body_text)
  lower_body = body_text.lower()
  supports_cash_out = (
    any(action.lower() == "cash out" for action in visible_actions)
    or "cash out" in lower_body
  )

  if any(phrase in lower_body for phrase in profile.login_phrases):
    return {
      "page": page,
      "status": "login_required",
      "open_bets": [],
      "open_bet_count": 0,
      "supports_cash_out": supports_cash_out,
    }

  count_hint = _extract_count_hint(lines=lines, lower_body=lower_body)
  open_bets = _extract_open_bets(lines=lines, supports_cash_out=supports_cash_out)
  if open_bets:
    return {
      "page": page,
      "status": "ready",
      "open_bets": open_bets,
      "open_bet_count": len(open_bets),
      "count_hint": count_hint,
      "supports_cash_out": supports_cash_out,
    }

  if count_hint == 0 or any(phrase in lower_body for phrase in profile.no_open_bets_phrases):
    status = "no_open_bets"
  elif any(marker in lower_body for marker in profile.navigation_markers):
    status = "navigation_required"
  else:
    status = "unknown"

  return {
    "page": page,
    "status": status,
    "open_bets": [],
    "open_bet_count": 0,
    "count_hint": count_hint,
    "supports_cash_out": supports_cash_out,
  }


def _extract_count_hint(*, lines: list[str], lower_body: str) -> int | None:
  for line in lines[:8]:
    match = COUNT_HINT_RE.search(line)
    if match is None:
      continue
    raw_value = match.group("prefix_count") or match.group("count")
    if raw_value is not None:
      return int(raw_value)

  match = COUNT_HINT_RE.search(lower_body)
  if match is None:
    return None
  raw_value = match.group("prefix_count") or match.group("count")
  return int(raw_value) if raw_value is not None else None


def _extract_open_bets(*, lines: list[str], supports_cash_out: bool) -> list[dict]:
  results: list[dict] = []
  seen: set[tuple[str, str, str, float, float]] = set()

  for index, line in enumerate(lines):
    odds = parse_odds(line)
    if odds is None:
      continue

    stake = _find_stake(lines=lines, odds_index=index)
    if stake is None:
      continue

    selection = _find_selection(lines=lines, odds_index=index)
    market = _find_market(lines=lines, odds_index=index)
    if selection is None or market is None:
      continue

    event = _find_event(lines=lines, odds_index=index, selection=selection)
    if event is None:
      event = selection

    dedupe_key = (event, selection, market, odds, stake)
    if dedupe_key in seen:
      continue
    seen.add(dedupe_key)

    results.append(
      {
        "label": selection,
        "market": market,
        "side": "back",
        "odds": odds,
        "stake": stake,
        "status": "cash_out" if supports_cash_out else "open",
        "event": event,
        "supports_cash_out": supports_cash_out,
      }
    )

  return results


def _find_stake(*, lines: list[str], odds_index: int) -> float | None:
  for offset in range(1, 6):
    line_index = odds_index + offset
    if line_index >= len(lines):
      break
    candidate = parse_money(lines[line_index])
    if candidate is not None:
      return candidate

  for offset in range(1, 4):
    line_index = odds_index - offset
    if line_index < 0:
      break
    candidate = parse_money(lines[line_index])
    if candidate is not None:
      return candidate

  return None


def _find_selection(*, lines: list[str], odds_index: int) -> str | None:
  for offset in range(1, 5):
    line_index = odds_index - offset
    if line_index < 0:
      break
    candidate = lines[line_index]
    if _is_noise_line(candidate):
      continue
    return candidate
  return None


def _find_event(*, lines: list[str], odds_index: int, selection: str) -> str | None:
  for offset in range(2, 8):
    line_index = odds_index - offset
    if line_index < 0:
      break
    candidate = lines[line_index]
    if _is_noise_line(candidate):
      continue
    if candidate == selection:
      continue
    return candidate
  return None


def _find_market(*, lines: list[str], odds_index: int) -> str | None:
  for offset in range(1, 5):
    line_index = odds_index + offset
    if line_index >= len(lines):
      break
    candidate = lines[line_index]
    if _is_noise_line(candidate):
      continue
    if MONEY_PREFIX_RE.match(candidate):
      continue
    return candidate
  return None


def _is_noise_line(value: str) -> bool:
  normalized = value.strip()
  if not normalized:
    return True
  lowered = normalized.lower()
  if lowered in IGNORE_MARKERS:
    return True
  if parse_money(normalized) is not None or parse_odds(normalized) is not None:
    return True
  if looks_like_date_fragment(normalized):
    return True
  if MONEY_PREFIX_RE.match(normalized):
    return True
  return False
