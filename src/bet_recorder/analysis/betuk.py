from __future__ import annotations

import re

from bet_recorder.analysis.open_bets_common import clean_lines, parse_money, parse_odds

COUNT_PREFIX_RE = re.compile(r"^\+(?P<count>\d+)$")


def analyze_betuk_page(
  *,
  page: str,
  body_text: str,
  inputs: dict[str, str],
  visible_actions: list[str],
) -> dict:
  if page != "my_bets":
    return {
      "page": page,
      "body_text_length": len(body_text),
      "visible_action_count": len(visible_actions),
    }

  lines = clean_lines(body_text)
  count_hint = _extract_count_hint(lines)
  open_bets = _extract_open_bets(lines)
  if open_bets:
    return {
      "page": page,
      "status": "ready",
      "open_bets": open_bets,
      "open_bet_count": len(open_bets),
      "count_hint": count_hint,
    }

  if count_hint == 0:
    status = "no_open_bets"
  elif any(action.lower() == "my bets" for action in visible_actions):
    status = "navigation_required"
  else:
    status = "unknown"

  return {
    "page": page,
    "status": status,
    "open_bets": [],
    "open_bet_count": 0,
    "count_hint": count_hint,
  }


def _extract_count_hint(lines: list[str]) -> int | None:
  for line in lines[:6]:
    match = COUNT_PREFIX_RE.fullmatch(line)
    if match is not None:
      return int(match.group("count"))
  return None


def _extract_open_bets(lines: list[str]) -> list[dict]:
  results: list[dict] = []
  for index, line in enumerate(lines):
    odds = parse_odds(line)
    if odds is None:
      continue
    if index < 3 or index + 2 >= len(lines):
      continue
    selection = lines[index - 1]
    event = lines[index - 2]
    market = lines[index + 1]
    stake = parse_money(lines[index + 2]) if index + 2 < len(lines) else None
    if stake is None:
      continue
    if not selection or not event or not market:
      continue
    results.append(
      {
        "label": selection,
        "market": market,
        "side": "back",
        "odds": odds,
        "stake": stake,
        "status": "open",
        "event": event,
      }
    )
  return results
