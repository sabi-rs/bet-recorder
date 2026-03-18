from __future__ import annotations

import re

from bet_recorder.analysis.open_bets_common import clean_lines, parse_money, parse_odds

BET_SLIP_RE = re.compile(r"(\d+)\s+Bet Slip", re.I)
ODDS_RE = re.compile(r"Odds\s+([0-9]+/[0-9]+|[0-9]+(?:\.[0-9]+)?)", re.I)
STAKE_RE = re.compile(r"Stake\s+([0-9]+(?:\.[0-9]+)?)", re.I)


def analyze_betway_page(
  *,
  page: str,
  body_text: str,
  inputs: dict[str, str],
  visible_actions: list[str],
) -> dict:
  if page == "betslip":
    return _analyze_betslip(body_text=body_text, inputs=inputs, visible_actions=visible_actions)
  if page == "my_bets":
    return _analyze_my_bets(body_text=body_text, visible_actions=visible_actions)
  return {
    "page": page,
    "body_text_length": len(body_text),
    "visible_action_count": len(visible_actions),
  }


def _analyze_betslip(*, body_text: str, inputs: dict[str, str], visible_actions: list[str]) -> dict:
  selection_count_match = BET_SLIP_RE.search(body_text)
  odds_match = ODDS_RE.search(body_text)
  stake_value = inputs.get("stake") or _match_group(STAKE_RE.search(body_text), 1)

  odds_literal = odds_match.group(1) if odds_match else None
  return {
    "page": "betslip",
    "selection_count": int(selection_count_match.group(1)) if selection_count_match else None,
    "odds_fractional": odds_literal if odds_literal and "/" in odds_literal else None,
    "odds_decimal": _parse_odds_literal(odds_literal) if odds_literal else None,
    "stake": float(stake_value) if stake_value else None,
    "free_bet": "free bet" in body_text.lower(),
    "can_place_bet": any(action.lower() == "place bet" for action in visible_actions),
  }


def _parse_odds_literal(value: str) -> float:
  if "/" in value:
    numerator, denominator = value.split("/", maxsplit=1)
    return 1.0 + (float(numerator) / float(denominator))
  return float(value)


def _match_group(match: re.Match[str] | None, index: int) -> str | None:
  if match is None:
    return None
  return match.group(index)


def _analyze_my_bets(*, body_text: str, visible_actions: list[str]) -> dict:
  lower_body = body_text.lower()
  if "please login to view your bets" in lower_body:
    return {
      "page": "my_bets",
      "status": "login_required",
      "open_bets": [],
      "open_bet_count": 0,
      "supports_cash_out": any(action.lower() == "cash out" for action in visible_actions),
    }

  if "my bets is empty" in lower_body:
    return {
      "page": "my_bets",
      "status": "no_open_bets",
      "open_bets": [],
      "open_bet_count": 0,
      "supports_cash_out": any(action.lower() == "cash out" for action in visible_actions),
    }

  lines = clean_lines(body_text)
  open_bets: list[dict] = []
  for index, line in enumerate(lines):
    odds = parse_odds(line)
    if odds is None or index < 2:
      continue
    stake = parse_money(lines[index - 2])
    if stake is None:
      continue
    selection = lines[index - 1]
    market = lines[index + 1] if index + 1 < len(lines) else "Selection"
    open_bets.append(
      {
        "label": selection,
        "market": market,
        "side": "back",
        "odds": odds,
        "stake": stake,
        "status": "open",
      }
    )

  return {
    "page": "my_bets",
    "status": "ready" if open_bets else "unknown",
    "open_bets": open_bets,
    "open_bet_count": len(open_bets),
    "supports_cash_out": any(action.lower() == "cash out" for action in visible_actions),
  }
