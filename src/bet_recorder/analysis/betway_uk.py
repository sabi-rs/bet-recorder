from __future__ import annotations

import re

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
