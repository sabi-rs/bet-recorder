from __future__ import annotations

from bet_recorder.analysis.open_bets_common import (
  clean_lines,
  looks_like_date_fragment,
  parse_money,
  parse_odds,
)

BET_TYPE_MARKERS = {"Single", "Double", "Treble", "Accumulator"}


def analyze_bet365_page(
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
  lower_body = body_text.lower()
  if "log in" in lower_body and "my bets" in lower_body:
    return {
      "page": page,
      "status": "login_required",
      "open_bets": [],
      "open_bet_count": 0,
    }

  open_bets = _extract_open_bets(lines)
  return {
    "page": page,
    "status": "ready",
    "open_bets": open_bets,
    "open_bet_count": len(open_bets),
    "supports_cash_out": "Cash Out" in visible_actions or "Cash Out" in body_text,
  }


def _extract_open_bets(lines: list[str]) -> list[dict]:
  results: list[dict] = []
  index = 0
  while index < len(lines):
    if lines[index] not in BET_TYPE_MARKERS:
      index += 1
      continue

    bet_type = lines[index]
    start_index = index
    index += 1

    selection = None
    while index < len(lines):
      current = lines[index]
      if current in {"Reuse Selections", "Stake", "Net Return"}:
        index += 1
        continue
      if parse_odds(current) is not None:
        break
      selection = current
      index += 1

    if selection is None or index >= len(lines):
      continue

    odds = parse_odds(lines[index])
    if odds is None or index + 1 >= len(lines):
      continue
    index += 1
    market = lines[index]
    index += 1

    event_parts: list[str] = []
    while index < len(lines):
      current = lines[index]
      if current == "Stake":
        break
      if current == "Reuse Selections":
        index += 1
        continue
      event_parts.append(current)
      index += 1

    if index >= len(lines) or lines[index] != "Stake":
      continue

    stake = parse_money(lines[index + 1]) if index + 1 < len(lines) else None
    net_return = None
    for search_index in range(index + 1, min(index + 6, len(lines) - 1)):
      if lines[search_index] == "Net Return":
        net_return = parse_money(lines[search_index + 1])
        break

    event = _build_event_label(selection=selection, event_parts=event_parts)
    results.append(
      {
        "label": selection,
        "market": market,
        "side": "back",
        "odds": odds,
        "stake": stake or 0.0,
        "status": "cash_out" if net_return is not None else "open",
        "bet_type": bet_type.lower(),
        "event": event,
        "net_return": net_return,
      }
    )

    if index == start_index:
      index += 1

  return results


def _build_event_label(*, selection: str, event_parts: list[str]) -> str:
  meaningful_parts = [
    part for part in event_parts if not looks_like_date_fragment(part)
  ]
  if len(meaningful_parts) >= 2:
    return f"{meaningful_parts[0]} v {meaningful_parts[-1]}"
  if meaningful_parts:
    return f"{selection} v {meaningful_parts[-1]}"
  return selection
