from __future__ import annotations

import re

POSITION_RE = re.compile(
  r"Sell\s+(?P<contract>.+?)\s+"
  r"(?P<market>Correct score|Full-time result)\s+"
  r"(?P<price>\d+(?:\.\d+)?)\s+"
  r"£(?P<stake>\d+(?:\.\d+)?)\s+"
  r"£(?P<liability>\d+(?:\.\d+)?)\s+"
  r"£(?P<return_amount>\d+(?:\.\d+)?)\s+"
  r"£(?P<current_value>\d+(?:\.\d+)?)\s+"
  r"(?P<pnl_sign>-?)£(?P<pnl_amount>\d+(?:\.\d+)?)\s+"
  r"\((?P<pnl_percent>\d+(?:\.\d+)?)%\)\s+"
  r"(?P<status>Order filled)",
  re.I,
)
AVAILABLE_BALANCE_RE = re.compile(r"Available balance\s+£(?P<amount>\d+(?:\.\d+)?)", re.I)
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


def analyze_smarkets_page(
  *,
  page: str,
  body_text: str,
  inputs: dict[str, str],
  visible_actions: list[str],
) -> dict:
  if page == "open_positions":
    positions = [_build_position(match) for match in POSITION_RE.finditer(body_text)]
    return {
      "page": page,
      "position_count": len(positions),
      "positions": positions,
      "account_stats": _extract_account_stats(body_text),
      "other_open_bets": [_build_open_bet(match) for match in OPEN_BET_RE.finditer(body_text)],
      "can_trade_out": any(action.lower() == "trade out" for action in visible_actions),
      "inputs": inputs,
    }

  return {
    "page": page,
    "body_text_length": len(body_text),
    "visible_action_count": len(visible_actions),
  }


def _build_position(match: re.Match[str]) -> dict:
  pnl_amount = float(match.group("pnl_amount"))
  if match.group("pnl_sign") == "-":
    pnl_amount = -pnl_amount

  return {
    "contract": match.group("contract").strip(),
    "market": match.group("market").strip(),
    "price": float(match.group("price")),
    "stake": float(match.group("stake")),
    "liability": float(match.group("liability")),
    "return_amount": float(match.group("return_amount")),
    "current_value": float(match.group("current_value")),
    "pnl_amount": pnl_amount,
    "pnl_percent": float(match.group("pnl_percent")),
    "status": match.group("status"),
    "can_trade_out": True,
  }


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
