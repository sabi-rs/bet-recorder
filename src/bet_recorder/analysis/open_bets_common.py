from __future__ import annotations

import re

MONEY_RE = re.compile(r"^£(?P<amount>\d+(?:\.\d+)?)$")
DECIMAL_ODDS_RE = re.compile(r"^\d+(?:\.\d+)?$")
FRACTIONAL_ODDS_RE = re.compile(r"^\d+/\d+$")
DATE_FRAGMENT_RE = re.compile(
  r"^(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)(?:\s+\d{1,2}\s+\w+)?$|^\d{1,2}:\d{2}$",
  re.I,
)


def clean_lines(body_text: str) -> list[str]:
  return [line.strip() for line in body_text.splitlines() if line.strip()]


def parse_money(value: str) -> float | None:
  match = MONEY_RE.fullmatch(value.strip())
  if match is None:
    return None
  return float(match.group("amount"))


def parse_odds(value: str) -> float | None:
  normalized = value.strip()
  if DECIMAL_ODDS_RE.fullmatch(normalized):
    return float(normalized)
  if FRACTIONAL_ODDS_RE.fullmatch(normalized):
    numerator, denominator = normalized.split("/", maxsplit=1)
    return 1.0 + (float(numerator) / float(denominator))
  return None


def looks_like_date_fragment(value: str) -> bool:
  return DATE_FRAGMENT_RE.fullmatch(value.strip()) is not None
