from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceCaptureProfile:
  source: str
  supported_pages: tuple[str, ...]
  transport_capture_default: bool
  screenshot_required: bool
  minimum_pages: tuple[str, ...]


SOURCE_PROFILES = {
  "betway_uk": SourceCaptureProfile(
    source="betway_uk",
    supported_pages=("market", "betslip", "confirmation", "my_bets", "settlement"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("market", "betslip", "confirmation"),
  ),
  "bet365": SourceCaptureProfile(
    source="bet365",
    supported_pages=("my_bets",),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
  ),
  "betuk": SourceCaptureProfile(
    source="betuk",
    supported_pages=("my_bets", "market"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
  ),
  "betfred": SourceCaptureProfile(
    source="betfred",
    supported_pages=("my_bets", "market"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
  ),
  "betdaq": SourceCaptureProfile(
    source="betdaq",
    supported_pages=("open_positions", "market"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("open_positions",),
  ),
  "smarkets_exchange": SourceCaptureProfile(
    source="smarkets_exchange",
    supported_pages=("market", "open_positions", "history", "settlement"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("market", "open_positions", "settlement"),
  ),
}


def get_source_profile(source: str) -> SourceCaptureProfile:
  profile = SOURCE_PROFILES.get(source)
  if profile is None:
    raise ValueError(f"Unsupported source profile: {source}")
  return profile
