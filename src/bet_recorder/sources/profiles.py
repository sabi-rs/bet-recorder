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
  "rebelbetting_vb": SourceCaptureProfile(
    source="rebelbetting_vb",
    supported_pages=("dashboard", "filters", "bookmakers", "reports"),
    transport_capture_default=True,
    screenshot_required=True,
    minimum_pages=("dashboard", "filters", "bookmakers", "reports"),
  ),
  "rebelbetting_rb": SourceCaptureProfile(
    source="rebelbetting_rb",
    supported_pages=("dashboard", "filters", "bookmakers", "reports"),
    transport_capture_default=True,
    screenshot_required=True,
    minimum_pages=("dashboard", "filters", "bookmakers", "reports"),
  ),
  "fairodds_terminal": SourceCaptureProfile(
    source="fairodds_terminal",
    supported_pages=("drops", "ev", "sidebar"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("drops", "ev", "sidebar"),
  ),
  "profitmaximiser_members": SourceCaptureProfile(
    source="profitmaximiser_members",
    supported_pages=(
      "home",
      "training",
      "tools",
      "calculator",
      "offer_table",
      "calendar",
      "settings",
      "utility",
    ),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("home", "tools", "offer_table", "calculator"),
  ),
  "betway_uk": SourceCaptureProfile(
    source="betway_uk",
    supported_pages=("market", "betslip", "confirmation", "my_bets", "settlement"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("market", "betslip", "confirmation"),
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
