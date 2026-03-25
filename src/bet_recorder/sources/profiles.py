from __future__ import annotations

from dataclasses import dataclass, replace

from bet_recorder.sources.history_adapters import get_source_history_adapter


@dataclass(frozen=True)
class SourceCaptureProfile:
  source: str
  supported_pages: tuple[str, ...]
  transport_capture_default: bool
  screenshot_required: bool
  minimum_pages: tuple[str, ...]
  history_navigation_supported: bool
  history_parser_supported: bool
  history_reconciliation_tested: bool
  history_fixture_names: tuple[str, ...]
  explicit_funding_supported: bool

  @property
  def historical_reconciliation_supported(self) -> bool:
    return (
      self.history_navigation_supported
      and self.history_parser_supported
      and self.history_reconciliation_tested
      and bool(self.history_fixture_names)
      and self.explicit_funding_supported
    )


SOURCE_PROFILES = {
  "betway_uk": SourceCaptureProfile(
    source="betway_uk",
    supported_pages=("market", "betslip", "confirmation", "my_bets", "settlement"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("market", "betslip", "confirmation"),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "bet365": SourceCaptureProfile(
    source="bet365",
    supported_pages=("my_bets", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=True,
  ),
  "betuk": SourceCaptureProfile(
    source="betuk",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=True,
  ),
  "betfred": SourceCaptureProfile(
    source="betfred",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "coral": SourceCaptureProfile(
    source="coral",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "ladbrokes": SourceCaptureProfile(
    source="ladbrokes",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "kwik": SourceCaptureProfile(
    source="kwik",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "bet600": SourceCaptureProfile(
    source="bet600",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "betano": SourceCaptureProfile(
    source="betano",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "betmgm": SourceCaptureProfile(
    source="betmgm",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "betvictor": SourceCaptureProfile(
    source="betvictor",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "skybet": SourceCaptureProfile(
    source="skybet",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "talksportbet": SourceCaptureProfile(
    source="talksportbet",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "paddypower": SourceCaptureProfile(
    source="paddypower",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "boylesports": SourceCaptureProfile(
    source="boylesports",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "williamhill": SourceCaptureProfile(
    source="williamhill",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "sportingindex": SourceCaptureProfile(
    source="sportingindex",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "leovegas": SourceCaptureProfile(
    source="leovegas",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "fanteam": SourceCaptureProfile(
    source="fanteam",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "midnite": SourceCaptureProfile(
    source="midnite",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "bet10": SourceCaptureProfile(
    source="bet10",
    supported_pages=("my_bets", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("my_bets",),
    history_navigation_supported=True,
    history_parser_supported=True,
    history_reconciliation_tested=True,
    history_fixture_names=("bet10_betting_history.txt", "bet10_payment_history.csv"),
    explicit_funding_supported=True,
  ),
  "betfair": SourceCaptureProfile(
    source="betfair",
    supported_pages=("open_positions", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("open_positions",),
    history_navigation_supported=True,
    history_parser_supported=True,
    history_reconciliation_tested=True,
    history_fixture_names=("betfair_account_statement.csv",),
    explicit_funding_supported=True,
  ),
  "matchbook": SourceCaptureProfile(
    source="matchbook",
    supported_pages=("open_positions", "market", "history"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("open_positions",),
    history_navigation_supported=True,
    history_parser_supported=True,
    history_reconciliation_tested=True,
    history_fixture_names=("matchbook_transactions.csv",),
    explicit_funding_supported=True,
  ),
  "betdaq": SourceCaptureProfile(
    source="betdaq",
    supported_pages=("open_positions", "market"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("open_positions",),
    history_navigation_supported=False,
    history_parser_supported=False,
    history_reconciliation_tested=False,
    history_fixture_names=(),
    explicit_funding_supported=False,
  ),
  "smarkets_exchange": SourceCaptureProfile(
    source="smarkets_exchange",
    supported_pages=("market", "open_positions", "history", "settlement"),
    transport_capture_default=False,
    screenshot_required=True,
    minimum_pages=("market", "open_positions", "settlement"),
    history_navigation_supported=True,
    history_parser_supported=True,
    history_reconciliation_tested=True,
    history_fixture_names=("smarkets_history",),
    explicit_funding_supported=True,
  ),
}


def get_source_profile(source: str) -> SourceCaptureProfile:
  profile = SOURCE_PROFILES.get(source)
  if profile is None:
    raise ValueError(f"Unsupported source profile: {source}")
  adapter = get_source_history_adapter(source)
  if adapter is None:
    return profile
  return replace(
    profile,
    history_navigation_supported=(
      profile.history_navigation_supported or adapter.history_navigation_supported
    ),
    history_parser_supported=(
      profile.history_parser_supported or adapter.history_parser_supported
    ),
    history_reconciliation_tested=(
      profile.history_reconciliation_tested or adapter.reconciliation_tested
    ),
    history_fixture_names=profile.history_fixture_names or adapter.fixture_names,
    explicit_funding_supported=(
      profile.explicit_funding_supported or adapter.explicit_funding_supported
    ),
  )
