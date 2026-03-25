from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class LiveVenueHistoryAdapter:
  venue: str
  source: str
  history_page: str = "my_bets"
  history_urls: tuple[str, ...] = ()
  frame_url_fragments: tuple[str, ...] = ()
  history_api_endpoints: tuple[tuple[str, str], ...] = ()
  history_labels: tuple[str, ...] = ()
  date_range_labels: tuple[str, ...] = ()
  history_submit_labels: tuple[str, ...] = ()
  pagination_labels: tuple[str, ...] = ()
  parser_name: str = ""
  fixture_names: tuple[str, ...] = ()
  reconciliation_tested: bool = False
  explicit_funding_supported: bool = False

  @property
  def history_navigation_supported(self) -> bool:
    return bool(self.history_urls or self.history_labels)

  @property
  def history_parser_supported(self) -> bool:
    return bool(self.parser_name)

  @property
  def historical_reconciliation_supported(self) -> bool:
    return (
      self.history_navigation_supported
      and self.history_parser_supported
      and self.reconciliation_tested
      and bool(self.fixture_names)
      and self.explicit_funding_supported
    )

  def missing_capabilities(self) -> tuple[str, ...]:
    missing: list[str] = []
    if not self.history_navigation_supported:
      missing.append("history navigation")
    if not self.history_parser_supported:
      missing.append("history parser")
    if not self.reconciliation_tested:
      missing.append("reconciliation tests")
    if not self.fixture_names:
      missing.append("captured fixtures")
    if not self.explicit_funding_supported:
      missing.append("funding/free-bet classification")
    return tuple(missing)


LIVE_VENUE_HISTORY_ADAPTERS = {
  "betfair": LiveVenueHistoryAdapter(
    venue="betfair",
    source="betfair",
    history_page="history",
    history_urls=("https://www.betfair.com/exchange/plus/",),
    history_labels=("History", "Settled", "Closed"),
    parser_name="betfair_account_statement",
    fixture_names=("betfair_account_statement.csv",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "bet365": LiveVenueHistoryAdapter(
    venue="bet365",
    source="bet365",
    history_page="history",
    history_urls=(
      "https://www.bet365.com/?#/ME/K/HI//",
      "https://www.bet365.com/#/ME/X8020",
      "https://www.bet365.com/#/MB/",
    ),
    frame_url_fragments=("members.bet365.com",),
    history_labels=("Settled", "Settled Bets", "History"),
    date_range_labels=("Last 48 Hours", "Date Range", "From", "To"),
    history_submit_labels=("Show History",),
    pagination_labels=("View More", "Show More"),
    parser_name="bet365_history",
    fixture_names=("bet365_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "betuk": LiveVenueHistoryAdapter(
    venue="betuk",
    source="betuk",
    history_page="history",
    history_urls=("https://www.betuk.com/betting#bethistory",),
    history_labels=("History", "Settled"),
    parser_name="betuk_history",
    fixture_names=("betuk_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "betway": LiveVenueHistoryAdapter(
    venue="betway",
    source="betway_uk",
    history_page="history",
    history_labels=("Settled", "History", "Closed"),
  ),
  "matchbook": LiveVenueHistoryAdapter(
    venue="matchbook",
    source="matchbook",
    history_page="history",
    history_urls=("https://www.matchbook.com/login",),
    history_labels=("History", "Settled", "Closed"),
    parser_name="matchbook_transactions",
    fixture_names=("matchbook_transactions.csv",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "betfred": LiveVenueHistoryAdapter(
    venue="betfred",
    source="betfred",
    history_page="history",
    history_urls=("https://www.betfred.com/sport/my-bets",),
    history_labels=("Settled", "History"),
    parser_name="generic_history",
    fixture_names=("betfred_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "coral": LiveVenueHistoryAdapter(
    venue="coral",
    source="coral",
    history_page="history",
    history_urls=("https://sports.coral.co.uk/my-bets",),
    history_labels=("Settled", "History"),
    parser_name="generic_history",
    fixture_names=("coral_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "ladbrokes": LiveVenueHistoryAdapter(
    venue="ladbrokes",
    source="ladbrokes",
    history_page="history",
    history_urls=("https://sports.ladbrokes.com/my-bets",),
    history_labels=("Settled", "History"),
    parser_name="generic_history",
    fixture_names=("ladbrokes_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "kwik": LiveVenueHistoryAdapter(
    venue="kwik",
    source="kwik",
    history_page="history",
    history_urls=("https://sports.kwiff.com/my-bets",),
    history_labels=("Settled", "History"),
    parser_name="generic_history",
    fixture_names=("kwik_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "bet600": LiveVenueHistoryAdapter(
    venue="bet600",
    source="bet600",
    history_page="history",
    history_urls=("https://www.bet600.co.uk/my-bets",),
    history_labels=("Settled", "History"),
    parser_name="generic_history",
    fixture_names=("bet600_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "betano": LiveVenueHistoryAdapter(
    venue="betano",
    source="betano",
    history_page="history",
    history_api_endpoints=(
      ("settled", "https://www.betano.co.uk/bv_api/account_history/settled/1/bets"),
    ),
    history_labels=("Bet History", "Settled", "History"),
    parser_name="betano_history",
    fixture_names=("betano_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "betmgm": LiveVenueHistoryAdapter(
    venue="betmgm",
    source="betmgm",
    history_page="history",
    history_urls=("https://www.betmgm.co.uk/sports#bethistory",),
    history_labels=("My Bets", "Settled"),
    parser_name="kambi_history",
    fixture_names=("betmgm_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "betvictor": LiveVenueHistoryAdapter(
    venue="betvictor",
    source="betvictor",
    history_page="history",
    history_urls=("https://www.betvictor.com/en-gb/bv_cashier/history?first_modal=true",),
    history_api_endpoints=(
      ("settled", "https://www.betvictor.com/bv_api/account_history/settled/1/bets"),
    ),
    history_labels=("Bet History", "Settled", "History"),
    parser_name="betvictor_history",
    fixture_names=("betvictor_history.json",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "skybet": LiveVenueHistoryAdapter(
    venue="skybet",
    source="skybet",
    history_page="history",
    history_labels=("My Bets", "History", "Settled"),
  ),
  "talksportbet": LiveVenueHistoryAdapter(
    venue="talksportbet",
    source="talksportbet",
    history_page="history",
    history_urls=("https://www.talksportbet.com/en-gb/bv_cashier/history?first_modal=true",),
    history_api_endpoints=(
      ("settled", "https://www.talksportbet.com/bv_api/account_history/settled/1/bets"),
    ),
    history_labels=("Bet History", "Settled", "History"),
    parser_name="betvictor_history",
    fixture_names=("talksportbet_history.json",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "paddypower": LiveVenueHistoryAdapter(
    venue="paddypower",
    source="paddypower",
    history_page="history",
    history_urls=("https://www.paddypower.com/my-bets?tab=settledBets",),
    history_labels=("My Bets", "Settled", "Transaction History"),
    parser_name="paddypower_history",
    fixture_names=("paddypower_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "boylesports": LiveVenueHistoryAdapter(
    venue="boylesports",
    source="boylesports",
    history_page="history",
    history_urls=("https://account.boylesports.com/#account-bethistory+Opened",),
    history_labels=("Bet History", "Settled Bets", "View betting history"),
    parser_name="boylesports_history",
    fixture_names=("boylesports_history.html",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "williamhill": LiveVenueHistoryAdapter(
    venue="williamhill",
    source="williamhill",
    history_page="history",
    history_labels=("My Bets", "History", "Settled"),
  ),
  "sportingindex": LiveVenueHistoryAdapter(
    venue="sportingindex",
    source="sportingindex",
    history_page="history",
    history_labels=("My Bets", "History", "Settled"),
  ),
  "leovegas": LiveVenueHistoryAdapter(
    venue="leovegas",
    source="leovegas",
    history_page="history",
    history_urls=("https://www.leovegas.co.uk/betting#bethistory",),
    history_labels=("My Bets", "Settled"),
    parser_name="kambi_history",
    fixture_names=("leovegas_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "fanteam": LiveVenueHistoryAdapter(
    venue="fanteam",
    source="fanteam",
    history_page="history",
    history_urls=("https://www.fanteam.com/sportsbook",),
    history_labels=("My Bets", "History", "Settled"),
    parser_name="fanteam_history",
    fixture_names=("fanteam_history.json",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "midnite": LiveVenueHistoryAdapter(
    venue="midnite",
    source="midnite",
    history_page="history",
    history_urls=("https://www.midnite.com/sports/bets/settled",),
    history_labels=("My Bets", "Settled"),
    parser_name="midnite_history",
    fixture_names=("midnite_history.txt",),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
  "bet10": LiveVenueHistoryAdapter(
    venue="bet10",
    source="bet10",
    history_page="history",
    history_urls=("https://www.10bet.co.uk/betting-history",),
    history_labels=("My Bets", "History", "Settled"),
    parser_name="bet10_history",
    fixture_names=("bet10_betting_history.txt", "bet10_payment_history.csv"),
    reconciliation_tested=True,
    explicit_funding_supported=True,
  ),
}


def get_live_venue_history_adapter(venue: str) -> LiveVenueHistoryAdapter | None:
  return LIVE_VENUE_HISTORY_ADAPTERS.get(venue)


def get_source_history_adapter(source: str) -> LiveVenueHistoryAdapter | None:
  for adapter in LIVE_VENUE_HISTORY_ADAPTERS.values():
    if adapter.source == source:
      return adapter
  return None


def supported_live_venue_history_adapters() -> tuple[LiveVenueHistoryAdapter, ...]:
  return tuple(
    adapter
    for adapter in LIVE_VENUE_HISTORY_ADAPTERS.values()
    if adapter.historical_reconciliation_supported
  )
