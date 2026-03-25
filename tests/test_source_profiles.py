import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.sources.history_adapters import (  # noqa: E402
  get_live_venue_history_adapter,
  supported_live_venue_history_adapters,
)
from bet_recorder.sources.profiles import get_source_profile  # noqa: E402


def test_bet365_profile_exposes_fixture_backed_history_support() -> None:
  profile = get_source_profile("bet365")

  assert profile.source == "bet365"
  assert profile.supported_pages == ("my_bets", "history")
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("my_bets",)
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is True
  assert profile.history_reconciliation_tested is True
  assert profile.history_fixture_names == ("bet365_history.txt",)
  assert profile.historical_reconciliation_supported is True
  assert profile.explicit_funding_supported is True


def test_betano_profile_exposes_fixture_backed_history_support() -> None:
  profile = get_source_profile("betano")

  assert profile.source == "betano"
  assert profile.supported_pages == ("my_bets", "market", "history")
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("my_bets",)
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is True
  assert profile.history_reconciliation_tested is True
  assert profile.history_fixture_names == ("betano_history.txt",)
  assert profile.historical_reconciliation_supported is True
  assert profile.explicit_funding_supported is True


@pytest.mark.parametrize("source", ["betfred", "coral", "ladbrokes", "kwik", "bet600"])
def test_generic_sportsbook_profiles_expose_my_bets(source: str) -> None:
  profile = get_source_profile(source)

  assert profile.source == source
  assert profile.supported_pages == ("my_bets", "market", "history")
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("my_bets",)
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is True
  assert profile.history_reconciliation_tested is True
  assert profile.history_fixture_names == (f"{source}_history.txt",)
  assert profile.historical_reconciliation_supported is True
  assert profile.explicit_funding_supported is True


@pytest.mark.parametrize(
  "source",
  [
    "skybet",
    "williamhill",
    "sportingindex",
  ],
)
def test_new_generic_sportsbook_profiles_fail_closed_on_history(source: str) -> None:
  profile = get_source_profile(source)

  assert profile.source == source
  assert profile.supported_pages == ("my_bets", "market", "history")
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("my_bets",)
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is False
  assert profile.history_reconciliation_tested is False
  assert profile.history_fixture_names == ()
  assert profile.historical_reconciliation_supported is False
  assert profile.explicit_funding_supported is False


@pytest.mark.parametrize(
  ("source", "fixture_name"),
  [
    ("talksportbet", "talksportbet_history.json"),
    ("paddypower", "paddypower_history.txt"),
    ("boylesports", "boylesports_history.html"),
    ("midnite", "midnite_history.txt"),
    ("betmgm", "betmgm_history.txt"),
    ("leovegas", "leovegas_history.txt"),
    ("fanteam", "fanteam_history.json"),
  ],
)
def test_new_fixture_backed_sportsbook_profiles_expose_history_support(
  source: str,
  fixture_name: str,
) -> None:
  profile = get_source_profile(source)
  fixtures_dir = Path(__file__).resolve().parent / "fixtures" / "bookmaker_history"

  assert profile.source == source
  assert profile.supported_pages == ("my_bets", "market", "history")
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is True
  assert profile.history_reconciliation_tested is True
  assert profile.history_fixture_names == (fixture_name,)
  assert profile.historical_reconciliation_supported is True
  assert profile.explicit_funding_supported is True
  assert (fixtures_dir / fixture_name).exists()


def test_betvictor_profile_exposes_fixture_backed_history_support() -> None:
  profile = get_source_profile("betvictor")

  assert profile.source == "betvictor"
  assert profile.supported_pages == ("my_bets", "market", "history")
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("my_bets",)
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is True
  assert profile.history_reconciliation_tested is True
  assert profile.history_fixture_names == ("betvictor_history.json",)
  assert profile.historical_reconciliation_supported is True
  assert profile.explicit_funding_supported is True


def test_bet10_profile_exposes_export_backed_history_support() -> None:
  profile = get_source_profile("bet10")
  fixtures_dir = Path(__file__).resolve().parent / "fixtures" / "bookmaker_history"

  assert profile.source == "bet10"
  assert profile.supported_pages == ("my_bets", "market", "history")
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("my_bets",)
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is True
  assert profile.history_reconciliation_tested is True
  assert profile.history_fixture_names == (
    "bet10_betting_history.txt",
    "bet10_payment_history.csv",
  )
  assert profile.historical_reconciliation_supported is True
  assert profile.explicit_funding_supported is True
  for fixture_name in profile.history_fixture_names:
    assert (fixtures_dir / fixture_name).exists()


def test_betway_profile_exposes_betslip_and_confirmation_pages() -> None:
  profile = get_source_profile("betway_uk")

  assert profile.source == "betway_uk"
  assert profile.supported_pages == (
    "market",
    "betslip",
    "confirmation",
    "my_bets",
    "settlement",
  )
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("market", "betslip", "confirmation")
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is False
  assert profile.history_reconciliation_tested is False
  assert profile.history_fixture_names == ()
  assert profile.historical_reconciliation_supported is False
  assert profile.explicit_funding_supported is False


def test_betuk_profile_exposes_fixture_backed_history_support() -> None:
  profile = get_source_profile("betuk")

  assert profile.source == "betuk"
  assert profile.supported_pages == ("my_bets", "market", "history")
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is True
  assert profile.history_reconciliation_tested is True
  assert profile.history_fixture_names == ("betuk_history.txt",)
  assert profile.historical_reconciliation_supported is True
  assert profile.explicit_funding_supported is True


def test_smarkets_profile_exposes_positions_and_settlement_pages() -> None:
  profile = get_source_profile("smarkets_exchange")

  assert profile.source == "smarkets_exchange"
  assert profile.supported_pages == (
    "market",
    "open_positions",
    "history",
    "settlement",
  )
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("market", "open_positions", "settlement")
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is True
  assert profile.history_reconciliation_tested is True
  assert profile.history_fixture_names == ("smarkets_history",)
  assert profile.historical_reconciliation_supported is True
  assert profile.explicit_funding_supported is True


@pytest.mark.parametrize(
  ("source", "fixture_name"),
  [
    ("betfair", "betfair_account_statement.csv"),
    ("matchbook", "matchbook_transactions.csv"),
  ],
)
def test_exchange_profiles_expose_export_backed_history_support(
  source: str,
  fixture_name: str,
) -> None:
  profile = get_source_profile(source)
  fixtures_dir = Path(__file__).resolve().parent / "fixtures" / "bookmaker_history"

  assert profile.source == source
  assert profile.supported_pages == ("open_positions", "market", "history")
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("open_positions",)
  assert profile.history_navigation_supported is True
  assert profile.history_parser_supported is True
  assert profile.history_reconciliation_tested is True
  assert profile.history_fixture_names == (fixture_name,)
  assert profile.historical_reconciliation_supported is True
  assert profile.explicit_funding_supported is True
  assert (fixtures_dir / fixture_name).exists()


def test_get_source_profile_rejects_unknown_sources() -> None:
  try:
    get_source_profile("unknown")
  except ValueError as exc:
    assert str(exc) == "Unsupported source profile: unknown"
  else:
    raise AssertionError("Expected ValueError for unknown source")


def test_supported_history_adapters_are_fixture_backed() -> None:
  fixtures_dir = Path(__file__).resolve().parent / "fixtures" / "bookmaker_history"

  supported = {adapter.venue for adapter in supported_live_venue_history_adapters()}
  assert supported == {
    "betfair",
    "bet365",
    "bet10",
    "betano",
    "betuk",
    "betvictor",
    "betfred",
    "coral",
    "ladbrokes",
    "kwik",
    "bet600",
    "betmgm",
    "boylesports",
    "leovegas",
    "matchbook",
    "midnite",
    "paddypower",
    "talksportbet",
    "fanteam",
  }

  for adapter in supported_live_venue_history_adapters():
    assert adapter.history_navigation_supported is True
    assert adapter.history_parser_supported is True
    assert adapter.reconciliation_tested is True
    assert adapter.explicit_funding_supported is True
    for fixture_name in adapter.fixture_names:
      assert (fixtures_dir / fixture_name).exists()


@pytest.mark.parametrize(
  "venue",
  [
    "betway",
    "skybet",
    "williamhill",
    "sportingindex",
  ],
)
def test_unsupported_history_adapters_fail_closed(venue: str) -> None:
  adapter = get_live_venue_history_adapter(venue)

  assert adapter is not None
  assert adapter.history_navigation_supported is True
  assert adapter.history_parser_supported is False
  assert adapter.historical_reconciliation_supported is False
  assert "history parser" in adapter.missing_capabilities()
  assert "reconciliation tests" in adapter.missing_capabilities()
  assert "captured fixtures" in adapter.missing_capabilities()
