from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.sources.profiles import get_source_profile  # noqa: E402


def test_bet365_profile_exposes_my_bets_page_without_transport() -> None:
  profile = get_source_profile("bet365")

  assert profile.source == "bet365"
  assert profile.supported_pages == ("my_bets",)
  assert profile.transport_capture_default is False
  assert profile.screenshot_required is True
  assert profile.minimum_pages == ("my_bets",)


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


def test_get_source_profile_rejects_unknown_sources() -> None:
  try:
    get_source_profile("unknown")
  except ValueError as exc:
    assert str(exc) == "Unsupported source profile: unknown"
  else:
    raise AssertionError("Expected ValueError for unknown source")
