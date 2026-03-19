from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.generic_sportsbooks import (  # noqa: E402
  analyze_bet600_page,
  analyze_betfred_page,
  analyze_coral_page,
  analyze_kwik_page,
  analyze_ladbrokes_page,
)


@pytest.mark.parametrize(
  ("analyzer", "body_text", "expected_event", "expected_selection"),
  [
    (
      analyze_betfred_page,
      "My Bets\nOpen\nCash Out\nEverton v Liverpool\nLiverpool\n7/4\nMatch Result\nStake\n£12.00",
      "Everton v Liverpool",
      "Liverpool",
    ),
    (
      analyze_coral_page,
      "Open Bets\nCheltenham 15:20\nDesert Hero\n3.50\nWin Market\nStake\n£8.00\nCash Out",
      "Cheltenham 15:20",
      "Desert Hero",
    ),
    (
      analyze_ladbrokes_page,
      "My Bets\nArsenal v Everton\nArsenal\n2.40\nMatch Odds\n£10.00\nCash Out",
      "Arsenal v Everton",
      "Arsenal",
    ),
    (
      analyze_kwik_page,
      "Open Bets\nEngland v France\nFrance\n2.80\nMatch Odds\n£5.00",
      "England v France",
      "France",
    ),
    (
      analyze_bet600_page,
      "My Bets\nOpen\nBarcelona v Real Madrid\nBoth Teams To Score\n1.95\nSpecials\n£7.50",
      "Barcelona v Real Madrid",
      "Both Teams To Score",
    ),
  ],
)
def test_generic_sportsbook_analyzers_extract_open_bets(
  analyzer,
  body_text: str,
  expected_event: str,
  expected_selection: str,
) -> None:
  analysis = analyzer(
    page="my_bets",
    body_text=body_text,
    inputs={},
    visible_actions=["My Bets", "Cash Out"],
  )

  assert analysis["status"] == "ready"
  assert analysis["open_bet_count"] == 1
  assert analysis["open_bets"][0]["event"] == expected_event
  assert analysis["open_bets"][0]["label"] == expected_selection
  assert analysis["open_bets"][0]["supports_cash_out"] is True


def test_generic_sportsbook_analyzers_detect_login_required() -> None:
  analysis = analyze_coral_page(
    page="my_bets",
    body_text="My Bets Please log in to view your bets.",
    inputs={},
    visible_actions=["Log In"],
  )

  assert analysis["status"] == "login_required"
  assert analysis["open_bets"] == []


def test_generic_sportsbook_analyzers_detect_empty_open_bets_from_count_hint() -> None:
  analysis = analyze_kwik_page(
    page="my_bets",
    body_text="+0\nMy Bets\nOpen Bets\n",
    inputs={},
    visible_actions=["My Bets"],
  )

  assert analysis["status"] == "no_open_bets"
  assert analysis["count_hint"] == 0
