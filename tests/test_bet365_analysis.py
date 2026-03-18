from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.bet365 import analyze_bet365_page  # noqa: E402


def test_analyze_bet365_my_bets_extracts_open_bet() -> None:
  analysis = analyze_bet365_page(
    page="my_bets",
    body_text=(
      "All Sports\nIn-Play\nMy Bets\n1\nCasino\nOffers\n£0.00\n"
      "Open\nCash Out\nLive\nSettled\n£10.00\nSingle\nBrumbies\nReuse Selections\n"
      "Brumbies\n3.10\nTo Win\nBrumbies\nFri 20 Mar\nChiefs\n08:35\nStake\n£10.00\n"
      "£10.00 Bet Credits\nNet Return\n£21.00"
    ),
    inputs={},
    visible_actions=["All Sports", "In-Play", "My Bets", "Cash Out"],
  )

  assert analysis["status"] == "ready"
  assert analysis["open_bet_count"] == 1
  assert analysis["open_bets"][0]["label"] == "Brumbies"
  assert analysis["open_bets"][0]["market"] == "To Win"
  assert analysis["open_bets"][0]["event"] == "Brumbies v Chiefs"
  assert analysis["open_bets"][0]["stake"] == 10.0
  assert analysis["open_bets"][0]["odds"] == 3.1
  assert analysis["open_bets"][0]["status"] == "cash_out"
