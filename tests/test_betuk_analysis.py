from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.betuk import analyze_betuk_page  # noqa: E402


def test_analyze_betuk_my_bets_reports_zero_open_bets_from_count_hint() -> None:
  analysis = analyze_betuk_page(
    page="my_bets",
    body_text=(
      "+0\n£0\nSPORTS\nCASINO\nLIVE CASINO\nFootball\n"
      "Featured\nAll Sports\nIn-Play(36)\nSearch\nMy Bets\n"
      "Bournemouth\nManchester United\n11/5\n14/5\n11/10"
    ),
    inputs={},
    visible_actions=["Sports", "Search", "My Bets", "Logout"],
  )

  assert analysis["status"] == "no_open_bets"
  assert analysis["open_bets"] == []
  assert analysis["count_hint"] == 0
