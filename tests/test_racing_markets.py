from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.racing_markets import analyze_racing_market_page  # noqa: E402


def test_analyze_racing_market_page_extracts_sportsbook_quotes() -> None:
  analysis = analyze_racing_market_page(
    venue="betfred",
    page="market",
    url="https://www.betfred.com/racing/cheltenham-15-20",
    document_title="15:20 Cheltenham - Win Market",
    body_text=(
      "15:20 Cheltenham\n"
      "Win Market\n"
      "Desert Hero\n5.20\n"
      "Golden Ace\n7/2\n"
      "Bambino Fever\n9.00\n"
      "Place Terms\n"
    ),
    interactive_snapshot=[],
  )

  assert analysis["status"] == "ready"
  assert analysis["event_name"] == "15:20 Cheltenham"
  assert analysis["market_name"] == "Win"
  assert [quote["selection_name"] for quote in analysis["quotes"]] == [
    "Desert Hero",
    "Golden Ace",
    "Bambino Fever",
  ]
  assert analysis["quotes"][1]["odds"] == 4.5


def test_analyze_racing_market_page_extracts_exchange_lay_quotes() -> None:
  analysis = analyze_racing_market_page(
    venue="smarkets",
    page="market",
    url="https://smarkets.com/event/horse-racing/cheltenham-15-20",
    document_title="15:20 Cheltenham - To Win",
    body_text=(
      "15:20 Cheltenham\n"
      "To Win\n"
      "Desert Hero 5.10 5.30 £124\n"
      "Golden Ace 6.80 7.00 £88\n"
      "Bambino Fever Back 8.20 Sell 8.60 £45\n"
    ),
    interactive_snapshot=[],
  )

  assert analysis["status"] == "ready"
  assert analysis["event_name"] == "15:20 Cheltenham"
  assert [quote["selection_name"] for quote in analysis["quotes"]] == [
    "Desert Hero",
    "Golden Ace",
    "Bambino Fever",
  ]
  assert analysis["quotes"][0]["side"] == "lay"
  assert analysis["quotes"][0]["odds"] == 5.30
  assert analysis["quotes"][0]["liquidity"] == 124.0
  assert analysis["quotes"][2]["odds"] == 8.60


def test_analyze_racing_market_page_ignores_non_racing_pages() -> None:
  analysis = analyze_racing_market_page(
    venue="betfred",
    page="market",
    url="https://www.betfred.com/football/arsenal-everton",
    document_title="Arsenal v Everton",
    body_text="Arsenal\n2.10\nDraw\n3.40\nEverton\n4.10\n",
    interactive_snapshot=[],
  )

  assert analysis["status"] == "ignored"
  assert analysis["quotes"] == []
