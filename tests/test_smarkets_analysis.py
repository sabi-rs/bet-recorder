from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.smarkets_exchange import analyze_smarkets_page  # noqa: E402


def test_analyze_smarkets_open_positions_extracts_trade_out_rows() -> None:
  analysis = analyze_smarkets_page(
    page="open_positions",
    body_text=(
      "Available balance £120.45 Exposure £41.63 Unrealized P/L -£0.49 "
      "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
      "Back Both Teams To Score Bet Builder 1.74 £3.50 Open "
      "Lazio vs Sassuolo "
      "Sell 1 - 1 Correct score 7.2 £2.55 £15.81 £18.36 £2.46 -£0.09 (3.53%) Order filled Trade out "
      "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 (3.13%) Order filled Trade out"
    ),
    inputs={},
    visible_actions=["Trade out"],
  )

  assert analysis["page"] == "open_positions"
  assert analysis["position_count"] == 2
  assert analysis["positions"][0]["contract"] == "1 - 1"
  assert analysis["positions"][0]["market"] == "Correct score"
  assert analysis["positions"][0]["price"] == 7.2
  assert analysis["positions"][0]["stake"] == 2.55
  assert analysis["positions"][0]["liability"] == 15.81
  assert analysis["positions"][0]["current_value"] == 2.46
  assert analysis["positions"][0]["pnl_amount"] == -0.09
  assert analysis["positions"][1]["contract"] == "Draw"
  assert analysis["positions"][1]["market"] == "Full-time result"
  assert analysis["positions"][1]["can_trade_out"] is True
  assert analysis["account_stats"]["available_balance"] == 120.45
  assert analysis["account_stats"]["currency"] == "GBP"
  assert len(analysis["other_open_bets"]) == 2
  assert analysis["other_open_bets"][0]["label"] == "Arsenal"
  assert analysis["other_open_bets"][1]["market"] == "Bet Builder"
