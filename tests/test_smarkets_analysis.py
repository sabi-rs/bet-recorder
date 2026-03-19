from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.smarkets_exchange import (  # noqa: E402
    analyze_smarkets_page,
    parse_event_page_body_summary,
    parse_event_page_summary,
)


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
    assert analysis["account_stats"]["cumulative_pnl"] is None
    assert analysis["account_stats"]["currency"] == "GBP"
    assert len(analysis["other_open_bets"]) == 2
    assert analysis["other_open_bets"][0]["label"] == "Arsenal"
    assert analysis["other_open_bets"][1]["market"] == "Bet Builder"


def test_analyze_smarkets_open_positions_extracts_portfolio_cumulative_pnl() -> None:
    analysis = analyze_smarkets_page(
        page="open_positions",
        body_text=(
            "Portfolio £0.00 Your balance £35.40 Exposure Deposit Withdraw £43.80 "
            "Profit & Loss £20.40 Potential Winnings "
            "P&L since Jan 2026 £253.69 Last year: £0.00 Wed "
            "Tottenham vs Atlético Madrid 1 In 44 Minutes|UEFA Champions League "
            "Sell Tottenham Full-time result 2.46 £10.00 £14.60 £24.60 £10.08 +£0.08 (0.8%) Order filled Trade out"
        ),
        inputs={},
        visible_actions=["Trade out"],
    )

    assert analysis["account_stats"] == {
        "available_balance": 35.40,
        "exposure": 43.80,
        "unrealized_pnl": 20.40,
        "cumulative_pnl": 253.69,
        "cumulative_pnl_label": "P&L since Jan 2026",
        "currency": "GBP",
    }


def test_analyze_smarkets_open_positions_extracts_live_trade_out_back_odds_when_present() -> (
    None
):
    analysis = analyze_smarkets_page(
        page="open_positions",
        body_text=(
            "Sell 1 - 1 Correct score 7.2 £2.55 £15.81 £18.36 £2.46 -£0.09 (3.53%) "
            "Order filled Trade out Back 10.87 "
            "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£0.31 (3.13%) "
            "Order filled Trade out Back 2.80"
        ),
        inputs={},
        visible_actions=["Trade out"],
    )

    assert analysis["positions"][0]["current_back_odds"] == 10.87
    assert analysis["positions"][1]["current_back_odds"] == 2.80


def test_analyze_smarkets_open_positions_includes_to_win_positions() -> None:
    analysis = analyze_smarkets_page(
        page="open_positions",
        body_text=(
            "13:20 - Cheltenham "
            "Sell Bambino Fever To win 2.26 £2.00 £2.52 £4.52 £1.96 -£0.04 (2%) Order filled Trade out "
            "Lille OSC vs Aston Villa "
            "Sell Aston Villa Full-time result 2.48 £4.50 £6.66 £11.16 £4.43 -£0.07 (1.56%) Order filled Trade out"
        ),
        inputs={},
        visible_actions=["Trade out"],
    )

    assert analysis["position_count"] == 2
    assert analysis["positions"][0]["contract"] == "Bambino Fever"
    assert analysis["positions"][0]["market"] == "To win"
    assert analysis["positions"][1]["contract"] == "Aston Villa"


def test_analyze_smarkets_open_positions_enriches_event_url_score_and_implied_probability() -> (
    None
):
    analysis = analyze_smarkets_page(
        page="open_positions",
        body_text=(
            "West Ham vs Man City "
            "27'|Premier League "
            "Sell Man City Full-time result 2.40 £10.00 £14.00 £24.00 £8.40 -£1.60 (16%) "
            "Order filled Trade out Back 1.91"
        ),
        inputs={},
        visible_actions=["Trade out"],
        links=[
            "https://smarkets.com/football/england-premier-league/2026/03/14/20-00/west-ham-vs-manchester-city/44919693/",
        ],
        event_summaries=[
            {
                "url": "https://smarkets.com/football/england-premier-league/2026/03/14/20-00/west-ham-vs-manchester-city/44919693/",
                "current_score": "0-0",
                "current_score_home": 0,
                "current_score_away": 0,
                "live_clock": "27'",
                "quotes": [
                    {
                        "market": "Full-time result",
                        "contract": "Man City",
                        "buy_implied_probability": 0.81,
                        "sell_implied_probability": 0.80,
                        "buy_odds": 1 / 0.81,
                        "sell_odds": 1 / 0.80,
                    }
                ],
            }
        ],
    )

    position = analysis["positions"][0]
    assert (
        position["event_url"]
        == "https://smarkets.com/football/england-premier-league/2026/03/14/20-00/west-ham-vs-manchester-city/44919693/"
    )
    assert position["current_score"] == "0-0"
    assert position["current_score_home"] == 0
    assert position["current_score_away"] == 0
    assert position["live_clock"] == "27'"
    assert round(position["current_buy_implied_probability"], 4) == 0.81
    assert round(position["current_sell_implied_probability"], 4) == 0.80
    assert round(position["current_buy_odds"], 4) == round(1 / 0.81, 4)
    assert round(position["current_sell_odds"], 4) == round(1 / 0.80, 4)
    assert round(position["current_implied_probability"], 6) == round(1 / 1.91, 6)
    assert round(position["current_implied_percentage"], 4) == round(
        100 / 1.91, 4
    )


def test_analyze_smarkets_open_positions_extracts_scheduled_event_context_from_live_portfolio() -> (
    None
):
    analysis = analyze_smarkets_page(
        page="open_positions",
        body_text=(
            "Tottenham vs Atlético Madrid\n"
            "1\n"
            "In 44 Minutes|UEFA Champions League\n"
            "-£14.60\n"
            "Worst Outcome\n"
            "£10.00\n"
            "Best Outcome\n"
            "Contract\n"
            "Price\n"
            "Stake/\n"
            "Liability\n"
            "Return\n"
            "Current Value\n"
            "Status\n"
            "Sell Tottenham\n"
            "Full-time result\n"
            "2.46\n"
            "£10.00\n"
            "£14.60\n"
            "£24.60\n"
            "£10.08\n"
            "+£0.08 (0.8%)\n"
            "Order filled\n"
            "Trade out\n"
            "Brumbies vs Chiefs\n"
            "1\n"
            "20 Mar 8:35 AM|Super Rugby\n"
            "-£20.80\n"
            "Worst Outcome\n"
            "£10.40\n"
            "Best Outcome\n"
            "Contract\n"
            "Price\n"
            "Stake/\n"
            "Liability\n"
            "Return\n"
            "Current Value\n"
            "Status\n"
            "Sell Brumbies\n"
            "Winner (including overtime)\n"
            "3.00\n"
            "£10.40\n"
            "£20.80\n"
            "£31.20\n"
            "£7.23\n"
            "-£3.17 (30.48%)\n"
            "Order filled\n"
            "Trade out\n"
        ),
        inputs={},
        visible_actions=["Trade out"],
        links=[
            "https://smarkets.com/football/uefa-champions-league/2026/03/18/20-00/tottenham-hotspur-vs-atletico-de-madrid/44941563/",
            "https://smarkets.com/rugby/super-rugby/2026/03/20/08-35/brumbies-vs-chiefs/44907713/",
        ],
    )

    assert analysis["positions"][0]["event"] == "Tottenham vs Atlético Madrid"
    assert (
        analysis["positions"][0]["event_status"]
        == "In 44 Minutes|UEFA Champions League"
    )
    assert (
        analysis["positions"][0]["event_url"]
        == "https://smarkets.com/football/uefa-champions-league/2026/03/18/20-00/tottenham-hotspur-vs-atletico-de-madrid/44941563/"
    )
    assert analysis["positions"][1]["event"] == "Brumbies vs Chiefs"
    assert analysis["positions"][1]["event_status"] == "20 Mar 8:35 AM|Super Rugby"
    assert (
        analysis["positions"][1]["event_url"]
        == "https://smarkets.com/rugby/super-rugby/2026/03/20/08-35/brumbies-vs-chiefs/44907713/"
    )


def test_parse_event_page_body_summary_extracts_score_clock_and_quotes() -> None:
    summary = parse_event_page_body_summary(
        url="https://smarkets.com/football/event/44919693/",
        body_text=(
            "Home\nFootball\nUK\nPremier League\n"
            "West Ham vs Man City\n"
            "West Ham\nMan City\n0\n1\n33'\nLondon Stadium\n"
            "Graph\nOrder Book\nTRADED: £100,720\n"
            "BUY\nSELL\n"
            "West Ham\n6%\n£199\n5%\n£28,583\n"
            "Draw\n16%\n£481\n14%\n£6,558\n"
            "Man City\n81%\n£5,416\n80%\n£646\n"
            "Popular\nWinner\nTeams\nTotals\nPlayers\nGoals\nHandicap\nHalf\nCards\nCorners\nOther\n"
            "Correct Score\nOrder Book\nTRADED: £222,193\n"
            "BUY\nSELL\n"
            "0 - 1\n13%\n£269\n12%\n£4,133\n"
            "1 - 1\n10%\n£184\n8%\n£6,827\n"
        ),
        positions=[
            {"event": "West Ham vs Man City", "market": "Full-time result", "contract": "Man City"},
            {"event": "West Ham vs Man City", "market": "Correct score", "contract": "1 - 1"},
        ],
    )

    assert summary == {
        "url": "https://smarkets.com/football/event/44919693/",
        "current_score": "0-1",
        "current_score_home": 0,
        "current_score_away": 1,
        "live_clock": "33'",
        "quotes": [
            {
                "market": "Full-time result",
                "contract": "Man City",
                "buy_implied_probability": 0.81,
                "sell_implied_probability": 0.80,
                "buy_odds": 1 / 0.81,
                "sell_odds": 1 / 0.80,
            },
            {
                "market": "Correct score",
                "contract": "1 - 1",
                "buy_implied_probability": 0.10,
                "sell_implied_probability": 0.08,
                "buy_odds": 10.0,
                "sell_odds": 12.5,
            },
        ],
    }


def test_parse_event_page_summary_extracts_current_score() -> None:
    summary = parse_event_page_summary(
        url="https://smarkets.com/football/event/44919693/",
        html=(
            '<html><body>{"scores":{"current":[2,1],"periods":[]},'
            '"match_period":"second_half"}</body></html>'
        ),
    )

    assert summary == {
        "url": "https://smarkets.com/football/event/44919693/",
        "current_score": "2-1",
        "current_score_home": 2,
        "current_score_away": 1,
        "live_clock": "",
        "quotes": [],
    }
