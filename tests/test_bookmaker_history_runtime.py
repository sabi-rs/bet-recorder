from pathlib import Path
import json
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.analysis.bet365 import analyze_bet365_page  # noqa: E402
from bet_recorder.analysis.betuk import analyze_betuk_page  # noqa: E402
from bet_recorder.analysis.generic_sportsbooks import (  # noqa: E402
    analyze_bet600_page,
    analyze_bet10_page,
    analyze_betano_page,
    analyze_betfred_page,
    analyze_betmgm_page,
    analyze_boylesports_page,
    analyze_betvictor_page,
    analyze_coral_page,
    analyze_fanteam_page,
    analyze_kwik_page,
    analyze_ladbrokes_page,
    analyze_leovegas_page,
    analyze_midnite_page,
    analyze_paddypower_page,
    analyze_talksportbet_page,
)
from bet_recorder.bookmaker_history_runtime import extract_live_bookmaker_ledger_entries  # noqa: E402
from bet_recorder.tracked_bets_runtime import event_matches, market_matches  # noqa: E402


def fixture_text(name: str) -> str:
    return (
        Path(__file__).resolve().parent / "fixtures" / "bookmaker_history" / name
    ).read_text()


def fixture_json(name: str) -> dict:
    return json.loads(fixture_text(name))


def test_extract_live_bookmaker_ledger_entries_reads_settled_bet365_rows() -> None:
    body_text = fixture_text("bet365_history.txt")
    payload = {
        "page": "history",
        "url": "https://members.bet365.com/?bs=0&displaymode=desktop&handler=rdapi&mh=2&pid=8020&platform=1&prdid=1#/HICS/BSSB/",
        "body_text": body_text,
        "captured_at": "2026-03-22T17:27:58Z",
    }
    analysis = analyze_bet365_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Cash Out"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="bet365",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 2
    assert entries[0]["platform"] == "bet365"
    assert entries[0]["event"] == "Everton v Chelsea"
    assert entries[0]["selection"] == "Chelsea"
    assert entries[0]["funding_kind"] == "free_bet"
    assert entries[0]["realised_pnl_gbp"] == 0.0
    assert entries[1]["event"] == "Brighton v Liverpool"
    assert entries[1]["selection"] == "Liverpool"


def test_extract_live_bookmaker_ledger_entries_ignores_bet365_homepage_noise() -> None:
    body_text = (
        "Offers\n£10.00\nHome\nAll Sports\nIn-Play\nMy Bets\n"
        "£10 stake returns £36 ACCA BOOST\n"
        "Martin Landaluce\nKaren Khachanov\n1.28\nBoth Teams to Score 70 Points\n"
    )
    payload = {
        "page": "my_bets",
        "url": "https://www.bet365.com/#/HO/",
        "body_text": body_text,
        "captured_at": "2026-03-22T17:27:58Z",
    }
    analysis = analyze_bet365_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Offers"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="bet365",
        payload=payload,
        analysis=analysis,
    )

    assert entries == []


def test_extract_live_bookmaker_ledger_entries_reads_betano_settled_rows() -> None:
    body_text = fixture_text("betano_history.txt")
    payload = {
        "page": "history",
        "body_text": body_text,
        "captured_at": "2026-03-22T17:27:58Z",
    }
    analysis = analyze_betano_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Settled", "Bet History"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="betano",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 2
    assert entries[0]["platform"] == "betano"
    assert entries[0]["event"] == "Carlisle"
    assert entries[0]["market"] == "Outright, Race"
    assert entries[0]["selection"] == "Spadestep"
    assert entries[0]["realised_pnl_gbp"] == -10.0
    assert entries[0]["funding_kind"] == "cash"
    assert entries[1]["event"] == "Newbury"
    assert entries[1]["selection"] == "Escapeandevade"


def test_extract_live_bookmaker_ledger_entries_reads_betvictor_settled_rows() -> None:
    body_text = (
        "Bet History\nSports\nCasino\nOpen\nSettled\n"
        "Double\n@2.679\nStake\nPotential Returns\n£10.00\n£16.77"
    )
    payload = {
        "page": "history",
        "body_text": body_text,
        "captured_at": "2026-03-22T17:27:58Z",
        "history_api_responses": {
            "settled": {
                "url": "https://www.betvictor.com/bv_api/account_history/settled/1/bets",
                "status": 200,
                "ok": True,
                "body": fixture_json("betvictor_history.json"),
            },
        },
    }
    analysis = analyze_betvictor_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["Bet History", "Open", "Settled"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="betvictor",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 3
    assert entries[0]["platform"] == "betvictor"
    assert entries[0]["event"] == "Roma v Bologna"
    assert entries[0]["selection"] == "DRAW"
    assert entries[0]["market"] == "Match Betting - 90 Mins"
    assert entries[0]["funding_kind"] == "free_bet"
    assert entries[0]["realised_pnl_gbp"] == 8.0
    assert entries[1]["event"] == "Tottenham Hotspur v Atletico Madrid"
    assert entries[1]["selection"] == "TOTTENHAM HOTSPUR"
    assert entries[1]["funding_kind"] == "cash"
    assert entries[1]["occurred_at"] == "2026-03-18T21:54:16Z"
    assert entries[2]["market"] == "Multiple"
    assert entries[2]["selection"] == "Double"


def test_extract_live_bookmaker_ledger_entries_reads_talksportbet_settled_rows() -> None:
    payload = {
        "page": "history",
        "body_text": "",
        "captured_at": "2026-03-23T00:00:00Z",
        "history_api_responses": {
            "settled": {
                "url": "https://www.talksportbet.com/bv_api/account_history/settled/1/bets",
                "status": 200,
                "ok": True,
                "body": fixture_json("talksportbet_history.json"),
            },
        },
    }
    analysis = analyze_talksportbet_page(
        page="my_bets",
        body_text="",
        inputs={},
        visible_actions=["Bet History", "Open", "Settled"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="talksportbet",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) >= 5
    assert entries[0]["platform"] == "talksportbet"
    assert entries[0]["event"] == "Willie Mullins Double"
    assert entries[0]["market"] == "Odds Boost - Race"
    assert entries[0]["selection"] == "Majborough (16:00 Che) and Love Sign D'aunou (17:20 Che) both to win"
    assert entries[1]["funding_kind"] == "free_bet"
    assert entries[1]["realised_pnl_gbp"] == 0.0


def test_extract_live_bookmaker_ledger_entries_reads_betmgm_history_rows() -> None:
    body_text = fixture_text("betmgm_history.txt")
    payload = {
        "page": "history",
        "url": "https://www.betmgm.co.uk/sports#bethistory",
        "body_text": body_text,
        "captured_at": "2026-03-23T01:10:00Z",
    }
    analysis = analyze_betmgm_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Settled"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="betmgm",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) >= 6
    assert entries[0]["platform"] == "betmgm"
    assert entries[0]["event"] == "15:20 Cheltenham Friday"
    assert entries[0]["selection"] == "Doctor Steinberg"
    assert entries[0]["funding_kind"] == "free_bet"
    assert entries[0]["realised_pnl_gbp"] == 0.0
    assert any(entry["selection"] == "Supremely West" and entry["funding_kind"] == "free_bet" for entry in entries)
    assert any(entry["event"] == "Tottenham - Crystal Palace" and entry["status"] == "void" for entry in entries)


def test_extract_live_bookmaker_ledger_entries_reads_betfair_account_statement_rows() -> None:
    body_text = fixture_text("betfair_account_statement.csv")
    payload = {
        "page": "history",
        "url": "https://www.betfair.com/exchange/plus/",
        "body_text": body_text,
        "captured_at": "2026-03-23T01:15:00Z",
    }

    entries = extract_live_bookmaker_ledger_entries(
        venue="betfair",
        payload=payload,
        analysis={},
    )

    assert len(entries) >= 3
    assert entries[0]["platform"] == "betfair"
    assert entries[0]["source_kind"] == "betfair_account_statement"
    assert any(
        entry["event"] == "Wolves v Liverpool"
        and entry["market"] == "Match Odds"
        and entry["platform_kind"] == "exchange"
        for entry in entries
    )


def test_extract_live_bookmaker_ledger_entries_reads_matchbook_transaction_rows() -> None:
    body_text = fixture_text("matchbook_transactions.csv")
    payload = {
        "page": "history",
        "url": "https://www.matchbook.com/login",
        "body_text": body_text,
        "captured_at": "2026-03-23T01:16:00Z",
    }

    entries = extract_live_bookmaker_ledger_entries(
        venue="matchbook",
        payload=payload,
        analysis={},
    )

    assert len(entries) >= 1
    assert entries[0]["platform"] == "matchbook"
    assert entries[0]["source_kind"] == "matchbook_transactions"
    assert entries[0]["event"] == "Columbus Crew vs Chicago Fire"
    assert entries[0]["market"] == "Match Odds"


def test_extract_live_bookmaker_ledger_entries_reads_bet10_export_rows() -> None:
    body_text = fixture_text("bet10_betting_history.txt")
    payload = {
        "page": "history",
        "url": "https://www.10bet.co.uk/betting-history",
        "body_text": body_text,
        "captured_at": "2026-03-23T01:17:00Z",
    }
    analysis = analyze_bet10_page(
        page="history",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "History", "Settled"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="bet10",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 2
    assert entries[0]["platform"] == "bet10"
    assert entries[0]["source_kind"] == "bet10_betting_history"
    assert entries[1]["selection"] == "rosemary's rose"
    assert entries[1]["realised_pnl_gbp"] == 2.75


def test_extract_live_bookmaker_ledger_entries_reads_leovegas_history_rows() -> None:
    body_text = fixture_text("leovegas_history.txt")
    payload = {
        "page": "history",
        "url": "https://www.leovegas.co.uk/betting#bethistory/2026-03-23",
        "body_text": body_text,
        "captured_at": "2026-03-23T01:12:00Z",
    }
    analysis = analyze_leovegas_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Settled"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="leovegas",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 1
    assert entries[0]["platform"] == "leovegas"
    assert entries[0]["event"] == "Crystal Palace - Leeds United"
    assert entries[0]["selection"] == "Leeds United"
    assert entries[0]["market"] == "Full Time"
    assert entries[0]["funding_kind"] == "bonus"
    assert entries[0]["realised_pnl_gbp"] == -10.0


def test_extract_live_bookmaker_ledger_entries_marks_betano_free_bet_rows() -> None:
    body_text = fixture_text("betano_history.txt").replace("£10.00\nTotal Returns", "£10.00 Free Bet\nTotal Returns", 1)
    payload = {
        "page": "history",
        "body_text": body_text,
        "captured_at": "2026-03-22T17:27:58Z",
    }
    analysis = analyze_betano_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Settled", "Bet History"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="betano",
        payload=payload,
        analysis=analysis,
    )

    assert entries[0]["funding_kind"] == "free_bet"
    assert entries[0]["realised_pnl_gbp"] == 0.0


def test_extract_live_bookmaker_ledger_entries_prefers_betano_history_api_rows() -> None:
    body_text = "Bet History\nOpen\nSettled\nLost\nSpadestep"
    payload = {
        "page": "history",
        "body_text": body_text,
        "captured_at": "2026-03-22T17:27:58Z",
        "history_api_responses": {
            "settled": {
                "url": "https://www.betano.co.uk/bv_api/account_history/settled/1/bets",
                "status": 200,
                "ok": True,
                "body": {
                    "groups": [
                        {
                            "date": "2026-03-01",
                            "bets": [
                                {
                                    "createdDate": "2026-03-22T11:42:26Z",
                                    "settledDate": "2026-03-22T15:23:12Z",
                                    "betType": "Spadestep",
                                    "stake": 10.0,
                                    "returns": 0.0,
                                    "bonusFunds": True,
                                    "odds": "3.75",
                                    "result": "lost",
                                    "description": "Bonus Funds: GBP 10.00 Single: 3 SPADESTEP @ 11/4 [Outright - Race] - 15:17 - Carlisle - HORSE RACING",
                                    "summary": {"name": "Spadestep"},
                                }
                            ],
                        }
                    ],
                    "load_more": False,
                },
            },
        },
    }
    analysis = analyze_betano_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Settled", "Bet History"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="betano",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 1
    assert entries[0]["event"] == "Carlisle"
    assert entries[0]["market"] == "Outright, Race"
    assert entries[0]["selection"] == "Spadestep"
    assert entries[0]["funding_kind"] == "free_bet"
    assert entries[0]["realised_pnl_gbp"] == 0.0


def test_extract_live_bookmaker_ledger_entries_handles_empty_fanteam_history_api() -> None:
    body_text = "GAMES LIVE RESULTS SPORTSBOOK"
    payload = {
        "page": "history",
        "url": "https://www.fanteam.com/sportsbook#/overview",
        "body_text": body_text,
        "captured_at": "2026-03-23T14:30:00Z",
        "history_api_responses": json.loads(fixture_text("fanteam_history.json")),
    }
    analysis = analyze_fanteam_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["Deposit"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="fanteam",
        payload=payload,
        analysis=analysis,
    )

    assert entries == []


def test_extract_live_bookmaker_ledger_entries_trims_betuk_footer_noise() -> None:
    body_text = (
        "My Bets\nRewards (18)\nAll\nOpen\nCash Out\nSettled\nFilter your bets by date\n"
        "Single\n@\n3.30\nLost\n18 Mar 2026 • 06:22:14\nCoupon ID: 12385048588\n"
        "Full Time: Tokyo Verdy\nTokyo Verdy - Kawasaki Frontale\nStake:\n£10.00\n"
        "Settings\nONLINE SPORTSBOOK MARKETS AT BETUK\n"
    )
    payload = {
        "page": "history",
        "url": "https://www.betuk.com/betting#bethistory",
        "body_text": body_text,
        "captured_at": "2026-03-22T17:27:58Z",
    }
    analysis = analyze_betuk_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Settled"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="betuk",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 1
    assert entries[0]["event"] == "Tokyo Verdy - Kawasaki Frontale"
    assert entries[0]["payout_gbp"] == 0.0
    assert entries[0]["realised_pnl_gbp"] == -10.0
    assert "ONLINE SPORTSBOOK MARKETS" not in entries[0]["description"]


@pytest.mark.parametrize(
    ("venue", "fixture_name", "analyzer", "expected_event", "expected_selection", "expected_pnl", "expected_funding"),
    [
        (
            "betfred",
            "betfred_history.txt",
            analyze_betfred_page,
            "Arsenal v Everton",
            "Draw",
            12.0,
            "cash",
        ),
        (
            "coral",
            "coral_history.txt",
            analyze_coral_page,
            "Cheltenham 15:20",
            "Desert Hero",
            20.0,
            "cash",
        ),
        (
            "ladbrokes",
            "ladbrokes_history.txt",
            analyze_ladbrokes_page,
            "Arsenal v Everton",
            "Arsenal",
            0.0,
            "free_bet",
        ),
        (
            "kwik",
            "kwik_history.txt",
            analyze_kwik_page,
            "England v France",
            "France",
            -5.0,
            "cash",
        ),
        (
            "bet600",
            "bet600_history.txt",
            analyze_bet600_page,
            "Barcelona v Real Madrid",
            "Both Teams To Score",
            7.12,
            "cash",
        ),
    ],
)
def test_extract_live_bookmaker_ledger_entries_reads_supported_generic_vendor_history(
    venue: str,
    fixture_name: str,
    analyzer,
    expected_event: str,
    expected_selection: str,
    expected_pnl: float,
    expected_funding: str,
) -> None:
    body_text = fixture_text(fixture_name)
    payload = {
        "page": "history",
        "body_text": body_text,
        "captured_at": "2026-03-22T17:27:58Z",
    }
    analysis = analyzer(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Cash Out"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue=venue,
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 1
    assert entries[0]["platform"] == venue
    assert entries[0]["event"] == expected_event
    assert entries[0]["selection"] == expected_selection
    assert entries[0]["realised_pnl_gbp"] == expected_pnl
    assert entries[0]["funding_kind"] == expected_funding


def test_extract_live_bookmaker_ledger_entries_reads_paddypower_history_rows() -> None:
    body_text = fixture_text("paddypower_history.txt")
    payload = {
        "page": "history",
        "body_text": body_text,
        "captured_at": "2026-03-23T00:00:00Z",
    }
    analysis = analyze_paddypower_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Settled", "Transaction History"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="paddypower",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 4
    assert entries[1]["event"] == "Braga v Nottm Forest"
    assert entries[1]["selection"] == "Draw"
    assert entries[1]["market"] == "Match Odds"
    assert entries[1]["funding_kind"] == "free_bet"
    assert entries[2]["event"] == "US Cremonese v Verona"
    assert entries[2]["realised_pnl_gbp"] == 10.0


def test_extract_live_bookmaker_ledger_entries_reads_boylesports_history_rows() -> None:
    body_text = fixture_text("boylesports_history.txt")
    payload = {
        "page": "history",
        "body_text": body_text,
        "body_html": fixture_text("boylesports_history.html"),
        "captured_at": "2026-03-23T00:00:00Z",
    }
    analysis = analyze_boylesports_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["Bet History", "Settled Bets"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="boylesports",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 2
    assert entries[0]["event"] == "Newcastle Jets v Wellington Phoenix"
    assert entries[0]["selection"] == "Draw"
    assert entries[0]["market"] == "Match Betting"
    assert entries[0]["funding_kind"] == "free_bet"
    assert entries[1]["event"] == "Tottenham v Borussia Dortmund"
    assert entries[1]["selection"] == "Borussia Dortmund"
    assert entries[1]["realised_pnl_gbp"] == -10.0


def test_extract_live_bookmaker_ledger_entries_reads_midnite_history_rows() -> None:
    body_text = fixture_text("midnite_history.txt")
    payload = {
        "page": "history",
        "body_text": body_text,
        "captured_at": "2026-03-23T00:00:00Z",
    }
    analysis = analyze_midnite_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["My Bets", "Open", "Settled"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="midnite",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) >= 5
    assert entries[0]["event"] == "Ipswich Town v Hull City"
    assert entries[0]["selection"] == "£5.00 Bet Builder"
    assert entries[0]["funding_kind"] == "free_bet"
    assert entries[0]["realised_pnl_gbp"] == 27.5
    assert entries[1]["market"] == "Multiple"
    assert entries[2]["status"] == "void"
    assert any(entry["status"] == "cash_out" for entry in entries)


def test_extract_live_bookmaker_ledger_entries_reads_betuk_settled_rows() -> None:
    body_text = fixture_text("betuk_history.txt")
    payload = {
        "page": "history",
        "body_text": body_text,
        "captured_at": "2026-03-22T17:27:58Z",
    }
    analysis = analyze_betuk_page(
        page="my_bets",
        body_text=body_text,
        inputs={},
        visible_actions=["MY BETS", "History", "Settled"],
    )

    entries = extract_live_bookmaker_ledger_entries(
        venue="betuk",
        payload=payload,
        analysis=analysis,
    )

    assert len(entries) == 2
    assert entries[0]["platform"] == "betuk"
    assert entries[0]["event"] == "RB Leipzig - TSG Hoffenheim"
    assert entries[0]["selection"] == "No"
    assert entries[0]["market"] == "Both Teams To Score"
    assert entries[0]["realised_pnl_gbp"] == 21.0
    assert entries[1]["event"] == "Tokyo Verdy - Kawasaki Frontale"
    assert entries[1]["selection"] == "Tokyo Verdy"


def test_event_matches_handles_bookmaker_dash_and_team_prefix_variants() -> None:
    assert event_matches(
        "Tokyo Verdy - Kawasaki Frontale",
        "Tokyo Verdy vs Kawasaki Frontale",
    )
    assert event_matches(
        "RB Leipzig - TSG Hoffenheim",
        "RB Leipzig vs Hoffenheim",
    )


def test_market_matches_handles_full_time_aliases() -> None:
    assert market_matches("Full Time", "Full-time result")
