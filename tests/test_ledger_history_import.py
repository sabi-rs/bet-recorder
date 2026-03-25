from pathlib import Path
import json
import sys

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.cli import app  # noqa: E402
from bet_recorder.ledger import history_import  # noqa: E402
from bet_recorder.ledger.history_import import (  # noqa: E402
    Bet10Receipt,
    import_statement_history,
    parse_betfair_account_statement_csv,
    parse_matchbook_transactions_csv,
)


def history_fixture_path(name: str) -> Path:
    return Path(__file__).resolve().parent / "fixtures" / "bookmaker_history" / name


def test_import_statement_history_normalizes_bank_betfair_and_matchbook_rows(
    tmp_path: Path,
) -> None:
    (tmp_path / "DAWETE19254912-20260312.csv").write_text(
        "\n".join(
            [
                "Date,Type,Description,Value,Balance,Account Name,Account Number",
                "11 Mar 2026,BAC,\"SMARKETS , TRUELAYER-WITHDRAW, FP 11/03/26 1231 , REF001\",10.00,79.08,DAWE TE,831705-19254912",
                "10 Mar 2026,POS,\"1322 09MAR26 , MATCHBOOK , LONDON GB\",-10.00,52.07,DAWE TE,831705-19254912",
            ]
        )
        + "\n"
    )
    (tmp_path / "AccountStatement_.csv").write_text(
        "\n".join(
            [
                "Date,Description,Cash In (£),Bonus In (£),Cash Out (£),Bonus Out (£),Cash Balance (£)",
                "06-Mar-26 21:54:12,Exchange: Wolves v Liverpool / Match Odds Ref: 177283405352,2.24,--,--,--,59.68",
                "06-Mar-26 21:56:34,Withdrawal Ref: 177283419353,--,--,-59.68,--,0.00",
                "02-Mar-26 14:24:35,Sportsbook: Bet Placed (Transaction ID: S/27450139/02638919640) Ref: 177246147536,--,--,-5.00,--,14.46",
            ]
        )
        + "\n"
    )
    matchbook_dir = tmp_path / "matchbook"
    matchbook_dir.mkdir()
    (matchbook_dir / "matchbook-transactions.csv").write_text(
        "\n".join(
            [
                "currency,id,time,transaction-type,product,detail,debit,credit,balance,third-party-transaction-id",
                "GBP,32706738456501028,2026-03-09T02:44:16.003Z,Manual,Account,Withdrawal via card,-13.55000,0.00000,0.00203,",
                "GBP,32698044763900018,2026-03-08T02:35:19.069Z,Payout,Exchange,Columbus Crew vs Chicago Fire | Match Odds,-21.44797,0.00000,3.55203,",
            ]
        )
        + "\n"
    )
    # duplicate export in root should be deduped by entry id
    (tmp_path / "transactions.csv").write_text(
        (matchbook_dir / "matchbook-transactions.csv").read_text()
    )
    (tmp_path / "smarkets_account_overview.csv").write_text(
        "\n".join(
            [
                "\"Smarkets Account Overview - 15 Mar 2026, 00:42 GMT\"",
                "\"Event\",\"Details\",\"Date\",\"Backers Stake (GBP)\",\"Odds\",\"Exposure (GBP)\",\"In/Out (GBP)\",\"Balance (GBP)\"",
                "\"Deposit\",\"\",\"14 Mar 2026, 15:48 GMT\",\"\",\"\",\"0.00\",\"43.00\",\"43.00\"",
                "\"Bet Placed · Against Draw\",\"Chelsea vs Newcastle United / Full-time result\",\"14 Mar 2026, 16:00 GMT\",\"7.14\",\"4.3\",\"-23.56\",\"\",\"43.00\"",
                "\"Bet Won · Against Draw\",\"Chelsea vs Newcastle United / Full-time result\",\"14 Mar 2026, 19:28 GMT\",\"7.14\",\"4.3\",\"-12.99\",\"\",\"74.00\"",
                "\"Market Settled\",\"Chelsea vs Newcastle United / Full-time result\",\"14 Mar 2026, 19:28 GMT\",\"\",\"\",\"-12.99\",\"7.14\",\"81.13\"",
                "\"Withdraw\",\"\",\"14 Mar 2026, 21:56 GMT\",\"\",\"\",\"0.00\",\"-103.21\",\"0.00\"",
            ]
        )
        + "\n"
    )
    (tmp_path / "rbs_incremental_2026-03-12_2026-03-14.txt").write_text(
        "\n\n".join(
            [
                "\n".join(
                    [
                        "14 Mar 2026",
                        "Online",
                        "SMARKETS",
                        "-£20.00",
                        "-",
                    ]
                ),
                "\n".join(
                    [
                        "13 Mar 2026",
                        "Automated credit",
                        "KWIFF",
                        "+£15.40",
                        "£143.96",
                    ]
                ),
            ]
        )
        + "\n"
    )
    (tmp_path / "manual_pending_withdrawals.csv").write_text(
        "\n".join(
            [
                "Date,Platform,Amount,Currency,Description,Reference",
                "2026-03-15,manual,200.00,GBP,Manual pending withdrawals total,pending:2026-03-15",
            ]
        )
        + "\n"
    )

    payload = import_statement_history(tmp_path)

    assert payload["summary"]["source_count"] == 7
    assert payload["summary"]["entry_count"] == 15
    assert payload["summary"]["account_activity_count"] == 9

    bank_entry = next(
        entry
        for entry in payload["ledger_entries"]
        if entry["source_kind"] == "bank_statement" and entry["platform"] == "smarkets"
    )
    assert bank_entry["activity_type"] == "withdrawal"
    assert bank_entry["amount_gbp"] == 10.0

    betfair_exchange = next(
        entry
        for entry in payload["ledger_entries"]
        if entry["source_kind"] == "betfair_account_statement"
        and entry["activity_type"] == "exchange_settlement"
    )
    assert betfair_exchange["event"] == "Wolves v Liverpool"
    assert betfair_exchange["market"] == "Match Odds"
    assert betfair_exchange["platform_kind"] == "exchange"
    assert betfair_exchange["exchange"] == "betfair_exchange"

    matchbook_entry = next(
        entry
        for entry in payload["ledger_entries"]
        if entry["entry_id"] == "matchbook:32698044763900018"
    )
    assert matchbook_entry["event"] == "Columbus Crew vs Chicago Fire"
    assert matchbook_entry["market"] == "Match Odds"
    assert matchbook_entry["activity_type"] == "bet_settled"

    smarkets_entry = next(
        entry
        for entry in payload["ledger_entries"]
        if entry["source_kind"] == "smarkets_account_overview"
        and entry["activity_type"] == "bet_placed"
    )
    assert smarkets_entry["exchange"] == "smarkets"
    assert smarkets_entry["selection"] == "Draw"
    assert smarkets_entry["side"] == "lay"
    assert smarkets_entry["stake_gbp"] == 7.14
    assert smarkets_entry["odds_decimal"] == 4.3
    assert smarkets_entry["exposure_gbp"] == -23.56
    assert smarkets_entry["market"] == "Full-time result"

    rbs_entry = next(
        entry
        for entry in payload["ledger_entries"]
        if entry["source_kind"] == "rbs_incremental_text"
        and entry["platform"] == "smarkets"
    )
    assert rbs_entry["activity_type"] == "deposit"
    assert rbs_entry["amount_gbp"] == -20.0

    pending_entry = next(
        entry
        for entry in payload["ledger_entries"]
        if entry["source_kind"] == "manual_pending_withdrawals"
    )
    assert pending_entry["activity_type"] == "pending_withdrawal"
    assert pending_entry["status"] == "pending"
    assert pending_entry["amount_gbp"] == 200.0


def test_parse_betfair_account_statement_fixture_reads_exchange_and_sportsbook_rows() -> None:
    entries, account_activities = parse_betfair_account_statement_csv(
        history_fixture_path("betfair_account_statement.csv")
    )

    assert len(entries) >= 5
    exchange_entry = next(
        entry for entry in entries if entry["activity_type"] == "exchange_settlement"
    )
    assert exchange_entry["platform"] == "betfair"
    assert exchange_entry["platform_kind"] == "exchange"
    assert exchange_entry["exchange"] == "betfair_exchange"
    assert exchange_entry["event"] == "Wolves v Liverpool"
    assert exchange_entry["market"] == "Match Odds"

    sportsbook_entry = next(
        entry
        for entry in entries
        if entry["activity_type"] == "bet_placed"
        and entry["platform_kind"] == "sportsbook"
    )
    assert sportsbook_entry["platform"] == "betfair"
    assert sportsbook_entry["source_kind"] == "betfair_account_statement"
    assert len(account_activities) >= 2


def test_parse_matchbook_transactions_fixture_reads_exchange_and_account_rows() -> None:
    entries, account_activities = parse_matchbook_transactions_csv(
        history_fixture_path("matchbook_transactions.csv")
    )

    assert len(entries) >= 3
    exchange_entry = next(
        entry for entry in entries if entry["activity_type"] == "bet_settled"
    )
    assert exchange_entry["platform"] == "matchbook"
    assert exchange_entry["platform_kind"] == "exchange"
    assert exchange_entry["exchange"] == "matchbook"
    assert exchange_entry["event"] == "Columbus Crew vs Chicago Fire"
    assert exchange_entry["market"] == "Match Odds"

    account_entry = next(
        entry for entry in entries if entry["activity_type"] == "deposit"
    )
    assert account_entry["platform"] == "matchbook"
    assert account_entry["source_kind"] == "matchbook_transactions"
    assert len(account_activities) >= 2


def test_import_ledger_history_command_writes_json_payload(tmp_path: Path) -> None:
    (tmp_path / "DAWETE19254912-20260312.csv").write_text(
        "\n".join(
            [
                "Date,Type,Description,Value,Balance,Account Name,Account Number",
                "11 Mar 2026,BAC,\"SMARKETS , TRUELAYER-WITHDRAW, FP 11/03/26 1231 , REF001\",10.00,79.08,DAWE TE,831705-19254912",
            ]
        )
        + "\n"
    )
    output_path = tmp_path / "out" / "ledger-history.json"

    result = CliRunner().invoke(
        app,
        [
            "import-ledger-history",
            "--source-dir",
            str(tmp_path),
            "--output-path",
            str(output_path),
        ],
    )

    assert result.exit_code == 0
    assert output_path.exists()
    payload = json.loads(output_path.read_text())
    assert payload["summary"]["entry_count"] == 1
    assert payload["ledger_entries"][0]["platform"] == "smarkets"


def test_import_statement_history_includes_bet10_betting_and_payment_history(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bet10_dir = tmp_path / "bet10"
    bet10_dir.mkdir()
    (bet10_dir / "betting history.txt").write_text(
        "\n".join(
            [
                "Single",
                "03/03/2026, 14:08",
                "Lost",
                "reece james (chelsea)",
                "4.50",
                "Total Stake",
                "£2.00",
                "Returns",
                "£0.00",
                "Single",
                "02/03/2026, 14:17",
                "Won",
                "rosemary's rose",
                "3.75",
                "Total Stake",
                "£1.00",
                "Returns",
                "£3.75",
            ]
        )
        + "\n"
    )
    payment_history = "\n".join(
        [
            "Account summary,Total wins,Total bets,Total deposits,Total withdrawals,Beginning balance,Ending balance",
            "\"09-12-2025 - 09-03-2026\",\"£ 7.45\",\"£ 25.80\",\"£ 50.00\",\"£ 31.65\",\"£ 0.00\",\"£ 0.00\"",
            "",
            "Player:",
            "\"6BR8844492\"",
            "",
            "Date and time,Payment type,Amount,Status,Method,Account,Payment ID,Product,Fee",
            "\"04-03-2026 23:52\",\"Withdrawal\",\"£ 31.65\",\"Approved\",\"Trustly\",\"5657311158\",\"6090615942\",\"sportsbook\",\"\"",
            "\"02-03-2026 01:25\",\"Deposit\",\"£ 50.00\",\"Approved\",\"Trustly\",\"5657311158\",\"6082665972\",\"sportsbook\",\"\"",
        ]
    )
    (bet10_dir / "payment_history_2025-12-09_2026-03-09.csv").write_text(
        payment_history + "\n"
    )
    (tmp_path / "payment_history_2025-12-02_2026-03-02.csv").write_text(
        payment_history + "\n"
    )

    monkeypatch.setattr(
        history_import,
        "collect_bet10_receipts",
        lambda _source_dir: (
            [
                Bet10Receipt(
                    receipt_id="45081069",
                    placed_at="2026-03-03T14:08:30",
                    selection="Reece James (Chelsea)",
                    market="Player To Receive A Card",
                    event="Aston Villa v Chelsea",
                    bet_type="single",
                    odds_decimal=4.5,
                    stake_gbp=2.0,
                    source_file="45081069.jpeg",
                    source_kind="bet10_receipt_image",
                ),
                Bet10Receipt(
                    receipt_id="45042793",
                    selection="Rosemary's Rose",
                    market="Win or Each Way (Day of Race)",
                    event="2 Mar, 17:40 Kempton",
                    bet_type="single",
                    odds_decimal=3.75,
                    stake_gbp=1.0,
                    source_file="45042793.jpeg",
                    source_kind="bet10_receipt_image",
                ),
            ],
            [],
        ),
    )

    payload = import_statement_history(tmp_path)

    bet10_bets = [
        entry
        for entry in payload["ledger_entries"]
        if entry["source_kind"] == "bet10_betting_history"
    ]
    assert len(bet10_bets) == 2
    winning_bet = next(entry for entry in bet10_bets if entry["selection"] == "rosemary's rose")
    assert winning_bet["platform"] == "bet10"
    assert winning_bet["activity_type"] == "bet_settled"
    assert winning_bet["status"] == "settled"
    assert winning_bet["odds_decimal"] == 3.75
    assert winning_bet["stake_gbp"] == 1.0
    assert winning_bet["payout_gbp"] == 3.75
    assert winning_bet["realised_pnl_gbp"] == 2.75
    assert winning_bet["reference"] == "45042793"
    assert winning_bet["market"] == "Win or Each Way (Day of Race)"
    assert winning_bet["event"] == "2 Mar, 17:40 Kempton"
    assert winning_bet["sport_name"] == "Horse Racing"

    bet10_payments = [
        entry
        for entry in payload["ledger_entries"]
        if entry["source_kind"] == "bet10_payment_history"
    ]
    assert len(bet10_payments) == 2
    assert len(payload["account_activities"]) == 2
    withdrawal = next(entry for entry in bet10_payments if entry["activity_type"] == "withdrawal")
    assert withdrawal["reference"] == "6090615942"
    deposit = next(entry for entry in bet10_payments if entry["activity_type"] == "deposit")
    assert deposit["reference"] == "6082665972"


def test_bet10_receipt_matcher_falls_back_when_ocr_odds_are_broken() -> None:
    matched = history_import._match_bet10_receipt(
        placed_at="2026-03-03T14:08:00",
        selection="reece james (chelsea)",
        odds=4.5,
        stake=2.0,
        receipts=[
            Bet10Receipt(
                receipt_id="45081069",
                selection="Reece James (Chelsea)",
                market="Player To Receive A Card",
                event="Aston Villa v Chelsea",
                bet_type="single",
                odds_decimal=None,
                stake_gbp=2.0,
                source_file="45081069.jpeg",
                source_kind="bet10_receipt_image",
            )
        ],
        used_receipt_ids=set(),
    )

    assert matched is not None
    assert matched.receipt_id == "45081069"


def test_import_statement_history_parses_smarkets_bst_timestamps(tmp_path: Path) -> None:
    (tmp_path / "smarkets_account_overview.csv").write_text(
        "\n".join(
            [
                "\"Smarkets Account Overview - 15 Mar 2026, 02:32 GMT\"",
                "\"Event\",\"Details\",\"Date\",\"Backers Stake (GBP)\",\"Odds\",\"Exposure (GBP)\",\"In/Out (GBP)\",\"Balance (GBP)\"",
                "\"Commission Update (0.02)\",\"\",\"07 May 2025, 16:39 BST\",\"\",\"\",\"0.00\",\"\",\"0.00\"",
                "\"Deposit\",\"\",\"07 May 2025, 16:40 BST\",\"\",\"\",\"0.00\",\"10.00\",\"10.00\"",
            ]
        )
        + "\n"
    )

    payload = import_statement_history(tmp_path)

    deposit_entry = next(
        entry
        for entry in payload["ledger_entries"]
        if entry["activity_type"] == "deposit"
    )
    assert deposit_entry["occurred_at"] == "2025-05-07T15:40:00Z"
