from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
import csv
import re
import shutil
import subprocess
from zoneinfo import ZoneInfo

from bet_recorder.ledger.models import AccountActivity, LedgerEntry
from bet_recorder.ledger.taxonomy import infer_market_family, normalize_vendor


BETFAIR_ACCOUNT_STATEMENT_GLOB = "**/AccountStatement*.csv"
MATCHBOOK_TRANSACTION_GLOB = "**/*transactions*.csv"
BANK_STATEMENT_GLOB = "DAWETE*.csv"
SMARKETS_ACCOUNT_OVERVIEW_GLOB = "**/smarkets_account_overview.csv"
RBS_INCREMENTAL_GLOB = "**/rbs_incremental*.txt"
MANUAL_PENDING_WITHDRAWALS_GLOB = "**/manual_pending_withdrawals.csv"
BET10_BETTING_HISTORY_GLOB = "**/betting history.txt"
BET10_PAYMENT_HISTORY_GLOB = "**/payment_history*.csv"
BET10_RECEIPT_PDF_GLOB = "**/bet-receipt*.pdf"
BET10_RECEIPT_IMAGE_GLOB = "bet10/*.jpeg"

REFERENCE_RE = re.compile(r"\bRef:\s*(?P<reference>[A-Za-z0-9/_-]+)")
TRANSACTION_ID_RE = re.compile(
    r"\bTransaction ID(?:\s*-\s*|\s*:\s*)(?P<reference>[A-Za-z0-9/_-]+)"
)
BET_REF_RE = re.compile(r"\bBet Ref:\s*(?P<reference>[A-Za-z0-9/_-]+)")
EXCHANGE_DESCRIPTION_RE = re.compile(
    r"^Exchange:\s*(?P<event>.+?)\s*/\s*(?P<market>.+?)\s+Ref:\s*(?P<reference>[A-Za-z0-9/_-]+)$"
)
MATCHBOOK_MARKET_RE = re.compile(r"^(?P<event>.+?)\s+\|\s+(?P<market>.+)$")

BOOKMAKER_TOKENS: tuple[tuple[str, str], ...] = (
    ("SMARKETS", "smarkets"),
    ("MATCHBOOK", "matchbook"),
    ("BET365", "bet365"),
    ("BETMGM", "betmgm"),
    ("MIDNITE", "midnite"),
    ("MIDNITE.COM", "midnite"),
    ("KWIFF", "kwiff"),
    ("BETWAY", "betway"),
    ("BETFAIR UK", "betfair"),
    ("TALKSPORTBET", "talksportbet"),
    ("LADBROKES", "ladbrokes"),
    ("LEOVEGAS", "leovegas"),
    ("BETFRED", "betfred"),
    ("TOTE", "tote"),
    ("CHEDDAR PAYMENTS L", "cheddar_payments_l"),
    ("SCOUT & CO LTD", "scout_and_co_ltd"),
    ("TRADING 212", "trading_212"),
    ("PAYWARD SERVICES L", "payward_services_l"),
    ("QUINNBET", "quinnbet"),
    ("BALLY CASINO UK", "bally_casino_uk"),
    ("GAMESYS OPERATIONS", "gamesys"),
    ("TOMBOLA", "tombola"),
    ("FANTEAM", "fanteam"),
)


@dataclass(frozen=True)
class StatementSourceSummary:
    path: str
    source_kind: str
    platform: str
    entry_count: int
    account_activity_count: int

    def to_payload(self) -> dict:
        return {
            "path": self.path,
            "source_kind": self.source_kind,
            "platform": self.platform,
            "entry_count": self.entry_count,
            "account_activity_count": self.account_activity_count,
        }


@dataclass(frozen=True)
class Bet10Receipt:
    receipt_id: str
    placed_at: str = ""
    selection: str = ""
    market: str = ""
    event: str = ""
    bet_type: str = ""
    odds_decimal: float | None = None
    stake_gbp: float | None = None
    source_file: str = ""
    source_kind: str = ""


def import_statement_history(source_dir: Path) -> dict:
    entries: list[dict] = []
    account_activities: list[dict] = []
    sources: list[dict] = []
    seen_entry_ids: set[str] = set()
    seen_activity_ids: set[str] = set()
    bet10_receipts, bet10_receipt_sources = collect_bet10_receipts(source_dir)

    for path in sorted(source_dir.glob(BANK_STATEMENT_GLOB)):
        parsed_entries, parsed_activities = parse_bank_statement_csv(path)
        _extend_unique(entries, parsed_entries, seen_entry_ids, key="entry_id")
        _extend_unique(
            account_activities,
            parsed_activities,
            seen_activity_ids,
            key=lambda item: (
                item["occurred_at"],
                item["platform"],
                item["activity_type"],
                item["amount_gbp"],
                item["reference"],
            ),
        )
        sources.append(
            StatementSourceSummary(
                path=str(path),
                source_kind="bank_statement",
                platform="bank",
                entry_count=len(parsed_entries),
                account_activity_count=len(parsed_activities),
            ).to_payload()
        )

    for path in sorted(source_dir.glob(RBS_INCREMENTAL_GLOB)):
        parsed_entries, parsed_activities = parse_rbs_incremental_text(path)
        _extend_unique(entries, parsed_entries, seen_entry_ids, key="entry_id")
        _extend_unique(
            account_activities,
            parsed_activities,
            seen_activity_ids,
            key=lambda item: (
                item["occurred_at"],
                item["platform"],
                item["activity_type"],
                item["amount_gbp"],
                item["reference"],
            ),
        )
        sources.append(
            StatementSourceSummary(
                path=str(path),
                source_kind="rbs_incremental_text",
                platform="bank",
                entry_count=len(parsed_entries),
                account_activity_count=len(parsed_activities),
            ).to_payload()
        )

    for path in sorted(source_dir.glob(MANUAL_PENDING_WITHDRAWALS_GLOB)):
        parsed_entries, parsed_activities = parse_manual_pending_withdrawals_csv(path)
        _extend_unique(entries, parsed_entries, seen_entry_ids, key="entry_id")
        _extend_unique(
            account_activities,
            parsed_activities,
            seen_activity_ids,
            key=lambda item: (
                item["occurred_at"],
                item["platform"],
                item["activity_type"],
                item["amount_gbp"],
                item["reference"],
            ),
        )
        sources.append(
            StatementSourceSummary(
                path=str(path),
                source_kind="manual_pending_withdrawals",
                platform="manual",
                entry_count=len(parsed_entries),
                account_activity_count=len(parsed_activities),
            ).to_payload()
        )

    for path in sorted(source_dir.glob(BET10_BETTING_HISTORY_GLOB)):
        parsed_entries = parse_bet10_betting_history_text(path, receipts=bet10_receipts)
        _extend_unique(entries, parsed_entries, seen_entry_ids, key="entry_id")
        sources.append(
            StatementSourceSummary(
                path=str(path),
                source_kind="bet10_betting_history",
                platform="bet10",
                entry_count=len(parsed_entries),
                account_activity_count=0,
            ).to_payload()
        )

    sources.extend(bet10_receipt_sources)

    for path in sorted(source_dir.glob(BET10_PAYMENT_HISTORY_GLOB)):
        parsed_entries, parsed_activities = parse_bet10_payment_history_csv(path)
        _extend_unique(entries, parsed_entries, seen_entry_ids, key="entry_id")
        _extend_unique(
            account_activities,
            parsed_activities,
            seen_activity_ids,
            key=lambda item: (
                item["occurred_at"],
                item["platform"],
                item["activity_type"],
                item["amount_gbp"],
                item["reference"],
            ),
        )
        sources.append(
            StatementSourceSummary(
                path=str(path),
                source_kind="bet10_payment_history",
                platform="bet10",
                entry_count=len(parsed_entries),
                account_activity_count=len(parsed_activities),
            ).to_payload()
        )

    for path in sorted(source_dir.glob(SMARKETS_ACCOUNT_OVERVIEW_GLOB)):
        parsed_entries, parsed_activities = parse_smarkets_account_overview_csv(path)
        _extend_unique(entries, parsed_entries, seen_entry_ids, key="entry_id")
        _extend_unique(
            account_activities,
            parsed_activities,
            seen_activity_ids,
            key=lambda item: (
                item["occurred_at"],
                item["platform"],
                item["activity_type"],
                item["amount_gbp"],
                item["reference"],
            ),
        )
        sources.append(
            StatementSourceSummary(
                path=str(path),
                source_kind="smarkets_account_overview",
                platform="smarkets",
                entry_count=len(parsed_entries),
                account_activity_count=len(parsed_activities),
            ).to_payload()
        )

    for path in sorted(source_dir.glob(BETFAIR_ACCOUNT_STATEMENT_GLOB)):
        parsed_entries, parsed_activities = parse_betfair_account_statement_csv(path)
        _extend_unique(entries, parsed_entries, seen_entry_ids, key="entry_id")
        _extend_unique(
            account_activities,
            parsed_activities,
            seen_activity_ids,
            key=lambda item: (
                item["occurred_at"],
                item["platform"],
                item["activity_type"],
                item["amount_gbp"],
                item["reference"],
            ),
        )
        sources.append(
            StatementSourceSummary(
                path=str(path),
                source_kind="betfair_account_statement",
                platform="betfair",
                entry_count=len(parsed_entries),
                account_activity_count=len(parsed_activities),
            ).to_payload()
        )

    for path in sorted(source_dir.glob(MATCHBOOK_TRANSACTION_GLOB)):
        if "matchbook" not in str(path).lower() and path.name != "transactions.csv":
            continue
        parsed_entries, parsed_activities = parse_matchbook_transactions_csv(path)
        _extend_unique(entries, parsed_entries, seen_entry_ids, key="entry_id")
        _extend_unique(
            account_activities,
            parsed_activities,
            seen_activity_ids,
            key=lambda item: (
                item["occurred_at"],
                item["platform"],
                item["activity_type"],
                item["amount_gbp"],
                item["reference"],
            ),
        )
        sources.append(
            StatementSourceSummary(
                path=str(path),
                source_kind="matchbook_transactions",
                platform="matchbook",
                entry_count=len(parsed_entries),
                account_activity_count=len(parsed_activities),
            ).to_payload()
        )

    entries.sort(key=lambda item: (item["occurred_at"], item["entry_id"]))
    account_activities.sort(
        key=lambda item: (
            item["occurred_at"],
            item["platform"],
            item["activity_type"],
            item["reference"],
        )
    )

    return {
        "generated_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_dir": str(source_dir),
        "ledger_entries": entries,
        "account_activities": account_activities,
        "sources": sources,
        "summary": {
            "entry_count": len(entries),
            "account_activity_count": len(account_activities),
            "source_count": len(sources),
        },
    }


def parse_bank_statement_csv(path: Path) -> tuple[list[dict], list[dict]]:
    entries: list[dict] = []
    account_activities: list[dict] = []
    for row in _read_csv_rows(path):
        occurred_at = _parse_monzo_date(row.get("Date", ""))
        amount = _optional_float(row.get("Value"))
        balance = _optional_float(row.get("Balance"))
        description = str(row.get("Description", "") or "").strip()
        platform = _infer_bank_vendor(description)
        activity_type = _bank_activity_type(description=description, amount=amount)
        reference = _extract_bank_reference(description)

        entries.append(
            LedgerEntry(
                entry_id=f"bank:{path.name}:{occurred_at}:{reference or description}",
                occurred_at=occurred_at,
                platform=platform,
                activity_type=activity_type,
                status="posted",
                platform_kind="bank",
                currency="GBP",
                amount_gbp=amount,
                balance_after_gbp=balance,
                reference=reference,
                source_file=path.name,
                source_kind="bank_statement",
                description=description,
                raw_fields=_clean_raw_fields(row),
            ).to_payload()
        )

        if platform != "bank":
            account_activities.append(
                AccountActivity(
                    occurred_at=occurred_at,
                    platform=platform,
                    activity_type=activity_type,
                    amount_gbp=amount,
                    balance_after_gbp=balance,
                    currency="GBP",
                    reference=reference,
                    source_file=path.name,
                    source_kind="bank_statement",
                    description=description,
                    raw_fields=_clean_raw_fields(row),
                ).to_payload()
            )
    return entries, account_activities


def parse_rbs_incremental_text(path: Path) -> tuple[list[dict], list[dict]]:
    entries: list[dict] = []
    account_activities: list[dict] = []
    lines = [line.rstrip() for line in path.read_text(encoding="utf-8").splitlines()]
    blocks = [block for block in _split_nonempty_blocks(lines) if len(block) >= 4]

    for index, block in enumerate(blocks):
        occurred_at = _parse_rbs_incremental_date(block[0])
        channel = block[1].strip()
        description = block[2].strip()
        amount = _parse_gbp_value(block[3])
        balance = _parse_optional_balance(block[4]) if len(block) >= 5 else None
        platform = _infer_bank_vendor(description)
        activity_type = _bank_activity_type(description=description, amount=amount)
        reference = f"{path.stem}:{index + 1}"

        payload = LedgerEntry(
            entry_id=f"rbs_incremental:{path.name}:{index + 1}",
            occurred_at=occurred_at,
            platform=platform,
            activity_type=activity_type,
            status="posted",
            platform_kind="bank",
            currency="GBP",
            amount_gbp=amount,
            balance_after_gbp=balance,
            reference=reference,
            source_file=path.name,
            source_kind="rbs_incremental_text",
            description=description,
            raw_fields={
                "date": block[0].strip(),
                "channel": channel,
                "description": description,
                "amount": block[3].strip(),
                "balance": block[4].strip() if len(block) >= 5 else "",
            },
        ).to_payload()
        entries.append(payload)

        if platform != "bank":
            account_activities.append(
                AccountActivity(
                    occurred_at=occurred_at,
                    platform=platform,
                    activity_type=activity_type,
                    amount_gbp=amount,
                    balance_after_gbp=balance,
                    currency="GBP",
                    reference=reference,
                    source_file=path.name,
                    source_kind="rbs_incremental_text",
                    description=description,
                    raw_fields=payload["raw_fields"],
                ).to_payload()
            )
    return entries, account_activities


def parse_smarkets_account_overview_csv(path: Path) -> tuple[list[dict], list[dict]]:
    entries: list[dict] = []
    account_activities: list[dict] = []
    rows = _read_smarkets_overview_rows(path)

    for index, row in enumerate(rows):
        label = row["event_label"]
        occurred_at = _parse_smarkets_overview_datetime(row["date"])
        event, market = _split_event_details(row["details"])
        activity_type, status, side, selection = _parse_smarkets_overview_label(label)
        amount = _optional_float(row["in_out_gbp"])
        balance = _optional_float(row["balance_gbp"])
        stake = _optional_float(row["backers_stake_gbp"])
        odds = _optional_float(row["odds"])
        exposure = _optional_float(row["exposure_gbp"])

        payload = LedgerEntry(
            entry_id=f"smarkets_overview:{path.name}:{index + 1}",
            occurred_at=occurred_at,
            platform="smarkets",
            activity_type=activity_type,
            status=status,
            platform_kind="exchange",
            exchange="smarkets",
            event=event,
            market=market,
            selection=selection,
            side=side,
            bet_type="single" if market else "",
            market_family=infer_market_family(market=market, explicit_market_family=None)
            if market
            else "",
            currency="GBP",
            amount_gbp=amount,
            balance_after_gbp=balance,
            stake_gbp=stake,
            odds_decimal=odds,
            exposure_gbp=exposure,
            payout_gbp=amount if amount and amount > 0 else None,
            realised_pnl_gbp=amount if activity_type == "market_settled" else None,
            reference=f"{path.stem}:{index + 1}",
            source_file=path.name,
            source_kind="smarkets_account_overview",
            description=label,
            raw_fields={
                "details": row["details"],
                "event_label": label,
                "side": side,
                "odds": odds,
                "exposure_gbp": exposure,
            },
        ).to_payload()
        entries.append(payload)

        if activity_type in {"deposit", "withdrawal"}:
            account_activities.append(
                AccountActivity(
                    occurred_at=occurred_at,
                    platform="smarkets",
                    activity_type=activity_type,
                    amount_gbp=amount,
                    balance_after_gbp=balance,
                    currency="GBP",
                    reference=f"{path.stem}:{index + 1}",
                    source_file=path.name,
                    source_kind="smarkets_account_overview",
                    description=label,
                    raw_fields=payload["raw_fields"],
                ).to_payload()
            )
    return entries, account_activities


def parse_manual_pending_withdrawals_csv(path: Path) -> tuple[list[dict], list[dict]]:
    entries: list[dict] = []
    account_activities: list[dict] = []
    for index, row in enumerate(_read_csv_rows(path)):
        occurred_at = _parse_manual_date(str(row.get("Date", "") or ""))
        platform = normalize_vendor(str(row.get("Platform", "") or "manual"))
        amount = abs(_optional_float(row.get("Amount")) or 0.0)
        description = str(row.get("Description", "") or "").strip()
        reference = str(row.get("Reference", "") or f"{path.stem}:{index + 1}").strip()

        payload = LedgerEntry(
            entry_id=f"manual_pending_withdrawal:{path.name}:{index + 1}",
            occurred_at=occurred_at,
            platform=platform,
            activity_type="pending_withdrawal",
            status="pending",
            platform_kind="manual",
            currency=str(row.get("Currency", "GBP") or "GBP"),
            amount_gbp=amount,
            reference=reference,
            source_file=path.name,
            source_kind="manual_pending_withdrawals",
            description=description or "Pending withdrawal",
            raw_fields=_clean_raw_fields(row),
        ).to_payload()
        entries.append(payload)
        account_activities.append(
            AccountActivity(
                occurred_at=occurred_at,
                platform=platform,
                activity_type="pending_withdrawal",
                amount_gbp=amount,
                currency=str(row.get("Currency", "GBP") or "GBP"),
                reference=reference,
                source_file=path.name,
                source_kind="manual_pending_withdrawals",
                description=description or "Pending withdrawal",
                raw_fields=payload["raw_fields"],
            ).to_payload()
        )
    return entries, account_activities


def collect_bet10_receipts(source_dir: Path) -> tuple[list[Bet10Receipt], list[dict]]:
    receipts_by_id: dict[str, Bet10Receipt] = {}
    sources: list[dict] = []

    pdf_paths = sorted(source_dir.glob(BET10_RECEIPT_PDF_GLOB))
    parsed_pdf_receipts = [receipt for path in pdf_paths if (receipt := parse_bet10_receipt_pdf(path)) is not None]
    for receipt in parsed_pdf_receipts:
        receipts_by_id[receipt.receipt_id] = receipt
    if parsed_pdf_receipts:
        sources.append(
            StatementSourceSummary(
                path=str(source_dir),
                source_kind="bet10_receipt_pdf",
                platform="bet10",
                entry_count=len(parsed_pdf_receipts),
                account_activity_count=0,
            ).to_payload()
        )

    image_paths = sorted(source_dir.glob(BET10_RECEIPT_IMAGE_GLOB))
    parsed_image_receipts = [
        receipt
        for path in image_paths
        if (receipt := parse_bet10_receipt_image(path)) is not None
        and receipt.receipt_id not in receipts_by_id
    ]
    for receipt in parsed_image_receipts:
        receipts_by_id[receipt.receipt_id] = receipt
    if parsed_image_receipts:
        sources.append(
            StatementSourceSummary(
                path=str(source_dir / "bet10"),
                source_kind="bet10_receipt_image",
                platform="bet10",
                entry_count=len(parsed_image_receipts),
                account_activity_count=0,
            ).to_payload()
        )

    return list(receipts_by_id.values()), sources


def parse_bet10_betting_history_text(
    path: Path,
    *,
    receipts: list[Bet10Receipt] | None = None,
) -> list[dict]:
    entries: list[dict] = []
    lines = [line.strip() for line in path.read_text(encoding="utf-8").splitlines()]
    compact_lines = [line for line in lines if line]
    available_receipts = receipts or []
    used_receipt_ids: set[str] = set()

    index = 0
    while index + 8 < len(compact_lines):
        if compact_lines[index + 5] == "Total Stake" and compact_lines[index + 7] == "Returns":
            bet_type = compact_lines[index]
            placed_at = _parse_day_first_datetime(compact_lines[index + 1])
            result_label = compact_lines[index + 2]
            selection = compact_lines[index + 3]
            odds = _optional_float(compact_lines[index + 4])
            stake = _parse_gbp_value(compact_lines[index + 6])
            returns = _parse_gbp_value(compact_lines[index + 8])
            status = _map_bet10_result_status(result_label)
            receipt = _match_bet10_receipt(
                placed_at=placed_at,
                selection=selection,
                odds=odds,
                stake=stake,
                receipts=available_receipts,
                used_receipt_ids=used_receipt_ids,
            )
            market = receipt.market if receipt is not None else ""
            event = receipt.event if receipt is not None else ""

            entries.append(
                LedgerEntry(
                    entry_id=f"bet10_betting:{path.name}:{placed_at}:{selection}:{stake}",
                    occurred_at=placed_at,
                    platform="bet10",
                    activity_type="bet_settled",
                    status=status,
                    platform_kind="sportsbook",
                    event=event,
                    market=market,
                    selection=selection,
                    bet_type=_normalize_bet10_bet_type(
                        receipt.bet_type if receipt is not None else bet_type
                    ),
                    market_family=infer_market_family(
                        market=market,
                        explicit_market_family=None,
                    )
                    if market
                    else "custom",
                    sport_name=_infer_bet10_sport_name(event=event, market=market),
                    currency="GBP",
                    amount_gbp=_calculate_realised_pnl(stake=stake, payout=returns),
                    stake_gbp=stake,
                    odds_decimal=odds,
                    payout_gbp=returns,
                    realised_pnl_gbp=_calculate_realised_pnl(stake=stake, payout=returns),
                    reference=receipt.receipt_id if receipt is not None else "",
                    source_file=path.name,
                    source_kind="bet10_betting_history",
                    description=result_label,
                    raw_fields={
                        "bet_type": bet_type,
                        "result": result_label,
                        "selection": selection,
                        "odds": compact_lines[index + 4],
                        "stake_label": compact_lines[index + 5],
                        "stake": compact_lines[index + 6],
                        "returns_label": compact_lines[index + 7],
                        "returns": compact_lines[index + 8],
                        "receipt_source_file": receipt.source_file if receipt is not None else "",
                        "receipt_source_kind": receipt.source_kind if receipt is not None else "",
                    },
                ).to_payload()
            )
            index += 9
            continue
        index += 1

    return entries


def parse_bet10_payment_history_csv(path: Path) -> tuple[list[dict], list[dict]]:
    entries: list[dict] = []
    account_activities: list[dict] = []
    for row in _read_payment_history_rows(path):
        occurred_at = _parse_bet10_payment_datetime(str(row.get("Date and time", "") or ""))
        payment_type = str(row.get("Payment type", "") or "").strip()
        amount = _parse_gbp_value(str(row.get("Amount", "") or ""))
        reference = str(row.get("Payment ID", "") or "").strip()
        description = payment_type or "Payment history row"
        activity_type = _map_payment_type(payment_type, amount)

        payload = LedgerEntry(
            entry_id=f"bet10_payment:{reference or path.name}:{occurred_at}",
            occurred_at=occurred_at,
            platform="bet10",
            activity_type=activity_type,
            status=str(row.get("Status", "posted") or "posted").strip().lower(),
            platform_kind="sportsbook",
            currency="GBP",
            amount_gbp=amount,
            reference=reference,
            source_file=path.name,
            source_kind="bet10_payment_history",
            description=description,
            raw_fields=_clean_raw_fields(row),
        ).to_payload()
        entries.append(payload)
        account_activities.append(
            AccountActivity(
                occurred_at=occurred_at,
                platform="bet10",
                activity_type=activity_type,
                amount_gbp=amount,
                currency="GBP",
                reference=reference,
                source_file=path.name,
                source_kind="bet10_payment_history",
                description=description,
                raw_fields=payload["raw_fields"],
            ).to_payload()
        )
    return entries, account_activities


def parse_bet10_receipt_pdf(path: Path) -> Bet10Receipt | None:
    rendered = _run_text_extractor(
        binary_name="pdftotext",
        command=(str(path), "-"),
    )
    if not rendered:
        return None
    receipt = _parse_bet10_pdf_receipt_text(rendered, source_file=path.name)
    if receipt is None:
        return None
    return receipt


def parse_bet10_receipt_image(path: Path) -> Bet10Receipt | None:
    rendered = _run_text_extractor(
        binary_name="tesseract",
        command=(str(path), "stdout", "--psm", "6"),
    )
    if not rendered:
        return None
    receipt = _parse_bet10_image_receipt_text(
        rendered,
        source_file=path.name,
        fallback_receipt_id=path.stem,
    )
    if receipt is None:
        return None
    return receipt


def _match_bet10_receipt(
    *,
    placed_at: str,
    selection: str,
    odds: float | None,
    stake: float | None,
    receipts: list[Bet10Receipt],
    used_receipt_ids: set[str],
) -> Bet10Receipt | None:
    exact_candidates = [
        receipt
        for receipt in receipts
        if receipt.receipt_id not in used_receipt_ids
        and _normalize_match_text(receipt.selection) == _normalize_match_text(selection)
        and _floats_close(receipt.odds_decimal, odds)
        and _floats_close(receipt.stake_gbp, stake)
    ]
    candidates = exact_candidates
    if not candidates:
        candidates = [
            receipt
            for receipt in receipts
            if receipt.receipt_id not in used_receipt_ids
            and _normalize_match_text(receipt.selection) == _normalize_match_text(selection)
            and _floats_close(receipt.stake_gbp, stake)
        ]
        if not candidates:
            return None

    dated_candidates = [
        receipt
        for receipt in candidates
        if receipt.placed_at and _datetimes_close(receipt.placed_at, placed_at)
    ]
    chosen = None
    if len(dated_candidates) == 1:
        chosen = dated_candidates[0]
    elif len(candidates) == 1:
        chosen = candidates[0]

    if chosen is not None:
        used_receipt_ids.add(chosen.receipt_id)
    return chosen


def _parse_bet10_pdf_receipt_text(
    rendered: str,
    *,
    source_file: str,
) -> Bet10Receipt | None:
    lines = _compact_text_lines(rendered)
    if "Webticket" not in lines:
        return None

    receipt_id = ""
    if "Webticket" in lines:
        webticket_index = lines.index("Webticket")
        if webticket_index + 1 < len(lines):
            receipt_id = lines[webticket_index + 1]

    placed_at = next(
        (
            _parse_bet10_receipt_datetime(line)
            for line in lines
            if re.fullmatch(r"\d{2}\.\d{2}\.\d{4} \d{2}:\d{2}:\d{2}", line)
        ),
        "",
    )
    try:
        single_index = lines.index("Single")
    except ValueError:
        return None
    if single_index + 4 >= len(lines):
        return None

    selection = lines[single_index + 1]
    market = lines[single_index + 2]
    event = lines[single_index + 3]
    odds = _optional_float(lines[single_index + 4])
    stake = _extract_pdf_stake(lines)

    if not receipt_id:
        return None

    return Bet10Receipt(
        receipt_id=receipt_id,
        placed_at=placed_at,
        selection=selection,
        market=market,
        event=event,
        bet_type="single",
        odds_decimal=odds,
        stake_gbp=stake,
        source_file=source_file,
        source_kind="bet10_receipt_pdf",
    )


def _parse_bet10_image_receipt_text(
    rendered: str,
    *,
    source_file: str,
    fallback_receipt_id: str,
) -> Bet10Receipt | None:
    lines = _compact_text_lines(rendered)
    if not lines or lines[0].lower() != "single":
        return None
    if len(lines) < 4:
        return None

    selection_and_odds = _cleanup_ocr_text(lines[1])
    market = _cleanup_ocr_text(lines[2])
    event = _cleanup_ocr_text(lines[3])
    selection, odds = _split_selection_and_odds(selection_and_odds)
    stake = _extract_image_stake(lines)
    if not selection:
        return None

    return Bet10Receipt(
        receipt_id=fallback_receipt_id,
        selection=selection,
        market=market,
        event=event,
        bet_type="single",
        odds_decimal=odds,
        stake_gbp=stake,
        source_file=source_file,
        source_kind="bet10_receipt_image",
    )


def parse_betfair_account_statement_csv(path: Path) -> tuple[list[dict], list[dict]]:
    entries: list[dict] = []
    account_activities: list[dict] = []
    for row in _read_csv_rows(path):
        occurred_at = _parse_betfair_datetime(row.get("Date", ""))
        description = str(row.get("Description", "") or "").strip()
        cash_in = _optional_float(row.get("Cash In (£)"))
        bonus_in = _optional_float(row.get("Bonus In (£)"))
        cash_out = _optional_float(row.get("Cash Out (£)"))
        bonus_out = _optional_float(row.get("Bonus Out (£)"))
        balance = _optional_float(row.get("Cash Balance (£)"))
        amount = sum(
            value
            for value in (cash_in, bonus_in, cash_out, bonus_out)
            if value is not None
        )
        reference = _extract_statement_reference(description)
        activity_type, status = _betfair_activity_type_and_status(description=description)
        event, market = _extract_betfair_event_market(description)
        platform_kind = "exchange" if description.startswith("Exchange:") else "sportsbook"
        exchange = "betfair_exchange" if platform_kind == "exchange" else None
        payout_gbp = amount if amount > 0 else None
        stake_gbp = abs(amount) if amount < 0 else None

        entries.append(
            LedgerEntry(
                entry_id=f"betfair:{reference or path.name}:{occurred_at}:{activity_type}",
                occurred_at=occurred_at,
                platform="betfair",
                activity_type=activity_type,
                status=status,
                platform_kind=platform_kind,
                exchange=exchange,
                event=event,
                market=market,
                bet_type="single" if event or market else "",
                market_family=infer_market_family(market=market, explicit_market_family=None)
                if market
                else "",
                currency="GBP",
                amount_gbp=amount,
                balance_after_gbp=balance,
                stake_gbp=stake_gbp,
                payout_gbp=payout_gbp,
                reference=reference,
                source_file=path.name,
                source_kind="betfair_account_statement",
                description=description,
                raw_fields=_clean_raw_fields(row),
            ).to_payload()
        )

        if activity_type in {"deposit", "withdrawal", "bonus_credit"}:
            account_activities.append(
                AccountActivity(
                    occurred_at=occurred_at,
                    platform="betfair",
                    activity_type=activity_type,
                    amount_gbp=amount,
                    balance_after_gbp=balance,
                    currency="GBP",
                    reference=reference,
                    source_file=path.name,
                    source_kind="betfair_account_statement",
                    description=description,
                    raw_fields=_clean_raw_fields(row),
                ).to_payload()
            )
    return entries, account_activities


def parse_matchbook_transactions_csv(path: Path) -> tuple[list[dict], list[dict]]:
    entries: list[dict] = []
    account_activities: list[dict] = []
    for row in _read_csv_rows(path):
        occurred_at = _parse_iso_datetime(row.get("time", ""))
        description = str(row.get("detail", "") or "").strip()
        transaction_id = str(row.get("id", "") or "").strip()
        product = str(row.get("product", "") or "").strip()
        amount = _net_amount(
            debit=_optional_float(row.get("debit")),
            credit=_optional_float(row.get("credit")),
        )
        balance = _optional_float(row.get("balance"))
        event, market = _extract_matchbook_event_market(description)
        activity_type = _matchbook_activity_type(
            product=product,
            transaction_type=str(row.get("transaction-type", "") or ""),
            amount=amount,
        )
        platform_kind = "exchange" if product.lower() == "exchange" else "sportsbook"

        entries.append(
            LedgerEntry(
                entry_id=f"matchbook:{transaction_id}",
                occurred_at=occurred_at,
                platform="matchbook",
                activity_type=activity_type,
                status="settled" if product.lower() == "exchange" else "posted",
                platform_kind=platform_kind,
                exchange="matchbook" if product.lower() == "exchange" else None,
                event=event,
                market=market,
                bet_type="single" if event or market else "",
                market_family=infer_market_family(market=market, explicit_market_family=None)
                if market
                else "",
                currency=str(row.get("currency", "GBP") or "GBP"),
                amount_gbp=amount,
                balance_after_gbp=balance,
                payout_gbp=amount if amount > 0 and product.lower() == "exchange" else None,
                reference=transaction_id,
                source_file=path.name,
                source_kind="matchbook_transactions",
                description=description,
                raw_fields=_clean_raw_fields(row),
            ).to_payload()
        )

        if product.lower() == "account":
            account_activities.append(
                AccountActivity(
                    occurred_at=occurred_at,
                    platform="matchbook",
                    activity_type=activity_type,
                    amount_gbp=amount,
                    balance_after_gbp=balance,
                    currency=str(row.get("currency", "GBP") or "GBP"),
                    reference=transaction_id,
                    source_file=path.name,
                    source_kind="matchbook_transactions",
                    description=description,
                    raw_fields=_clean_raw_fields(row),
                ).to_payload()
            )
    return entries, account_activities


def _extend_unique(
    items: list[dict],
    new_items: list[dict],
    seen: set[str | tuple],
    *,
    key: str | Callable[[dict], object],
) -> None:
    for item in new_items:
        item_key = item[key] if isinstance(key, str) else key(item)
        if item_key in seen:
            continue
        seen.add(item_key)
        items.append(item)


def _read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        return list(csv.DictReader(handle))


def _read_smarkets_overview_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        reader = csv.reader(handle)
        rows = list(reader)
    if len(rows) < 2:
        return []

    normalized_rows: list[dict[str, str]] = []
    for row in rows[2:]:
        if not row or not any(cell.strip() for cell in row):
            continue
        padded = row + [""] * max(0, 8 - len(row))
        normalized_rows.append(
            {
                "event_label": padded[0].strip().strip('"'),
                "details": padded[1].strip(),
                "date": padded[2].strip(),
                "backers_stake_gbp": padded[3].strip(),
                "odds": padded[4].strip(),
                "exposure_gbp": padded[5].strip(),
                "in_out_gbp": padded[6].strip(),
                "balance_gbp": padded[7].strip(),
            }
        )
    return normalized_rows


def _read_payment_history_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8-sig", errors="replace") as handle:
        reader = csv.reader(handle)
        raw_rows = list(reader)

    header_index = next(
        (
            index
            for index, row in enumerate(raw_rows)
            if row and row[0].strip() == "Date and time"
        ),
        None,
    )
    if header_index is None:
        return []

    headers = [cell.strip() for cell in raw_rows[header_index]]
    rows: list[dict[str, str]] = []
    for row in raw_rows[header_index + 1 :]:
        if not row or not any(cell.strip() for cell in row):
            continue
        padded = row + [""] * max(0, len(headers) - len(row))
        rows.append(
            {
                header: padded[index].strip()
                for index, header in enumerate(headers)
            }
        )
    return rows


def _run_text_extractor(*, binary_name: str, command: tuple[str, ...]) -> str:
    binary_path = shutil.which(binary_name)
    if binary_path is None:
        return ""
    result = subprocess.run(
        [binary_path, *command],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        return ""
    return result.stdout


def _compact_text_lines(rendered: str) -> list[str]:
    return [line.strip() for line in rendered.splitlines() if line.strip()]


def _parse_bet10_receipt_datetime(value: str) -> str:
    return datetime.strptime(value.strip(), "%d.%m.%Y %H:%M:%S").strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def _extract_pdf_stake(lines: list[str]) -> float | None:
    for index, line in enumerate(lines):
        if line == "Total Stake" and index + 1 < len(lines):
            return _parse_gbp_value(lines[index + 1])
    return None


def _extract_image_stake(lines: list[str]) -> float | None:
    for line in lines:
        match = re.search(r"£\s*([0-9]+(?:\.[0-9]+)?)\s+wager", line, re.IGNORECASE)
        if match is not None:
            return float(match.group(1))
    return None


def _split_selection_and_odds(value: str) -> tuple[str, float | None]:
    cleaned = value.replace("©", "").replace("@", "").strip()
    match = re.search(r"(?P<odds>\d+(?:[.,]\d+)?)$", cleaned)
    if match is None:
        return cleaned, None
    odds_literal = match.group("odds")
    if match.start() > 0 and cleaned[match.start() - 1] in {".", ","}:
        odds = None
    elif "." not in odds_literal and "," not in odds_literal and len(odds_literal) >= 3:
        odds = float(odds_literal) / 100
    else:
        odds = float(odds_literal.replace(",", "."))
    selection = cleaned[: match.start()].strip()
    if selection.endswith("."):
        selection = selection[:-1].strip()
    if selection.endswith("on\""):
        selection = selection[:-3].strip()
    return selection, odds


def _cleanup_ocr_text(value: str) -> str:
    return (
        value.replace("Plaer", "Player")
        .replace("Vilav", "Villa v")
        .replace("Getofe", "Getafe")
        .replace("Pisav", "Pisa v")
        .replace("Celtic:", "Celtic")
        .strip()
    )


def _normalize_match_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _floats_close(left: float | None, right: float | None) -> bool:
    if left is None or right is None:
        return left is right
    return abs(left - right) <= 0.02


def _datetimes_close(left: str, right: str) -> bool:
    if not left or not right:
        return False
    left_dt = datetime.fromisoformat(left.replace("Z", "+00:00"))
    right_dt = datetime.fromisoformat(right.replace("Z", "+00:00"))
    return abs((left_dt - right_dt).total_seconds()) <= 90


def _infer_bet10_sport_name(*, event: str, market: str) -> str:
    lowered_event = event.lower()
    lowered_market = market.lower()
    if "win or each way" in lowered_market or re.search(r"\bkempton\b", lowered_event):
        return "Horse Racing"
    if "player to receive a card" in lowered_market:
        return "Football"
    if "number of shots" in lowered_market or "total shots" in lowered_market:
        return "Football"
    if "full time result" in lowered_market or "handicap match result" in lowered_market:
        return "Football"
    if " v " in lowered_event or " vs " in lowered_event:
        return "Football"
    return ""


def _optional_float(value: object) -> float | None:
    if value in (None, "", "--"):
        return None
    return float(str(value).replace("£", "").replace(",", ""))


def _parse_monzo_date(value: str) -> str:
    return datetime.strptime(value.strip(), "%d %b %Y").strftime("%Y-%m-%d")


def _parse_rbs_incremental_date(value: str) -> str:
    return datetime.strptime(value.strip(), "%d %b %Y").strftime("%Y-%m-%d")


def _parse_manual_date(value: str) -> str:
    stripped = value.strip()
    if "T" in stripped:
        return datetime.fromisoformat(stripped.replace("Z", "+00:00")).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
    return datetime.strptime(stripped, "%Y-%m-%d").strftime("%Y-%m-%d")


def _parse_day_first_datetime(value: str) -> str:
    return datetime.strptime(value.strip(), "%d/%m/%Y, %H:%M").strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def _parse_bet10_payment_datetime(value: str) -> str:
    return datetime.strptime(value.strip(), "%d-%m-%Y %H:%M").strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def _parse_betfair_datetime(value: str) -> str:
    return datetime.strptime(value.strip(), "%d-%b-%y %H:%M:%S").strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def _parse_iso_datetime(value: str) -> str:
    normalized = value.strip().replace("Z", "+00:00")
    return datetime.fromisoformat(normalized).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_smarkets_overview_datetime(value: str) -> str:
    stripped = value.strip()
    for timezone_label in ("GMT", "BST"):
        suffix = f" {timezone_label}"
        if not stripped.endswith(suffix):
            continue
        naive = datetime.strptime(
            stripped.removesuffix(suffix),
            "%d %b %Y, %H:%M",
        )
        localized = naive.replace(tzinfo=ZoneInfo("Europe/London"))
        return localized.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    raise ValueError(f"Unsupported Smarkets overview datetime: {value!r}")


def _normalize_bet10_bet_type(value: str) -> str:
    normalized = value.strip().lower().replace(" ", "_")
    return normalized or "single"


def _map_bet10_result_status(value: str) -> str:
    normalized = value.strip().lower()
    if normalized == "void":
        return "voided"
    return "settled"


def _map_payment_type(payment_type: str, amount: float | None) -> str:
    normalized = payment_type.strip().lower()
    if normalized == "deposit":
        return "deposit"
    if normalized == "withdrawal":
        return "withdrawal"
    if amount is not None and amount < 0:
        return "deposit"
    if amount is not None and amount > 0:
        return "withdrawal"
    return "account_activity"


def _calculate_realised_pnl(*, stake: float | None, payout: float | None) -> float | None:
    if stake is None or payout is None:
        return None
    return payout - stake


def _infer_bank_vendor(description: str) -> str:
    normalized_description = description.upper()
    for token, vendor in BOOKMAKER_TOKENS:
        if token in normalized_description:
            return normalize_vendor(vendor)
    return "bank"


def _bank_activity_type(*, description: str, amount: float | None) -> str:
    normalized_description = description.upper()
    if amount is None:
        return "statement_entry"
    if "TRUELAYER-WITHDRAW" in normalized_description or normalized_description.endswith(
        "CREDIT"
    ):
        return "withdrawal"
    if amount < 0:
        return "deposit"
    if amount > 0:
        return "withdrawal"
    return "statement_entry"


def _extract_bank_reference(description: str) -> str:
    parts = [part.strip() for part in description.split(",") if part.strip()]
    if not parts:
        return ""
    return parts[-1]


def _split_nonempty_blocks(lines: list[str]) -> list[list[str]]:
    blocks: list[list[str]] = []
    current: list[str] = []
    for line in lines:
        if line.strip():
            current.append(line)
            continue
        if current:
            blocks.append(current)
            current = []
    if current:
        blocks.append(current)
    return blocks


def _parse_gbp_value(value: str) -> float | None:
    stripped = value.strip()
    if not stripped or stripped == "-":
        return None
    normalized = stripped.replace("£", "").replace(",", "")
    return float(normalized)


def _parse_optional_balance(value: str) -> float | None:
    stripped = value.strip()
    if not stripped or stripped == "-":
        return None
    return _parse_gbp_value(stripped)


def _extract_statement_reference(description: str) -> str:
    for pattern in (BET_REF_RE, TRANSACTION_ID_RE, REFERENCE_RE):
        match = pattern.search(description)
        if match is not None:
            return match.group("reference")
    return ""


def _betfair_activity_type_and_status(*, description: str) -> tuple[str, str]:
    if description.startswith("Deposit Ref"):
        return "deposit", "posted"
    if description.startswith("Withdrawal Ref"):
        return "withdrawal", "posted"
    if description.startswith("Sports Bonus Awarded") or description.startswith(
        "Exchange Bonus Awarded"
    ):
        return "bonus_credit", "posted"
    if description.startswith("Sportsbook: Bet Placed"):
        return "bet_placed", "placed"
    if description.startswith("Sportsbook: Bet Settled"):
        return "bet_settled", "settled"
    if description.startswith("Sportsbook: Cash Out"):
        return "cash_out", "settled"
    if description.startswith("Exchange:"):
        return "exchange_settlement", "settled"
    return "statement_entry", "posted"


def _extract_betfair_event_market(description: str) -> tuple[str, str]:
    match = EXCHANGE_DESCRIPTION_RE.match(description)
    if match is None:
        return "", ""
    return match.group("event").strip(), match.group("market").strip()


def _split_event_details(details: str) -> tuple[str, str]:
    if " / " not in details:
        return details.strip(), ""
    event, market = details.split(" / ", 1)
    return event.strip(), market.strip()


def _parse_smarkets_overview_label(
    label: str,
) -> tuple[str, str, str, str]:
    normalized = label.strip()
    side = ""
    selection = ""
    if "·" in normalized:
        base, qualifier = [part.strip() for part in normalized.split("·", 1)]
        normalized = base
        if qualifier.startswith("For "):
            side = "back"
            selection = qualifier.removeprefix("For ").strip()
        elif qualifier.startswith("Against "):
            side = "lay"
            selection = qualifier.removeprefix("Against ").strip()
    mappings = [
        ("Withdraw", ("withdrawal", "posted")),
        ("Deposit", ("deposit", "posted")),
        ("Market Settled", ("market_settled", "settled")),
        ("Bet Won", ("bet_won", "settled")),
        ("Bet Lost", ("bet_lost", "settled")),
        ("Bet Match Confirmed", ("bet_match_confirmed", "matched")),
        ("Bet Fully Matched (Pending)", ("bet_fully_matched_pending", "pending")),
        ("Bet Fully Matched", ("bet_fully_matched", "matched")),
        ("Bet Partially Matched", ("bet_partially_matched", "matched")),
        ("Bet Placed", ("bet_placed", "placed")),
    ]
    for prefix, (activity_type, status) in mappings:
        if normalized.startswith(prefix):
            return activity_type, status, side, selection
    return "statement_entry", "posted", side, selection


def _extract_matchbook_event_market(description: str) -> tuple[str, str]:
    match = MATCHBOOK_MARKET_RE.match(description)
    if match is None:
        return "", ""
    return match.group("event").strip(), match.group("market").strip()


def _matchbook_activity_type(
    *, product: str, transaction_type: str, amount: float | None
) -> str:
    normalized_product = product.lower()
    normalized_type = transaction_type.lower()
    if normalized_product == "account":
        if "manual" in normalized_type:
            return "deposit" if (amount or 0.0) > 0 else "withdrawal"
        return "account_activity"
    if normalized_product == "exchange":
        if "payout" in normalized_type:
            return "bet_settled"
        return "exchange_activity"
    return "statement_entry"


def _net_amount(*, debit: float | None, credit: float | None) -> float | None:
    values = [value for value in (debit, credit) if value is not None]
    if not values:
        return None
    return sum(values)


def _clean_raw_fields(row: dict[str, str]) -> dict[str, str]:
    return {
        str(key): str(value)
        for key, value in row.items()
        if value not in (None, "")
    }
