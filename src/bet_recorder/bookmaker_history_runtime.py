from __future__ import annotations

from pathlib import Path
import html
import hashlib
import json
import os
import re
import tempfile

from bet_recorder.analysis.open_bets_common import (
    clean_lines,
    looks_like_date_fragment,
    parse_money,
    parse_odds,
)
from bet_recorder.ledger.taxonomy import (
    infer_funding_kind,
    infer_market_family,
    normalize_funding_kind,
    normalize_vendor,
)
from bet_recorder.ledger.history_import import (
    parse_bet10_betting_history_text,
    parse_betfair_account_statement_csv,
    parse_matchbook_transactions_csv,
)
from bet_recorder.sources.history_adapters import get_live_venue_history_adapter

AUTO_BOOKMAKER_HISTORY_FILENAME = "auto-bookmaker-history.json"
HISTORY_STATUS_HINTS = {
    "won": "won",
    "lost": "lost",
    "lose": "lost",
    "settled": "settled",
    "cashed out": "cash_out",
    "void": "void",
}
OPEN_STATUS_HINTS = ("open", "cash out", "in-play", "in play")
BET_TYPE_MARKERS = {"single", "double", "treble", "acca", "acca(s)", "accumulator"}
BET365_HEADER_MONEY_PATTERN = re.compile(r"^£\s*([0-9]+(?:\.[0-9]{2})?)")
INLINE_GBP_PATTERN = re.compile(r"£\s*([0-9]+(?:\.[0-9]{2})?)")
BETVICTOR_DESCRIPTION_PREFIX_RE = re.compile(
    r"^(?:Bonus Funds:\s*)?GBP\s*[0-9]+(?:\.[0-9]{2})?\s+(?P<bet_type>[^:]+):\s*(?P<body>.+)$",
    re.IGNORECASE,
)
KAMBI_HISTORY_BLOCK_RE = re.compile(
    r"^(?P<bet_type>single|double|treble|acca|accumulator|bet builder)(?P<status>won|lost|void|cashed out|cash out|placed|settled)?$",
    re.IGNORECASE,
)
KAMBI_DATE_LINE_RE = re.compile(r"^\d{1,2}\s+\w{3}\s+\d{4}\s+•\s+\d{2}:\d{2}:\d{2}$")
BETVICTOR_LEG_RE = re.compile(
    r"^(?P<selection>.+?)\s+@\s+[^\[]+\s+\[(?P<market>[^\]]+)\]\s+-\s+(?P<event>.+?)\s+-\s+(?P<competition>.+?)\s+-\s+(?P<sport>.+)$",
    re.IGNORECASE,
)
IGNORE_LINES = {
    "my bets",
    "open bets",
    "open",
    "settled",
    "cash out",
    "stake",
    "returns",
    "return",
    "net return",
    "reuse selections",
}


def runtime_bookmaker_history_path(run_dir: Path | None) -> Path | None:
    if run_dir is None:
        return None
    return run_dir / AUTO_BOOKMAKER_HISTORY_FILENAME


def load_runtime_bookmaker_history_payload(path: Path | None) -> dict:
    if path is None or not path.exists():
        return {
            "source": "auto_live_bookmaker_history",
            "updated_at": "",
            "venue_updated_at": {},
            "sync_reports": {},
            "ledger_entries": [],
        }
    payload = json.loads(path.read_text())
    entries = payload.get("ledger_entries")
    venue_updated_at = payload.get("venue_updated_at")
    sync_reports = payload.get("sync_reports")
    return {
        "source": str(payload.get("source", "auto_live_bookmaker_history")),
        "updated_at": str(payload.get("updated_at", "") or ""),
        "venue_updated_at": (
            {
                str(key): str(value)
                for key, value in venue_updated_at.items()
                if str(key).strip() and str(value).strip()
            }
            if isinstance(venue_updated_at, dict)
            else {}
        ),
        "sync_reports": (
            {
                str(key): dict(value)
                for key, value in sync_reports.items()
                if str(key).strip() and isinstance(value, dict)
            }
            if isinstance(sync_reports, dict)
            else {}
        ),
        "ledger_entries": [dict(entry) for entry in entries if isinstance(entry, dict)]
        if isinstance(entries, list)
        else [],
    }


def load_runtime_bookmaker_history_entries(path: Path | None) -> list[dict]:
    return list(load_runtime_bookmaker_history_payload(path).get("ledger_entries", []))


def write_runtime_bookmaker_history_entries(
    path: Path,
    entries: list[dict],
    *,
    updated_at: str,
    venue_updated_at: dict[str, str] | None = None,
    sync_reports: dict[str, dict] | None = None,
) -> None:
    if path.parent:
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        try:
            path.parent.chmod(0o700)
        except OSError:
            pass
    payload = {
        "source": "auto_live_bookmaker_history",
        "updated_at": updated_at,
        "venue_updated_at": dict(venue_updated_at or {}),
        "sync_reports": dict(sync_reports or {}),
        "ledger_entries": entries,
    }
    temp_path = path.with_name(f".{path.name}.tmp")
    temp_path.write_text(json.dumps(payload, indent=2) + "\n")
    try:
        os.chmod(temp_path, 0o600)
    except OSError:
        pass
    temp_path.replace(path)
    try:
        os.chmod(path, 0o600)
    except OSError:
        pass


def merge_runtime_bookmaker_history_entries(
    existing_entries: list[dict],
    incoming_entries: list[dict],
) -> list[dict]:
    merged: list[dict] = []
    seen: dict[str, dict] = {}
    for entry in [*(existing_entries or []), *(incoming_entries or [])]:
        if not isinstance(entry, dict):
            continue
        entry_id = str(entry.get("entry_id", "") or "")
        if not entry_id:
            entry_id = _build_entry_id(entry)
            entry["entry_id"] = entry_id
        existing = seen.get(entry_id)
        if existing is None:
            normalized = dict(entry)
            seen[entry_id] = normalized
            merged.append(normalized)
            continue
        for field in (
            "status",
            "description",
            "funding_kind",
            "realised_pnl_gbp",
            "payout_gbp",
            "source_file",
            "source_kind",
        ):
            if existing.get(field) in (None, "", {}, []) and entry.get(field) not in (None, "", {}, []):
                existing[field] = entry[field]
    merged.sort(
        key=lambda entry: (
            str(entry.get("occurred_at", "") or ""),
            str(entry.get("entry_id", "") or ""),
        )
    )
    return merged


def extract_live_bookmaker_ledger_entries(
    *,
    venue: str,
    payload: dict,
    analysis: dict,
) -> list[dict]:
    adapter = get_live_venue_history_adapter(venue)
    if adapter is None or not adapter.history_parser_supported:
        return []
    if str(payload.get("page", "") or "") not in {"my_bets", adapter.history_page}:
        return []
    lines = clean_lines(str(payload.get("body_text", "") or ""))
    if (
        not lines
        and adapter.parser_name not in {"betano_history", "betvictor_history", "fanteam_history"}
    ):
        return []
    open_bet_keys = {
        _visible_bet_key(
            event=str(row.get("event", "") or ""),
            market=str(row.get("market", "") or ""),
            selection=str(row.get("label", "") or ""),
            odds=float(row.get("odds", 0.0) or 0.0),
            stake=float(row.get("stake", 0.0) or 0.0),
        )
        for row in analysis.get("open_bets", []) or []
        if isinstance(row, dict)
    }
    if adapter.parser_name == "bet365_history":
        if not _bet365_payload_looks_like_history_surface(payload=payload, lines=lines):
            return []
        rows = _extract_bet365_history_rows(lines=lines, open_bet_keys=open_bet_keys)
    elif adapter.parser_name == "betfair_account_statement":
        return _extract_betfair_account_statement_entries(payload=payload)
    elif adapter.parser_name == "matchbook_transactions":
        return _extract_matchbook_transaction_entries(payload=payload)
    elif adapter.parser_name == "bet10_history":
        return _extract_bet10_history_entries(payload=payload)
    elif adapter.parser_name == "betano_history":
        rows = _extract_betano_api_history_rows(payload.get("history_api_responses"))
        if not rows:
            rows = _extract_betano_history_rows(lines=lines)
    elif adapter.parser_name == "betuk_history":
        rows = _extract_betuk_history_rows(lines=lines)
    elif adapter.parser_name == "betvictor_history":
        rows = _extract_betvictor_history_rows(payload.get("history_api_responses"))
    elif adapter.parser_name == "fanteam_history":
        rows = _extract_fanteam_history_rows(payload.get("history_api_responses"))
    elif adapter.parser_name == "paddypower_history":
        rows = _extract_paddypower_history_rows(lines=lines)
    elif adapter.parser_name == "boylesports_history":
        rows = _extract_boylesports_history_rows(
            body_html=str(payload.get("body_html", "") or ""),
        )
    elif adapter.parser_name == "midnite_history":
        rows = _extract_midnite_history_rows(lines=lines)
    elif adapter.parser_name == "kambi_history":
        rows = _extract_kambi_history_rows(lines=lines)
    elif adapter.parser_name == "generic_history":
        rows = _extract_generic_history_rows(lines=lines)
    else:
        return []
    captured_at = str(payload.get("captured_at", "") or "")
    return [
        _build_runtime_ledger_entry(
            venue=venue,
            row=row,
            occurred_at=captured_at,
        )
        for row in rows
    ]


def _extract_betfair_account_statement_entries(*, payload: dict) -> list[dict]:
    body_text = str(payload.get("body_text", "") or "")
    if not body_text.strip():
        return []
    entries, _ = _parse_history_import_entries_from_text(
        body_text=body_text,
        suffix=".csv",
        parser=lambda path: parse_betfair_account_statement_csv(path)[0],
    )
    return [
        entry
        for entry in entries
        if entry.get("activity_type") in {"exchange_settlement", "bet_placed", "bet_settled"}
    ]


def _extract_matchbook_transaction_entries(*, payload: dict) -> list[dict]:
    body_text = str(payload.get("body_text", "") or "")
    if not body_text.strip():
        return []
    entries, _ = _parse_history_import_entries_from_text(
        body_text=body_text,
        suffix=".csv",
        parser=lambda path: parse_matchbook_transactions_csv(path)[0],
    )
    return [
        entry
        for entry in entries
        if entry.get("activity_type") in {"bet_settled", "bet_placed", "exchange_settlement"}
    ]


def _extract_bet10_history_entries(*, payload: dict) -> list[dict]:
    body_text = str(payload.get("body_text", "") or "")
    if not body_text.strip():
        return []
    entries, _ = _parse_history_import_entries_from_text(
        body_text=body_text,
        suffix=".txt",
        parser=lambda path: parse_bet10_betting_history_text(path, receipts=[]),
    )
    return list(entries)


def _parse_history_import_entries_from_text(
    *,
    body_text: str,
    suffix: str,
    parser,
) -> tuple[list[dict], Path]:
    with tempfile.NamedTemporaryFile(
        mode="w",
        encoding="utf-8",
        suffix=suffix,
        delete=False,
    ) as handle:
        handle.write(body_text)
        temp_path = Path(handle.name)
    try:
        entries = parser(temp_path)
        return list(entries), temp_path
    finally:
        temp_path.unlink(missing_ok=True)


def _bet365_payload_looks_like_history_surface(*, payload: dict, lines: list[str]) -> bool:
    url = str(payload.get("url", "") or "").lower()
    if any(
        marker in url
        for marker in (
            "members.bet365.com",
            "#/me/x8020",
            "#/me/k/hi",
            "#/mb/",
        )
    ):
        return True
    lowered = {line.strip().lower() for line in lines if line.strip()}
    required_markers = {"settled bets", "show history"}
    if required_markers.issubset(lowered):
        return True
    return False


def _extract_betvictor_history_rows(history_api_responses: object) -> list[dict]:
    if not isinstance(history_api_responses, dict):
        return []
    settled = history_api_responses.get("settled")
    if not isinstance(settled, dict):
        return []
    payload = settled.get("body")
    if not isinstance(payload, dict):
        return []

    results: list[dict] = []
    for group in payload.get("groups", []):
        if not isinstance(group, dict):
            continue
        bets = group.get("bets", [])
        if not isinstance(bets, list):
            continue
        for bet in bets:
            if not isinstance(bet, dict):
                continue
            row = _parse_betvictor_history_bet(bet)
            if row is not None:
                results.append(row)
    return results


def _extract_betano_api_history_rows(history_api_responses: object) -> list[dict]:
    if not isinstance(history_api_responses, dict):
        return []
    settled = history_api_responses.get("settled")
    if not isinstance(settled, dict):
        return []
    payload = settled.get("body")
    if not isinstance(payload, dict):
        return []

    results: list[dict] = []
    for group in payload.get("groups", []):
        if not isinstance(group, dict):
            continue
        bets = group.get("bets", [])
        if not isinstance(bets, list):
            continue
        for bet in bets:
            if not isinstance(bet, dict):
                continue
            row = _parse_betano_api_history_bet(bet)
            if row is not None:
                results.append(row)
    return results


def _extract_fanteam_history_rows(history_api_responses: object) -> list[dict]:
    if not isinstance(history_api_responses, dict):
        return []
    history = history_api_responses.get("history")
    if not isinstance(history, dict):
        return []
    body = history.get("body")
    rows = _collect_fanteam_history_candidates(body)
    results: list[dict] = []
    for row in rows:
        parsed = _parse_fanteam_history_row(row)
        if parsed is not None:
            results.append(parsed)
    return results


def _collect_fanteam_history_candidates(payload: object) -> list[dict]:
    results: list[dict] = []
    seen: set[int] = set()

    def visit(node: object) -> None:
        if isinstance(node, dict):
            marker = id(node)
            if marker in seen:
                return
            seen.add(marker)
            if _looks_like_fanteam_history_row(node):
                results.append(node)
            for key in (
                "bets",
                "items",
                "rows",
                "results",
                "data",
                "history",
                "settledBets",
                "archivedBets",
            ):
                if key in node:
                    visit(node.get(key))
            for value in node.values():
                if isinstance(value, (dict, list)):
                    visit(value)
        elif isinstance(node, list):
            for item in node:
                visit(item)

    visit(payload)
    return results


def _looks_like_fanteam_history_row(row: dict) -> bool:
    return any(
        key in row
        for key in (
            "eventName",
            "marketName",
            "selectionName",
            "outcomeName",
            "stake",
            "returns",
            "betStatus",
        )
    )


def _parse_fanteam_history_row(row: dict) -> dict | None:
    event = str(
        row.get("eventName")
        or row.get("event")
        or row.get("matchName")
        or row.get("fixtureName")
        or ""
    ).strip()
    market = str(
        row.get("marketName")
        or row.get("market")
        or row.get("betTypeName")
        or row.get("betType")
        or ""
    ).strip()
    selection = str(
        row.get("selectionName")
        or row.get("selection")
        or row.get("outcomeName")
        or row.get("oddName")
        or row.get("name")
        or ""
    ).strip()
    stake = _parse_fanteam_float(
        row.get("stake")
        or row.get("stakeAmount")
        or row.get("betAmount")
        or row.get("amount")
    )
    odds = _parse_fanteam_float(
        row.get("odds")
        or row.get("price")
        or row.get("oddValue")
    )
    payout = _parse_fanteam_float(
        row.get("returns")
        or row.get("returnAmount")
        or row.get("payout")
        or row.get("winAmount")
    )
    status = _parse_status_token(
        str(
            row.get("status")
            or row.get("result")
            or row.get("outcome")
            or row.get("betStatus")
            or ""
        )
    )
    if not event or not selection or stake is None or odds is None:
        return None
    notes = str(row.get("description") or row.get("notes") or status or "settled")
    return {
        "event": event,
        "market": market or "Unknown",
        "selection": selection,
        "bet_type": str(row.get("betType") or row.get("betKind") or "single"),
        "status": status or "settled",
        "stake": stake,
        "odds": odds,
        "payout_gbp": payout,
        "funding_kind": _infer_fanteam_funding_kind(row=row, notes=notes),
        "notes": notes,
        "occurred_at": str(
            row.get("settledAt")
            or row.get("settledDate")
            or row.get("createdAt")
            or row.get("placedAt")
            or ""
        ),
    }


def _extract_paddypower_history_rows(*, lines: list[str]) -> list[dict]:
    lower_lines = [line.strip().lower() for line in lines]
    try:
        start = lower_lines.index("transaction history") + 1
    except ValueError:
        start = 0
    try:
        end = next(
            index
            for index in range(start, len(lines))
            if lower_lines[index].startswith("warning:")
        )
    except StopIteration:
        end = len(lines)

    results: list[dict] = []
    block: list[str] = []
    for line in lines[start:end]:
        stripped = line.strip()
        if not stripped:
            continue
        block.append(stripped)
        if stripped.startswith("Bet ID:"):
            row = _parse_paddypower_history_block(block)
            if row is not None:
                results.append(row)
            block = []
    return results


def _parse_paddypower_history_block(block: list[str]) -> dict | None:
    if not block:
        return None
    stake = _find_labeled_money(lines=block, label="Stake")
    payout = _find_labeled_money(lines=block, label="Returns")
    if stake is None:
        return None
    notes_text = " ".join(block)
    funding_kind = infer_funding_kind(
        explicit_funding_kind="",
        notes=notes_text,
        bet_type="single",
        status="lost" if (payout or 0.0) <= 0.0 else "won",
        free_bet="free bet" in notes_text.lower(),
    )

    first = block[0]
    if first.lower().startswith("bet builder"):
        odds = parse_odds(block[1]) if len(block) > 1 else None
        event = block[2] if len(block) > 2 else ""
        if odds is None or not event:
            return None
        status = "won" if (payout or 0.0) > 0.0 else "lost"
        return {
            "event": event,
            "market": "Multiple",
            "selection": first,
            "odds": odds,
            "stake": stake,
            "status": status,
            "payout_gbp": payout if payout is not None else 0.0,
            "bet_type": "multiple",
            "funding_kind": funding_kind,
            "notes": notes_text,
        }

    if len(block) < 4:
        return None
    status = _normalize_paddypower_status(block[0], payout=payout)
    odds = parse_odds(block[2])
    market_event = block[3]
    if status is None or odds is None or " - " not in market_event:
        return None
    market, event = market_event.split(" - ", 1)
    return {
        "event": event.strip(),
        "market": market.strip(),
        "selection": block[1].strip(),
        "odds": odds,
        "stake": stake,
        "status": status,
        "payout_gbp": payout if payout is not None else 0.0,
        "bet_type": "single",
        "funding_kind": funding_kind,
        "notes": notes_text,
    }


def _normalize_paddypower_status(value: str, *, payout: float | None) -> str | None:
    lowered = value.strip().lower()
    if lowered == "w":
        return "won"
    if lowered == "l":
        return "lost"
    if lowered == "v":
        return "void"
    if payout is not None and payout > 0.0:
        return "won"
    if payout is not None:
        return "lost"
    return None


def _find_labeled_money(*, lines: list[str], label: str) -> float | None:
    lowered_label = label.strip().lower()
    for index, line in enumerate(lines):
        if line.strip().lower() != lowered_label:
            continue
        if index + 1 < len(lines):
            parsed = parse_money(lines[index + 1])
            if parsed is not None:
                return parsed
            inline_match = INLINE_GBP_PATTERN.search(lines[index + 1])
            if inline_match is not None:
                return float(inline_match.group(1))
    return None


def _extract_boylesports_history_rows(*, body_html: str) -> list[dict]:
    if not body_html:
        return []
    results: list[dict] = []
    seen: set[tuple] = set()
    pattern = re.compile(
        r'<span class="creation-date"><strong>(?P<date>[^<]+)</strong></span>.*?'
        r'<span class="creation-time">(?P<time>[^<]+)</span>.*?'
        r'<span class="strong">(?P<bet_type>[^<]+)</span><br>\s*'
        r'<span>(?P<event>[^<]+)</span>.*?'
        r'<strong>Odds:\s*</strong>(?P<odds>[0-9.]+).*?'
        r'(?:(?P<free_label>Free bet stake)|Stake):\s*</strong>£(?P<stake>[0-9.]+).*?'
        r'<strong>Bet Status:\s*</strong><span[^>]*>(?P<status>[^<]+)</span>.*?'
        r'<div class="panel">.*?'
        r'<div class="one-column">\s*(?P<selection>[^<]+?)\s*@\s*(?P<detail_odds>[0-9.]+)\s*-\s*(?P<market>[^<]+)\s*</div>.*?'
        r'<div class="one-column">(?P<detail_event>[^<]+)</div>',
        re.S,
    )
    for match in pattern.finditer(body_html):
        event = html.unescape(match.group("detail_event") or match.group("event")).strip()
        market = html.unescape(match.group("market") or "").strip()
        selection = html.unescape(match.group("selection") or "").strip()
        odds = parse_odds(match.group("detail_odds") or match.group("odds") or "")
        stake = parse_money(f"£{match.group('stake')}")
        status = _normalize_history_status(match.group("status") or "")
        if not event or not market or not selection or odds is None or stake is None or status is None:
            continue
        key = _visible_bet_key(
            event=event,
            market=market,
            selection=selection,
            odds=odds,
            stake=stake,
        )
        if key in seen:
            continue
        seen.add(key)
        payout = 0.0 if status == "lost" else None
        notes_text = html.unescape(match.group(0))
        results.append(
            {
                "event": event,
                "market": market,
                "selection": selection,
                "odds": odds,
                "stake": stake,
                "status": status,
                "payout_gbp": payout,
                "bet_type": str(match.group("bet_type") or "single").strip().lower(),
                "funding_kind": "free_bet" if match.group("free_label") else "cash",
                "notes": notes_text,
            }
        )
    return results


def _extract_midnite_history_rows(*, lines: list[str]) -> list[dict]:
    lower_lines = [line.strip().lower() for line in lines]
    try:
        start = lower_lines.index("settled") + 1
    except ValueError:
        start = 0
    try:
        end = next(
            index
            for index in range(start, len(lines))
            if lower_lines[index] == "in-play betting terms"
        )
    except StopIteration:
        end = len(lines)

    results: list[dict] = []
    block: list[str] = []
    for line in lines[start:end]:
        stripped = line.strip()
        if not stripped:
            continue
        block.append(stripped)
        if stripped.startswith("Bet ID:"):
            row = _parse_midnite_history_block(block)
            if row is not None:
                results.append(row)
            block = []
    return results


def _parse_midnite_history_block(block: list[str]) -> dict | None:
    if len(block) < 6:
        return None
    display_label = block[0]
    notes_text = " ".join(block)
    stake = _find_labeled_money(lines=block, label="Stake")
    payout = _find_labeled_money(lines=block, label="Return")
    if stake is None:
        return None
    status = _normalize_midnite_status(block=block, payout=payout)
    if status is None:
        return None
    funding_kind = infer_funding_kind(
        explicit_funding_kind="free_bet" if "voucher used" in notes_text.lower() else "",
        notes=notes_text,
        bet_type="multiple",
        status=status,
        free_bet="voucher used" in notes_text.lower(),
    )

    if "bet builder" in display_label.lower() or "acca" in display_label.lower():
        row = _parse_midnite_multiple_block(
            block=block,
            display_label=display_label,
            stake=stake,
            payout=payout,
            status=status,
            funding_kind=funding_kind,
            notes_text=notes_text,
        )
        return row

    odds = parse_odds(block[2]) if len(block) > 2 else None
    market_event = block[3] if len(block) > 3 else ""
    if odds is None or " - " not in market_event:
        return None
    market, event = market_event.split(" - ", 1)
    return {
        "event": _strip_midnite_score(event),
        "market": market.strip(),
        "selection": block[1].strip(),
        "odds": odds,
        "stake": stake,
        "status": status,
        "payout_gbp": payout if payout is not None else 0.0,
        "bet_type": "single",
        "funding_kind": funding_kind,
        "notes": notes_text,
    }


def _parse_midnite_multiple_block(
    *,
    block: list[str],
    display_label: str,
    stake: float,
    payout: float | None,
    status: str,
    funding_kind: str,
    notes_text: str,
) -> dict | None:
    event = ""
    odds = None
    candidate_events: list[str] = []
    for index, line in enumerate(block[1:], start=1):
        if parse_odds(line) is not None and index > 0:
            odds = parse_odds(line)
            if index > 1:
                event = block[index - 1].strip()
            break
    for line in block:
        if " - " not in line or "Bet ID:" in line:
            continue
        _market, candidate_event = line.split(" - ", 1)
        candidate_events.append(_strip_midnite_score(candidate_event))
    if not event and candidate_events:
        event = "; ".join(candidate_events)
    if odds is None or not event:
        return None
    return {
        "event": event,
        "market": "Multiple",
        "selection": display_label,
        "odds": odds,
        "stake": stake,
        "status": status,
        "payout_gbp": payout if payout is not None else 0.0,
        "bet_type": "multiple",
        "funding_kind": funding_kind,
        "notes": notes_text,
    }


def _normalize_midnite_status(*, block: list[str], payout: float | None) -> str | None:
    lowered_block = [line.strip().lower() for line in block]
    if any(line == "cashed out" for line in lowered_block):
        return "cash_out"
    if any(line == "void" for line in lowered_block):
        return "void"
    if any("winner" in line for line in lowered_block):
        return "won"
    if payout is not None and payout > 0.0:
        return "won"
    if payout is not None:
        return "lost"
    return None


def _strip_midnite_score(value: str) -> str:
    return re.sub(r"\s+\d+-\d+\s*$", "", value).strip()


def _parse_betvictor_history_bet(bet: dict) -> dict | None:
    try:
        stake = float(bet.get("stake", 0.0) or 0.0)
        odds = float(bet.get("odds", 0.0) or 0.0)
    except (TypeError, ValueError):
        return None
    if stake <= 0.0 or odds <= 0.0:
        return None

    description = str(bet.get("description", "") or "").strip()
    bet_type = str(bet.get("betType", "") or "").strip() or "single"
    status = _normalize_history_status(str(bet.get("result", "") or ""))
    if status is None:
        return None

    parsed_description = _parse_betvictor_description(description)
    if parsed_description is None:
        selection = str(bet.get("summary", {}).get("name", "") or bet_type or "").strip()
        event = selection
        market = "Unknown"
    else:
        event = _clean_bv_markup(parsed_description["event"])
        market = _clean_bv_markup(parsed_description["market"])
        selection = _clean_bv_markup(parsed_description["selection"])
        summary_name = str(bet.get("summary", {}).get("name", "") or "").strip()
        if summary_name and ("[[" in parsed_description["selection"] or "{{" in parsed_description["selection"]):
            selection = summary_name
        bet_type = parsed_description["bet_type"]

    payout_gbp = None
    try:
        payout_gbp = float(bet.get("returns", 0.0) or 0.0)
    except (TypeError, ValueError):
        payout_gbp = None

    return {
        "event": event,
        "market": market,
        "selection": selection,
        "odds": odds,
        "stake": stake,
        "status": status,
        "payout_gbp": payout_gbp,
        "bet_type": bet_type.lower() or "single",
        "funding_kind": "free_bet" if bool(bet.get("bonusFunds")) else "cash",
        "notes": description,
        "occurred_at": str(bet.get("settledDate", "") or bet.get("createdDate", "") or ""),
    }


def _clean_bv_markup(value: str) -> str:
    cleaned = re.sub(r"\[\[([^\]]+)\]\]", r"\1", value)
    cleaned = re.sub(r"\{\{([^}]+)\}\}", r"\1", cleaned)
    return " ".join(cleaned.split()).strip()


def _parse_betano_api_history_bet(bet: dict) -> dict | None:
    try:
        stake = float(bet.get("stake", 0.0) or 0.0)
        odds = float(bet.get("odds", 0.0) or 0.0)
    except (TypeError, ValueError):
        return None
    if stake <= 0.0 or odds <= 0.0:
        return None

    description = str(bet.get("description", "") or "").strip()
    status = _normalize_history_status(str(bet.get("result", "") or ""))
    if status is None:
        return None
    event, market = _parse_betano_api_description(description)
    selection = str(bet.get("summary", {}).get("name", "") or bet.get("betType", "") or "").strip()
    if not selection:
        return None
    if not event:
        event = selection
    if not market:
        market = "Unknown"

    payout_gbp = None
    try:
        payout_gbp = float(bet.get("returns", 0.0) or 0.0)
    except (TypeError, ValueError):
        payout_gbp = None

    return {
        "event": event,
        "market": market,
        "selection": selection,
        "odds": odds,
        "stake": stake,
        "status": status,
        "payout_gbp": payout_gbp,
        "bet_type": str(bet.get("betType", "") or "single").lower(),
        "funding_kind": "free_bet" if bool(bet.get("bonusFunds")) else "cash",
        "notes": description,
        "occurred_at": str(bet.get("settledDate", "") or bet.get("createdDate", "") or ""),
    }


def _parse_betvictor_description(description: str) -> dict | None:
    if not description:
        return None
    match = BETVICTOR_DESCRIPTION_PREFIX_RE.match(description)
    if match is None:
        return None
    bet_type = str(match.group("bet_type") or "").strip()
    body = str(match.group("body") or "").strip()
    leg_matches: list[dict[str, str]] = []
    for part in [segment.strip() for segment in body.split(";") if segment.strip()]:
        leg_match = BETVICTOR_LEG_RE.match(part)
        if leg_match is None:
            continue
        leg_matches.append(
            {
                "selection": str(leg_match.group("selection") or "").strip(),
                "market": str(leg_match.group("market") or "").strip(),
                "event": str(leg_match.group("event") or "").strip(),
            }
        )
    if not leg_matches:
        return None
    if len(leg_matches) == 1:
        return {
            "bet_type": bet_type,
            "selection": leg_matches[0]["selection"],
            "market": leg_matches[0]["market"],
            "event": leg_matches[0]["event"],
        }
    return {
        "bet_type": bet_type,
        "selection": bet_type,
        "market": "Multiple",
        "event": "; ".join(leg["event"] for leg in leg_matches if leg["event"]),
    }


def _parse_betano_api_description(description: str) -> tuple[str, str]:
    if not description:
        return "", ""
    market_match = re.search(r"\[(?P<market>[^\]]+)\]", description)
    market = str(market_match.group("market") or "").replace(" - ", ", ").strip() if market_match else ""
    if "] - " not in description:
        return "", market
    trailing = description.split("] - ", 1)[1]
    segments = [segment.strip() for segment in trailing.split(" - ") if segment.strip()]
    if len(segments) >= 2:
        return segments[-2], market
    return "", market


def _extract_bet365_history_rows(*, lines: list[str], open_bet_keys: set[tuple]) -> list[dict]:
    results: list[dict] = []
    seen: set[tuple] = set()
    for block in _iter_bet365_history_blocks(lines):
        row = _parse_bet365_history_block(block)
        if row is None:
            continue
        key = _visible_bet_key(
            event=str(row.get("event", "")),
            market=str(row.get("market", "")),
            selection=str(row.get("selection", "")),
            odds=float(row.get("odds", 0.0)),
            stake=float(row.get("stake", 0.0)),
        )
        if key in open_bet_keys or key in seen:
            continue
        seen.add(key)
        results.append(row)
    return results


def _iter_bet365_history_blocks(lines: list[str]) -> list[list[str]]:
    header_indexes: list[int] = []
    for index, line in enumerate(lines):
        if _is_bet365_history_block_header(line):
            header_indexes.append(index)
            continue
        if _parse_bet365_header_stake(line) is None or index + 1 >= len(lines):
            continue
        if _is_bet365_history_type_line(lines[index + 1]):
            header_indexes.append(index)
    blocks: list[list[str]] = []
    for position, start in enumerate(header_indexes):
        end = header_indexes[position + 1] if position + 1 < len(header_indexes) else len(lines)
        block = [line for line in lines[start:end] if line.strip()]
        if (
            len(block) >= 2
            and _parse_bet365_header_stake(block[0]) is not None
            and _is_bet365_history_type_line(block[1])
            and not _is_bet365_history_block_header(block[0])
        ):
            block = [f"{block[0].strip()} {block[1].strip()}", *block[2:]]
        if block:
            blocks.append(block)
    return blocks


def _is_bet365_history_block_header(value: str) -> bool:
    lowered = value.strip().lower()
    if _parse_bet365_header_stake(value) is None:
        return False
    return _is_bet365_history_type_line(lowered)


def _is_bet365_history_type_line(value: str) -> bool:
    lowered = value.strip().lower()
    return any(
        marker in lowered
        for marker in ("single", "bet builder", "e/w", "double", "treble", "acca", "accumulator")
    )


def _parse_bet365_history_block(block: list[str]) -> dict | None:
    if not block:
        return None
    header = block[0]
    stake = _parse_bet365_header_stake(header)
    if stake is None:
        return None
    status = next((_normalize_history_status(line) for line in block[1:] if _looks_like_history_status(line)), None)
    if status is None:
        return None
    odds = next((parse_odds(line) for line in block[1:] if parse_odds(line) is not None), None)
    if odds is None:
        return None

    event = _find_bet365_block_event(block)
    market, selection = _find_bet365_block_market_and_selection(block, odds=odds)
    if event is None or market is None or selection is None:
        return None

    notes = " ".join(block)
    payout = _extract_bet365_block_payout(block=block, status=status)
    header_without_stake = BET365_HEADER_MONEY_PATTERN.sub("", header, count=1).strip().lower()
    return {
        "event": event,
        "market": market,
        "selection": selection,
        "odds": odds,
        "stake": stake,
        "status": status,
        "payout_gbp": payout,
        "bet_type": header_without_stake or "single",
        "funding_kind": infer_funding_kind(
            explicit_funding_kind="",
            notes=notes,
            bet_type=header_without_stake or "single",
            status=status,
            free_bet=("free bet" in notes.lower() or "bet credits" in notes.lower()),
        ),
        "notes": notes,
    }


def _parse_bet365_header_stake(value: str) -> float | None:
    match = BET365_HEADER_MONEY_PATTERN.match(value.strip())
    if match is None:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _extract_generic_history_rows(*, lines: list[str]) -> list[dict]:
    results: list[dict] = []
    seen: set[tuple] = set()
    for index, line in enumerate(lines):
        odds = parse_odds(line)
        if odds is None:
            continue
        selection = _find_previous_meaningful_line(lines=lines, start=index - 1)
        event = _find_previous_event_line(lines=lines, start=index - 2, selection=selection or "")
        market = _find_next_market_line(lines=lines, start=index + 1)
        if selection is None or market is None:
            continue
        if event is None:
            event = selection
        money_values = _collect_money_values(lines=lines, odds_index=index)
        stake = money_values[0] if money_values else None
        if stake is None:
            continue
        status, status_notes = _detect_history_status(lines=lines, odds_index=index)
        if status is None:
            continue
        payout = _infer_payout(status=status, money_values=money_values)
        key = _visible_bet_key(
            event=event,
            market=market,
            selection=selection,
            odds=odds,
            stake=stake,
        )
        if key in seen:
            continue
        seen.add(key)
        notes = " ".join(status_notes)
        results.append(
            {
                "event": event,
                "market": market,
                "selection": selection,
                "odds": odds,
                "stake": stake,
                "status": status,
                "payout_gbp": payout,
                "bet_type": _find_bet_type_marker(lines=lines, odds_index=index),
                "funding_kind": infer_funding_kind(
                    explicit_funding_kind="",
                    notes=notes,
                    bet_type=_find_bet_type_marker(lines=lines, odds_index=index),
                    status=status,
                    free_bet="free bet" in notes.lower(),
                ),
                "notes": notes,
            }
        )
    return results


def _extract_betano_history_rows(*, lines: list[str]) -> list[dict]:
    section_end = next(
        (
            index
            for index, line in enumerate(lines)
            if line.strip().lower() == "to see all settled bets check your"
        ),
        len(lines),
    )
    results: list[dict] = []
    seen: set[tuple] = set()
    index = 0
    while index < section_end:
        status = _normalize_betano_status_line(lines[index])
        if status is None:
            index += 1
            continue
        next_index = _find_next_betano_history_block_start(
            lines=lines,
            start=index + 1,
            end=section_end,
        )
        block = lines[index:next_index]
        row = _parse_betano_history_block(block)
        if row is not None:
            key = _visible_bet_key(
                event=str(row.get("event", "")),
                market=str(row.get("market", "")),
                selection=str(row.get("selection", "")),
                odds=float(row.get("odds", 0.0)),
                stake=float(row.get("stake", 0.0)),
            )
            if key not in seen:
                seen.add(key)
                results.append(row)
        index = next_index
    return results


def _find_next_betano_history_block_start(*, lines: list[str], start: int, end: int) -> int:
    for index in range(start, end):
        if _normalize_betano_status_line(lines[index]) is not None:
            return index
    return end


def _parse_betano_history_block(block: list[str]) -> dict | None:
    if len(block) < 8:
        return None
    status = _normalize_betano_status_line(block[0])
    if status is None:
        return None
    index = 1
    bet_type = "single"
    candidate_type = block[index].strip().lower() if index < len(block) else ""
    if candidate_type in BET_TYPE_MARKERS:
        bet_type = candidate_type
        index += 1
        if bet_type != "single":
            return None
    if index + 4 >= len(block):
        return None
    selection = block[index].strip()
    odds = parse_odds(block[index + 1])
    market = block[index + 2].strip()
    event = block[index + 4].strip()
    if not selection or odds is None or not market or not event:
        return None
    stake, payout, notes = _extract_betano_money_block(block=block)
    if stake is None:
        return None
    notes_text = " ".join(notes)
    return {
        "event": event,
        "market": market,
        "selection": selection,
        "odds": odds,
        "stake": stake,
        "status": status,
        "payout_gbp": payout,
        "bet_type": bet_type,
        "funding_kind": infer_funding_kind(
            explicit_funding_kind="",
            notes=notes_text,
            bet_type=bet_type,
            status=status,
            free_bet=("free bet" in notes_text.lower()),
        ),
        "notes": notes_text,
    }


def _extract_betano_money_block(*, block: list[str]) -> tuple[float | None, float | None, list[str]]:
    stake: float | None = None
    payout: float | None = None
    notes = list(block)
    for index, candidate in enumerate(block):
        lowered = candidate.strip().lower()
        if lowered == "total stake" and index + 1 < len(block):
            stake = _parse_inline_gbp(block[index + 1])
        if lowered == "total returns" and index + 1 < len(block):
            payout_line = block[index + 1].strip()
            payout = 0.0 if payout_line == "-" else _parse_inline_gbp(payout_line)
    return stake, payout, notes


def _normalize_betano_status_line(value: str) -> str | None:
    lowered = value.strip().lower()
    exact_statuses = {
        "lost": "lost",
        "won": "won",
        "settled": "settled",
        "void": "void",
        "cashed out": "cash_out",
        "cash out": "cash_out",
    }
    return exact_statuses.get(lowered)


def _parse_inline_gbp(value: str) -> float | None:
    parsed = parse_money(value)
    if parsed is not None:
        return parsed
    match = INLINE_GBP_PATTERN.search(value)
    if match is None:
        return None
    try:
        return float(match.group(1))
    except ValueError:
        return None


def _find_bet365_block_event(block: list[str]) -> str | None:
    content_lines = _bet365_block_content_lines(block)
    for line in content_lines[1:]:
        candidate = line.strip()
        lowered = candidate.lower()
        if " v " in lowered or " - " in candidate or re.match(r"^\d{1,2}:\d{2}\s*-\s*", candidate):
            return candidate
    team_lines: list[str] = []
    for line in content_lines[1:]:
        candidate = line.strip()
        if _is_bet365_meta_line(candidate):
            continue
        if _looks_like_market_line(candidate) or ":" in candidate:
            continue
        if looks_like_date_fragment(candidate):
            continue
        if parse_odds(candidate) is not None or parse_money(candidate) is not None:
            continue
        if candidate.isdigit():
            continue
        team_lines.append(candidate)
    if len(team_lines) >= 2:
        return f"{team_lines[-2]} v {team_lines[-1]}"
    return None


def _find_bet365_block_market_and_selection(
    block: list[str],
    *,
    odds: float,
) -> tuple[str | None, str | None]:
    content_lines = _bet365_block_content_lines(block)
    odds_index = next(
        (
            index
            for index, line in enumerate(content_lines)
            if parse_odds(line) is not None and float(parse_odds(line)) == float(odds)
        ),
        None,
    )
    if odds_index is None:
        return None, None

    for index in range(odds_index + 1, min(len(content_lines), odds_index + 4)):
        if ":" not in content_lines[index]:
            continue
        prefix, remainder = content_lines[index].split(":", 1)
        selection = remainder.strip()
        market = None
        for candidate in content_lines[index + 1 : min(len(content_lines), index + 4)]:
            if _looks_like_market_line(candidate):
                market = candidate.strip()
                break
        return market or prefix.strip(), selection or None

    market = None
    selection = None
    for candidate in reversed(content_lines[1:odds_index]):
        stripped = candidate.strip()
        if _is_bet365_selection_line(stripped):
            selection = stripped
            break
    for candidate in content_lines[odds_index + 1 :]:
        stripped = candidate.strip()
        if market is None and _looks_like_market_line(stripped):
            market = stripped
            continue
        if selection is None and market is not None and _is_bet365_selection_line(stripped):
            selection = stripped
            break
    return market, selection


def _extract_bet365_block_payout(*, block: list[str], status: str) -> float | None:
    for marker in ("Net Return", "Return", "Returns"):
        for index, candidate in enumerate(block):
            if candidate.strip().lower() != marker.lower():
                continue
            for following in block[index + 1 : min(len(block), index + 4)]:
                amount = parse_money(following)
                if amount is not None:
                    return amount
    if status == "lost":
        return 0.0
    returned_index = next(
        (index for index, candidate in enumerate(block) if candidate.strip().lower() == "returned"),
        None,
    )
    if returned_index is not None:
        for previous in reversed(block[max(0, returned_index - 3) : returned_index]):
            amount = parse_money(previous)
            if amount is not None:
                return amount
    return None


def _extract_kambi_history_rows(*, lines: list[str]) -> list[dict]:
    results: list[dict] = []
    seen: set[tuple] = set()
    start = 0
    for index, line in enumerate(lines):
        if line.strip().lower() == "filter your bets by date":
            start = index + 1
            break
    index = start
    while index < len(lines):
        if not _looks_like_kambi_history_block_start(lines[index]):
            index += 1
            continue
        next_index = _find_next_kambi_history_block_start(lines=lines, start=index + 1)
        block = [line for line in lines[index:next_index] if line.strip()]
        row = _parse_kambi_history_block(block)
        if row is not None:
            key = _visible_bet_key(
                event=str(row.get("event", "")),
                market=str(row.get("market", "")),
                selection=str(row.get("selection", "")),
                odds=float(row.get("odds", 0.0)),
                stake=float(row.get("stake", 0.0)),
            )
            if key not in seen:
                seen.add(key)
                results.append(row)
        index = next_index
    return results


def _looks_like_kambi_history_block_start(value: str) -> bool:
    stripped = value.strip()
    return bool(stripped and KAMBI_HISTORY_BLOCK_RE.match(stripped))


def _find_next_kambi_history_block_start(*, lines: list[str], start: int) -> int:
    for index in range(start, len(lines)):
        if _looks_like_kambi_history_block_start(lines[index]):
            return index
    return len(lines)


def _parse_kambi_history_block(block: list[str]) -> dict | None:
    if not block:
        return None
    header_match = KAMBI_HISTORY_BLOCK_RE.match(block[0].strip())
    if header_match is None:
        return None
    bet_type = str(header_match.group("bet_type") or "single").strip().lower()
    status = _normalize_history_status(str(header_match.group("status") or ""))
    if status is None:
        status = next(
            (
                normalized
                for normalized in (
                    _normalize_history_status(candidate)
                    for candidate in block[1:6]
                )
                if normalized is not None
            ),
            None,
        )
    if status is None or status == "placed":
        return None

    date_index = next(
        (
            index
            for index, candidate in enumerate(block)
            if KAMBI_DATE_LINE_RE.match(candidate.strip())
        ),
        None,
    )
    if date_index is None:
        return None
    detail_start = date_index + 1
    if detail_start < len(block) and block[detail_start].strip().lower().startswith("coupon id:"):
        detail_start += 1
    detail_end = next(
        (
            index
            for index, candidate in enumerate(block[detail_start:], start=detail_start)
            if candidate.strip().lower() in {
                "stake:",
                "stake",
                "settings",
                "online sportsbook betting at leovegas",
                "online sports betting at leovegas",
                "casino",
            }
        ),
        len(block),
    )
    detail_lines = [line.strip() for line in block[detail_start:detail_end] if line.strip()]
    if not detail_lines:
        return None

    if bet_type == "single":
        market, selection, event = _parse_kambi_single_selection(detail_lines)
    else:
        market, selection, event = _parse_kambi_multiple_selection(
            detail_lines=detail_lines,
            bet_type=bet_type,
        )
    if not selection or not market or not event:
        return None

    odds = _parse_kambi_odds(block)
    stake = _find_labeled_money(lines=block, label="Stake:") or _find_labeled_money(lines=block, label="Stake")
    payout = _find_labeled_money(lines=block, label="Payout:") or _find_labeled_money(lines=block, label="Payout")
    if payout is None and status == "lost":
        payout = 0.0
    if payout is None and status == "void":
        payout = stake
    if odds is None or stake is None:
        return None

    notes_text = " ".join(block)
    return {
        "event": event,
        "market": market,
        "selection": selection,
        "odds": odds,
        "stake": stake,
        "status": status,
        "payout_gbp": payout,
        "bet_type": bet_type,
        "funding_kind": _infer_kambi_funding_kind(notes_text),
        "notes": notes_text,
    }


def _parse_kambi_single_selection(detail_lines: list[str]) -> tuple[str | None, str | None, str | None]:
    first_line = detail_lines[0]
    if ":" not in first_line:
        return None, None, None
    market, selection = first_line.split(":", 1)
    normalized_market = market.strip()
    normalized_selection = selection.strip()
    if normalized_market.lower() == "to win":
        event = detail_lines[1].strip() if len(detail_lines) > 1 else normalized_selection
        selection_name, market_name = _split_kambi_to_win_selection(normalized_selection)
        return market_name or "Winner", selection_name, event
    event = detail_lines[1].strip() if len(detail_lines) > 1 else normalized_selection
    return normalized_market, normalized_selection, event


def _split_kambi_to_win_selection(value: str) -> tuple[str, str]:
    stripped = value.strip()
    if stripped.endswith(")") and "(" in stripped:
        selection, market = stripped.rsplit("(", 1)
        return selection.strip(), market.rstrip(")").strip()
    return stripped, "Winner"


def _parse_kambi_multiple_selection(*, detail_lines: list[str], bet_type: str) -> tuple[str | None, str | None, str | None]:
    events: list[str] = []
    for index, line in enumerate(detail_lines):
        if ":" not in line or index + 1 >= len(detail_lines):
            continue
        candidate_event = detail_lines[index + 1].strip()
        if candidate_event and candidate_event not in events:
            events.append(candidate_event)
    event = "; ".join(events).strip() or detail_lines[0].strip()
    return "Multiple", bet_type.title(), event


def _parse_kambi_odds(block: list[str]) -> float | None:
    for index, line in enumerate(block):
        if line.strip() != "@":
            continue
        for following in block[index + 1:index + 4]:
            normalized_following = following.strip().strip("()")
            odds = parse_odds(normalized_following)
            if odds is not None:
                return odds
    for line in block:
        odds = parse_odds(line.strip().strip("()"))
        if odds is not None:
            return odds
    return None


def _infer_kambi_funding_kind(notes: str) -> str:
    lowered = notes.lower()
    if "free bet" in lowered:
        return "free_bet"
    if any(marker in lowered for marker in ("second chance", "profit boost", "odds boost")):
        return "bonus"
    return "cash"


def _bet365_block_content_lines(block: list[str]) -> list[str]:
    for index, line in enumerate(block):
        if line.strip().lower() in {"stake", "return", "returns", "net return"}:
            return block[:index]
    return block


def _looks_like_market_line(value: str) -> bool:
    lowered = value.strip().lower()
    return any(
        keyword in lowered
        for keyword in (
            "result",
            "score",
            "both teams",
            "winner",
            "to win",
            "handicap",
            "goals",
            "match odds",
            "each way",
        )
    )


def _is_bet365_meta_line(value: str) -> bool:
    lowered = value.strip().lower()
    if not lowered:
        return True
    if lowered in {"lost", "won", "returned", "cash out", "cashed out", "view more"}:
        return True
    if lowered.startswith("(") and lowered.endswith(")"):
        return True
    if "bet credits" in lowered or "money back" in lowered:
        return True
    return _looks_like_history_status(value)


def _is_bet365_selection_line(value: str) -> bool:
    stripped = value.strip()
    if _is_bet365_meta_line(stripped):
        return False
    if _looks_like_market_line(stripped):
        return False
    if parse_money(stripped) is not None or parse_odds(stripped) is not None:
        return False
    if stripped.isdigit():
        return False
    if looks_like_date_fragment(stripped):
        return False
    if " v " in stripped.lower() or " - " in stripped:
        return False
    return True


def _extract_betuk_history_rows(*, lines: list[str]) -> list[dict]:
    results: list[dict] = []
    seen: set[tuple] = set()
    index = 0
    while index < len(lines):
        bet_type = lines[index].strip().lower()
        if bet_type not in BET_TYPE_MARKERS:
            index += 1
            continue

        odds_index = index + 1
        if odds_index < len(lines) and lines[odds_index] == "@":
            odds_index += 1
        odds = parse_odds(lines[odds_index]) if odds_index < len(lines) else None
        status_line = lines[odds_index + 1] if odds_index + 1 < len(lines) else ""
        if odds is None or not _looks_like_history_status(status_line):
            index += 1
            continue
        if bet_type != "single":
            index = _find_next_history_block_start(lines=lines, start=odds_index + 2)
            continue

        detail_start = odds_index + 2
        stake_marker_index = _find_betuk_stake_marker_index(lines=lines, start=detail_start)
        if stake_marker_index is None:
            index += 1
            continue
        detail_lines = [
            line
            for line in lines[detail_start:stake_marker_index]
            if not _is_betuk_meta_line(line)
        ]
        market, selection = _parse_betuk_market_and_selection(detail_lines)
        event = _find_betuk_event_line(detail_lines=detail_lines)
        stake, payout, notes = _extract_betuk_money_block(
            lines=lines,
            start=stake_marker_index,
        )
        status = _normalize_history_status(status_line)
        if (
            selection is None
            or event is None
            or market is None
            or stake is None
            or status is None
        ):
            index = _find_next_history_block_start(lines=lines, start=stake_marker_index + 1)
            continue
        key = _visible_bet_key(
            event=event,
            market=market,
            selection=selection,
            odds=odds,
            stake=stake,
        )
        if key not in seen:
            seen.add(key)
            if payout is None and status == "lost":
                payout = 0.0
            notes_text = " ".join([status_line, *detail_lines, *notes])
            results.append(
                {
                    "event": event,
                    "market": market,
                    "selection": selection,
                    "odds": odds,
                    "stake": stake,
                    "status": status,
                    "payout_gbp": payout,
                    "bet_type": bet_type,
                    "funding_kind": infer_funding_kind(
                        explicit_funding_kind="",
                        notes=notes_text,
                        bet_type=bet_type,
                        status=status,
                        free_bet="free bet" in notes_text.lower(),
                    ),
                    "notes": notes_text,
                }
            )
        index = _find_next_history_block_start(lines=lines, start=stake_marker_index + 1)
    return results


def _find_previous_meaningful_line(*, lines: list[str], start: int) -> str | None:
    for index in range(start, max(-1, start - 6), -1):
        if index < 0:
            break
        candidate = lines[index]
        if _is_noise_line(candidate):
            continue
        return candidate
    return None


def _find_previous_event_line(*, lines: list[str], start: int, selection: str) -> str | None:
    for index in range(start, max(-1, start - 8), -1):
        if index < 0:
            break
        candidate = lines[index]
        if _is_noise_line(candidate):
            continue
        if candidate == selection:
            continue
        return candidate
    return None


def _find_next_market_line(*, lines: list[str], start: int) -> str | None:
    for index in range(start, min(len(lines), start + 6)):
        candidate = lines[index]
        if _is_noise_line(candidate):
            continue
        if parse_money(candidate) is not None or parse_odds(candidate) is not None:
            continue
        return candidate
    return None


def _build_bet365_event_label(*, lines: list[str], odds_index: int, selection: str, market: str) -> str:
    event_parts: list[str] = []
    for index in range(odds_index + 1, min(len(lines), odds_index + 8)):
        candidate = lines[index]
        if candidate in {"Stake", "Returns", "Net Return"}:
            break
        if _is_noise_line(candidate):
            continue
        if parse_odds(candidate) is not None:
            continue
        if candidate == market:
            continue
        if looks_like_date_fragment(candidate):
            continue
        event_parts.append(candidate)
    meaningful_parts = [part for part in event_parts if not looks_like_date_fragment(part)]
    if len(meaningful_parts) >= 2:
        return f"{meaningful_parts[0]} v {meaningful_parts[-1]}"
    if meaningful_parts:
        return f"{selection} v {meaningful_parts[-1]}"
    return selection


def _find_betuk_stake_marker_index(*, lines: list[str], start: int) -> int | None:
    for index in range(start, len(lines)):
        lowered = lines[index].strip().lower()
        if lowered in BET_TYPE_MARKERS:
            return None
        if lowered in {"stake", "stake:"}:
            return index
    return None


def _find_betuk_event_line(*, detail_lines: list[str]) -> str | None:
    for candidate in reversed(detail_lines):
        if _is_betuk_meta_line(candidate):
            continue
        if " - " in candidate:
            return candidate
        if re.match(r"^\d{1,2}:\d{2}\s", candidate):
            return candidate
    return detail_lines[-1] if detail_lines else None


def _parse_betuk_market_and_selection(detail_lines: list[str]) -> tuple[str | None, str | None]:
    for candidate in detail_lines:
        if ":" not in candidate:
            continue
        market, selection = candidate.split(":", 1)
        normalized_selection = selection.strip()
        normalized_selection = re.sub(r"\s*\([^)]*\)\s*$", "", normalized_selection).strip()
        return market.strip(), normalized_selection or None
    if not detail_lines:
        return None, None
    return detail_lines[0], detail_lines[0]


def _extract_betuk_money_block(*, lines: list[str], start: int) -> tuple[float | None, float | None, list[str]]:
    stake: float | None = None
    payout: float | None = None
    notes: list[str] = []
    for index in range(start, min(len(lines), start + 8)):
        candidate = lines[index]
        lowered = candidate.strip().lower()
        if index > start and lowered in BET_TYPE_MARKERS:
            break
        if lowered in {"settings", "online sportsbook markets at betuk", "featured", "all sports"}:
            break
        notes.append(candidate)
        amount = parse_money(candidate)
        if amount is None:
            continue
        previous = lines[index - 1].strip().lower() if index > 0 else ""
        if previous in {"stake", "stake:"} and stake is None:
            stake = amount
        elif previous in {"payout", "payout:", "return", "return:"}:
            payout = amount
    return stake, payout, notes


def _find_next_history_block_start(*, lines: list[str], start: int) -> int:
    for index in range(start, len(lines)):
        if lines[index].strip().lower() in BET_TYPE_MARKERS:
            return index
    return len(lines)


def _collect_money_values(*, lines: list[str], odds_index: int) -> list[float]:
    values: list[float] = []
    seen_lines: set[int] = set()
    for index in range(max(0, odds_index - 3), min(len(lines), odds_index + 12)):
        if index in seen_lines:
            continue
        seen_lines.add(index)
        amount = parse_money(lines[index])
        if amount is None:
            continue
        values.append(amount)
    return values


def _detect_history_status(*, lines: list[str], odds_index: int) -> tuple[str | None, list[str]]:
    notes: list[str] = []
    start = max(0, odds_index - 5)
    end = min(len(lines), odds_index + 10)
    window = lines[start:end]
    closest_history: tuple[int, str] | None = None
    closest_open_distance: int | None = None
    for relative_index, line in enumerate(window, start=start - odds_index):
        lowered = line.lower()
        if any(hint in lowered for hint in OPEN_STATUS_HINTS) and "cashed out" not in lowered:
            notes.append(line)
            distance = abs(relative_index)
            if closest_open_distance is None or distance < closest_open_distance:
                closest_open_distance = distance
        for hint, status in HISTORY_STATUS_HINTS.items():
            if hint in lowered:
                distance = abs(relative_index)
                if closest_history is None or distance < closest_history[0]:
                    closest_history = (distance, status)
    if closest_history is None:
        return None, notes
    if closest_open_distance is not None and closest_open_distance < closest_history[0]:
        return None, notes
    notes.extend(window)
    return closest_history[1], notes


def _looks_like_history_status(value: str) -> bool:
    lowered = value.strip().lower()
    return any(hint in lowered for hint in HISTORY_STATUS_HINTS)


def _normalize_history_status(value: str) -> str | None:
    lowered = value.strip().lower()
    for hint, status in HISTORY_STATUS_HINTS.items():
        if hint in lowered:
            return status
    return None


def _infer_payout(*, status: str, money_values: list[float]) -> float | None:
    if status in {"lost"}:
        return 0.0
    if len(money_values) >= 2:
        return max(money_values[1:])
    if status in {"won", "cash_out", "settled"} and money_values:
        return money_values[-1]
    return None


def _find_bet_type_marker(*, lines: list[str], odds_index: int) -> str:
    for index in range(max(0, odds_index - 6), odds_index + 1):
        candidate = lines[index].strip().lower()
        if candidate in BET_TYPE_MARKERS:
            return candidate
    return "single"


def _build_runtime_ledger_entry(*, venue: str, row: dict, occurred_at: str) -> dict:
    payout_gbp = row.get("payout_gbp")
    stake_gbp = float(row["stake"])
    funding_kind = normalize_funding_kind(row.get("funding_kind"))
    if not funding_kind or funding_kind == "unknown":
        funding_kind = infer_funding_kind(
            explicit_funding_kind="",
            notes=row.get("notes"),
            bet_type=row.get("bet_type"),
            status=row.get("status"),
            free_bet=False,
        )
    if not funding_kind or funding_kind == "unknown":
        funding_kind = "cash"
    realised_pnl_gbp = None
    if payout_gbp is not None:
        realised_pnl_gbp = round(float(payout_gbp) - stake_gbp, 2)
        if funding_kind == "free_bet" and str(row.get("status", "") or "").lower() == "lost":
            realised_pnl_gbp = 0.0
    entry = {
        "occurred_at": str(row.get("occurred_at", "") or occurred_at),
        "platform": normalize_vendor(venue),
        "activity_type": "bet_settled",
        "status": str(row.get("status", "") or "settled"),
        "platform_kind": "sportsbook",
        "event": str(row.get("event", "") or ""),
        "market": str(row.get("market", "") or ""),
        "selection": str(row.get("selection", "") or ""),
        "bet_type": str(row.get("bet_type", "") or "single"),
        "market_family": infer_market_family(
            explicit_market_family=None,
            market=str(row.get("market", "") or ""),
        ),
        "funding_kind": funding_kind,
        "currency": "GBP",
        "stake_gbp": stake_gbp,
        "odds_decimal": float(row["odds"]),
        "payout_gbp": float(payout_gbp) if payout_gbp is not None else None,
        "realised_pnl_gbp": realised_pnl_gbp,
        "source_file": "live_browser",
        "source_kind": "auto_live_bookmaker_history",
        "description": str(row.get("notes", "") or str(row.get("status", "") or "")),
    }
    entry["entry_id"] = _build_entry_id(entry)
    return entry


def _visible_bet_key(*, event: str, market: str, selection: str, odds: float, stake: float) -> tuple:
    return (
        event.strip().lower(),
        market.strip().lower(),
        selection.strip().lower(),
        round(float(odds), 4),
        round(float(stake), 2),
    )


def _build_entry_id(entry: dict) -> str:
    digest = hashlib.sha1(
        "|".join(
            [
                normalize_vendor(entry.get("platform")),
                str(entry.get("event", "") or "").strip().lower(),
                str(entry.get("market", "") or "").strip().lower(),
                str(entry.get("selection", "") or "").strip().lower(),
                str(entry.get("status", "") or "").strip().lower(),
                f"{float(entry.get('stake_gbp', 0.0) or 0.0):.2f}",
                f"{float(entry.get('odds_decimal', 0.0) or 0.0):.4f}",
                f"{float(entry.get('payout_gbp', 0.0) or 0.0):.2f}",
            ]
        ).encode("utf-8")
    ).hexdigest()[:12]
    return f"live_history:{normalize_vendor(entry.get('platform'))}:{digest}"


def _is_noise_line(value: str) -> bool:
    lowered = value.strip().lower()
    if not lowered:
        return True
    if lowered in IGNORE_LINES:
        return True
    if lowered in BET_TYPE_MARKERS:
        return True
    if parse_money(value) is not None:
        return True
    return False


def _parse_fanteam_float(value: object) -> float | None:
    if value in (None, "", [], {}):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    parsed = parse_money(str(value))
    if parsed is not None:
        return parsed
    parsed = parse_odds(str(value))
    if parsed is not None:
        return float(parsed)
    try:
        return float(str(value).strip())
    except ValueError:
        return None


def _infer_fanteam_funding_kind(*, row: dict, notes: str) -> str:
    explicit = normalize_funding_kind(
        str(row.get("funding_kind") or row.get("fundingKind") or "")
    )
    if explicit and explicit != "unknown":
        return explicit
    if row.get("isFreeBet") or row.get("freeBet") or row.get("bonusStake"):
        return "free_bet"
    return infer_funding_kind(
        explicit_funding_kind="",
        notes=notes,
        bet_type=str(row.get("betType") or row.get("betKind") or ""),
        status=str(row.get("status") or row.get("betStatus") or row.get("result") or ""),
        free_bet=False,
    ) or "cash"


def _is_betuk_meta_line(value: str) -> bool:
    lowered = value.strip().lower()
    if not lowered:
        return True
    if lowered.startswith("coupon id:"):
        return True
    if "•" in value:
        return True
    return False
