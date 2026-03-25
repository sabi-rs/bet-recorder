from __future__ import annotations

from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse
import json

BETS_OBSERVATIONS_FILENAME = "bets-observations.json"
TRACKER_CACHE_STORAGE_KEY = "widget_cache_bets_data"
DISPLAY_STORAGE_KEY_PREFIXES = (
  "fo_colwidths:",
  "fo_visible_cols:",
  "fo_display_visibility:",
)
EXCHANGE_BOOKS = {"smarkets", "matchbook", "betdaq", "betfair"}


def build_bets_page_metadata(*, url: str, local_storage: dict[str, Any]) -> dict[str, Any]:
  storage_snapshot = select_bets_storage_snapshot(local_storage)
  if not storage_snapshot:
    return {}

  metadata: dict[str, Any] = {
    "storage_snapshot": storage_snapshot,
  }
  tracker_cache_probe = build_tracker_cache_probe(storage_snapshot)
  if tracker_cache_probe is not None:
    metadata["trackerCacheProbe"] = tracker_cache_probe
  if "/bets" in urlparse(url).path:
    metadata["page_kind"] = "bets"
  return metadata


def select_bets_storage_snapshot(local_storage: dict[str, Any]) -> dict[str, Any]:
  snapshot: dict[str, Any] = {}
  for key, value in local_storage.items():
    if key == TRACKER_CACHE_STORAGE_KEY or any(
      key.startswith(prefix) for prefix in DISPLAY_STORAGE_KEY_PREFIXES
    ):
      snapshot[key] = _coerce_jsonish_value(value)
  return snapshot


def build_tracker_cache_probe(storage_snapshot: dict[str, Any]) -> dict[str, Any] | None:
  raw_payload = _coerce_jsonish_value(storage_snapshot.get(TRACKER_CACHE_STORAGE_KEY))
  if not isinstance(raw_payload, dict):
    return None

  data = raw_payload.get("data")
  bets = data.get("bets") if isinstance(data, dict) else None
  first_bet = bets[0] if isinstance(bets, list) and bets and isinstance(bets[0], dict) else None
  return {
    "storageKey": TRACKER_CACHE_STORAGE_KEY,
    "topLevelKeys": list(raw_payload.keys()),
    "dataKeys": list(data.keys()) if isinstance(data, dict) else [],
    "betCount": len(bets) if isinstance(bets, list) else 0,
    "firstBet": dict(first_bet) if first_bet is not None else None,
  }


def refresh_bets_observations(*, run_dir: Path, events_path: Path, transport_path: Path | None) -> None:
  page_url, captured_at, storage_snapshot, tracker_cache_probe = _load_latest_bets_page_context(
    events_path
  )
  transport_events = _load_jsonl(transport_path) if transport_path is not None else []

  observations: dict[str, Any] = {}
  if tracker_cache_probe is not None:
    observations["trackerCacheProbe"] = tracker_cache_probe

  transport_probe = build_bets_transport_and_math_probe(
    captured_at=captured_at,
    page_url=page_url,
    transport_events=transport_events,
    storage_snapshot=storage_snapshot,
  )
  if transport_probe is not None:
    observations["betsTransportAndMathProbe"] = transport_probe

  output_path = run_dir / BETS_OBSERVATIONS_FILENAME
  if not observations:
    output_path.unlink(missing_ok=True)
    return

  output_path.write_text(json.dumps(observations, indent=2) + "\n", encoding="utf-8")


def build_bets_transport_and_math_probe(
  *,
  captured_at: str | None,
  page_url: str | None,
  transport_events: list[dict[str, Any]],
  storage_snapshot: dict[str, Any] | None,
) -> dict[str, Any] | None:
  tracker_cache = _tracker_cache_data(storage_snapshot)
  books_payload = _extract_latest_payload(
    transport_events,
    predicate=lambda request: request["path"] == "/rest/v1/books",
  )
  bets_payload = _extract_latest_payload(
    transport_events,
    predicate=lambda request: request["path"] == "/rest/v1/bets",
  )

  books = _coerce_books_payload(books_payload)
  if not books and isinstance(tracker_cache, dict):
    books = _coerce_books_payload(tracker_cache.get("books"))

  bets = _coerce_bets_payload(bets_payload)
  if not bets and isinstance(tracker_cache, dict):
    bets = _coerce_bets_payload(tracker_cache.get("bets"))

  requests = _extract_network_requests(transport_events, page_url=page_url)
  contract = _build_rest_query_contract(transport_events)
  shipped_storage_keys = _shipped_storage_keys(storage_snapshot)
  visible_rows = _build_visible_rows_audit(bets, books)
  live_metrics = _build_live_metrics(visible_rows)
  auditable_math = _build_auditable_math(visible_rows)

  if (
    not requests
    and not contract
    and not shipped_storage_keys
    and not visible_rows
    and not live_metrics
  ):
    return None

  return {
    "captured_at": captured_at,
    "pageUrl": page_url,
    "networkRequests": requests,
    "restQueryContract": contract,
    "shippedStorageKeys": shipped_storage_keys,
    "liveMetrics": live_metrics,
    "visibleRowsAudit": visible_rows,
    "auditableMath": auditable_math,
    "shippedProfitLogic": {
      "payoutFormula": "odds_decimal * (stake_cents / 100)",
      "wonProfitPriority": [
        "gross profit minus commission_cents when present",
        "else gross profit minus commission_pct",
        "else fallback exchange commission from settings for exchange books",
      ],
      "clvDisplay": "close_odds_decimal when present",
      "beatClvMetric": "percentage of filtered bets where clv_pct > 0",
    },
  }


def _load_latest_bets_page_context(
  events_path: Path,
) -> tuple[str | None, str | None, dict[str, Any] | None, dict[str, Any] | None]:
  if not events_path.exists():
    return None, None, None, None

  latest_url: str | None = None
  latest_captured_at: str | None = None
  latest_storage_snapshot: dict[str, Any] | None = None
  latest_tracker_cache_probe: dict[str, Any] | None = None
  for event in _load_jsonl(events_path):
    metadata = event.get("metadata")
    if not isinstance(metadata, dict):
      continue
    storage_snapshot = metadata.get("storage_snapshot")
    tracker_cache_probe = metadata.get("trackerCacheProbe")
    page_kind = str(metadata.get("page_kind", "") or "")
    if storage_snapshot is None and tracker_cache_probe is None and page_kind != "bets":
      continue
    latest_url = str(event.get("url", "") or "") or latest_url
    latest_captured_at = str(event.get("captured_at", "") or "") or latest_captured_at
    if isinstance(storage_snapshot, dict):
      latest_storage_snapshot = storage_snapshot
    if isinstance(tracker_cache_probe, dict):
      latest_tracker_cache_probe = tracker_cache_probe

  return latest_url, latest_captured_at, latest_storage_snapshot, latest_tracker_cache_probe


def _load_jsonl(path: Path | None) -> list[dict[str, Any]]:
  if path is None or not path.exists():
    return []
  rows: list[dict[str, Any]] = []
  with path.open(encoding="utf-8") as handle:
    for line in handle:
      if not line.strip():
        continue
      payload = json.loads(line)
      if isinstance(payload, dict):
        rows.append(payload)
  return rows


def _tracker_cache_data(storage_snapshot: dict[str, Any] | None) -> dict[str, Any] | None:
  if not isinstance(storage_snapshot, dict):
    return None
  payload = _coerce_jsonish_value(storage_snapshot.get(TRACKER_CACHE_STORAGE_KEY))
  if not isinstance(payload, dict):
    return None
  data = payload.get("data")
  return dict(data) if isinstance(data, dict) else None


def _extract_network_requests(
  transport_events: list[dict[str, Any]],
  *,
  page_url: str | None,
) -> list[str]:
  rendered: list[str] = []
  seen: set[str] = set()
  for event in transport_events:
    request = _request_descriptor(event)
    if request is None:
      continue
    line = f"{request['method']} {_display_url(request['url'], page_url=page_url)}"
    if line in seen:
      continue
    seen.add(line)
    rendered.append(line)
  return rendered


def _build_rest_query_contract(transport_events: list[dict[str, Any]]) -> dict[str, Any]:
  contract: dict[str, Any] = {}
  for event in transport_events:
    request = _request_descriptor(event)
    if request is None:
      continue
    if request["path"] == "/rest/v1/books":
      fields = _select_fields(request["url"])
      if fields:
        contract["booksSelectFields"] = fields
    elif request["path"] == "/rest/v1/bets":
      fields = _select_fields(request["url"])
      if fields:
        contract["betsSelectFields"] = fields
    elif request["path"].startswith("/rest/v1/rpc/"):
      contract["leaderboardRpc"] = request["path"].split("/rest/v1/rpc/", 1)[1]
    elif request["path"] == "/api/clv/compute":
      contract["clvComputeEndpoint"] = request["path"]
      auth_headers = _normalized_auth_headers(request["headers"])
      if auth_headers:
        contract["clvComputeAuth"] = auth_headers
  return contract


def _extract_latest_payload(
  transport_events: list[dict[str, Any]],
  *,
  predicate,
) -> Any:
  latest_payload = None
  for event in transport_events:
    request = _request_descriptor(event)
    if request is None or not predicate(request):
      continue
    body = _response_payload(event)
    if body is not None:
      latest_payload = body
  return latest_payload


def _request_descriptor(event: dict[str, Any]) -> dict[str, Any] | None:
  if str(event.get("method", "")) == "Network.requestWillBeSent":
    request = event.get("params", {}).get("request", {})
    if not isinstance(request, dict):
      return None
    url = str(request.get("url", "") or "")
    return {
      "method": str(request.get("method", "GET") or "GET"),
      "url": url,
      "path": urlparse(url).path,
      "headers": dict(request.get("headers", {}))
      if isinstance(request.get("headers"), dict)
      else {},
    }

  url = str(event.get("url", "") or event.get("request_url", "") or "")
  if not url:
    metadata = event.get("metadata")
    if isinstance(metadata, dict):
      url = str(metadata.get("url", "") or metadata.get("request_url", "") or "")
  if not url:
    return None

  method = str(
    event.get("request_method", "")
    or event.get("http_method", "")
    or event.get("method_override", "")
    or ""
  )
  if not method:
    metadata = event.get("metadata")
    if isinstance(metadata, dict):
      method = str(
        metadata.get("request_method", "")
        or metadata.get("http_method", "")
        or metadata.get("method", "")
        or ""
      )
  if not method:
    method = "GET"

  headers = event.get("headers")
  if not isinstance(headers, dict):
    metadata = event.get("metadata")
    headers = metadata.get("headers", {}) if isinstance(metadata, dict) else {}

  return {
    "method": method,
    "url": url,
    "path": urlparse(url).path,
    "headers": dict(headers) if isinstance(headers, dict) else {},
  }


def _response_payload(event: dict[str, Any]) -> Any:
  for key in ("response_body", "body"):
    if key in event:
      return _coerce_jsonish_value(event.get(key))
  metadata = event.get("metadata")
  if isinstance(metadata, dict):
    for key in ("response_body", "body"):
      if key in metadata:
        return _coerce_jsonish_value(metadata.get(key))
  return None


def _select_fields(url: str) -> list[str]:
  query = parse_qs(urlparse(url).query)
  raw_fields = query.get("select", [])
  if not raw_fields:
    return []
  return [field for field in raw_fields[0].split(",") if field]


def _normalized_auth_headers(headers: dict[str, Any]) -> list[str]:
  rendered: list[str] = []
  for key, value in headers.items():
    lowered = key.lower()
    if lowered == "authorization":
      rendered.append("Authorization: Bearer <session.access_token>")
    elif lowered == "content-type":
      rendered.append(f"Content-Type: {value}")
  return rendered


def _coerce_books_payload(payload: Any) -> list[dict[str, Any]]:
  if isinstance(payload, list):
    return [dict(row) for row in payload if isinstance(row, dict)]
  if isinstance(payload, dict):
    rows = payload.get("books")
    if isinstance(rows, list):
      return [dict(row) for row in rows if isinstance(row, dict)]
  return []


def _coerce_bets_payload(payload: Any) -> list[dict[str, Any]]:
  if isinstance(payload, list):
    return [dict(row) for row in payload if isinstance(row, dict)]
  if isinstance(payload, dict):
    for key in ("bets", "data"):
      rows = payload.get(key)
      if isinstance(rows, list):
        return [dict(row) for row in rows if isinstance(row, dict)]
  return []


def _build_visible_rows_audit(
  bets: list[dict[str, Any]],
  books: list[dict[str, Any]],
) -> list[dict[str, Any]]:
  if not bets:
    return []

  book_names = {
    str(row.get("id", "")): str(row.get("name", "") or "")
    for row in books
    if row.get("id") is not None
  }
  rows: list[dict[str, Any]] = []
  for bet in bets:
    stake = _stake_amount(bet)
    odds = _as_float(bet.get("odds_decimal"))
    if stake is None or odds is None:
      continue

    payout = round(stake * odds, 2)
    status = str(bet.get("status", "") or "").lower()
    commission_pct = _as_float(bet.get("commission_pct")) or 0.0
    commission_cents = _as_float(bet.get("commission_cents"))
    gross_profit, net_profit = _profit_for_bet(
      bet=bet,
      stake=stake,
      payout=payout,
      status=status,
      commission_pct=commission_pct,
      commission_cents=commission_cents,
      book_names=book_names,
    )

    row = {
      "book": _book_name(bet, book_names),
      "status": status,
      "stake": round(stake, 2),
      "odds": round(odds, 2),
      "payout": payout,
      "evPct": _rounded_float(bet.get("ev_pct")),
      "commissionPct": round(commission_pct, 2),
      "netProfit": net_profit,
    }
    if gross_profit is not None:
      row["grossProfit"] = gross_profit
    rows.append(row)
  return rows


def _build_live_metrics(rows: list[dict[str, Any]]) -> dict[str, Any]:
  if not rows:
    return {}
  total_profit = round(sum(float(row.get("netProfit", 0.0) or 0.0) for row in rows), 2)
  total_stake = sum(float(row.get("stake", 0.0) or 0.0) for row in rows)
  roi_pct = round((total_profit / total_stake) * 100, 2) if total_stake else 0.0
  weighted_ev = sum(
    float(row.get("stake", 0.0) or 0.0) * float(row.get("evPct", 0.0) or 0.0)
    for row in rows
    if row.get("evPct") is not None
  )
  expected_roi = round(weighted_ev / total_stake, 2) if total_stake else 0.0

  return {
    "totalProfitEur": total_profit,
    "roiPct": roi_pct,
    "expectedRoiPct": expected_roi,
    "beatClv": None,
    "totalBets": len(rows),
  }


def _build_auditable_math(rows: list[dict[str, Any]]) -> dict[str, Any]:
  if not rows:
    return {}
  total_stake = round(sum(float(row.get("stake", 0.0) or 0.0) for row in rows), 2)
  total_profit = round(sum(float(row.get("netProfit", 0.0) or 0.0) for row in rows), 2)
  roi_pct = round((total_profit / total_stake) * 100, 2) if total_stake else 0.0
  weighted_ev = sum(
    float(row.get("stake", 0.0) or 0.0) * float(row.get("evPct", 0.0) or 0.0)
    for row in rows
    if row.get("evPct") is not None
  )
  expected_roi = round(weighted_ev / total_stake, 2) if total_stake else 0.0
  return {
    "totalStake": total_stake,
    "totalProfit": total_profit,
    "roiPctFromRows": roi_pct,
    "expectedRoiPctFromStakeWeightedEv": expected_roi,
  }


def _profit_for_bet(
  *,
  bet: dict[str, Any],
  stake: float,
  payout: float,
  status: str,
  commission_pct: float,
  commission_cents: float | None,
  book_names: dict[str, str],
) -> tuple[float | None, float]:
  if status in {"won", "win"}:
    gross_profit = round(payout - stake, 2)
    if commission_cents is not None:
      net_profit = round(gross_profit - (commission_cents / 100), 2)
    elif commission_pct:
      net_profit = round(gross_profit * (1 - (commission_pct / 100)), 2)
    elif _book_name(bet, book_names).strip().lower() in EXCHANGE_BOOKS:
      net_profit = gross_profit
    else:
      net_profit = gross_profit
    return gross_profit, net_profit
  if status in {"lost", "lose"}:
    return None, round(-stake, 2)
  return None, 0.0


def _book_name(bet: dict[str, Any], book_names: dict[str, str]) -> str:
  explicit_name = str(bet.get("book", "") or bet.get("book_name", "") or "").strip()
  if explicit_name:
    return explicit_name
  book_id = bet.get("book_id")
  if book_id is None:
    return ""
  return book_names.get(str(book_id), "")


def _stake_amount(bet: dict[str, Any]) -> float | None:
  stake_cents = _as_float(bet.get("stake_cents"))
  if stake_cents is not None:
    return stake_cents / 100
  return _as_float(bet.get("stake"))


def _shipped_storage_keys(storage_snapshot: dict[str, Any] | None) -> list[str]:
  if not isinstance(storage_snapshot, dict):
    return []
  return [
    key
    for key in storage_snapshot.keys()
    if any(key.startswith(prefix) for prefix in DISPLAY_STORAGE_KEY_PREFIXES)
  ]


def _display_url(url: str, *, page_url: str | None) -> str:
  parsed = urlparse(url)
  page_parsed = urlparse(page_url or "")
  path = parsed.path or "/"
  if parsed.query:
    path = f"{path}?{parsed.query}"
  if parsed.netloc and parsed.netloc == page_parsed.netloc:
    return path
  if not parsed.netloc:
    return path
  return url


def _coerce_jsonish_value(value: Any) -> Any:
  if not isinstance(value, str):
    return value
  stripped = value.strip()
  if not stripped or stripped[0] not in "[{":
    return value
  try:
    return json.loads(stripped)
  except json.JSONDecodeError:
    return value


def _as_float(value: Any) -> float | None:
  if value in (None, ""):
    return None
  try:
    return float(value)
  except (TypeError, ValueError):
    return None


def _rounded_float(value: Any) -> float | None:
  number = _as_float(value)
  return round(number, 2) if number is not None else None
