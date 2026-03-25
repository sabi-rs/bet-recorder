from __future__ import annotations

from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.capture.bets_observations import (  # noqa: E402
  build_bets_page_metadata,
  build_bets_transport_and_math_probe,
  refresh_bets_observations,
)


def test_build_bets_page_metadata_captures_tracker_cache_probe() -> None:
  metadata = build_bets_page_metadata(
    url="https://app.fairoddsterminal.com/bets",
    local_storage={
      "widget_cache_bets_data": json.dumps(
        {
          "data": {
            "bets": [
              {
                "id": 10901,
                "selection": "Synthetic Epsilon",
              }
            ],
            "books": [{"id": 16, "name": "Pinnacle"}],
            "sports": ["soccer_uefa_champs_league"],
            "userId": "user-1",
            "timestamp": "2026-03-09T20:21:22Z",
          },
          "timestamp": "2026-03-09T20:21:22Z",
          "version": 3,
        }
      ),
      "fo_colwidths:bets:v3": '{"stake":120}',
      "ignored": "value",
    },
  )

  assert metadata["page_kind"] == "bets"
  assert metadata["trackerCacheProbe"]["storageKey"] == "widget_cache_bets_data"
  assert metadata["trackerCacheProbe"]["betCount"] == 1
  assert metadata["trackerCacheProbe"]["firstBet"]["selection"] == "Synthetic Epsilon"
  assert set(metadata["storage_snapshot"].keys()) == {
    "widget_cache_bets_data",
    "fo_colwidths:bets:v3",
  }


def test_build_bets_transport_and_math_probe_derives_contract_and_metrics() -> None:
  storage_snapshot = {
    "widget_cache_bets_data": {
      "data": {
        "bets": [
          {
            "id": 10901,
            "odds_decimal": 2.15,
            "stake_cents": 800,
            "status": "won",
            "selection": "Synthetic Epsilon",
            "event_name": "Synthetic Epsilon vs Synthetic Zeta",
            "book_id": 16,
            "bet_type": "dropping_odds",
            "sport_key": "soccer_uefa_champs_league",
            "currency": "EUR",
            "market": "Moneyline (OT incl)",
            "event_start": "2026-03-09T18:22:15.629+00:00",
            "ev_pct": 3.1,
            "commission_pct": 0,
          },
          {
            "id": 10902,
            "odds_decimal": 1.95,
            "stake_cents": 1000,
            "status": "lost",
            "selection": "Synthetic Zeta",
            "event_name": "Synthetic Alpha vs Synthetic Zeta",
            "book_id": 2,
            "currency": "EUR",
            "market": "Moneyline (OT incl)",
            "ev_pct": 6.8,
            "commission_pct": 0,
          },
          {
            "id": 10903,
            "odds_decimal": 2.4,
            "stake_cents": 1250,
            "status": "won",
            "selection": "Draw",
            "event_name": "Synthetic Draw vs Synthetic Draw",
            "book_id": 9,
            "currency": "EUR",
            "market": "Match Odds",
            "ev_pct": 4.2,
            "commission_pct": 2,
          },
        ],
        "books": [
          {"id": 16, "name": "Pinnacle"},
          {"id": 2, "name": "Bet365"},
          {"id": 9, "name": "Smarkets"},
        ],
      },
      "timestamp": "2026-03-09T20:21:22Z",
      "version": 3,
    },
    "fo_colwidths:bets:v3": {"stake": 120},
    "fo_visible_cols:bets:v1": ["book", "stake"],
    "fo_display_visibility:bets:v1": {"show_profit": True},
  }
  transport_events = [
    {
      "method": "Network.requestWillBeSent",
      "params": {
        "request": {"method": "GET", "url": "https://app.fairoddsterminal.com/bets"}
      },
    },
    {
      "method": "Network.requestWillBeSent",
      "params": {
        "request": {
          "method": "GET",
          "url": "https://auth.fairoddsterminal.com/rest/v1/books?select=id,name",
        }
      },
    },
    {
      "method": "Network.requestWillBeSent",
      "params": {
        "request": {
          "method": "GET",
          "url": (
            "https://auth.fairoddsterminal.com/rest/v1/bets?"
            "select=id,user_id,created_at,settled_at,odds_decimal,close_odds_decimal,"
            "clv_pct,beat_clv,stake_cents,status,market,selection,line,event_name,"
            "currency,book_id,notes,event_start,sport_key,bet_type,ev_pct,"
            "commission_pct,commission_cents&order=created_at.desc&limit=5000"
          ),
        }
      },
    },
    {
      "url": "https://auth.fairoddsterminal.com/rest/v1/books?select=id,name",
      "request_method": "GET",
      "body": [{"id": 16, "name": "Pinnacle"}],
    },
    {
      "url": "https://auth.fairoddsterminal.com/rest/v1/rpc/leaderboard_last_month",
      "request_method": "POST",
      "body": {"rank": 1},
    },
    {
      "url": "https://app.fairoddsterminal.com/api/clv/compute",
      "request_method": "POST",
      "headers": {
        "Authorization": "Bearer actual-token",
        "Content-Type": "application/json",
      },
      "body": {"ok": True},
    },
  ]

  probe = build_bets_transport_and_math_probe(
    captured_at="2026-03-09T23:32:37Z",
    page_url="https://app.fairoddsterminal.com/bets",
    transport_events=transport_events,
    storage_snapshot=storage_snapshot,
  )

  assert probe is not None
  assert probe["networkRequests"][:3] == [
    "GET /bets",
    "GET https://auth.fairoddsterminal.com/rest/v1/books?select=id,name",
    (
      "GET https://auth.fairoddsterminal.com/rest/v1/bets?"
      "select=id,user_id,created_at,settled_at,odds_decimal,close_odds_decimal,"
      "clv_pct,beat_clv,stake_cents,status,market,selection,line,event_name,"
      "currency,book_id,notes,event_start,sport_key,bet_type,ev_pct,"
      "commission_pct,commission_cents&order=created_at.desc&limit=5000"
    ),
  ]
  assert probe["restQueryContract"]["booksSelectFields"] == ["id", "name"]
  assert "commission_cents" in probe["restQueryContract"]["betsSelectFields"]
  assert probe["restQueryContract"]["leaderboardRpc"] == "leaderboard_last_month"
  assert probe["restQueryContract"]["clvComputeEndpoint"] == "/api/clv/compute"
  assert probe["restQueryContract"]["clvComputeAuth"] == [
    "Authorization: Bearer <session.access_token>",
    "Content-Type: application/json",
  ]
  assert probe["shippedStorageKeys"] == [
    "fo_colwidths:bets:v3",
    "fo_visible_cols:bets:v1",
    "fo_display_visibility:bets:v1",
  ]
  assert probe["liveMetrics"]["totalProfitEur"] == 16.35
  assert probe["liveMetrics"]["roiPct"] == 53.61
  assert probe["liveMetrics"]["expectedRoiPct"] == 4.76
  assert probe["visibleRowsAudit"][0]["book"] == "Pinnacle"
  assert probe["visibleRowsAudit"][2]["grossProfit"] == 17.5
  assert probe["visibleRowsAudit"][2]["netProfit"] == 17.15
  assert probe["auditableMath"] == {
    "totalStake": 30.5,
    "totalProfit": 16.35,
    "roiPctFromRows": 53.61,
    "expectedRoiPctFromStakeWeightedEv": 4.76,
  }


def test_refresh_bets_observations_writes_consolidated_run_artifact(tmp_path: Path) -> None:
  run_dir = tmp_path / "run"
  run_dir.mkdir()
  events_path = run_dir / "events.jsonl"
  transport_path = run_dir / "transport.jsonl"

  events_path.write_text(
    json.dumps(
      {
        "captured_at": "2026-03-09T23:32:37Z",
        "url": "https://app.fairoddsterminal.com/bets",
        "metadata": build_bets_page_metadata(
          url="https://app.fairoddsterminal.com/bets",
          local_storage={
            "widget_cache_bets_data": json.dumps(
              {
                "data": {
                  "bets": [{"id": 1, "odds_decimal": 2.0, "stake_cents": 500, "status": "won"}],
                  "books": [{"id": 1, "name": "Pinnacle"}],
                },
                "timestamp": "2026-03-09T20:21:22Z",
                "version": 3,
              }
            ),
          },
        ),
      }
    )
    + "\n",
    encoding="utf-8",
  )
  transport_path.write_text(
    json.dumps(
      {
        "method": "Network.requestWillBeSent",
        "params": {"request": {"method": "GET", "url": "https://app.fairoddsterminal.com/bets"}},
      }
    )
    + "\n",
    encoding="utf-8",
  )

  refresh_bets_observations(
    run_dir=run_dir,
    events_path=events_path,
    transport_path=transport_path,
  )

  artifact = json.loads((run_dir / "bets-observations.json").read_text())
  assert artifact["trackerCacheProbe"]["betCount"] == 1
  assert artifact["betsTransportAndMathProbe"]["networkRequests"] == ["GET /bets"]
