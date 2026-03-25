from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.browser.cdp import DebugTarget  # noqa: E402
from bet_recorder import exchange_worker_live as exchange_worker_live_module  # noqa: E402


def fixture_text(name: str) -> str:
    return (
        Path(__file__).resolve().parent / "fixtures" / "bookmaker_history" / name
    ).read_text()


def test_capture_current_live_venue_payload_drives_kwik_to_my_bets_view(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="kwik-live",
        target_type="page",
        title="Kwiff Sports",
        url="https://kwiff.com/sports/featured/",
        websocket_debugger_url="ws://kwik",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://kwiff.com/sports/featured/",
            "document_title": "kwiff Sports",
            "body_text": "Featured\nMy Bets\nSportsbook",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-22T17:40:00Z",
        },
        {
            "page": "my_bets",
            "url": "https://kwiff.com/sports/my-bets",
            "document_title": "kwiff My Bets",
            "body_text": (
                "My Bets\nChelsea v Newcastle\nChelsea\n2.10\nMatch Odds\n£10.00\nCash Out"
            ),
            "inputs": {},
            "visible_actions": ["Cash Out"],
            "captured_at": "2026-03-22T17:40:03Z",
        },
    ]
    navigated_urls: list[str] = []
    clicked: list[tuple[str, ...]] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **kwargs: clicked.append(tuple(kwargs["labels"])),
    )

    payload = exchange_worker_live_module.capture_current_live_venue_payload("kwik")

    assert navigated_urls == ["https://sports.kwiff.com/my-bets"]
    assert clicked == []
    assert payload["url"] == "https://kwiff.com/sports/my-bets"


def test_capture_live_venue_history_payload_uses_betuk_history_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="betuk-live",
        target_type="page",
        title="BetUK Sports",
        url="https://www.betuk.com/betting#featured",
        websocket_debugger_url="ws://betuk",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.betuk.com/betting#featured",
            "document_title": "BetUK Sports",
            "body_text": "Featured\nMY BETS",
            "inputs": {},
            "visible_actions": ["MY BETS"],
            "captured_at": "2026-03-22T17:44:00Z",
        },
        {
            "page": "my_bets",
            "url": "https://www.betuk.com/betting#bethistory",
            "document_title": "BetUK History",
            "body_text": (
                "My Bets\nSettled\nSingle\n@\n3.30\nLost\n18 Mar 2026 • 06:22:14\n"
                "Coupon ID: 12385048588\nFull Time: Tokyo Verdy\n"
                "Tokyo Verdy - Kawasaki Frontale\nStake:\n£10.00"
            ),
            "inputs": {},
            "visible_actions": ["MY BETS", "History"],
            "captured_at": "2026-03-22T17:44:05Z",
        },
    ]
    navigated_urls: list[str] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **_: (_ for _ in ()).throw(AssertionError("history URL should be enough")),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("betuk")

    assert navigated_urls == ["https://www.betuk.com/betting#bethistory"]
    assert payload["url"] == "https://www.betuk.com/betting#bethistory"


def test_capture_live_venue_history_payload_uses_betmgm_history_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="betmgm-live",
        target_type="page",
        title="BetMGM Sports",
        url="https://www.betmgm.co.uk/sports#featured",
        websocket_debugger_url="ws://betmgm",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.betmgm.co.uk/sports#featured",
            "document_title": "BetMGM Sports",
            "body_text": "Featured\nMy Bets\nSportsbook",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-23T01:10:00Z",
        },
        {
            "page": "history",
            "url": "https://www.betmgm.co.uk/sports#bethistory",
            "document_title": "BetMGM My Bets",
            "body_text": fixture_text("betmgm_history.txt"),
            "inputs": {},
            "visible_actions": ["My Bets", "Settled"],
            "captured_at": "2026-03-23T01:10:05Z",
        },
    ]
    navigated_urls: list[str] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **_: (_ for _ in ()).throw(AssertionError("history URL should be enough")),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("betmgm")

    assert navigated_urls == ["https://www.betmgm.co.uk/sports#bethistory"]
    assert payload["url"] == "https://www.betmgm.co.uk/sports#bethistory"


def test_capture_live_venue_history_payload_uses_leovegas_history_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="leovegas-live",
        target_type="page",
        title="LeoVegas Sports",
        url="https://www.leovegas.co.uk/betting#featured",
        websocket_debugger_url="ws://leovegas",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.leovegas.co.uk/betting#featured",
            "document_title": "LeoVegas Sports",
            "body_text": "Featured\nMy Bets\nSportsbook",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-23T01:12:00Z",
        },
        {
            "page": "history",
            "url": "https://www.leovegas.co.uk/betting#bethistory/2026-03-23",
            "document_title": "LeoVegas My Bets",
            "body_text": fixture_text("leovegas_history.txt"),
            "inputs": {},
            "visible_actions": ["My Bets", "Settled"],
            "captured_at": "2026-03-23T01:12:05Z",
        },
    ]
    navigated_urls: list[str] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **_: (_ for _ in ()).throw(AssertionError("history URL should be enough")),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("leovegas")

    assert navigated_urls == ["https://www.leovegas.co.uk/betting#bethistory"]
    assert payload["url"] == "https://www.leovegas.co.uk/betting#bethistory/2026-03-23"


def test_capture_live_venue_history_payload_fetches_fanteam_history_api_in_browser(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="fanteam-live",
        target_type="page",
        title="FanTeam Sportsbook",
        url="https://www.fanteam.com/sportsbook#/overview",
        websocket_debugger_url="ws://fanteam",
    )
    payload = {
        "page": "history",
        "url": "https://www.fanteam.com/sportsbook#/overview",
        "document_title": "FanTeam - Daily Fantasy & Betting",
        "body_text": "GAMES LIVE RESULTS SPORTSBOOK Deposit",
        "inputs": {},
        "visible_actions": ["Deposit"],
        "captured_at": "2026-03-23T14:30:00Z",
    }

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: dict(payload),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **_: None,
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **_: None,
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "evaluate_debug_target_value",
        lambda **_: {
            "login": {"status": 200, "ok": True, "body": {"token": "ft-token"}, "body_text": "{\"token\":\"ft-token\"}"},
            "sportsbook_session": {"status": 201, "ok": True, "body": {"token": "sb-token"}, "body_text": "{\"token\":\"sb-token\"}"},
            "signin": {"status": 200, "ok": True, "body": {"accessToken": "alt-token"}, "body_text": "{\"accessToken\":\"alt-token\"}"},
            "count": {
                "status": 200,
                "ok": True,
                "url": "https://sb2bethistory-gateway-altenar2.biahosted.com/api/WidgetReports/GetBetsCountWithEvents?culture=en-GB&timezoneOffset=0&integration=fanteam&deviceType=1&numFormat=en-GB&countryCode=GB",
                "body": {"open": 0, "events": {}, "error": None},
                "body_text": "{\"open\":0,\"events\":{},\"error\":null}",
            },
            "history": {
                "status": 405,
                "ok": False,
                "url": "https://sb2bethistory-gateway-altenar2.biahosted.com/api/WidgetReports/widgetBetHistory?culture=en-GB&timezoneOffset=0&integration=fanteam&deviceType=1&numFormat=en-GB&countryCode=GB",
                "body": "",
                "body_text": "",
            },
        },
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("fanteam")

    assert payload["history_api_responses"]["count"]["status"] == 200
    assert payload["history_api_responses"]["history"]["status"] == 405
    assert any(
        "Fanteam history API count" in step["detail"]
        for step in payload["history_capture_trace"]
    )


def test_capture_live_venue_history_payload_clicks_bet365_settled_tab(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="bet365-live",
        target_type="page",
        title="bet365 Sports",
        url="https://www.bet365.com/#/HO/",
        websocket_debugger_url="ws://bet365",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.bet365.com/#/HO/",
            "document_title": "bet365 Home",
            "body_text": "All Sports\nMy Bets\n1",
            "inputs": {},
            "visible_actions": ["My Bets 1"],
            "captured_at": "2026-03-22T17:46:00Z",
        },
        {
            "page": "history",
            "url": "https://www.bet365.com/#/ME/X8020",
            "document_title": "bet365 Account History",
            "body_text": "History\nSettled Bets\nLast 48 Hours\nShow History",
            "inputs": {},
            "visible_actions": ["Settled Bets", "Last 48 Hours", "Show History"],
            "captured_at": "2026-03-22T17:46:02Z",
        },
        {
            "page": "history",
            "url": "https://members.bet365.com/?#/HICS/BSSB/",
            "document_title": "bet365 Account History",
            "body_text": "History\nSettled Bets\nLast 48 Hours\nShow History",
            "inputs": {},
            "visible_actions": ["Settled Bets", "Last 48 Hours", "Show History"],
            "captured_at": "2026-03-22T17:46:04Z",
        },
        {
            "page": "history",
            "url": "https://members.bet365.com/?#/HICS/BSSB/",
            "document_title": "bet365 Settled Bets",
            "body_text": (
                "History\nSettled Bets\nLast 48 Hours\nShow History\n"
                "£5.00\nSingle\nTottenham\n2.40\nFull-time result\nTottenham\n"
                "Sun 22 Mar\nAtletico Madrid\n17:27\nReturns\n£12.00\nWon"
            ),
            "inputs": {},
            "visible_actions": ["Settled Bets", "Last 48 Hours", "Show History"],
            "captured_at": "2026-03-22T17:46:08Z",
        },
    ]
    navigated_urls: list[str] = []
    clicked: list[tuple[str, ...]] = []
    capture_calls: list[dict] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **kwargs: (capture_calls.append(kwargs), payloads.pop(0))[1],
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **kwargs: clicked.append(tuple(kwargs["labels"])),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("bet365")

    assert navigated_urls == [
        "https://www.bet365.com/?#/ME/K/HI//",
    ]
    assert clicked == [
        ("Last 48 Hours", "Date Range", "From", "To"),
        ("Show History",),
    ]
    assert capture_calls[0].get("frame_url_fragments") == ("members.bet365.com",)
    assert any(
        call.get("frame_url_fragments") == ("members.bet365.com",)
        for call in capture_calls
    )
    assert payload["document_title"] == "bet365 Settled Bets"


def test_capture_current_live_venue_payload_clicks_generic_betano_my_bets_tab(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="betano-live",
        target_type="page",
        title="Betano Sports",
        url="https://www.betano.co.uk/",
        websocket_debugger_url="ws://betano",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.betano.co.uk/",
            "document_title": "Betano Sports",
            "body_text": "Home\nMy Bets\nSports",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-22T18:20:00Z",
        },
        {
            "page": "my_bets",
            "url": "https://www.betano.co.uk/mybets",
            "document_title": "Betano My Bets",
            "body_text": "My Bets\nNo open bets",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-22T18:20:03Z",
        },
    ]
    clicked: list[tuple[str, ...]] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **kwargs: clicked.append(tuple(kwargs["labels"])),
    )

    payload = exchange_worker_live_module.capture_current_live_venue_payload("betano")

    assert clicked == [("My Bets",)]
    assert payload["document_title"] == "Betano My Bets"


def test_capture_live_venue_history_payload_clicks_betano_bet_history(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="betano-live",
        target_type="page",
        title="Betano Sports",
        url="https://www.betano.co.uk/en-gb/sport",
        websocket_debugger_url="ws://betano",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.betano.co.uk/en-gb/sport",
            "document_title": "Betano Sports",
            "body_text": "My Bets\nOpen\nSettled",
            "inputs": {},
            "visible_actions": ["My Bets", "Settled", "Bet History"],
            "captured_at": "2026-03-22T18:20:00Z",
        },
        {
            "page": "my_bets",
            "url": "https://www.betano.co.uk/en-gb/mybets",
            "document_title": "Betano My Bets",
            "body_text": "My Bets\nOpen\nSettled\nBet History",
            "inputs": {},
            "visible_actions": ["My Bets", "Settled", "Bet History"],
            "captured_at": "2026-03-22T18:20:03Z",
        },
        {
            "page": "history",
            "url": "https://www.betano.co.uk/en-gb/bv_cashier/history/4",
            "document_title": "Betano",
            "body_text": fixture_text("betano_history.txt"),
            "inputs": {},
            "visible_actions": ["Open", "Settled", "Bet History"],
            "captured_at": "2026-03-22T18:20:08Z",
        },
    ]
    clicked: list[tuple[str, ...]] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **kwargs: clicked.append(tuple(kwargs["labels"])),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("betano")

    assert clicked == [("My Bets",), ("Bet History", "Settled", "History")]
    assert payload["url"] == "https://www.betano.co.uk/en-gb/bv_cashier/history/4"


def test_capture_live_venue_history_payload_fetches_betano_settled_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="betano-live",
        target_type="page",
        title="Betano",
        url="https://www.betano.co.uk/en-gb/bv_cashier/history/4",
        websocket_debugger_url="ws://betano",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.betano.co.uk/en-gb/bv_cashier/history/4",
            "document_title": "Betano",
            "body_text": fixture_text("betano_history.txt"),
            "inputs": {},
            "visible_actions": ["Open", "Settled", "Bet History"],
            "captured_at": "2026-03-22T18:20:08Z",
        },
    ]
    api_urls: list[str] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "fetch_debug_target_json",
        lambda **kwargs: api_urls.append(kwargs["url"]) or {
            "url": kwargs["url"],
            "status": 200,
            "ok": True,
            "body": {"groups": [{"date": "2026-03-01", "bets": []}], "load_more": False},
            "body_text": "{\"groups\":[],\"load_more\":false}",
        },
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **_: (_ for _ in ()).throw(AssertionError("Betano should not need a direct navigation here")),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **_: (_ for _ in ()).throw(AssertionError("Betano should not need an extra click here")),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("betano")

    assert api_urls == ["https://www.betano.co.uk/bv_api/account_history/settled/1/bets"]
    assert payload["history_api_responses"]["settled"]["status"] == 200


def test_capture_live_venue_history_payload_fetches_betvictor_settled_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="betvictor-live",
        target_type="page",
        title="BetVictor",
        url="https://www.betvictor.com/en-gb/my_transactions?first_modal=true",
        websocket_debugger_url="ws://betvictor",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.betvictor.com/en-gb/my_transactions?first_modal=true",
            "document_title": "BetVictor",
            "body_text": "My Account\nBet History",
            "inputs": {},
            "visible_actions": ["My Account", "Bet History"],
            "captured_at": "2026-03-22T18:20:00Z",
        },
        {
            "page": "my_bets",
            "url": "https://www.betvictor.com/en-gb/bv_cashier/history?first_modal=true",
            "document_title": "BetVictor",
            "body_text": (
                "Bet History\nSports\nCasino\nOpen\nSettled\n"
                "Double\n@2.679\nStake\nPotential Returns\n£10.00\n£16.77"
            ),
            "inputs": {},
            "visible_actions": ["Bet History", "Open", "Settled"],
            "captured_at": "2026-03-22T18:20:05Z",
        },
    ]
    navigated_urls: list[str] = []
    api_urls: list[str] = []
    clicked: list[tuple[str, ...]] = []
    clicked: list[tuple[str, ...]] = []
    clicked: list[tuple[str, ...]] = []
    clicked: list[tuple[str, ...]] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "fetch_debug_target_json",
        lambda **kwargs: api_urls.append(kwargs["url"]) or {
            "url": kwargs["url"],
            "status": 200,
            "ok": True,
            "body": {
                "groups": [
                    {
                        "date": "2026-03-01",
                        "bets": [
                            {
                                "createdDate": "2026-03-18T12:35:58Z",
                                "settledDate": "2026-03-18T21:54:16Z",
                                "betType": "Tottenham Hotspur",
                                "stake": 10.0,
                                "returns": 23.75,
                                "bonusFunds": False,
                                "odds": "2.375",
                                "result": "won",
                                "description": "GBP 10.00 Single: TOTTENHAM HOTSPUR @ 11/8 [Match Betting - 90 Mins] - Tottenham Hotspur v Atletico Madrid - UEFA Champions League - FOOTBALL",
                            }
                        ],
                    }
                ],
                "load_more": False,
            },
            "body_text": "{\"groups\":[{\"date\":\"2026-03-01\",\"bets\":[{\"betType\":\"Tottenham Hotspur\"}]}],\"load_more\":false}",
        },
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **_: (_ for _ in ()).throw(AssertionError("BetVictor should use the history URL directly")),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("betvictor")

    assert navigated_urls == ["https://www.betvictor.com/en-gb/bv_cashier/history?first_modal=true"]
    assert api_urls == ["https://www.betvictor.com/bv_api/account_history/settled/1/bets"]
    assert payload["history_api_responses"]["settled"]["status"] == 200


def test_capture_live_venue_history_payload_fetches_talksportbet_settled_api(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="talksportbet-live",
        target_type="page",
        title="talkSPORT BET",
        url="https://www.talksportbet.com/en-gb/sports",
        websocket_debugger_url="ws://talksportbet",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.talksportbet.com/en-gb/sports",
            "document_title": "talkSPORT BET",
            "body_text": "Sports\nMy Bets",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-23T00:00:00Z",
        },
        {
            "page": "history",
            "url": "https://www.talksportbet.com/en-gb/bv_cashier/history?first_modal=true",
            "document_title": "talkSPORT BET History",
            "body_text": "Bet History\nOpen\nSettled",
            "inputs": {},
            "visible_actions": ["Bet History", "Open", "Settled"],
            "captured_at": "2026-03-23T00:00:03Z",
        },
        {
            "page": "history",
            "url": "https://www.talksportbet.com/en-gb/bv_cashier/history?first_modal=true",
            "document_title": "talkSPORT BET History",
            "body_text": "Bet History\nOpen\nSettled",
            "inputs": {},
            "visible_actions": ["Bet History", "Open", "Settled"],
            "captured_at": "2026-03-23T00:00:05Z",
        },
    ]
    navigated_urls: list[str] = []
    api_urls: list[str] = []
    clicked: list[tuple[str, ...]] = []
    capture_count = {"value": 0}

    def capture_payload(**_kwargs):
        capture_count["value"] += 1
        if capture_count["value"] == 1:
            return payloads[0]
        return payloads[-1]

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        capture_payload,
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "fetch_debug_target_json",
        lambda **kwargs: api_urls.append(kwargs["url"]) or {
            "url": kwargs["url"],
            "status": 200,
            "ok": True,
            "body": {"groups": [{"date": "2026-03-01", "bets": []}], "load_more": False},
            "body_text": "{\"groups\":[],\"load_more\":false}",
        },
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **kwargs: clicked.append(tuple(kwargs["labels"])),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("talksportbet")

    assert clicked == [
        ("My Bets", "Open Bets"),
        ("Bet History", "Settled", "History"),
    ]
    assert navigated_urls == ["https://www.talksportbet.com/en-gb/bv_cashier/history?first_modal=true"]
    assert api_urls
    assert set(api_urls) == {"https://www.talksportbet.com/bv_api/account_history/settled/1/bets"}
    assert payload["history_api_responses"]["settled"]["status"] == 200


def test_capture_live_venue_history_payload_uses_paddypower_settled_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="paddypower-live",
        target_type="page",
        title="Paddy Power",
        url="https://www.paddypower.com/bet",
        websocket_debugger_url="ws://paddypower",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.paddypower.com/bet",
            "document_title": "Paddy Power",
            "body_text": "Sports\nMy Bets",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-23T00:00:00Z",
        },
        {
            "page": "history",
            "url": "https://www.paddypower.com/my-bets?tab=settledBets",
            "document_title": "Paddy Power My Bets",
            "body_text": fixture_text("paddypower_history.txt"),
            "inputs": {},
            "visible_actions": ["My Bets", "Settled", "Transaction History"],
            "captured_at": "2026-03-23T00:00:03Z",
        },
    ]
    navigated_urls: list[str] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **_: (_ for _ in ()).throw(AssertionError("Paddy Power should use the settled history URL directly")),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("paddypower")

    assert navigated_urls == ["https://www.paddypower.com/my-bets?tab=settledBets"]
    assert payload["url"] == "https://www.paddypower.com/my-bets?tab=settledBets"


def test_capture_live_venue_history_payload_enriches_boylesports_with_html(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="boylesports-live",
        target_type="page",
        title="BoyleSports",
        url="https://www.boylesports.com/sports",
        websocket_debugger_url="ws://boylesports",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.boylesports.com/sports",
            "document_title": "BoyleSports",
            "body_text": "Sports\nMy Bets",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-23T00:00:00Z",
        },
        {
            "page": "history",
            "url": "https://account.boylesports.com/#account-bethistory+Opened",
            "document_title": "BoyleSports Account History",
            "body_text": fixture_text("boylesports_history.txt"),
            "inputs": {},
            "visible_actions": ["Bet History", "Settled Bets"],
            "captured_at": "2026-03-23T00:00:03Z",
        },
    ]
    navigated_urls: list[str] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "evaluate_debug_target_value",
        lambda **kwargs: fixture_text("boylesports_history.html"),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **_: (_ for _ in ()).throw(AssertionError("BoyleSports should use the account history URL directly")),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("boylesports")

    assert navigated_urls == ["https://account.boylesports.com/#account-bethistory+Opened"]
    assert payload["body_html"].lstrip().startswith("<input")


def test_capture_live_venue_history_payload_uses_midnite_settled_url(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target = DebugTarget(
        target_id="midnite-live",
        target_type="page",
        title="Midnite",
        url="https://www.midnite.com/sports",
        websocket_debugger_url="ws://midnite",
    )
    payloads = [
        {
            "page": "my_bets",
            "url": "https://www.midnite.com/sports",
            "document_title": "Midnite",
            "body_text": "Sports\nMy Bets",
            "inputs": {},
            "visible_actions": ["My Bets"],
            "captured_at": "2026-03-23T00:00:00Z",
        },
        {
            "page": "history",
            "url": "https://www.midnite.com/sports/bets/settled",
            "document_title": "Midnite Settled Bets",
            "body_text": fixture_text("midnite_history.txt"),
            "inputs": {},
            "visible_actions": ["My Bets", "Open", "Settled"],
            "captured_at": "2026-03-23T00:00:03Z",
        },
    ]
    navigated_urls: list[str] = []

    monkeypatch.setattr(exchange_worker_live_module, "list_debug_targets", lambda: [target])
    monkeypatch.setattr(
        exchange_worker_live_module,
        "capture_debug_target_page_state",
        lambda **_: payloads.pop(0),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "navigate_debug_target",
        lambda **kwargs: navigated_urls.append(kwargs["url"]),
    )
    monkeypatch.setattr(
        exchange_worker_live_module,
        "click_debug_target_by_labels",
        lambda **_: (_ for _ in ()).throw(AssertionError("Midnite should use the settled history URL directly")),
    )

    payload = exchange_worker_live_module.capture_live_venue_history_payload("midnite")

    assert navigated_urls == ["https://www.midnite.com/sports/bets/settled"]
    assert payload["url"] == "https://www.midnite.com/sports/bets/settled"
