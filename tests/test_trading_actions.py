from pathlib import Path
import sys

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.actions import trading_actions  # noqa: E402


class FakeAgentBrowserClient:
    def __init__(self, session: str | None = None) -> None:
        self.session = session
        self.opened_urls: list[str] = []
        self.waits: list[int] = []
        self._current_url = "about:blank"

    def open_url(self, url: str) -> None:
        self._current_url = url
        self.opened_urls.append(url)

    def current_url(self) -> str:
        return self._current_url

    def wait(self, milliseconds: int) -> None:
        self.waits.append(milliseconds)


def sample_intent(**overrides) -> dict:
    payload = {
        "action_kind": "place_bet",
        "source": "positions",
        "venue": "smarkets",
        "mode": "review",
        "side": "buy",
        "request_id": "positions-1",
        "source_ref": "bet-001",
        "event_name": "Arsenal v Everton",
        "market_name": "Full-time result",
        "selection_name": "Draw",
        "stake": 10.0,
        "expected_price": 5.0,
        "event_url": "https://smarkets.com/event/arsenal-everton",
        "deep_link_url": None,
        "betslip_market_id": None,
        "betslip_selection_id": None,
        "execution_policy": {
            "time_in_force": "fill_or_kill",
            "cancel_unmatched_after_ms": 1500,
            "require_full_fill": True,
            "max_price_drift": 0.0,
        },
        "risk_report": {
            "summary": "Ready with 1 warning(s).",
            "checks": [],
            "warning_count": 1,
            "blocking_review_count": 0,
            "blocking_submit_count": 0,
            "reduce_only": True,
        },
        "source_context": {
            "is_in_play": True,
            "event_status": "27'",
            "market_status": "tradable",
            "live_clock": "27'",
            "can_trade_out": True,
            "current_pnl_amount": 1.2,
            "baseline_stake": 9.91,
            "baseline_liability": 23.29,
            "baseline_price": 3.35,
        },
        "notes": ["positions"],
    }
    payload.update(overrides)
    return payload


def test_execute_trading_action_review_mode_uses_existing_betslip(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = FakeAgentBrowserClient()
    recorded: dict[str, object] = {}

    monkeypatch.setattr(
        trading_actions,
        "AgentBrowserClient",
        lambda session=None: _bind_session(client, session),
    )
    monkeypatch.setattr(trading_actions, "_betslip_is_visible", lambda client: True)
    monkeypatch.setattr(
        trading_actions,
        "_set_smarkets_stake",
        lambda *, client, stake: recorded.setdefault("stake", stake),
    )
    monkeypatch.setattr(
        trading_actions,
        "_verify_smarkets_bet_slip",
        lambda *, client: {
            "contractText": "Draw",
            "sideText": "Buy",
            "stakeValue": "10.00",
            "priceValue": "5.00",
        },
    )
    monkeypatch.setattr(
        trading_actions,
        "_record_trading_action",
        lambda **kwargs: recorded.setdefault("recorded", kwargs["action_status"]),
    )

    result = trading_actions.execute_trading_action(
        intent_payload=sample_intent(),
        agent_browser_session="helium-copy",
        run_dir=tmp_path,
    )

    assert client.session == "helium-copy"
    assert client.opened_urls == ["https://smarkets.com/event/arsenal-everton"]
    assert recorded["stake"] == 10.0
    assert recorded["recorded"] == "review_ready"
    assert "review mode" in result.detail


def test_execute_trading_action_confirm_gtc_submits_without_fok_flow(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    client = FakeAgentBrowserClient()
    recorded: dict[str, object] = {"clicked": False}

    monkeypatch.setattr(
        trading_actions,
        "AgentBrowserClient",
        lambda session=None: _bind_session(client, session),
    )
    monkeypatch.setattr(trading_actions, "_betslip_is_visible", lambda client: False)
    monkeypatch.setattr(
        trading_actions,
        "_populate_smarkets_bet_slip",
        lambda *, client, intent: recorded.setdefault("populated", True),
    )
    monkeypatch.setattr(
        trading_actions,
        "_set_smarkets_stake",
        lambda *, client, stake: recorded.setdefault("stake", stake),
    )
    monkeypatch.setattr(
        trading_actions,
        "_verify_smarkets_bet_slip",
        lambda *, client: {
            "contractText": "Draw",
            "sideText": "Sell",
            "stakeValue": "12.50",
            "priceValue": "5.00",
        },
    )
    monkeypatch.setattr(
        trading_actions,
        "_click_smarkets_place_bet",
        lambda *, client: recorded.__setitem__("clicked", True),
    )
    monkeypatch.setattr(
        trading_actions,
        "_record_trading_action",
        lambda **kwargs: recorded.setdefault("recorded", kwargs["action_status"]),
    )

    result = trading_actions.execute_trading_action(
        intent_payload=sample_intent(
            mode="confirm",
            side="sell",
            stake=12.5,
            deep_link_url="https://smarkets.com/betslip/example",
            execution_policy={
                "time_in_force": "good_til_cancel",
                "cancel_unmatched_after_ms": 0,
                "require_full_fill": False,
                "max_price_drift": 0.0,
            },
        ),
        agent_browser_session="helium-copy",
        run_dir=tmp_path,
    )

    assert recorded["clicked"] is True
    assert recorded["recorded"] == "submitted"
    assert "submitted" in result.detail


def test_complete_smarkets_fill_or_kill_cancels_unmatched_residue(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = FakeAgentBrowserClient()
    cancelled: list[str] = []

    monkeypatch.setattr(
        trading_actions,
        "_poll_smarkets_submission_state",
        lambda *, client, timeout_ms: {"state": "partial", "message": "Bet Partially Matched"},
    )
    monkeypatch.setattr(
        trading_actions,
        "_set_smarkets_activity_filter",
        lambda *, client, label: None,
    )
    portfolio_states = iter(
        [
            {
                "unmatched_present": True,
                "filled_present": True,
                "cancel_text": "cancel row",
                "trade_out_text": "trade out row",
                "body_text": "Bet Partially Matched Cancel Trade out",
            },
            {
                "unmatched_present": False,
                "filled_present": True,
                "cancel_text": "",
                "trade_out_text": "trade out row",
                "body_text": "Trade out",
            },
        ]
    )
    monkeypatch.setattr(
        trading_actions,
        "_inspect_smarkets_portfolio_order_state",
        lambda *, client, intent: next(portfolio_states),
    )
    monkeypatch.setattr(
        trading_actions,
        "_cancel_smarkets_portfolio_order",
        lambda *, client, intent: cancelled.append(intent.request_id),
    )

    result = trading_actions._complete_smarkets_fill_or_kill(
        client=client,
        intent=trading_actions.TradingActionIntent.from_dict(
            sample_intent(mode="confirm")
        ),
    )

    assert client.opened_urls == [trading_actions.SMARKETS_PORTFOLIO_URL]
    assert cancelled == ["positions-1"]
    assert result.action_status == "partially_filled_killed"


def test_bet_slip_quote_drift_fails_closed() -> None:
    intent = trading_actions.TradingActionIntent.from_dict(sample_intent())

    with pytest.raises(ValueError, match="quote drift exceeded"):
        trading_actions._assert_smarkets_bet_slip_matches_intent(
            intent=intent,
            verification={
                "contractText": "Draw",
                "sideText": "Buy",
                "stakeValue": "10.00",
                "priceValue": "5.10",
            },
        )


def test_execute_trading_action_requires_session() -> None:
    with pytest.raises(ValueError, match="agent_browser_session"):
        trading_actions.execute_trading_action(
            intent_payload=sample_intent(),
            agent_browser_session=None,
            run_dir=None,
        )


def _bind_session(
    client: FakeAgentBrowserClient,
    session: str | None,
) -> FakeAgentBrowserClient:
    client.session = session
    return client
