from io import BytesIO
from pathlib import Path
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlparse
import json
import sys

import pytest
from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.cli import app
from bet_recorder.owls_insight import (
    AuthenticationError,
    OwlsInsightClient,
    OwlsInsightConfig,
    RateLimitError,
    build_client,
    load_config,
)


class FakeResponse:
    def __init__(
        self,
        payload: dict,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
    ) -> None:
        self.payload = payload
        self.status = status
        self.headers = headers or {}

    def read(self) -> bytes:
        return json.dumps(self.payload).encode("utf-8")

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


class FakeSocket:
    def __init__(self) -> None:
        self.handlers: dict[str, object] = {}
        self.connect_calls: list[tuple[str, tuple, dict]] = []
        self.emits: list[tuple[str, dict]] = []
        self.disconnected = False

    def on(self, event_name, handler):
        self.handlers[event_name] = handler

    def connect(self, *args, **kwargs):
        self.connect_calls.append((args[0], args[1:], kwargs))
        handler = self.handlers.get("connect")
        if callable(handler):
            handler()

    def emit(self, event_name, payload):
        self.emits.append((event_name, payload))

    def disconnect(self):
        self.disconnected = True


def test_load_config_reads_dotenv_file(tmp_path: Path, monkeypatch) -> None:
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text(
        "\n".join(
            [
                "OWLS_INSIGHT_API_KEY=dotenv-secret",
                "OWLS_INSIGHT_BASE_URL=https://api.example.test",
                "OWLS_INSIGHT_WS_URL=wss://socket.example.test",
                "OWLS_INSIGHT_TIMEOUT_SECONDS=9.5",
            ]
        )
    )
    monkeypatch.delenv("OWLS_INSIGHT_API_KEY", raising=False)
    monkeypatch.delenv("OWLS_INSIGHT_BASE_URL", raising=False)
    monkeypatch.delenv("OWLS_INSIGHT_WS_URL", raising=False)
    monkeypatch.delenv("OWLS_INSIGHT_TIMEOUT_SECONDS", raising=False)

    config = load_config(dotenv_path=dotenv_path)

    assert config == OwlsInsightConfig(
        api_key="dotenv-secret",
        base_url="https://api.example.test",
        ws_url="wss://socket.example.test",
        timeout_seconds=9.5,
    )


@pytest.mark.parametrize(
    ("method_name", "kwargs", "expected_path", "expected_query"),
    [
        (
            "get_odds",
            {
                "sport": "NBA",
                "books": ["Bet365", "BetMGM"],
                "alternates": True,
                "league": "Premier League",
            },
            "/api/v1/nba/odds",
            {
                "books": ["bet365,betmgm"],
                "alternates": ["true"],
                "league": ["Premier League"],
            },
        ),
        (
            "get_book_props",
            {
                "sport": "nba",
                "book": "FanDuel",
                "player": "LeBron",
                "category": "points",
            },
            "/api/v1/nba/props/fanduel",
            {
                "player": ["LeBron"],
                "category": ["points"],
            },
        ),
        (
            "get_scores",
            {"sport": "soccer"},
            "/api/v1/soccer/scores/live",
            {},
        ),
        (
            "get_stats_averages",
            {
                "sport": "nba",
                "player_name": "Jalen Brunson",
                "opponent": "Boston Celtics",
            },
            "/api/v1/nba/stats/averages",
            {
                "playerName": ["Jalen Brunson"],
                "opponent": ["Boston Celtics"],
            },
        ),
        (
            "get_kalshi_series_markets",
            {
                "series_ticker": "NBA-CHAMP",
                "status": "open",
                "limit": 25,
            },
            "/api/v1/kalshi/series/NBA-CHAMP/markets",
            {
                "status": ["open"],
                "limit": ["25"],
            },
        ),
        (
            "get_history_props",
            {
                "event_id": "nba:game-1",
                "player_name": "Jalen Brunson",
                "prop_type": "points",
                "book": "bet365",
                "opening": True,
            },
            "/api/v1/history/props",
            {
                "eventId": ["nba:game-1"],
                "playerName": ["Jalen Brunson"],
                "propType": ["points"],
                "book": ["bet365"],
                "opening": ["true"],
            },
        ),
        (
            "get_realtime",
            {"sport": "tennis", "league": "ATP"},
            "/api/v1/tennis/realtime",
            {"league": ["ATP"]},
        ),
    ],
)
def test_client_methods_build_expected_requests(
    monkeypatch,
    method_name: str,
    kwargs: dict,
    expected_path: str,
    expected_query: dict[str, list[str]],
) -> None:
    captured: dict[str, object] = {}

    def fake_urlopen(request, timeout):
        captured["url"] = request.full_url
        captured["authorization"] = request.get_header("Authorization")
        captured["timeout"] = timeout
        return FakeResponse(
            {"success": True, "data": {"ok": True}},
            headers={
                "X-RateLimit-Remaining-Minute": "87",
                "X-RateLimit-Remaining-Month": "62340",
            },
        )

    monkeypatch.setattr("bet_recorder.owls_insight.urlopen", fake_urlopen)

    client = build_client(
        OwlsInsightConfig(api_key="top-secret", timeout_seconds=7.5)
    )
    response = getattr(client, method_name)(**kwargs)
    parsed = urlparse(str(captured["url"]))

    assert parsed.path == expected_path
    assert parse_qs(parsed.query) == expected_query
    assert captured["authorization"] == "Bearer top-secret"
    assert captured["timeout"] == 7.5
    assert response.rate_limits is not None
    assert response.rate_limits.remaining_minute == 87
    assert response.rate_limits.remaining_month == 62340


def test_client_surfaces_authentication_error(monkeypatch) -> None:
    def fake_urlopen(request, timeout):
        raise HTTPError(
            url=request.full_url,
            code=401,
            msg="Unauthorized",
            hdrs={"Retry-After": "3", "X-RateLimit-Remaining-Minute": "0"},
            fp=BytesIO(b'{"error":"invalid api key"}'),
        )

    monkeypatch.setattr("bet_recorder.owls_insight.urlopen", fake_urlopen)

    with pytest.raises(AuthenticationError) as error:
        OwlsInsightClient(OwlsInsightConfig(api_key="bad-key")).get_odds(sport="nba")

    assert "HTTP 401" in str(error.value)
    assert error.value.rate_limits is not None
    assert error.value.rate_limits.retry_after_ms == 3000


def test_client_surfaces_rate_limit_error(monkeypatch) -> None:
    def fake_urlopen(request, timeout):
        raise HTTPError(
            url=request.full_url,
            code=429,
            msg="Too Many Requests",
            hdrs={"Retry-After": "1.5"},
            fp=BytesIO(b'{"error":"slow down"}'),
        )

    monkeypatch.setattr("bet_recorder.owls_insight.urlopen", fake_urlopen)

    with pytest.raises(RateLimitError) as error:
        OwlsInsightClient(OwlsInsightConfig(api_key="slow")).get_scores(sport="nba")

    assert "HTTP 429" in str(error.value)
    assert error.value.rate_limits is not None
    assert error.value.rate_limits.retry_after_ms == 1500


def test_websocket_subscribe_and_wait_for() -> None:
    fake_socket = FakeSocket()
    client = OwlsInsightClient(
        OwlsInsightConfig(api_key="ws-secret", ws_url="wss://api.example.test")
    )
    websocket = client.websocket(socket_factory=lambda: fake_socket)

    websocket.connect()
    websocket.subscribe(sports=["nba", "nfl"], books=["bet365"], alternates=True)
    websocket.subscribe_props(sports=["nba"], book="fanduel")

    odds_handler = fake_socket.handlers["odds-update"]
    assert callable(odds_handler)
    odds_handler({"sports": {"nba": [{"id": "game-1"}]}})

    assert fake_socket.connect_calls
    assert fake_socket.connect_calls[0][0] == "wss://api.example.test?apiKey=ws-secret"
    assert fake_socket.emits == [
        (
            "subscribe",
            {
                "sports": ["nba", "nfl"],
                "books": ["bet365"],
                "alternates": True,
            },
        ),
        ("subscribe-fanduel-props", {"sports": ["nba"]}),
    ]
    assert websocket.wait_for("odds-update", timeout_ms=10)["sports"]["nba"][0]["id"] == "game-1"

    websocket.destroy()
    assert fake_socket.disconnected is True


def test_ws_wait_for_times_out() -> None:
    websocket = OwlsInsightClient(OwlsInsightConfig(api_key="ws-secret")).websocket(
        socket_factory=lambda: FakeSocket()
    )
    with pytest.raises(TimeoutError):
        websocket.wait_for("odds-update", timeout_ms=1)


def test_owls_cli_odds_uses_dotenv_and_renders_json(monkeypatch, tmp_path: Path) -> None:
    runner = CliRunner()
    dotenv_path = tmp_path / ".env"
    dotenv_path.write_text("OWLS_INSIGHT_API_KEY=dotenv-secret\n")
    captured: dict[str, object] = {}

    def fake_client_factory(**kwargs):
        captured["config_kwargs"] = kwargs

        class FakeClient:
            def get_odds(self, **call_kwargs):
                captured["call_kwargs"] = call_kwargs
                return FakeResponsePayload({"success": True, "data": {"bet365": []}})

        return FakeClient()

    monkeypatch.setattr("bet_recorder.cli._owls_client", fake_client_factory)

    result = runner.invoke(
        app,
        [
            "owls",
            "odds",
            "--sport",
            "nba",
            "--books",
            "bet365,betmgm",
            "--alternates",
            "--dotenv-path",
            str(dotenv_path),
        ],
    )

    assert result.exit_code == 0
    assert json.loads(result.output) == {"success": True, "data": {"bet365": []}}
    assert captured["call_kwargs"] == {
        "sport": "nba",
        "books": ["bet365", "betmgm"],
        "alternates": True,
        "league": None,
    }
    assert captured["config_kwargs"] == {
        "api_key": None,
        "base_url": None,
        "timeout_seconds": None,
        "dotenv_path": dotenv_path,
    }


def test_legacy_owls_alias_still_works(monkeypatch) -> None:
    runner = CliRunner()
    monkeypatch.setattr(
        "bet_recorder.cli.load_owls_insight_config",
        lambda **kwargs: OwlsInsightConfig(api_key="secret"),
    )
    monkeypatch.setattr(
        "bet_recorder.cli.fetch_owls_insight_odds",
        lambda **kwargs: {"success": True, "data": {"ok": True}},
    )

    result = runner.invoke(
        app,
        ["owls-insight-odds", "--sport", "nba", "--endpoint", "moneyline"],
    )

    assert result.exit_code == 0
    assert json.loads(result.output) == {"success": True, "data": {"ok": True}}


class FakeResponsePayload:
    def __init__(self, payload: dict) -> None:
        self.payload = payload
