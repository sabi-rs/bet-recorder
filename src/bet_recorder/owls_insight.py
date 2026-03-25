from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from json import JSONDecodeError
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Callable
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen
import json
import os

DEFAULT_BASE_URL = "https://api.owlsinsight.com"
DEFAULT_WS_URL = "wss://api.owlsinsight.com"
DEFAULT_TIMEOUT_SECONDS = 12.0
DEFAULT_DOTENV_BASENAMES = (".env", ".env.local")
API_KEY_ENV_NAMES = ("OWLS_INSIGHT_API_KEY", "OWLSINSIGHT_API_KEY")
SUPPORTED_ODDS_ENDPOINTS = frozenset({"odds", "moneyline", "spreads", "totals"})
SUPPORTED_PROP_BOOKS = frozenset(
    {"fanduel", "draftkings", "bet365", "betmgm", "caesars", "pinnacle"}
)
PROP_SUBSCRIPTION_EVENTS = {
    None: "subscribe-props",
    "pinnacle": "subscribe-props",
    "fanduel": "subscribe-fanduel-props",
    "draftkings": "subscribe-draftkings-props",
    "bet365": "subscribe-bet365-props",
    "betmgm": "subscribe-betmgm-props",
    "caesars": "subscribe-caesars-props",
}


@dataclass(frozen=True)
class RateLimitStatus:
    remaining_minute: int | None = None
    remaining_month: int | None = None
    reset_minute: str | None = None
    reset_month: str | None = None
    retry_after_ms: int | None = None


@dataclass(frozen=True)
class OwlsInsightConfig:
    api_key: str
    base_url: str = DEFAULT_BASE_URL
    ws_url: str = DEFAULT_WS_URL
    timeout_seconds: float = DEFAULT_TIMEOUT_SECONDS


@dataclass(frozen=True)
class OwlsInsightResponse:
    status: int
    payload: dict[str, Any]
    headers: dict[str, str]
    rate_limits: RateLimitStatus | None = None

    @property
    def success(self) -> bool:
        return bool(self.payload.get("success"))

    @property
    def data(self) -> Any:
        return self.payload.get("data")

    @property
    def meta(self) -> Any:
        return self.payload.get("meta")


class OwlsInsightError(RuntimeError):
    pass


class OwlsInsightApiError(OwlsInsightError):
    def __init__(
        self,
        message: str,
        *,
        status: int,
        headers: dict[str, str] | None = None,
        rate_limits: RateLimitStatus | None = None,
    ) -> None:
        super().__init__(message)
        self.status = status
        self.headers = headers or {}
        self.rate_limits = rate_limits


class AuthenticationError(OwlsInsightApiError):
    pass


class ForbiddenError(OwlsInsightApiError):
    pass


class NotFoundError(OwlsInsightApiError):
    pass


class RateLimitError(OwlsInsightApiError):
    pass


class ServerError(OwlsInsightApiError):
    pass


def load_config(
    *,
    api_key: str | None = None,
    base_url: str | None = None,
    ws_url: str | None = None,
    timeout_seconds: float | None = None,
    dotenv_path: str | Path | None = None,
    search_dotenv: bool = True,
) -> OwlsInsightConfig:
    dotenv_values = _load_dotenv_values(
        dotenv_path=Path(dotenv_path) if dotenv_path is not None else None,
        search=search_dotenv,
    )

    effective_api_key = (api_key or _lookup_env_alias(API_KEY_ENV_NAMES, dotenv_values)).strip()
    if not effective_api_key:
        raise ValueError(
            "Owls Insight API key is required. Set OWLS_INSIGHT_API_KEY or OWLSINSIGHT_API_KEY, add it to .env, or pass --api-key."
        )

    effective_base_url = (
        base_url
        or os.getenv("OWLS_INSIGHT_BASE_URL")
        or dotenv_values.get("OWLS_INSIGHT_BASE_URL")
        or DEFAULT_BASE_URL
    ).strip()
    if not effective_base_url:
        raise ValueError("Owls Insight base URL cannot be blank.")

    effective_ws_url = (
        ws_url
        or os.getenv("OWLS_INSIGHT_WS_URL")
        or dotenv_values.get("OWLS_INSIGHT_WS_URL")
        or DEFAULT_WS_URL
    ).strip()
    if not effective_ws_url:
        raise ValueError("Owls Insight WebSocket URL cannot be blank.")

    effective_timeout_seconds = _resolve_timeout_seconds(
        timeout_seconds=timeout_seconds,
        dotenv_values=dotenv_values,
    )

    return OwlsInsightConfig(
        api_key=effective_api_key,
        base_url=effective_base_url.rstrip("/"),
        ws_url=effective_ws_url.rstrip("/"),
        timeout_seconds=effective_timeout_seconds,
    )


def build_client(config: OwlsInsightConfig) -> OwlsInsightClient:
    return OwlsInsightClient(config)


def parse_csv_values(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []
    return [value.strip() for value in raw_value.split(",") if value.strip()]


def fetch_odds(
    *,
    config: OwlsInsightConfig,
    sport: str,
    endpoint: str = "odds",
    books: list[str] | None = None,
    alternates: bool = False,
    league: str | None = None,
) -> dict[str, Any]:
    client = OwlsInsightClient(config)
    method = {
        "odds": client.get_odds,
        "moneyline": client.get_moneyline,
        "spreads": client.get_spreads,
        "totals": client.get_totals,
    }.get(endpoint.strip().lower())
    if method is None:
        raise ValueError(
            "Owls Insight endpoint must be one of: "
            + ", ".join(sorted(SUPPORTED_ODDS_ENDPOINTS))
            + "."
        )
    return method(
        sport=sport,
        books=books,
        alternates=alternates,
        league=league,
    ).payload


class OwlsInsightClient:
    def __init__(self, config: OwlsInsightConfig) -> None:
        self.config = config

    def get_odds(
        self,
        *,
        sport: str,
        books: list[str] | None = None,
        alternates: bool = False,
        league: str | None = None,
    ) -> OwlsInsightResponse:
        return self._get_sport_market(
            sport=sport,
            endpoint="odds",
            books=books,
            alternates=alternates,
            league=league,
        )

    def get_moneyline(
        self,
        *,
        sport: str,
        books: list[str] | None = None,
        alternates: bool = False,
        league: str | None = None,
    ) -> OwlsInsightResponse:
        return self._get_sport_market(
            sport=sport,
            endpoint="moneyline",
            books=books,
            alternates=alternates,
            league=league,
        )

    def get_spreads(
        self,
        *,
        sport: str,
        books: list[str] | None = None,
        alternates: bool = False,
        league: str | None = None,
    ) -> OwlsInsightResponse:
        return self._get_sport_market(
            sport=sport,
            endpoint="spreads",
            books=books,
            alternates=alternates,
            league=league,
        )

    def get_totals(
        self,
        *,
        sport: str,
        books: list[str] | None = None,
        alternates: bool = False,
        league: str | None = None,
    ) -> OwlsInsightResponse:
        return self._get_sport_market(
            sport=sport,
            endpoint="totals",
            books=books,
            alternates=alternates,
            league=league,
        )

    def get_props(
        self,
        *,
        sport: str,
        game_id: str | None = None,
        player: str | None = None,
        category: str | None = None,
        books: list[str] | None = None,
    ) -> OwlsInsightResponse:
        return self._request(
            f"/api/v1/{_normalize_sport(sport)}/props",
            params={
                "game_id": game_id,
                "player": player,
                "category": category,
                "books": books,
            },
        )

    def get_book_props(
        self,
        *,
        sport: str,
        book: str,
        game_id: str | None = None,
        player: str | None = None,
        category: str | None = None,
    ) -> OwlsInsightResponse:
        normalized_book = _normalize_book(book)
        return self._request(
            f"/api/v1/{_normalize_sport(sport)}/props/{normalized_book}",
            params={
                "game_id": game_id,
                "player": player,
                "category": category,
            },
        )

    def get_props_history(
        self,
        *,
        sport: str,
        game_id: str,
        player: str,
        category: str,
        hours: int | None = None,
    ) -> OwlsInsightResponse:
        _require_value(game_id, "game_id")
        _require_value(player, "player")
        _require_value(category, "category")
        return self._request(
            f"/api/v1/{_normalize_sport(sport)}/props/history",
            params={
                "game_id": game_id,
                "player": player,
                "category": category,
                "hours": hours,
            },
        )

    def get_props_stats(self, *, book: str | None = None) -> OwlsInsightResponse:
        if book is None:
            return self._request("/api/v1/props/stats")
        return self._request(f"/api/v1/props/{_normalize_book(book)}/stats")

    def get_scores(self, *, sport: str | None = None) -> OwlsInsightResponse:
        if sport is None:
            return self._request("/api/v1/scores/live")
        return self._request(f"/api/v1/{_normalize_sport(sport)}/scores/live")

    def get_stats(
        self,
        *,
        sport: str,
        date: str | None = None,
        player: str | None = None,
    ) -> OwlsInsightResponse:
        return self._request(
            f"/api/v1/{_normalize_sport(sport)}/stats",
            params={"date": date, "player": player},
        )

    def get_stats_averages(
        self,
        *,
        sport: str,
        player_name: str,
        opponent: str | None = None,
    ) -> OwlsInsightResponse:
        _require_value(player_name, "player_name")
        return self._request(
            f"/api/v1/{_normalize_sport(sport)}/stats/averages",
            params={"playerName": player_name, "opponent": opponent},
        )

    def get_kalshi_markets(
        self,
        *,
        sport: str,
        status: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        event_ticker: str | None = None,
    ) -> OwlsInsightResponse:
        return self._request(
            f"/api/v1/kalshi/{_normalize_sport(sport)}/markets",
            params={
                "status": status,
                "limit": limit,
                "cursor": cursor,
                "eventTicker": event_ticker,
            },
        )

    def get_kalshi_series_markets(
        self,
        *,
        series_ticker: str,
        status: str | None = None,
        limit: int | None = None,
        cursor: str | None = None,
        event_ticker: str | None = None,
    ) -> OwlsInsightResponse:
        _require_value(series_ticker, "series_ticker")
        return self._request(
            f"/api/v1/kalshi/series/{series_ticker.strip()}/markets",
            params={
                "status": status,
                "limit": limit,
                "cursor": cursor,
                "eventTicker": event_ticker,
            },
        )

    def list_kalshi_series(self) -> OwlsInsightResponse:
        return self._request("/api/v1/kalshi/series")

    def get_polymarket_markets(self, *, sport: str) -> OwlsInsightResponse:
        return self._request(f"/api/v1/polymarket/{_normalize_sport(sport)}/markets")

    def get_history_games(
        self,
        *,
        sport: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> OwlsInsightResponse:
        return self._request(
            "/api/v1/history/games",
            params={
                "sport": _normalize_sport(sport) if sport else None,
                "startDate": start_date,
                "endDate": end_date,
                "limit": limit,
                "offset": offset,
            },
        )

    def get_history_odds(
        self,
        *,
        event_id: str,
        book: str | None = None,
        market: str | None = None,
        side: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        opening: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> OwlsInsightResponse:
        _require_value(event_id, "event_id")
        return self._request(
            "/api/v1/history/odds",
            params={
                "eventId": event_id,
                "book": _normalize_book(book) if book else None,
                "market": market,
                "side": side,
                "startTime": start_time,
                "endTime": end_time,
                "opening": opening,
                "limit": limit,
                "offset": offset,
            },
        )

    def get_history_props(
        self,
        *,
        event_id: str,
        player_name: str | None = None,
        prop_type: str | None = None,
        book: str | None = None,
        start_time: str | None = None,
        end_time: str | None = None,
        opening: bool | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> OwlsInsightResponse:
        _require_value(event_id, "event_id")
        return self._request(
            "/api/v1/history/props",
            params={
                "eventId": event_id,
                "playerName": player_name,
                "propType": prop_type,
                "book": _normalize_book(book) if book else None,
                "startTime": start_time,
                "endTime": end_time,
                "opening": opening,
                "limit": limit,
                "offset": offset,
            },
        )

    def get_history_stats(
        self,
        *,
        event_id: str | None = None,
        player_name: str | None = None,
        sport: str | None = None,
        position: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> OwlsInsightResponse:
        if not (event_id or player_name):
            raise ValueError("History stats require event_id or player_name.")
        return self._request(
            "/api/v1/history/stats",
            params={
                "eventId": event_id,
                "playerName": player_name,
                "sport": _normalize_sport(sport) if sport else None,
                "position": position,
                "startDate": start_date,
                "endDate": end_date,
                "limit": limit,
                "offset": offset,
            },
        )

    def get_history_stats_averages(
        self,
        *,
        player_name: str,
        sport: str,
        opponent: str | None = None,
    ) -> OwlsInsightResponse:
        _require_value(player_name, "player_name")
        return self._request(
            "/api/v1/history/stats/averages",
            params={
                "playerName": player_name,
                "sport": _normalize_sport(sport),
                "opponent": opponent,
            },
        )

    def get_history_tennis_stats(self, *, event_id: str) -> OwlsInsightResponse:
        _require_value(event_id, "event_id")
        return self._request(
            "/api/v1/history/tennis-stats",
            params={"eventId": event_id},
        )

    def get_history_cs2_matches(
        self,
        *,
        team: str | None = None,
        event: str | None = None,
        stars: int | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> OwlsInsightResponse:
        return self._request(
            "/api/v1/history/cs2/matches",
            params={
                "team": team,
                "event": event,
                "stars": stars,
                "startDate": start_date,
                "endDate": end_date,
                "limit": limit,
                "offset": offset,
            },
        )

    def get_history_cs2_match(self, *, match_id: str) -> OwlsInsightResponse:
        _require_value(match_id, "match_id")
        return self._request(f"/api/v1/history/cs2/matches/{match_id.strip()}")

    def get_history_cs2_players(
        self,
        *,
        player_name: str | None = None,
        team: str | None = None,
        event: str | None = None,
        map_name: str | None = None,
        min_rating: float | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        limit: int | None = None,
        offset: int | None = None,
    ) -> OwlsInsightResponse:
        return self._request(
            "/api/v1/history/cs2/players",
            params={
                "playerName": player_name,
                "team": team,
                "event": event,
                "mapName": map_name,
                "minRating": min_rating,
                "startDate": start_date,
                "endDate": end_date,
                "limit": limit,
                "offset": offset,
            },
        )

    def get_realtime(
        self,
        *,
        sport: str,
        league: str | None = None,
    ) -> OwlsInsightResponse:
        return self._request(
            f"/api/v1/{_normalize_sport(sport)}/realtime",
            params={"league": league},
        )

    def websocket(self, *, socket_factory: Callable[[], Any] | None = None) -> OwlsInsightWebSocket:
        return OwlsInsightWebSocket(self.config, socket_factory=socket_factory)

    def _get_sport_market(
        self,
        *,
        sport: str,
        endpoint: str,
        books: list[str] | None,
        alternates: bool,
        league: str | None,
    ) -> OwlsInsightResponse:
        normalized_endpoint = endpoint.strip().lower()
        if normalized_endpoint not in SUPPORTED_ODDS_ENDPOINTS:
            raise ValueError(
                "Owls Insight endpoint must be one of: "
                + ", ".join(sorted(SUPPORTED_ODDS_ENDPOINTS))
                + "."
            )
        return self._request(
            f"/api/v1/{_normalize_sport(sport)}/{normalized_endpoint}",
            params={
                "books": [_normalize_book(book) for book in (books or [])],
                "alternates": alternates if normalized_endpoint in {"odds", "spreads", "totals"} else None,
                "league": league,
            },
        )

    def _request(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> OwlsInsightResponse:
        return _fetch_json(config=self.config, path=path, query_params=params or {})


class OwlsInsightWebSocket:
    def __init__(
        self,
        config: OwlsInsightConfig,
        *,
        socket_factory: Callable[[], Any] | None = None,
    ) -> None:
        self.config = config
        self._socket_factory = socket_factory or _default_socket_factory
        self._client: Any | None = None
        self._queues: dict[str, Queue[Any]] = defaultdict(Queue)
        self._handlers: dict[str, list[Callable[[Any], None]]] = defaultdict(list)
        self._registered_events: set[str] = set()

    def connect(self) -> None:
        if self._client is not None:
            return

        client = self._socket_factory()
        self._client = client
        for event_name in (
            "connect",
            "connect_error",
            "disconnect",
            "odds-update",
            "player-props-update",
            "fanduel-props-update",
            "draftkings-props-update",
            "bet365-props-update",
            "betmgm-props-update",
            "caesars-props-update",
            "esports-update",
            "pinnacle-realtime",
        ):
            self._ensure_event_registration(event_name)

        connection_url = f"{self.config.ws_url}?{urlencode({'apiKey': self.config.api_key})}"
        try:
            client.connect(
                connection_url,
                transports=["websocket"],
                wait_timeout=self.config.timeout_seconds,
            )
        except TypeError:
            client.connect(connection_url, transports=["websocket"])
        except Exception as exc:
            self._client = None
            raise OwlsInsightError(f"Owls Insight WebSocket connect failed: {exc}") from exc

    def destroy(self) -> None:
        if self._client is None:
            return
        try:
            self._client.disconnect()
        finally:
            self._client = None

    def on(self, event_name: str, handler: Callable[[Any], None]) -> None:
        normalized_event = event_name.strip()
        if not normalized_event:
            raise ValueError("WebSocket event name cannot be blank.")
        self._handlers[normalized_event].append(handler)
        if self._client is not None:
            self._ensure_event_registration(normalized_event)

    def wait_for(self, event_name: str, timeout_ms: int = 10_000) -> Any:
        normalized_event = event_name.strip()
        if not normalized_event:
            raise ValueError("WebSocket event name cannot be blank.")
        if timeout_ms <= 0:
            raise ValueError("WebSocket timeout must be greater than zero.")
        if self._client is not None:
            self._ensure_event_registration(normalized_event)
        try:
            return self._queues[normalized_event].get(timeout=timeout_ms / 1000)
        except Empty as exc:
            raise TimeoutError(
                f"Timed out waiting for Owls Insight WebSocket event {normalized_event}."
            ) from exc

    def subscribe(
        self,
        *,
        sports: list[str] | None = None,
        books: list[str] | None = None,
        alternates: bool | None = None,
        esports: bool | list[str] | None = None,
    ) -> None:
        self._emit(
            "subscribe",
            _compact_dict(
                {
                    "sports": [_normalize_sport(sport) for sport in (sports or [])],
                    "books": [_normalize_book(book) for book in (books or [])],
                    "alternates": alternates,
                    "esports": (
                        [_normalize_sport(sport) for sport in esports]
                        if isinstance(esports, list)
                        else esports
                    ),
                }
            ),
        )

    def subscribe_props(
        self,
        *,
        sports: list[str],
        book: str | None = None,
    ) -> None:
        normalized_book = _normalize_book(book) if book else None
        event_name = PROP_SUBSCRIPTION_EVENTS.get(normalized_book)
        if event_name is None:
            raise ValueError(
                "Props subscription book must be one of: "
                + ", ".join(sorted(SUPPORTED_PROP_BOOKS))
                + "."
            )
        self._emit(
            event_name,
            {"sports": [_normalize_sport(sport) for sport in sports if sport.strip()]},
        )

    def _emit(self, event_name: str, payload: dict[str, Any]) -> None:
        if self._client is None:
            raise OwlsInsightError("Owls Insight WebSocket is not connected.")
        try:
            self._client.emit(event_name, payload)
        except Exception as exc:
            raise OwlsInsightError(
                f"Owls Insight WebSocket emit failed for {event_name}: {exc}"
            ) from exc

    def _ensure_event_registration(self, event_name: str) -> None:
        normalized_event = event_name.strip()
        if not normalized_event or normalized_event in self._registered_events:
            return
        if self._client is None:
            self._registered_events.add(normalized_event)
            return
        self._client.on(
            normalized_event,
            lambda payload=None, _event=normalized_event: self._dispatch(_event, payload),
        )
        self._registered_events.add(normalized_event)

    def _dispatch(self, event_name: str, payload: Any) -> None:
        self._queues[event_name].put(payload)
        for handler in self._handlers.get(event_name, []):
            handler(payload)


def _resolve_timeout_seconds(
    *,
    timeout_seconds: float | None,
    dotenv_values: dict[str, str],
) -> float:
    if timeout_seconds is None:
        raw_timeout = (
            os.getenv("OWLS_INSIGHT_TIMEOUT_SECONDS")
            or dotenv_values.get("OWLS_INSIGHT_TIMEOUT_SECONDS")
            or str(DEFAULT_TIMEOUT_SECONDS)
        ).strip()
        try:
            effective_timeout_seconds = float(raw_timeout)
        except ValueError as exc:
            raise ValueError(
                "Owls Insight timeout must be a positive number of seconds."
            ) from exc
    else:
        effective_timeout_seconds = timeout_seconds

    if effective_timeout_seconds <= 0:
        raise ValueError("Owls Insight timeout must be greater than zero.")
    return effective_timeout_seconds


def _load_dotenv_values(
    *,
    dotenv_path: Path | None,
    search: bool,
) -> dict[str, str]:
    candidate_paths: list[Path] = []
    if dotenv_path is not None:
        candidate_paths.append(dotenv_path)
    elif search:
        candidate_paths.extend(_discover_dotenv_paths())

    for path in candidate_paths:
        if path.is_file():
            return _parse_dotenv(path.read_text())
    return {}


def _lookup_env_alias(names: tuple[str, ...], dotenv_values: dict[str, str]) -> str:
    for name in names:
        value = os.getenv(name)
        if value is not None and value.strip():
            return value
    for name in names:
        value = dotenv_values.get(name)
        if value is not None and value.strip():
            return value
    return ""


def _discover_dotenv_paths() -> list[Path]:
    start_points = [Path.cwd()]
    package_root = Path(__file__).resolve().parents[3]
    start_points.append(package_root)

    candidates: list[Path] = []
    seen: set[Path] = set()
    for start in start_points:
        for ancestor in [start, *start.parents]:
            for basename in DEFAULT_DOTENV_BASENAMES:
                candidate = ancestor / basename
                if candidate in seen:
                    continue
                seen.add(candidate)
                candidates.append(candidate)
    return candidates


def _parse_dotenv(contents: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in contents.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip().strip("'").strip('"')
        values[key] = value
    return values


def _fetch_json(
    *,
    config: OwlsInsightConfig,
    path: str,
    query_params: dict[str, Any],
) -> OwlsInsightResponse:
    normalized_params = _compact_dict(query_params)
    url = f"{config.base_url}{path}"
    if normalized_params:
        url = f"{url}?{urlencode(_stringify_params(normalized_params))}"

    request = Request(
        url,
        headers={
            "Accept": "application/json",
            "Authorization": f"Bearer {config.api_key}",
        },
    )

    try:
        with urlopen(request, timeout=config.timeout_seconds) as response:
            body = response.read().decode("utf-8")
            headers = _headers_to_dict(getattr(response, "headers", {}))
            status = int(getattr(response, "status", 200))
    except HTTPError as exc:
        headers = _headers_to_dict(getattr(exc, "headers", {}) or getattr(exc, "hdrs", {}))
        rate_limits = _parse_rate_limit_headers(headers)
        raise _build_api_error(
            status=exc.code,
            detail=_read_error_body(exc),
            headers=headers,
            rate_limits=rate_limits,
        ) from exc
    except URLError as exc:
        raise OwlsInsightError(f"Owls Insight request failed: {exc.reason}") from exc

    try:
        payload = json.loads(body)
    except JSONDecodeError as exc:
        raise OwlsInsightError(
            f"Owls Insight response was not valid JSON: {_truncate(body, 240)}"
        ) from exc

    if not isinstance(payload, dict):
        raise OwlsInsightError("Owls Insight response must decode to a JSON object.")

    rate_limits = _parse_rate_limit_headers(headers)
    return OwlsInsightResponse(
        status=status,
        payload=payload,
        headers=headers,
        rate_limits=rate_limits,
    )


def _stringify_params(query_params: dict[str, Any]) -> dict[str, str]:
    rendered: dict[str, str] = {}
    for key, value in query_params.items():
        if isinstance(value, bool):
            rendered[key] = "true" if value else "false"
            continue
        if isinstance(value, (list, tuple)):
            joined = ",".join(str(item).strip() for item in value if str(item).strip())
            if joined:
                rendered[key] = joined
            continue
        rendered[key] = str(value)
    return rendered


def _compact_dict(values: dict[str, Any]) -> dict[str, Any]:
    compacted: dict[str, Any] = {}
    for key, value in values.items():
        if value is None:
            continue
        if isinstance(value, str) and not value.strip():
            continue
        if isinstance(value, (list, tuple)) and not [
            item for item in value if str(item).strip()
        ]:
            continue
        compacted[key] = value
    return compacted


def _parse_rate_limit_headers(headers: dict[str, str]) -> RateLimitStatus | None:
    if not headers:
        return None
    retry_after = headers.get("retry-after")
    retry_after_ms = None
    if retry_after:
        try:
            retry_after_ms = int(float(retry_after) * 1000)
        except ValueError:
            retry_after_ms = None
    return RateLimitStatus(
        remaining_minute=_parse_optional_int(headers.get("x-ratelimit-remaining-minute")),
        remaining_month=_parse_optional_int(headers.get("x-ratelimit-remaining-month")),
        reset_minute=headers.get("x-ratelimit-reset-minute"),
        reset_month=headers.get("x-ratelimit-reset-month"),
        retry_after_ms=retry_after_ms,
    )


def _parse_optional_int(value: str | None) -> int | None:
    if value is None or not value.strip():
        return None
    try:
        return int(value)
    except ValueError:
        return None


def _build_api_error(
    *,
    status: int,
    detail: str,
    headers: dict[str, str],
    rate_limits: RateLimitStatus | None,
) -> OwlsInsightApiError:
    message = f"Owls Insight request failed with HTTP {status}: {detail}"
    if status == 401:
        return AuthenticationError(
            message, status=status, headers=headers, rate_limits=rate_limits
        )
    if status == 403:
        return ForbiddenError(
            message, status=status, headers=headers, rate_limits=rate_limits
        )
    if status == 404:
        return NotFoundError(
            message, status=status, headers=headers, rate_limits=rate_limits
        )
    if status == 429:
        return RateLimitError(
            message, status=status, headers=headers, rate_limits=rate_limits
        )
    if status >= 500:
        return ServerError(
            message, status=status, headers=headers, rate_limits=rate_limits
        )
    return OwlsInsightApiError(
        message, status=status, headers=headers, rate_limits=rate_limits
    )


def _read_error_body(error: HTTPError) -> str:
    try:
        payload = error.read().decode("utf-8").strip()
    except Exception:
        payload = ""
    return _truncate(payload or error.reason or "unknown error", 240)


def _headers_to_dict(headers: Any) -> dict[str, str]:
    if hasattr(headers, "items"):
        return {str(key).lower(): str(value) for key, value in headers.items()}
    return {}


def _normalize_sport(sport: str | None) -> str:
    if sport is None or not sport.strip():
        raise ValueError("Owls Insight sport cannot be blank.")
    return sport.strip().lower()


def _normalize_book(book: str | None) -> str:
    if book is None or not book.strip():
        raise ValueError("Owls Insight book cannot be blank.")
    return book.strip().lower()


def _require_value(value: str | None, field_name: str) -> None:
    if value is None or not value.strip():
        raise ValueError(f"Owls Insight {field_name} cannot be blank.")


def _truncate(value: str, limit: int) -> str:
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _default_socket_factory() -> Any:
    try:
        import socketio  # type: ignore
    except ImportError as exc:
        raise OwlsInsightError(
            "Owls Insight WebSocket support requires python-socketio. Install the project dependencies again to enable it."
        ) from exc
    return socketio.Client(reconnection=True)
