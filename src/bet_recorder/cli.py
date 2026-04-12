from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import sys
import time

import typer

from bet_recorder.capture.run_bundle import (
    finalize_run_bundle,
    initialize_run_bundle,
    load_run_bundle,
)
from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.browser.cdp import DEFAULT_DEBUG_BASE_URL
from bet_recorder.analysis.bet365 import analyze_bet365_page
from bet_recorder.analysis.betuk import analyze_betuk_page
from bet_recorder.analysis.betway_uk import analyze_betway_page
from bet_recorder.blackjack_assistant import (
    DEFAULT_BLACKJACK_STRATEGY_ROOT,
    BlackjackAdvice,
    BlackjackSnapshot,
    build_blackjack_rule_options,
    capture_blackjack_advice,
    format_blackjack_notification,
    render_blackjack_screen,
)
from bet_recorder.analysis.generic_sportsbooks import (
    analyze_bet600_page,
    analyze_betfred_page,
    analyze_coral_page,
    analyze_kwik_page,
    analyze_ladbrokes_page,
)
from bet_recorder.analysis.position_watch import build_smarkets_watch_plan
from bet_recorder.analysis.smarkets_exchange import analyze_smarkets_page
from bet_recorder.analysis.trade_out import lay_position_trade_out
from bet_recorder.exchange_worker import (
    handle_worker_request_line,
    iter_worker_session_responses,
    load_watch_snapshot,
    WorkerConfig,
)
from bet_recorder.ledger.history_import import import_statement_history
from bet_recorder.ledger.tracked_bets_backfill import (
    build_backfilled_tracked_bets,
    load_statement_history_payload,
)
from bet_recorder.live.agent_browser_capture import (
    capture_agent_browser_action,
    capture_agent_browser_page,
)
from bet_recorder.live.cdp_transport_capture import capture_cdp_transport
from bet_recorder.live.runner import (
    record_live_action,
    record_live_page,
    record_live_transport,
    record_watch_plan,
)
from bet_recorder.owls_insight import (
    OwlsInsightApiError,
    OwlsInsightClient,
    OwlsInsightError,
    OwlsInsightWebSocket,
    fetch_odds as fetch_owls_insight_odds,
    load_config as load_owls_config,
    load_config as load_owls_insight_config,
    parse_csv_values,
)
from bet_recorder.paths import default_data_dir
from bet_recorder.watcher import (
    WatcherConfig,
    run_smarkets_watcher,
)

app = typer.Typer(
    help="Recorder-first CLI for capturing betting observations.",
)
owls_app = typer.Typer(help="Owls Insight REST and WebSocket commands.")
app.add_typer(owls_app, name="owls")


@app.callback(invoke_without_command=True)
def cli(ctx: typer.Context) -> None:
    """bet-recorder command group."""
    if ctx.invoked_subcommand is None:
        typer.echo(ctx.get_help())
        raise typer.Exit(code=0)


@app.command()
def capture() -> None:
    """Show the default capture directory."""
    typer.echo(default_data_dir())


@app.command("blackjack-assist")
def blackjack_assist(
    debug_base_url: str = typer.Option(DEFAULT_DEBUG_BASE_URL),
    target_url_fragment: str | None = typer.Option(None),
    frame_url_fragment: str = typer.Option("blackjack"),
    strategy_complexity: str = typer.Option("advanced"),
    blackjack_strategy_root: Path = typer.Option(DEFAULT_BLACKJACK_STRATEGY_ROOT),
    number_of_decks: int = typer.Option(6),
    hit_soft_17: bool = typer.Option(True, "--hit-soft-17/--stand-soft-17"),
    surrender: str = typer.Option("late"),
    double: str = typer.Option("any"),
    double_after_split: bool = typer.Option(
        True,
        "--double-after-split/--no-double-after-split",
    ),
    resplit_aces: bool = typer.Option(False, "--resplit-aces/--no-resplit-aces"),
    offer_insurance: bool = typer.Option(
        True,
        "--offer-insurance/--no-offer-insurance",
    ),
    max_split_hands: int = typer.Option(4),
    json_output: bool = typer.Option(False, "--json"),
    watch: bool = typer.Option(False),
    interval_seconds: float = typer.Option(1.0),
    notify: bool = typer.Option(True, "--notify/--no-notify"),
) -> None:
    """Read the live blackjack table from Chrome CDP and suggest the best move."""

    if json_output and watch:
        raise typer.BadParameter("--json cannot be combined with --watch")
    if interval_seconds <= 0:
        raise typer.BadParameter("--interval-seconds must be greater than zero")

    rules = build_blackjack_rule_options(
        number_of_decks=number_of_decks,
        hit_soft_17=hit_soft_17,
        surrender=surrender,
        double=double,
        double_after_split=double_after_split,
        resplit_aces=resplit_aces,
        offer_insurance=offer_insurance,
        max_split_hands=max_split_hands,
    )

    def capture_once() -> tuple[BlackjackSnapshot | None, BlackjackAdvice | None]:
        return capture_blackjack_advice(
            debug_base_url=debug_base_url,
            blackjack_strategy_root=blackjack_strategy_root,
            strategy_complexity=strategy_complexity,
            target_url_fragment=target_url_fragment,
            frame_url_fragment=frame_url_fragment,
            rules=rules,
        )

    def render_once(snapshot, advice) -> tuple[dict | None, str | None]:
        if json_output:
            if snapshot is None or advice is None:
                return {"active_hand": False}, None
            payload = advice.to_payload()
            payload["active_hand"] = True
            payload["game_state"] = snapshot.game_state
            payload["target_url"] = snapshot.target_url
            payload["frame_url"] = snapshot.frame_url
            payload["available_actions"] = snapshot.available_actions
            return payload, None
        return None, render_blackjack_screen(
            snapshot=snapshot,
            advice=advice,
            debug_base_url=debug_base_url,
        )

    try:
        if watch:
            last_notification: str | None = None
            while True:
                snapshot, advice = capture_once()
                payload, screen = render_once(snapshot, advice)
                if notify:
                    current_notification = None
                    if snapshot is not None and advice is not None:
                        current_notification = format_blackjack_notification(
                            snapshot=snapshot,
                            advice=advice,
                        )
                    if (
                        current_notification
                        and current_notification != last_notification
                    ):
                        typer.echo(
                            f"\a[{datetime.now().isoformat(timespec='seconds')}] {current_notification}",
                            err=True,
                        )
                    last_notification = current_notification
                typer.echo("\x1b[2J\x1b[H" + (screen or ""), nl=False)
                time.sleep(interval_seconds)
        snapshot, advice = capture_once()
        payload, screen = render_once(snapshot, advice)
    except KeyboardInterrupt:
        raise typer.Exit(code=0)
    except Exception as exc:
        typer.echo(str(exc), err=True)
        raise typer.Exit(code=1) from exc

    if payload is not None:
        typer.echo(json.dumps(payload))
        return
    typer.echo(screen or "")


@app.command("extract-page")
def extract_page(
    source: str = typer.Option(...),
    payload_path: Path = typer.Option(...),
) -> None:
    """Run source-specific extraction against a captured page payload."""
    payload = json.loads(payload_path.read_text())

    if source == "bet365":
        analysis = analyze_bet365_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    elif source == "betuk":
        analysis = analyze_betuk_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    elif source == "betway_uk":
        analysis = analyze_betway_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    elif source == "betfred":
        analysis = analyze_betfred_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    elif source == "coral":
        analysis = analyze_coral_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    elif source == "ladbrokes":
        analysis = analyze_ladbrokes_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    elif source == "kwik":
        analysis = analyze_kwik_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    elif source == "bet600":
        analysis = analyze_bet600_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    elif source == "smarkets_exchange":
        analysis = analyze_smarkets_page(
            page=payload["page"],
            body_text=payload["body_text"],
            inputs=payload.get("inputs", {}),
            visible_actions=payload.get("visible_actions", []),
        )
    else:
        raise typer.BadParameter(f"Unsupported extract-page source: {source}")

    typer.echo(json.dumps(analysis))


@app.command("calc-trade-out-lay")
def calc_trade_out_lay(
    entry_lay_odds: float = typer.Option(...),
    lay_stake: float = typer.Option(...),
    current_back_odds: float = typer.Option(...),
    commission_rate: float = typer.Option(0.0),
) -> None:
    """Calculate full trade-out for an exchange lay position."""
    typer.echo(
        json.dumps(
            lay_position_trade_out(
                entry_lay_odds=entry_lay_odds,
                lay_stake=lay_stake,
                current_back_odds=current_back_odds,
                commission_rate=commission_rate,
            ),
        ),
    )


@app.command("watch-open-positions")
def watch_open_positions(
    payload_path: Path = typer.Option(...),
    commission_rate: float = typer.Option(0.0),
    target_profit: float = typer.Option(1.0),
    stop_loss: float = typer.Option(1.0),
) -> None:
    """Build a trade-out watch plan from a captured Smarkets open_positions payload."""
    watch_plan = load_watch_snapshot(
        payload_path=payload_path,
        commission_rate=commission_rate,
        target_profit=target_profit,
        stop_loss=stop_loss,
    )
    typer.echo(json.dumps(watch_plan))


def _owls_client(
    *,
    api_key: str | None,
    base_url: str | None,
    ws_url: str | None = None,
    timeout_seconds: float | None = None,
    dotenv_path: Path | None = None,
) -> OwlsInsightClient:
    config = load_owls_config(
        api_key=api_key,
        base_url=base_url,
        ws_url=ws_url,
        timeout_seconds=timeout_seconds,
        dotenv_path=dotenv_path,
    )
    return OwlsInsightClient(config)


def _emit_owls_payload(payload: dict) -> None:
    typer.echo(json.dumps(payload))


def _parse_esports_filter(value: str | None) -> bool | list[str] | None:
    if value is None or not value.strip():
        return None
    normalized = value.strip().lower()
    if normalized in {"1", "true", "yes", "all"}:
        return True
    return parse_csv_values(value)


def _handle_owls_error(exc: Exception) -> None:
    raise typer.BadParameter(str(exc)) from exc


@owls_app.command("odds")
def owls_odds(
    sport: str = typer.Option(...),
    books: str | None = typer.Option(None),
    alternates: bool = typer.Option(False),
    league: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch all main odds markets for a sport."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_odds(
            sport=sport,
            books=parse_csv_values(books),
            alternates=alternates,
            league=league,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("moneyline")
def owls_moneyline(
    sport: str = typer.Option(...),
    books: str | None = typer.Option(None),
    league: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch moneyline odds for a sport."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_moneyline(
            sport=sport,
            books=parse_csv_values(books),
            league=league,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("spreads")
def owls_spreads(
    sport: str = typer.Option(...),
    books: str | None = typer.Option(None),
    alternates: bool = typer.Option(False),
    league: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch point spread odds for a sport."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_spreads(
            sport=sport,
            books=parse_csv_values(books),
            alternates=alternates,
            league=league,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("totals")
def owls_totals(
    sport: str = typer.Option(...),
    books: str | None = typer.Option(None),
    alternates: bool = typer.Option(False),
    league: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch totals odds for a sport."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_totals(
            sport=sport,
            books=parse_csv_values(books),
            alternates=alternates,
            league=league,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("props")
def owls_props(
    sport: str = typer.Option(...),
    game_id: str | None = typer.Option(None),
    player: str | None = typer.Option(None),
    category: str | None = typer.Option(None),
    books: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch player props for a sport."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_props(
            sport=sport,
            game_id=game_id,
            player=player,
            category=category,
            books=parse_csv_values(books),
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("book-props")
def owls_book_props(
    sport: str = typer.Option(...),
    book: str = typer.Option(...),
    game_id: str | None = typer.Option(None),
    player: str | None = typer.Option(None),
    category: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch player props from one sportsbook."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_book_props(
            sport=sport,
            book=book,
            game_id=game_id,
            player=player,
            category=category,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("props-history")
def owls_props_history(
    sport: str = typer.Option(...),
    game_id: str = typer.Option(...),
    player: str = typer.Option(...),
    category: str = typer.Option(...),
    hours: int | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch line movement history for one player prop."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_props_history(
            sport=sport,
            game_id=game_id,
            player=player,
            category=category,
            hours=hours,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("props-stats")
def owls_props_stats(
    book: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch coverage stats for props endpoints."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_props_stats(book=book)
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("scores")
def owls_scores(
    sport: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch live scores across all sports or one sport."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_scores(sport=sport)
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("stats")
def owls_stats(
    sport: str = typer.Option(...),
    date: str | None = typer.Option(None),
    player: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch live player box scores for a sport."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_stats(sport=sport, date=date, player=player)
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("stats-averages")
def owls_stats_averages(
    sport: str = typer.Option(...),
    player_name: str = typer.Option(...),
    opponent: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch rolling player averages."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_stats_averages(
            sport=sport,
            player_name=player_name,
            opponent=opponent,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("kalshi-markets")
def owls_kalshi_markets(
    sport: str = typer.Option(...),
    status: str | None = typer.Option(None),
    limit: int | None = typer.Option(None),
    cursor: str | None = typer.Option(None),
    event_ticker: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch Kalshi markets for a sport."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_kalshi_markets(
            sport=sport,
            status=status,
            limit=limit,
            cursor=cursor,
            event_ticker=event_ticker,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("kalshi-series")
def owls_kalshi_series(
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """List Kalshi series metadata."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).list_kalshi_series()
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("kalshi-series-markets")
def owls_kalshi_series_markets(
    series_ticker: str = typer.Option(...),
    status: str | None = typer.Option(None),
    limit: int | None = typer.Option(None),
    cursor: str | None = typer.Option(None),
    event_ticker: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch Kalshi markets for one series."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_kalshi_series_markets(
            series_ticker=series_ticker,
            status=status,
            limit=limit,
            cursor=cursor,
            event_ticker=event_ticker,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("polymarket-markets")
def owls_polymarket_markets(
    sport: str = typer.Option(...),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch Polymarket markets for a sport."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_polymarket_markets(sport=sport)
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("history-games")
def owls_history_games(
    sport: str | None = typer.Option(None),
    start_date: str | None = typer.Option(None),
    end_date: str | None = typer.Option(None),
    limit: int | None = typer.Option(None),
    offset: int | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch archived game metadata."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_history_games(
            sport=sport,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("history-odds")
def owls_history_odds(
    event_id: str = typer.Option(...),
    book: str | None = typer.Option(None),
    market: str | None = typer.Option(None),
    side: str | None = typer.Option(None),
    start_time: str | None = typer.Option(None),
    end_time: str | None = typer.Option(None),
    opening: bool | None = typer.Option(None),
    limit: int | None = typer.Option(None),
    offset: int | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch archived odds snapshots."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_history_odds(
            event_id=event_id,
            book=book,
            market=market,
            side=side,
            start_time=start_time,
            end_time=end_time,
            opening=opening,
            limit=limit,
            offset=offset,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("history-props")
def owls_history_props(
    event_id: str = typer.Option(...),
    player_name: str | None = typer.Option(None),
    prop_type: str | None = typer.Option(None),
    book: str | None = typer.Option(None),
    start_time: str | None = typer.Option(None),
    end_time: str | None = typer.Option(None),
    opening: bool | None = typer.Option(None),
    limit: int | None = typer.Option(None),
    offset: int | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch archived props snapshots."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_history_props(
            event_id=event_id,
            player_name=player_name,
            prop_type=prop_type,
            book=book,
            start_time=start_time,
            end_time=end_time,
            opening=opening,
            limit=limit,
            offset=offset,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("history-stats")
def owls_history_stats(
    event_id: str | None = typer.Option(None),
    player_name: str | None = typer.Option(None),
    sport: str | None = typer.Option(None),
    position: str | None = typer.Option(None),
    start_date: str | None = typer.Option(None),
    end_date: str | None = typer.Option(None),
    limit: int | None = typer.Option(None),
    offset: int | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch archived player stats."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_history_stats(
            event_id=event_id,
            player_name=player_name,
            sport=sport,
            position=position,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("history-stats-averages")
def owls_history_stats_averages(
    player_name: str = typer.Option(...),
    sport: str = typer.Option(...),
    opponent: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch archived rolling player averages."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_history_stats_averages(
            player_name=player_name,
            sport=sport,
            opponent=opponent,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("history-tennis-stats")
def owls_history_tennis_stats(
    event_id: str = typer.Option(...),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch archived tennis match stats."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_history_tennis_stats(event_id=event_id)
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("history-cs2-matches")
def owls_history_cs2_matches(
    team: str | None = typer.Option(None),
    event: str | None = typer.Option(None),
    stars: int | None = typer.Option(None),
    start_date: str | None = typer.Option(None),
    end_date: str | None = typer.Option(None),
    limit: int | None = typer.Option(None),
    offset: int | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch archived CS2 matches."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_history_cs2_matches(
            team=team,
            event=event,
            stars=stars,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("history-cs2-match")
def owls_history_cs2_match(
    match_id: str = typer.Option(...),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch one archived CS2 match."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_history_cs2_match(match_id=match_id)
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("history-cs2-players")
def owls_history_cs2_players(
    player_name: str | None = typer.Option(None),
    team: str | None = typer.Option(None),
    event: str | None = typer.Option(None),
    map_name: str | None = typer.Option(None),
    min_rating: float | None = typer.Option(None),
    start_date: str | None = typer.Option(None),
    end_date: str | None = typer.Option(None),
    limit: int | None = typer.Option(None),
    offset: int | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch archived CS2 player stats."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_history_cs2_players(
            player_name=player_name,
            team=team,
            event=event,
            map_name=map_name,
            min_rating=min_rating,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            offset=offset,
        )
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("realtime")
def owls_realtime(
    sport: str = typer.Option(...),
    league: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Fetch low-latency Pinnacle realtime odds."""
    try:
        response = _owls_client(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        ).get_realtime(sport=sport, league=league)
    except (OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)
    _emit_owls_payload(response.payload)


@owls_app.command("ws-listen")
def owls_ws_listen(
    event_name: str = typer.Option("odds-update"),
    sports: str | None = typer.Option(None),
    books: str | None = typer.Option(None),
    alternates: bool = typer.Option(False),
    props_book: str | None = typer.Option(None),
    esports: str | None = typer.Option(None),
    timeout_ms: int = typer.Option(10_000),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    ws_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Connect to the Owls Insight Socket.IO feed and print the next matching event."""
    websocket: OwlsInsightWebSocket | None = None
    try:
        client = _owls_client(
            api_key=api_key,
            base_url=base_url,
            ws_url=ws_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        )
        websocket = client.websocket()
        websocket.connect()
        sport_values = parse_csv_values(sports)
        if props_book is not None:
            websocket.subscribe_props(sports=sport_values, book=props_book)
        elif sport_values or books or alternates or esports:
            websocket.subscribe(
                sports=sport_values,
                books=parse_csv_values(books),
                alternates=alternates if alternates else None,
                esports=_parse_esports_filter(esports),
            )
        payload = websocket.wait_for(event_name, timeout_ms=timeout_ms)
    except (OwlsInsightError, ValueError, TimeoutError) as exc:
        _handle_owls_error(exc)
    finally:
        if websocket is not None:
            websocket.destroy()
    typer.echo(json.dumps(payload))


@app.command("owls-insight-odds")
def owls_insight_odds(
    sport: str = typer.Option(...),
    endpoint: str = typer.Option("odds"),
    books: str | None = typer.Option(None),
    alternates: bool = typer.Option(False),
    league: str | None = typer.Option(None),
    api_key: str | None = typer.Option(None),
    base_url: str | None = typer.Option(None),
    timeout_seconds: float | None = typer.Option(None),
    dotenv_path: Path | None = typer.Option(None),
) -> None:
    """Compatibility alias for `owls odds` and related market endpoints."""
    try:
        config = load_owls_insight_config(
            api_key=api_key,
            base_url=base_url,
            timeout_seconds=timeout_seconds,
            dotenv_path=dotenv_path,
        )
        payload = fetch_owls_insight_odds(
            config=config,
            sport=sport,
            endpoint=endpoint,
            books=parse_csv_values(books),
            alternates=alternates,
            league=league,
        )
    except (OwlsInsightApiError, OwlsInsightError, ValueError) as exc:
        _handle_owls_error(exc)

    typer.echo(json.dumps(payload))


@app.command("watch-smarkets-session")
def watch_smarkets_session(
    run_dir: Path = typer.Option(...),
    session: str = typer.Option(...),
    profile_path: Path | None = typer.Option(None),
    interval_seconds: float = typer.Option(5.0),
    commission_rate: float = typer.Option(0.0),
    target_profit: float = typer.Option(1.0),
    stop_loss: float = typer.Option(1.0),
    max_iterations: int | None = typer.Option(None),
) -> None:
    """Run a long-lived Smarkets watcher against an agent-browser session."""
    state = run_smarkets_watcher(
        WatcherConfig(
            run_dir=run_dir,
            session=session,
            profile_path=profile_path,
            interval_seconds=interval_seconds,
            commission_rate=commission_rate,
            target_profit=target_profit,
            stop_loss=stop_loss,
        ),
        max_iterations=max_iterations,
    )
    typer.echo(json.dumps(state))


@app.command("exchange-worker-stdio")
def exchange_worker_stdio(
    positions_payload_path: Path | None = typer.Option(None),
    run_dir: Path | None = typer.Option(None),
    account_payload_path: Path | None = typer.Option(None),
    open_bets_payload_path: Path | None = typer.Option(None),
    agent_browser_session: str | None = typer.Option(None),
    commission_rate: float = typer.Option(0.0),
    target_profit: float = typer.Option(1.0),
    stop_loss: float = typer.Option(1.0),
) -> None:
    """Read one transport request from stdin and answer with one exchange snapshot response."""
    try:
        response, _ = handle_worker_request_line(
            request_line=sys.stdin.read(),
            config=(
                WorkerConfig(
                    positions_payload_path=positions_payload_path,
                    run_dir=run_dir,
                    account_payload_path=account_payload_path,
                    open_bets_payload_path=open_bets_payload_path,
                    companion_legs_path=None,
                    agent_browser_session=agent_browser_session,
                    commission_rate=commission_rate,
                    target_profit=target_profit,
                    stop_loss=stop_loss,
                    hard_margin_call_profit_floor=None,
                    warn_only_default=True,
                )
                if positions_payload_path is not None or run_dir is not None
                else None
            ),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    typer.echo(json.dumps(response))


@app.command("exchange-worker-session")
def exchange_worker_session() -> None:
    """Serve newline-delimited JSON worker requests over one stdin/stdout session."""
    try:
        for response in iter_worker_session_responses(
            request_lines=sys.stdin,
        ):
            sys.stdout.write(json.dumps(response) + "\n")
            sys.stdout.flush()
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc


@app.command("import-ledger-history")
def import_ledger_history(
    source_dir: Path = typer.Option(..., exists=True, file_okay=False, dir_okay=True),
    output_path: Path | None = typer.Option(None),
) -> None:
    """Import historical statement rows into a ledger-side JSON payload."""
    payload = import_statement_history(source_dir)
    rendered = json.dumps(payload, indent=2) + "\n"
    if output_path is None:
        typer.echo(rendered, nl=False)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered)
    typer.echo(str(output_path))


@app.command("backfill-tracked-bets")
def backfill_tracked_bets(
    source_dir: Path | None = typer.Option(
        None,
        exists=True,
        file_okay=False,
        dir_okay=True,
    ),
    statement_history_path: Path | None = typer.Option(
        None,
        exists=True,
        file_okay=True,
        dir_okay=False,
    ),
    output_path: Path | None = typer.Option(None),
    commission_rate: float = typer.Option(0.0),
    max_match_hours: float = typer.Option(168.0),
) -> None:
    """Backfill matched tracked bets from bookmaker history plus Smarkets account history."""
    if (source_dir is None) == (statement_history_path is None):
        raise typer.BadParameter(
            "Provide exactly one of --source-dir or --statement-history-path."
        )

    try:
        payload = (
            import_statement_history(source_dir)
            if source_dir is not None
            else load_statement_history_payload(statement_history_path)
        )
        backfill_payload = build_backfilled_tracked_bets(
            payload,
            commission_rate=commission_rate,
            max_match_hours=max_match_hours,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    rendered = json.dumps(backfill_payload, indent=2) + "\n"
    if output_path is None:
        typer.echo(rendered, nl=False)
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(rendered)
    typer.echo(str(output_path))


@app.command("record-watch-plan")
def record_watch_plan_command(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    payload_path: Path = typer.Option(...),
) -> None:
    """Append one computed watch plan into an existing run bundle."""
    bundle = load_run_bundle(source=source, run_dir=run_dir)
    try:
        record_watch_plan(
            source=source,
            bundle=bundle,
            payload=json.loads(payload_path.read_text()),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps({"source": source, "events_path": str(bundle.events_path)}))


@app.command("init-run")
def init_run(
    source: str = typer.Option(...),
    root_dir: Path = typer.Option(default_data_dir()),
    started_at: str = typer.Option(...),
    collector_version: str = typer.Option(...),
    browser_profile_used: str = typer.Option(...),
    transport: bool = typer.Option(False, "--transport"),
) -> None:
    """Initialize a raw capture bundle and print its paths."""
    parsed_started_at = datetime.fromisoformat(started_at.replace("Z", "+00:00"))
    bundle = initialize_run_bundle(
        source=source,
        root_dir=root_dir,
        started_at=parsed_started_at,
        collector_version=collector_version,
        browser_profile_used=browser_profile_used,
        transport_capture_enabled=transport,
    )
    typer.echo(
        json.dumps(
            {
                "source": bundle.source,
                "run_dir": str(bundle.run_dir),
                "events_path": str(bundle.events_path),
                "metadata_path": str(bundle.metadata_path),
                "transport_path": str(bundle.transport_path)
                if bundle.transport_path
                else None,
                "screenshots_dir": str(bundle.screenshots_dir),
                "transport_capture_enabled": transport,
            },
        ),
    )


@app.command("record-page")
def record_page(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    payload_path: Path = typer.Option(...),
) -> None:
    """Append one source-specific page snapshot into an existing run bundle."""
    bundle = load_run_bundle(source=source, run_dir=run_dir)
    try:
        record_live_page(
            source=source,
            bundle=bundle,
            payload=json.loads(payload_path.read_text()),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    typer.echo(
        json.dumps(
            {
                "source": source,
                "run_dir": str(run_dir),
                "events_path": str(bundle.events_path),
            }
        )
    )


@app.command("finalize-run")
def finalize_run(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    ended_at: str = typer.Option(...),
) -> None:
    """Finalize a run bundle by updating metadata counts and end time."""
    bundle = load_run_bundle(source=source, run_dir=run_dir)
    finalize_run_bundle(
        bundle,
        ended_at=datetime.fromisoformat(ended_at.replace("Z", "+00:00")),
    )
    typer.echo(
        json.dumps({"source": source, "metadata_path": str(bundle.metadata_path)})
    )


@app.command("record-transport")
def record_transport(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    payload_path: Path = typer.Option(...),
) -> None:
    """Append one sanitized transport event into an existing run bundle."""
    bundle = load_run_bundle(source=source, run_dir=run_dir)
    try:
        record_live_transport(
            bundle=bundle,
            payload=json.loads(payload_path.read_text()),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(
        json.dumps(
            {
                "source": source,
                "transport_path": str(bundle.transport_path),
            },
        ),
    )


@app.command("record-action")
def record_action(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    payload_path: Path = typer.Option(...),
) -> None:
    """Append one explicit action event into an existing run bundle."""
    bundle = load_run_bundle(source=source, run_dir=run_dir)
    try:
        record_live_action(
            source=source,
            bundle=bundle,
            payload=json.loads(payload_path.read_text()),
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    typer.echo(json.dumps({"source": source, "events_path": str(bundle.events_path)}))


@app.command("capture-live-page")
def capture_live_page(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    payload_path: Path = typer.Option(...),
) -> None:
    """Append one browser-derived page payload into an existing run bundle."""
    record_page(source=source, run_dir=run_dir, payload_path=payload_path)


@app.command("capture-live-transport")
def capture_live_transport(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    payload_path: Path = typer.Option(...),
) -> None:
    """Append one browser-derived transport payload into an existing run bundle."""
    record_transport(source=source, run_dir=run_dir, payload_path=payload_path)


@app.command("capture-live-action")
def capture_live_action(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    payload_path: Path = typer.Option(...),
) -> None:
    """Append one browser-derived action payload into an existing run bundle."""
    record_action(source=source, run_dir=run_dir, payload_path=payload_path)


@app.command("capture-agent-browser-page")
def capture_agent_browser_page_command(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    page: str = typer.Option(...),
    captured_at: str = typer.Option(...),
    session: str | None = typer.Option(None),
    cdp_port: int | None = typer.Option(None),
    executable_path: str | None = typer.Option(None),
    note: list[str] | None = typer.Option(None, "--note"),
) -> None:
    """Capture the current page from agent-browser into an existing run bundle."""
    bundle = load_run_bundle(source=source, run_dir=run_dir)
    client = AgentBrowserClient(
        session=session,
        cdp_port=cdp_port,
        executable_path=executable_path,
    )
    payload = capture_agent_browser_page(
        source=source,
        bundle=bundle,
        page=page,
        captured_at=datetime.fromisoformat(captured_at.replace("Z", "+00:00")),
        client=client,
        notes=note,
    )
    typer.echo(
        json.dumps(
            {
                "source": source,
                "events_path": str(bundle.events_path),
                "screenshot_path": payload["screenshot_path"],
            },
        ),
    )


@app.command("capture-agent-browser-action")
def capture_agent_browser_action_command(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    page: str = typer.Option(...),
    action: str = typer.Option(...),
    target: str = typer.Option(...),
    status: str = typer.Option(...),
    captured_at: str = typer.Option(...),
    session: str | None = typer.Option(None),
    cdp_port: int | None = typer.Option(None),
    executable_path: str | None = typer.Option(None),
    note: list[str] | None = typer.Option(None, "--note"),
    metadata_path: Path | None = typer.Option(None),
) -> None:
    """Capture the current browser state as an explicit action event."""
    bundle = load_run_bundle(source=source, run_dir=run_dir)
    client = AgentBrowserClient(
        session=session,
        cdp_port=cdp_port,
        executable_path=executable_path,
    )
    metadata = (
        json.loads(metadata_path.read_text()) if metadata_path is not None else {}
    )
    payload = capture_agent_browser_action(
        source=source,
        bundle=bundle,
        page=page,
        action=action,
        target=target,
        status=status,
        captured_at=datetime.fromisoformat(captured_at.replace("Z", "+00:00")),
        client=client,
        notes=note,
        metadata=metadata,
    )
    typer.echo(
        json.dumps(
            {
                "source": source,
                "events_path": str(bundle.events_path),
                "screenshot_path": payload["screenshot_path"],
            },
        ),
    )


@app.command("capture-cdp-transport")
def capture_cdp_transport_command(
    source: str = typer.Option(...),
    run_dir: Path = typer.Option(...),
    duration_ms: int = typer.Option(...),
    debug_base_url: str = typer.Option(DEFAULT_DEBUG_BASE_URL),
    url_contains: str | None = typer.Option(None),
    reload: bool = typer.Option(False, "--reload"),
) -> None:
    """Capture live transport events from a CDP page target into an existing run bundle."""
    bundle = load_run_bundle(source=source, run_dir=run_dir)
    try:
        captured_event_count = capture_cdp_transport(
            source=source,
            bundle=bundle,
            debug_base_url=debug_base_url,
            duration_ms=duration_ms,
            reload=reload,
            url_contains=url_contains,
        )
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc

    typer.echo(
        json.dumps(
            {
                "source": source,
                "transport_path": str(bundle.transport_path),
                "captured_event_count": captured_event_count,
            },
        ),
    )


def main() -> None:
    app()
