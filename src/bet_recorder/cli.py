from __future__ import annotations

from datetime import datetime
from pathlib import Path
import json
import sys

import typer

from bet_recorder.capture.run_bundle import (
    finalize_run_bundle,
    initialize_run_bundle,
    load_run_bundle,
)
from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.browser.cdp import DEFAULT_DEBUG_BASE_URL
from bet_recorder.analysis.betway_uk import analyze_betway_page
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
from bet_recorder.paths import default_data_dir
from bet_recorder.watcher import (
    WatcherConfig,
    run_smarkets_watcher,
)

app = typer.Typer(
    help="Recorder-first CLI for capturing betting observations.",
)


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


@app.command("extract-page")
def extract_page(
    source: str = typer.Option(...),
    payload_path: Path = typer.Option(...),
) -> None:
    """Run source-specific extraction against a captured page payload."""
    payload = json.loads(payload_path.read_text())

    if source == "betway_uk":
        analysis = analyze_betway_page(
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
