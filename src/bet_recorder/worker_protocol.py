from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class WorkerConfig:
    positions_payload_path: Path | None
    run_dir: Path | None
    account_payload_path: Path | None
    open_bets_payload_path: Path | None
    companion_legs_path: Path | None
    agent_browser_session: str | None
    commission_rate: float
    target_profit: float
    stop_loss: float
    hard_margin_call_profit_floor: float | None
    warn_only_default: bool

    @classmethod
    def from_dict(cls, payload: dict) -> WorkerConfig:
        if (
            payload.get("positions_payload_path") is None
            and payload.get("run_dir") is None
        ):
            raise ValueError(
                "Worker config requires positions_payload_path or run_dir."
            )
        try:
            return cls(
                positions_payload_path=(
                    Path(payload["positions_payload_path"])
                    if payload.get("positions_payload_path") is not None
                    else None
                ),
                run_dir=Path(payload["run_dir"])
                if payload.get("run_dir") is not None
                else None,
                account_payload_path=(
                    Path(payload["account_payload_path"])
                    if payload.get("account_payload_path") is not None
                    else None
                ),
                open_bets_payload_path=(
                    Path(payload["open_bets_payload_path"])
                    if payload.get("open_bets_payload_path") is not None
                    else None
                ),
                companion_legs_path=(
                    Path(payload["companion_legs_path"])
                    if payload.get("companion_legs_path") is not None
                    else None
                ),
                agent_browser_session=(
                    str(payload["agent_browser_session"])
                    if payload.get("agent_browser_session") is not None
                    else None
                ),
                commission_rate=float(payload["commission_rate"]),
                target_profit=float(payload["target_profit"]),
                stop_loss=float(payload["stop_loss"]),
                hard_margin_call_profit_floor=(
                    float(payload["hard_margin_call_profit_floor"])
                    if payload.get("hard_margin_call_profit_floor") is not None
                    else None
                ),
                warn_only_default=bool(payload.get("warn_only_default", True)),
            )
        except KeyError as exc:
            raise ValueError(f"Missing worker config field: {exc.args[0]}") from exc
        except (TypeError, ValueError) as exc:
            raise ValueError(
                "Worker config fields must be valid JSON scalars."
            ) from exc


def parse_worker_request(request: str | dict) -> tuple[str, dict | None]:
    if isinstance(request, str) and request in {
        "LoadDashboard",
        "Refresh",
        "RefreshCached",
        "RefreshLive",
    }:
        return request, None

    if isinstance(request, dict) and len(request) == 1:
        request_name, request_payload = next(iter(request.items()))
        if request_name in {
            "LoadDashboard",
            "Refresh",
            "RefreshCached",
            "RefreshLive",
        }:
            if request_payload is None:
                return request_name, None
            if not isinstance(request_payload, dict):
                raise ValueError(
                    f"{request_name} payload must be an object when provided."
                )
            return request_name, request_payload

        if request_name == "SelectVenue":
            if not isinstance(request_payload, dict):
                raise ValueError("SelectVenue payload must be an object.")
            return request_name, request_payload

        if request_name == "CashOutTrackedBet":
            if not isinstance(request_payload, dict):
                raise ValueError("CashOutTrackedBet payload must be an object.")
            return request_name, request_payload

        if request_name == "ExecuteTradingAction":
            if not isinstance(request_payload, dict):
                raise ValueError("ExecuteTradingAction payload must be an object.")
            return request_name, request_payload

        if request_name == "LoadHorseMatcher":
            if not isinstance(request_payload, dict):
                raise ValueError("LoadHorseMatcher payload must be an object.")
            return request_name, request_payload

    raise ValueError(f"Unsupported worker request: {request}")


def require_worker_config(
    config: WorkerConfig | None, request_name: str
) -> WorkerConfig:
    if config is None:
        raise ValueError(
            f"{request_name} requires worker config. Send LoadDashboard with config first.",
        )
    return config


def resolve_load_dashboard_config(
    request_payload: dict | None,
    *,
    config: WorkerConfig | None,
) -> WorkerConfig:
    next_config = config
    if request_payload is not None:
        request_config = request_payload.get("config")
        if request_config is not None:
            if not isinstance(request_config, dict):
                raise ValueError("LoadDashboard config must be an object.")
            next_config = WorkerConfig.from_dict(request_config)
    return require_worker_config(next_config, "LoadDashboard")


def build_worker_request_error_response(detail: str) -> dict:
    return {
        "snapshot": {
            "worker": {
                "name": "bet-recorder",
                "status": "error",
                "detail": detail,
            },
            "venues": [],
            "selected_venue": None,
            "events": [],
            "markets": [],
            "preflight": None,
            "status_line": detail,
            "watch": None,
        },
        "request_error": detail,
    }
