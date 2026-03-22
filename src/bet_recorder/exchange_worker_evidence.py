from __future__ import annotations

from pathlib import Path
import json

RECORDER_EVENT_HISTORY_LIMIT = 8
TRANSPORT_MARKER_HISTORY_LIMIT = 8


def load_run_bundle_events(run_dir: Path | None) -> list[dict]:
    if run_dir is None:
        return []

    events_path = run_dir / "events.jsonl"
    if not events_path.exists():
        return []

    events: list[dict] = []
    for line in events_path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def load_transport_capture_events(run_dir: Path | None) -> list[dict]:
    if run_dir is None:
        return []

    transport_path = run_dir / "transport.jsonl"
    if not transport_path.exists():
        return []

    events: list[dict] = []
    for line in transport_path.read_text().splitlines():
        if not line.strip():
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(event, dict):
            events.append(event)
    return events


def build_recorder_bundle_summary(run_dir: Path | None) -> dict | None:
    if run_dir is None:
        return None

    events = load_run_bundle_events(run_dir)
    latest_event = events[-1] if events else {}
    latest_positions_event = next(
        (event for event in reversed(events) if event.get("kind") == "positions_snapshot"),
        {},
    )
    latest_watch_plan_event = next(
        (event for event in reversed(events) if event.get("kind") == "watch_plan_snapshot"),
        {},
    )
    return {
        "run_dir": str(run_dir),
        "event_count": len(events),
        "latest_event_at": _event_timestamp(latest_event),
        "latest_event_kind": str(latest_event.get("kind", "") or ""),
        "latest_event_summary": _event_summary(latest_event),
        "latest_positions_at": _event_timestamp(latest_positions_event),
        "latest_watch_plan_at": _event_timestamp(latest_watch_plan_event),
    }


def load_recent_recorder_events(
    run_dir: Path | None,
    *,
    limit: int = RECORDER_EVENT_HISTORY_LIMIT,
) -> list[dict]:
    events = load_run_bundle_events(run_dir)
    if limit <= 0:
        return []
    return [_normalize_recorder_event(event) for event in events[-limit:]][::-1]


def build_transport_marker_summary(run_dir: Path | None) -> dict | None:
    if run_dir is None:
        return None

    markers = [
        event
        for event in load_transport_capture_events(run_dir)
        if event.get("kind") == "interaction_marker"
    ]
    latest_marker = markers[-1] if markers else {}
    return {
        "transport_path": str(run_dir / "transport.jsonl"),
        "marker_count": len(markers),
        "latest_marker_at": _event_timestamp(latest_marker),
        "latest_marker_action": str(latest_marker.get("action", "") or ""),
        "latest_marker_phase": str(latest_marker.get("phase", "") or ""),
        "latest_marker_summary": _transport_marker_summary(latest_marker),
    }


def load_recent_transport_markers(
    run_dir: Path | None,
    *,
    limit: int = TRANSPORT_MARKER_HISTORY_LIMIT,
) -> list[dict]:
    markers = [
        event
        for event in load_transport_capture_events(run_dir)
        if event.get("kind") == "interaction_marker"
    ]
    if limit <= 0:
        return []
    return [_normalize_transport_marker(event) for event in markers[-limit:]][::-1]


def _normalize_recorder_event(event: dict) -> dict:
    return {
        "captured_at": _event_timestamp(event),
        "kind": str(event.get("kind", "") or ""),
        "source": str(event.get("source", "") or ""),
        "page": str(event.get("page", "") or ""),
        "action": str(event.get("action", "") or ""),
        "status": str(event.get("status", "") or ""),
        "request_id": str(event.get("request_id", "") or ""),
        "reference_id": str(event.get("reference_id", "") or ""),
        "summary": _event_summary(event),
        "detail": _event_detail(event),
    }


def _normalize_transport_marker(event: dict) -> dict:
    return {
        "captured_at": _event_timestamp(event),
        "kind": str(event.get("kind", "") or ""),
        "action": str(event.get("action", "") or ""),
        "phase": str(event.get("phase", "") or ""),
        "request_id": str(event.get("request_id", "") or ""),
        "reference_id": str(event.get("reference_id", "") or ""),
        "summary": _transport_marker_summary(event),
        "detail": str(event.get("detail", "") or ""),
    }


def _event_timestamp(event: dict) -> str:
    return str(event.get("captured_at", "") or event.get("occurred_at", "") or "")


def _event_summary(event: dict) -> str:
    kind = str(event.get("kind", "") or "")
    source = _humanize_event_field(event.get("source"), fallback="recorder")
    page = _humanize_event_field(event.get("page"), fallback="capture")

    if kind == "watch_plan_snapshot":
        watch_count = int(event.get("watch_count", 0) or 0)
        position_count = int(event.get("position_count", 0) or 0)
        return (
            f"Watch plan refreshed with {watch_count} row(s) across "
            f"{position_count} position(s)."
        )
    if kind == "action_snapshot":
        action = str(event.get("action", "") or "action")
        status = str(event.get("status", "") or "unknown")
        target = str(event.get("target", "") or "").strip()
        if target:
            return f"{action} {target} -> {status}"
        return f"Action {action} -> {status}"
    if kind == "operator_interaction":
        action = str(event.get("action", "") or "interaction")
        status = str(event.get("status", "") or "unknown")
        reference_id = str(event.get("reference_id", "") or "").strip()
        if reference_id:
            return f"{action} {reference_id} -> {status}"
        return f"{action} -> {status}"
    if kind:
        return f"Captured {source} {page} ({kind})."
    return "Recorded bundle event."


def _event_detail(event: dict) -> str:
    for field in ("detail", "document_title", "url", "target", "status"):
        value = str(event.get(field, "") or "").strip()
        if value:
            return value
    notes = event.get("notes")
    if isinstance(notes, list):
        joined = ", ".join(str(note).strip() for note in notes if str(note).strip())
        if joined:
            return joined
    return ""


def _humanize_event_field(value, *, fallback: str) -> str:
    text = str(value or "").strip().replace("_", " ")
    return text or fallback


def _transport_marker_summary(event: dict) -> str:
    action = str(event.get("action", "") or "interaction")
    phase = str(event.get("phase", "") or "event")
    request_id = str(event.get("request_id", "") or "").strip()
    reference_id = str(event.get("reference_id", "") or "").strip()
    identifiers = " ".join(value for value in (request_id, reference_id) if value).strip()
    if identifiers:
        return f"{phase} {action} {identifiers}".strip()
    return f"{phase} {action}".strip()
