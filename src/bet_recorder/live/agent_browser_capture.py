from __future__ import annotations

from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile

from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.browser.adapter import build_action_payload
from bet_recorder.capture.run_bundle import RunBundle
from bet_recorder.capture.screenshots import write_screenshot
from bet_recorder.live.runner import record_live_action, record_live_page


def capture_agent_browser_page(
    *,
    source: str,
    bundle: RunBundle,
    page: str,
    captured_at: datetime,
    client: AgentBrowserClient,
    notes: list[str] | None = None,
    mime_type: str = "image/png",
) -> dict:
    effective_notes = notes or []
    temporary_path = _temporary_screenshot_path(suffix=_suffix_for_mime_type(mime_type))

    try:
        screenshot_path = client.capture_screenshot(temporary_path)
        relative_screenshot_path = write_screenshot(
            screenshots_dir=bundle.screenshots_dir,
            page=page,
            captured_at=captured_at,
            image_bytes=screenshot_path.read_bytes(),
            mime_type=mime_type,
        )
        payload = client.capture_page_state(
            page=page,
            captured_at=captured_at,
            screenshot_path=relative_screenshot_path,
            notes=effective_notes,
        ).to_payload()
        record_live_page(source=source, bundle=bundle, payload=payload)
        return payload
    finally:
        temporary_path.unlink(missing_ok=True)


def capture_agent_browser_page_state(
    *,
    source: str,
    bundle: RunBundle,
    page: str,
    captured_at: datetime,
    client: AgentBrowserClient,
    notes: list[str] | None = None,
) -> dict:
    payload = client.capture_page_state(
        page=page,
        captured_at=captured_at,
        screenshot_path=None,
        notes=notes or [],
    ).to_payload()
    record_live_page(source=source, bundle=bundle, payload=payload)
    return payload


def capture_agent_browser_action(
    *,
    source: str,
    bundle: RunBundle,
    page: str,
    action: str,
    target: str,
    status: str,
    captured_at: datetime,
    client: AgentBrowserClient,
    notes: list[str] | None = None,
    metadata: dict | None = None,
    mime_type: str = "image/png",
) -> dict:
    effective_notes = notes or []
    temporary_path = _temporary_screenshot_path(suffix=_suffix_for_mime_type(mime_type))

    try:
        screenshot_path = client.capture_screenshot(temporary_path)
        relative_screenshot_path = write_screenshot(
            screenshots_dir=bundle.screenshots_dir,
            page=page,
            captured_at=captured_at,
            image_bytes=screenshot_path.read_bytes(),
            mime_type=mime_type,
        )
        page_state = client.capture_page_state(
            page=page,
            captured_at=captured_at,
            screenshot_path=relative_screenshot_path,
            notes=effective_notes,
        )
        payload = build_action_payload(
            captured_at=captured_at,
            page=page,
            action=action,
            target=target,
            status=status,
            url=page_state.url,
            document_title=page_state.document_title,
            body_text=page_state.body_text,
            interactive_snapshot=page_state.interactive_snapshot,
            links=page_state.links,
            inputs=page_state.inputs,
            visible_actions=page_state.visible_actions,
            resource_hosts=page_state.resource_hosts,
            local_storage_keys=page_state.local_storage_keys,
            notes=effective_notes,
            metadata=metadata or {},
        )
        payload["screenshot_path"] = relative_screenshot_path
        record_live_action(source=source, bundle=bundle, payload=payload)
        return payload
    finally:
        temporary_path.unlink(missing_ok=True)


def _temporary_screenshot_path(*, suffix: str) -> Path:
    with NamedTemporaryFile(delete=False, suffix=suffix) as handle:
        return Path(handle.name)


def _suffix_for_mime_type(mime_type: str) -> str:
    if mime_type == "image/png":
        return ".png"
    if mime_type == "image/jpeg":
        return ".jpg"
    raise ValueError(f"Unsupported screenshot mime type: {mime_type}")
