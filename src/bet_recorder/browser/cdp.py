from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import asyncio
import json
from urllib.parse import urlparse
from urllib.request import urlopen

JsonFetcher = Callable[[str], str]

DEFAULT_DEBUG_BASE_URL = "http://127.0.0.1:9222"
DEFAULT_EVALUATE_TIMEOUT_SECONDS = 10.0
DEFAULT_INTERACTION_WAIT_MS = 1500
SOURCE_URL_FRAGMENTS = {
    "bet365": "bet365.com",
    "betuk": "betuk.com",
    "betfred": "betfred.com",
    "coral": "coral",
    "ladbrokes": "ladbrokes",
    "kwik": "kwiff.com",
    "bet600": "bet600",
    "betdaq": "betdaq.com",
    "betway_uk": "betway",
    "smarkets_exchange": "smarkets.com",
}
PAGE_STATE_JS = r"""
(() => {
  const normalizeText = (value) => (value || "").replace(/\s+/g, " ").trim();
  const interactiveSnapshot = Array.from(
    document.querySelectorAll("a, button, [role='button'], input, textarea, select")
  )
    .map((element, index) => ({
      ref: `e${index + 1}`,
      role: element.getAttribute("role") || element.tagName.toLowerCase(),
      name: normalizeText(
        element.getAttribute("aria-label")
          || element.innerText
          || element.textContent
          || element.getAttribute("placeholder")
          || element.getAttribute("name")
          || element.id
      ),
    }))
    .filter((item) => item.name);
  const links = Array.from(document.querySelectorAll("a[href]"))
    .map((anchor) => anchor.href)
    .filter(Boolean);
  const inputs = Object.fromEntries(
    Array.from(document.querySelectorAll("input, textarea, select")).map((element, index) => {
      const key = (
        element.getAttribute("name")
        || element.id
        || element.getAttribute("placeholder")
        || `input_${index}`
      );
      return [key, element.value ?? ""];
    })
  );
  const visibleActions = Array.from(document.querySelectorAll("a, button, [role='button']"))
    .map((element) => normalizeText(element.innerText || element.textContent))
    .filter(Boolean);
  const resourceHosts = Array.from(
    new Set(
      performance.getEntriesByType("resource")
        .map((entry) => {
          try {
            return new URL(entry.name).hostname;
          } catch {
            return null;
          }
        })
        .filter(Boolean)
    )
  );
  const localStorageKeys = [];
  try {
    for (let index = 0; index < localStorage.length; index += 1) {
      const key = localStorage.key(index);
      if (key) {
        localStorageKeys.push(key);
      }
    }
  } catch (error) {
    // Ignore storage access failures on restricted pages.
  }
  return {
    url: location.href,
    document_title: document.title,
    body_text: document.body?.innerText ?? "",
    interactive_snapshot: interactiveSnapshot,
    links,
    inputs,
    visible_actions: visibleActions,
    resource_hosts: resourceHosts,
    local_storage_keys: localStorageKeys,
  };
})()
"""


@dataclass(frozen=True)
class DebugTarget:
    target_id: str
    target_type: str
    title: str
    url: str
    websocket_debugger_url: str


def list_debug_targets(
    *,
    debug_base_url: str = DEFAULT_DEBUG_BASE_URL,
    fetch_text: JsonFetcher | None = None,
    target_types: Sequence[str] | None = ("page",),
) -> list[DebugTarget]:
    loader = fetch_text or _fetch_text
    payload = json.loads(loader(f"{debug_base_url}/json/list"))
    allowed_types = {target_type for target_type in (target_types or []) if target_type}
    return [
        DebugTarget(
            target_id=item["id"],
            target_type=item["type"],
            title=item.get("title", ""),
            url=item.get("url", ""),
            websocket_debugger_url=item["webSocketDebuggerUrl"],
        )
        for item in payload
        if item.get("webSocketDebuggerUrl")
        and (not allowed_types or item.get("type") in allowed_types)
    ]


def select_debug_target(
    *,
    source: str,
    targets: list[DebugTarget],
    url_contains: str | None = None,
) -> DebugTarget:
    effective_fragment = url_contains or SOURCE_URL_FRAGMENTS.get(source)
    if effective_fragment is not None:
        for target in targets:
            if effective_fragment in target.url:
                return target

    if len(targets) == 1:
        return targets[0]

    raise ValueError(f"Could not find a CDP target for source: {source}")


def select_debug_target_by_fragments(
    *,
    targets: Sequence[DebugTarget],
    url_fragments: Sequence[str],
) -> DebugTarget:
    lowered_fragments = [fragment.lower() for fragment in url_fragments if fragment]
    for fragment in lowered_fragments:
        for target in targets:
            if fragment in target.url.lower():
                return target
    raise ValueError(
        "Could not find a CDP target matching any fragment: " + ", ".join(url_fragments)
    )


def evaluate_debug_target_value(
    *,
    websocket_debugger_url: str,
    expression: str,
    await_promise: bool = False,
    frame_url_fragments: Sequence[str] | None = None,
):
    return asyncio.run(
        _evaluate_debug_target_expression(
            websocket_debugger_url=websocket_debugger_url,
            expression=expression,
            await_promise=await_promise,
            frame_url_fragments=frame_url_fragments,
        ),
    )


def evaluate_debug_target_main_world_value(
    *,
    websocket_debugger_url: str,
    expression: str,
    await_promise: bool = False,
    frame_url_fragments: Sequence[str] | None = None,
):
    return asyncio.run(
        _evaluate_debug_target_expression(
            websocket_debugger_url=websocket_debugger_url,
            expression=expression,
            await_promise=await_promise,
            frame_url_fragments=frame_url_fragments,
            use_main_world=True,
        ),
    )


def fetch_debug_target_json(
    *,
    websocket_debugger_url: str,
    url: str,
    frame_url_fragments: Sequence[str] | None = None,
) -> dict:
    payload = evaluate_debug_target_value(
        websocket_debugger_url=websocket_debugger_url,
        expression=f"""
      (async () => {{
        const response = await fetch({json.dumps(url)}, {{ credentials: "include" }});
        const text = await response.text();
        let body = null;
        try {{
          body = JSON.parse(text);
        }} catch (error) {{
          body = null;
        }}
        return {{
          url: {json.dumps(url)},
          status: response.status,
          ok: response.ok,
          body,
          body_text: text,
        }};
      }})()
    """,
        await_promise=True,
        frame_url_fragments=frame_url_fragments,
    )
    if not isinstance(payload, dict):
        raise ValueError(f"CDP fetch did not return an object payload for URL: {url}")
    return {
        "url": str(payload.get("url", url)),
        "status": int(payload.get("status", 0) or 0),
        "ok": bool(payload.get("ok", False)),
        "body": payload.get("body"),
        "body_text": str(payload.get("body_text", "") or ""),
    }


def navigate_debug_target(
    *,
    websocket_debugger_url: str,
    url: str,
    wait_ms: int = DEFAULT_INTERACTION_WAIT_MS,
) -> None:
    asyncio.run(
        _navigate_debug_target(
            websocket_debugger_url=websocket_debugger_url,
            url=url,
            wait_ms=wait_ms,
        ),
    )


def click_debug_target_by_labels(
    *,
    websocket_debugger_url: str,
    labels: Sequence[str],
    wait_ms: int = DEFAULT_INTERACTION_WAIT_MS,
    frame_url_fragments: Sequence[str] | None = None,
):
    return asyncio.run(
        _click_debug_target_by_labels(
            websocket_debugger_url=websocket_debugger_url,
            labels=labels,
            wait_ms=wait_ms,
            frame_url_fragments=frame_url_fragments,
        ),
    )


def capture_debug_target_page_state(
    *,
    websocket_debugger_url: str,
    page: str,
    captured_at: datetime,
    notes: Sequence[str] | None = None,
    frame_url_fragments: Sequence[str] | None = None,
) -> dict:
    payload = evaluate_debug_target_value(
        websocket_debugger_url=websocket_debugger_url,
        expression=PAGE_STATE_JS,
        frame_url_fragments=frame_url_fragments,
    )
    if not isinstance(payload, dict):
        raise ValueError("CDP page capture did not return an object payload.")
    return {
        "page": page,
        "url": str(payload.get("url", "")),
        "document_title": str(payload.get("document_title", "")),
        "body_text": str(payload.get("body_text", "")),
        "interactive_snapshot": list(payload.get("interactive_snapshot", [])),
        "links": list(payload.get("links", [])),
        "inputs": dict(payload.get("inputs", {})),
        "visible_actions": list(payload.get("visible_actions", [])),
        "resource_hosts": list(payload.get("resource_hosts", [])),
        "local_storage_keys": list(payload.get("local_storage_keys", [])),
        "screenshot_path": None,
        "notes": list(notes or []),
        "captured_at": captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def capture_transport_events(
    *,
    websocket_debugger_url: str,
    duration_ms: int,
    reload: bool = False,
) -> list[dict]:
    return asyncio.run(
        _capture_transport_events(
            websocket_debugger_url=websocket_debugger_url,
            duration_ms=duration_ms,
            reload=reload,
        ),
    )


async def _capture_transport_events(
    *,
    websocket_debugger_url: str,
    duration_ms: int,
    reload: bool,
) -> list[dict]:
    import websockets

    async with websockets.connect(websocket_debugger_url, max_size=None) as websocket:
        command_id = 0

        async def send(method: str, params: dict | None = None) -> None:
            nonlocal command_id
            command_id += 1
            await websocket.send(
                json.dumps(
                    {
                        "id": command_id,
                        "method": method,
                        "params": params or {},
                    },
                ),
            )

        await send("Network.enable")
        await send("Page.enable")
        if reload:
            await send("Page.reload")

        events: list[dict] = []
        loop = asyncio.get_running_loop()
        deadline = loop.time() + (duration_ms / 1000)

        while loop.time() < deadline:
            timeout = max(deadline - loop.time(), 0.01)
            try:
                raw_message = await asyncio.wait_for(websocket.recv(), timeout=timeout)
            except TimeoutError:
                break

            payload = json.loads(raw_message)
            if payload.get("method", "").startswith("Network."):
                events.append(payload)

        return events


async def _evaluate_debug_target_expression(
    *,
    websocket_debugger_url: str,
    expression: str,
    await_promise: bool = False,
    frame_url_fragments: Sequence[str] | None = None,
    use_main_world: bool = False,
):
    import websockets

    async with websockets.connect(websocket_debugger_url, max_size=None) as websocket:
        context_id = None
        if frame_url_fragments:
            if use_main_world:
                context_id = await _resolve_default_frame_execution_context(
                    websocket=websocket,
                    frame_url_fragments=frame_url_fragments,
                )
            else:
                context_id = await _create_frame_execution_context(
                    websocket=websocket,
                    frame_url_fragments=frame_url_fragments,
                )
        await _send_debug_target_command(
            websocket=websocket,
            command_id=1,
            method="Runtime.evaluate",
            params={
                "expression": expression,
                "returnByValue": True,
                "awaitPromise": await_promise,
                **({"contextId": context_id} if context_id is not None else {}),
            },
        )
        return _parse_debug_target_result(
            await _receive_debug_target_response(websocket=websocket, command_id=1),
        )


async def _navigate_debug_target(
    *,
    websocket_debugger_url: str,
    url: str,
    wait_ms: int,
) -> None:
    import websockets

    async with websockets.connect(websocket_debugger_url, max_size=None) as websocket:
        await _send_debug_target_command(
            websocket=websocket,
            command_id=1,
            method="Page.enable",
        )
        await _receive_debug_target_response(websocket=websocket, command_id=1)
        await _send_debug_target_command(
            websocket=websocket,
            command_id=2,
            method="Page.navigate",
            params={"url": url},
        )
        await _receive_debug_target_response(websocket=websocket, command_id=2)
        await asyncio.sleep(max(wait_ms, 0) / 1000)


async def _click_debug_target_by_labels(
    *,
    websocket_debugger_url: str,
    labels: Sequence[str],
    wait_ms: int,
    frame_url_fragments: Sequence[str] | None = None,
):
    effective_labels = [str(label).strip() for label in labels if str(label).strip()]
    if not effective_labels:
        raise ValueError("Cannot click a debug target action without labels.")

    expression = _build_click_labels_expression(labels=effective_labels)
    payload = await _evaluate_debug_target_expression(
        websocket_debugger_url=websocket_debugger_url,
        expression=expression,
        frame_url_fragments=frame_url_fragments,
    )
    if not isinstance(payload, dict) or not payload.get("clicked"):
        raise ValueError(
            "CDP click failed to find an interactive element for labels: "
            + ", ".join(effective_labels)
        )
    await asyncio.sleep(max(wait_ms, 0) / 1000)
    return payload


async def _send_debug_target_command(
    *,
    websocket,
    command_id: int,
    method: str,
    params: dict | None = None,
) -> None:
    await websocket.send(
        json.dumps(
            {
                "id": command_id,
                "method": method,
                "params": params or {},
            },
        ),
    )


async def _create_frame_execution_context(
    *,
    websocket,
    frame_url_fragments: Sequence[str],
) -> int:
    lowered_fragments = [
        fragment.lower() for fragment in frame_url_fragments if fragment
    ]
    if not lowered_fragments:
        raise ValueError("Cannot select a frame without URL fragments.")

    await _send_debug_target_command(
        websocket=websocket,
        command_id=90,
        method="Page.enable",
    )
    await _receive_debug_target_response(websocket=websocket, command_id=90)
    frame_id = None
    for _ in range(10):
        await _send_debug_target_command(
            websocket=websocket,
            command_id=91,
            method="Page.getFrameTree",
        )
        response = await _receive_debug_target_response(
            websocket=websocket, command_id=91
        )
        try:
            frame_id = _select_frame_id_by_fragments(
                frame_tree=response.get("result", {}).get("frameTree", {}),
                url_fragments=lowered_fragments,
            )
            break
        except ValueError:
            await asyncio.sleep(0.5)
    if frame_id is None:
        raise ValueError(
            "Could not find a CDP frame matching any fragment: "
            + ", ".join(frame_url_fragments)
        )
    await _send_debug_target_command(
        websocket=websocket,
        command_id=92,
        method="Page.createIsolatedWorld",
        params={
            "frameId": frame_id,
            "worldName": "codex-history",
            "grantUniveralAccess": True,
        },
    )
    context_id = (
        (await _receive_debug_target_response(websocket=websocket, command_id=92))
        .get("result", {})
        .get("executionContextId")
    )
    if not isinstance(context_id, int):
        raise ValueError(
            "CDP failed to create an execution context for frame fragments: "
            + ", ".join(frame_url_fragments)
        )
    return context_id


async def _resolve_default_frame_execution_context(
    *,
    websocket,
    frame_url_fragments: Sequence[str],
    max_retries: int = 5,
    retry_delay: float = 0.5,
) -> int:
    lowered_fragments = [
        fragment.lower() for fragment in frame_url_fragments if fragment
    ]
    await _send_debug_target_command(
        websocket=websocket,
        command_id=93,
        method="Runtime.enable",
    )
    _, runtime_events = await _receive_debug_target_response_with_events(
        websocket=websocket,
        command_id=93,
    )
    await _send_debug_target_command(
        websocket=websocket,
        command_id=94,
        method="Page.enable",
    )
    _, page_events = await _receive_debug_target_response_with_events(
        websocket=websocket,
        command_id=94,
    )
    context_id = None
    for attempt in range(max_retries):
        await _send_debug_target_command(
            websocket=websocket,
            command_id=95,
            method="Page.getFrameTree",
        )
        (
            frame_tree_payload,
            frame_tree_events,
        ) = await _receive_debug_target_response_with_events(
            websocket=websocket,
            command_id=95,
        )
        page_events.extend(frame_tree_events)
        try:
            frame_id = _select_frame_id_by_fragments(
                frame_tree=frame_tree_payload.get("result", {}).get("frameTree", {}),
                url_fragments=lowered_fragments,
            )
            context_id = _select_default_execution_context_id(
                events=[*runtime_events, *page_events],
                frame_id=frame_id,
            )
            if context_id is not None:
                break
        except ValueError:
            # Frame/context may not be available yet; retry until max_retries.
            pass
        if attempt < max_retries - 1:
            await asyncio.sleep(retry_delay)
    if context_id is None:
        raise ValueError(
            "Could not find a default execution context for frame fragments: "
            + ", ".join(frame_url_fragments)
        )
    return context_id


def _select_default_execution_context_id(
    *,
    events: Sequence[dict],
    frame_id: str,
) -> int | None:
    for event in events:
        if event.get("method") != "Runtime.executionContextCreated":
            continue
        context = event.get("params", {}).get("context", {})
        aux_data = context.get("auxData", {})
        if aux_data.get("frameId") != frame_id or not aux_data.get("isDefault"):
            continue
        context_id = context.get("id")
        if isinstance(context_id, int):
            return context_id
    return None


def _select_frame_id_by_fragments(
    *,
    frame_tree: dict,
    url_fragments: Sequence[str],
) -> str:
    for frame in _iter_frame_nodes(frame_tree):
        haystack = " ".join(
            str(frame.get(key, "") or "")
            for key in ("url", "urlFragment", "unreachableUrl", "name")
        ).lower()
        if any(fragment in haystack for fragment in url_fragments):
            frame_id = str(frame.get("id", "") or "")
            if frame_id:
                return frame_id
    raise ValueError(
        "Could not find a CDP frame matching any fragment: " + ", ".join(url_fragments)
    )


def _iter_frame_nodes(frame_tree: dict):
    frame = frame_tree.get("frame")
    if isinstance(frame, dict):
        yield frame
    for child in frame_tree.get("childFrames", []) or []:
        if isinstance(child, dict):
            yield from _iter_frame_nodes(child)


async def _receive_debug_target_response(
    *,
    websocket,
    command_id: int,
) -> dict:
    while True:
        try:
            raw_message = await asyncio.wait_for(
                websocket.recv(),
                timeout=DEFAULT_EVALUATE_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            raise ValueError(
                f"CDP evaluation timed out after {DEFAULT_EVALUATE_TIMEOUT_SECONDS:.0f}s"
            ) from exc
        payload = json.loads(raw_message)
        if payload.get("id") != command_id:
            continue
        if "error" in payload:
            message = payload["error"].get("message") or "unknown"
            raise ValueError(f"CDP command failed: {message}")
        return payload


async def _receive_debug_target_response_with_events(
    *,
    websocket,
    command_id: int,
) -> tuple[dict, list[dict]]:
    events: list[dict] = []
    while True:
        try:
            raw_message = await asyncio.wait_for(
                websocket.recv(),
                timeout=DEFAULT_EVALUATE_TIMEOUT_SECONDS,
            )
        except TimeoutError as exc:
            raise ValueError(
                f"CDP evaluation timed out after {DEFAULT_EVALUATE_TIMEOUT_SECONDS:.0f}s"
            ) from exc
        payload = json.loads(raw_message)
        if payload.get("id") != command_id:
            if "id" not in payload:
                events.append(payload)
            continue
        if "error" in payload:
            message = payload["error"].get("message") or "unknown"
            raise ValueError(f"CDP command failed: {message}")
        return payload, events


def _parse_debug_target_result(payload: dict):
    result = payload.get("result", {}).get("result", {})
    if "value" in result:
        return result["value"]
    description = result.get("description") or payload.get("error") or "unknown"
    raise ValueError(f"CDP evaluation failed: {description}")


def _build_click_labels_expression(*, labels: Sequence[str]) -> str:
    return """
(() => {
  const wanted = %s.map((label) => label.toLowerCase());
  const normalize = (value) => (value || "").replace(/\\s+/g, " ").trim().toLowerCase();
  const isVisible = (element) => {
    if (!(element instanceof Element)) {
      return false;
    }
    const style = window.getComputedStyle(element);
    if (style.display === "none" || style.visibility === "hidden" || Number(style.opacity || "1") === 0) {
      return false;
    }
    const rect = element.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };
  const labelFor = (element) => normalize(
    element.getAttribute("aria-label")
    || element.innerText
    || element.textContent
    || element.getAttribute("title")
    || element.getAttribute("name")
    || ""
  );
  const candidates = Array.from(
    document.body ? document.body.querySelectorAll("*") : []
  );
  for (const wantedLabel of wanted) {
    const matches = candidates
      .filter((candidate) => isVisible(candidate))
      .map((candidate) => ({
        candidate,
        label: labelFor(candidate),
      }))
      .filter(({ candidate, label }) => {
        if (!label) {
          return false;
        }
        if (!(label === wantedLabel || label.includes(wantedLabel) || wantedLabel.includes(label))) {
          return false;
        }
        if (label.length > Math.max((wantedLabel.length * 3), wantedLabel.length + 24)) {
          return false;
        }
        return !Array.from(candidate.children).some((child) => labelFor(child) === label);
      })
      .sort((left, right) => left.label.length - right.label.length);
    const match = matches.find(({ candidate }) => {
      const target = candidate.closest("a, button, [role='button'], summary, [aria-controls]") || candidate;
      return isVisible(target);
    });
    if (!match) {
      continue;
    }
    const element = match.candidate;
    const target = element.closest("a, button, [role='button'], summary, [aria-controls]") || element;
    if (!target) {
      continue;
    }
    const label = labelFor(element) || labelFor(target);
    if (!label) {
      continue;
    }
    const clickable = target;
    const nativeClick = clickable instanceof HTMLElement ? clickable : element;
    const targetLabel = labelFor(clickable) || label;
    const eventTarget = nativeClick instanceof HTMLElement ? nativeClick : element;
    const targetToClick = eventTarget || clickable;
    if (!(targetToClick instanceof Element)) {
      continue;
    }
    targetToClick.scrollIntoView({ block: "center", inline: "center" });
    const pointerEvents = ["pointerdown", "mousedown", "pointerup", "mouseup", "click"];
    for (const type of pointerEvents) {
      targetToClick.dispatchEvent(new MouseEvent(type, { bubbles: true, cancelable: true, view: window }));
    }
    if (targetToClick instanceof HTMLElement) {
      targetToClick.click();
    }
    return { clicked: targetLabel };
  }
  return { clicked: null };
})()
""" % json.dumps(list(labels))


def _fetch_text(url: str) -> str:
    parsed = urlparse(url)
    if parsed.scheme not in ('http', 'https'):
        raise ValueError(f"Invalid URL scheme: {parsed.scheme}")
    with urlopen(url, timeout=DEFAULT_EVALUATE_TIMEOUT_SECONDS) as response:
        return response.read().decode("utf-8")