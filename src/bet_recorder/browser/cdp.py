from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
import asyncio
import json
from urllib.request import urlopen

JsonFetcher = Callable[[str], str]

DEFAULT_DEBUG_BASE_URL = "http://127.0.0.1:9222"
SOURCE_URL_FRAGMENTS = {
  "rebelbetting_vb": "vb.rebelbetting.com",
  "rebelbetting_rb": "rb.rebelbetting.com",
  "fairodds_terminal": "app.fairoddsterminal.com",
  "profitmaximiser_members": "profitmaximiser.co.uk/members",
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
) -> list[DebugTarget]:
  loader = fetch_text or _fetch_text
  payload = json.loads(loader(f"{debug_base_url}/json/list"))
  return [
    DebugTarget(
      target_id=item["id"],
      target_type=item["type"],
      title=item.get("title", ""),
      url=item.get("url", ""),
      websocket_debugger_url=item["webSocketDebuggerUrl"],
    )
    for item in payload
    if item.get("type") == "page" and item.get("webSocketDebuggerUrl")
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
    "Could not find a CDP target matching any fragment: "
    + ", ".join(url_fragments)
  )


def evaluate_debug_target_value(
  *,
  websocket_debugger_url: str,
  expression: str,
):
  return asyncio.run(
    _evaluate_debug_target_value(
      websocket_debugger_url=websocket_debugger_url,
      expression=expression,
    ),
  )


def capture_debug_target_page_state(
  *,
  websocket_debugger_url: str,
  page: str,
  captured_at: datetime,
  notes: Sequence[str] | None = None,
) -> dict:
  payload = evaluate_debug_target_value(
    websocket_debugger_url=websocket_debugger_url,
    expression=PAGE_STATE_JS,
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


async def _evaluate_debug_target_value(
  *,
  websocket_debugger_url: str,
  expression: str,
):
  import websockets

  async with websockets.connect(websocket_debugger_url, max_size=None) as websocket:
    await websocket.send(
      json.dumps(
        {
          "id": 1,
          "method": "Runtime.evaluate",
          "params": {
            "expression": expression,
            "returnByValue": True,
          },
        },
      ),
    )
    while True:
      raw_message = await websocket.recv()
      payload = json.loads(raw_message)
      if payload.get("id") != 1:
        continue
      result = payload.get("result", {}).get("result", {})
      if "value" in result:
        return result["value"]
      description = result.get("description") or payload.get("error") or "unknown"
      raise ValueError(f"CDP evaluation failed: {description}")


def _fetch_text(url: str) -> str:
  with urlopen(url) as response:
    return response.read().decode("utf-8")
