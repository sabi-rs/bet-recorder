from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
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


def _fetch_text(url: str) -> str:
  with urlopen(url) as response:
    return response.read().decode("utf-8")
