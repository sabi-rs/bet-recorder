from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
import json
import subprocess

from bet_recorder.browser.models import BrowserPageState

BODY_TEXT_JS = "document.body?.innerText ?? ''"
LINKS_JS = (
  "Array.from(document.querySelectorAll('a[href]')).map((anchor) => anchor.href)"
)
INPUTS_JS = (
  "Object.fromEntries(Array.from(document.querySelectorAll('input, textarea, select'))"
  ".map((element, index) => {"
  "const key = element.name || element.id || element.getAttribute('placeholder') || "
  "`input_${index}`; return [key, element.value ?? '']; }))"
)
VISIBLE_ACTIONS_JS = (
  "Array.from(document.querySelectorAll('a, button, [role=\"button\"]'))"
  ".map((element) => (element.innerText || element.textContent || '').trim())"
  ".filter(Boolean)"
)
RESOURCE_HOSTS_JS = (
  "Array.from(new Set(performance.getEntriesByType('resource').map((entry) => {"
  "try { return new URL(entry.name).hostname; } catch { return null; }}"
  ").filter(Boolean)))"
)


@dataclass(frozen=True)
class AgentBrowserResponse:
  success: bool
  data: dict
  error: str | None


Runner = Callable[[list[str]], subprocess.CompletedProcess[str] | object]


class AgentBrowserClient:
  def __init__(
    self,
    *,
    session: str | None = None,
    cdp_port: int | None = None,
    executable_path: str | None = None,
    runner: Runner | None = None,
  ) -> None:
    self._session = session
    self._cdp_port = cdp_port
    self._executable_path = executable_path
    self._runner = runner or _default_runner

  def capture_page_state(
    self,
    *,
    page: str,
    captured_at: datetime,
    screenshot_path: str | None,
    notes: list[str],
  ) -> BrowserPageState:
    return BrowserPageState(
      captured_at=captured_at,
      page=page,
      url=self._get_url(),
      document_title=self._get_title(),
      body_text=self._eval_result(BODY_TEXT_JS),
      interactive_snapshot=self._get_interactive_snapshot(),
      links=self._eval_result(LINKS_JS),
      inputs=self._eval_result(INPUTS_JS),
      visible_actions=self._eval_result(VISIBLE_ACTIONS_JS),
      resource_hosts=self._eval_result(RESOURCE_HOSTS_JS),
      local_storage_keys=self._get_local_storage_keys(),
      screenshot_path=screenshot_path,
      notes=notes,
    )

  def capture_screenshot(self, output_path: Path) -> Path:
    response = self._run_json("screenshot", str(output_path))
    return Path(response.data["path"])

  def _get_title(self) -> str:
    response = self._run_json("get", "title")
    return response.data["title"]

  def _get_url(self) -> str:
    response = self._run_json("get", "url")
    return response.data["url"]

  def _get_interactive_snapshot(self) -> list[dict[str, str]]:
    response = self._run_json("snapshot", "-i")
    refs = response.data.get("refs", {})
    return [
      {
        "ref": ref,
        "role": payload.get("role", ""),
        "name": payload.get("name", ""),
      }
      for ref, payload in refs.items()
    ]

  def _get_local_storage_keys(self) -> list[str]:
    response = self._run_json("storage", "local")
    storage = response.data.get("data", {})
    return list(storage.keys())

  def _eval_result(self, script: str):
    response = self._run_json("eval", script)
    return response.data["result"]

  def _run_json(self, *args: str) -> AgentBrowserResponse:
    command = ["agent-browser", *self._base_args(), "--json", *args]
    completed = self._runner(command)
    returncode = getattr(completed, "returncode", 0)
    stdout = getattr(completed, "stdout", "")
    stderr = getattr(completed, "stderr", "")
    if returncode != 0:
      raise RuntimeError(stderr or stdout or f"agent-browser failed: {' '.join(command)}")
    payload = json.loads(stdout)
    if not payload.get("success"):
      raise RuntimeError(payload.get("error") or f"agent-browser failed: {' '.join(command)}")
    return AgentBrowserResponse(
      success=True,
      data=payload.get("data") or {},
      error=payload.get("error"),
    )

  def _base_args(self) -> list[str]:
    args: list[str] = []
    if self._session is not None:
      args.extend(["--session", self._session])
    if self._cdp_port is not None:
      args.extend(["--cdp", str(self._cdp_port)])
    if self._executable_path is not None:
      args.extend(["--executable-path", self._executable_path])
    return args


def _default_runner(command: list[str]) -> subprocess.CompletedProcess[str]:
  return subprocess.run(
    command,
    capture_output=True,
    check=False,
    text=True,
  )
