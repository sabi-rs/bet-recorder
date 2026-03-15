from __future__ import annotations

from datetime import datetime
import os
from pathlib import Path
import re

from bet_recorder.browser.agent_browser import AgentBrowserClient
from bet_recorder.browser.models import BrowserPageState


class PersistentSmarketsProfileClient:
    def __init__(
        self,
        profile_path: Path,
        *,
        executable_path: str = "/usr/bin/chromium",
    ) -> None:
        self.profile_path = profile_path.expanduser()
        self.executable_path = executable_path
        self._cdp_port: int | None = None
        self._client: AgentBrowserClient | None = None

    def __enter__(self) -> "PersistentSmarketsProfileClient":
        self.profile_path.mkdir(parents=True, exist_ok=True)
        self._cdp_port = find_running_profile_cdp_port(self.profile_path)
        if self._cdp_port is not None:
            self._client = AgentBrowserClient(cdp_port=self._cdp_port)
            return self

        self._client = AgentBrowserClient(
            profile_path=self.profile_path,
            executable_path=self.executable_path,
        )
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        return None

    def current_url(self) -> str:
        assert self._client is not None
        return self._client.current_url()

    def open_url(self, url: str) -> None:
        assert self._client is not None
        self._client.open_url(url)

    def wait(self, milliseconds: int) -> None:
        assert self._client is not None
        self._client.wait(milliseconds)

    def fill(self, selector: str, text: str) -> None:
        assert self._client is not None
        self._client.fill(selector, text)

    def click(self, selector: str) -> None:
        assert self._client is not None
        self._client.click(selector)

    def set_input_value(self, selector: str, text: str) -> None:
        assert self._client is not None
        self._client.set_input_value(selector, text)

    def evaluate(self, script: str):
        assert self._client is not None
        return self._client.evaluate(script)

    def capture_page_state(
        self,
        *,
        page: str,
        captured_at: datetime,
        screenshot_path: str | None,
        notes: list[str],
    ) -> BrowserPageState:
        assert self._client is not None
        return self._client.capture_page_state(
            page=page,
            captured_at=captured_at,
            screenshot_path=screenshot_path,
            notes=notes,
        )


def find_running_profile_cdp_port(profile_path: Path) -> int | None:
    for pid in sorted(entry for entry in os.listdir("/proc") if entry.isdigit()):
        try:
            command = (
                Path(f"/proc/{pid}/cmdline")
                .read_bytes()
                .replace(b"\x00", b" ")
                .decode("utf-8", "ignore")
            )
        except Exception:
            continue
        port = _extract_remote_debugging_port(command, profile_path)
        if port is not None:
            return port
    return None


def _extract_remote_debugging_port(command: str, profile_path: Path) -> int | None:
    resolved_profile = str(profile_path.expanduser())
    if resolved_profile not in command:
        return None
    match = re.search(r"--remote-debugging-port=(\d+)", command)
    if match is None:
        return None
    return int(match.group(1))
