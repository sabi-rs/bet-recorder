from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.live.smarkets_profile_capture import (  # noqa: E402
    _extract_remote_debugging_port,
    PersistentSmarketsProfileClient,
)


def test_extract_remote_debugging_port_matches_owned_profile() -> None:
    profile_path = Path("/home/thomas/.config/smarkets-automation/profile")
    command = (
        "/usr/lib/chromium/chromium "
        "--user-data-dir=/home/thomas/.config/smarkets-automation/profile "
        "--remote-debugging-port=44349 about:blank"
    )

    assert _extract_remote_debugging_port(command, profile_path) == 44349


def test_extract_remote_debugging_port_ignores_other_profiles() -> None:
    profile_path = Path("/home/thomas/.config/smarkets-automation/profile")
    command = (
        "/usr/lib/chromium/chromium "
        "--user-data-dir=/tmp/other-profile "
        "--remote-debugging-port=9333 about:blank"
    )

    assert _extract_remote_debugging_port(command, profile_path) is None


def test_persistent_profile_client_falls_back_to_profile_launch_when_cdp_is_unavailable(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured_kwargs: dict[str, object] = {}

    class FakeAgentBrowserClient:
        def __init__(self, **kwargs) -> None:
            captured_kwargs.update(kwargs)

    monkeypatch.setattr(
        "bet_recorder.live.smarkets_profile_capture.find_running_profile_cdp_port",
        lambda profile_path: None,
    )
    monkeypatch.setattr(
        "bet_recorder.live.smarkets_profile_capture.AgentBrowserClient",
        FakeAgentBrowserClient,
    )

    client = PersistentSmarketsProfileClient(tmp_path / "owned-profile")

    with client as attached:
        assert attached is client

    assert captured_kwargs == {
        "profile_path": tmp_path / "owned-profile",
        "executable_path": "/usr/bin/chromium",
    }


def test_persistent_profile_client_prefers_cdp_when_owned_browser_is_running(
    tmp_path: Path,
    monkeypatch,
) -> None:
    captured_kwargs: dict[str, object] = {}

    class FakeAgentBrowserClient:
        def __init__(self, **kwargs) -> None:
            captured_kwargs.update(kwargs)

    monkeypatch.setattr(
        "bet_recorder.live.smarkets_profile_capture.find_running_profile_cdp_port",
        lambda profile_path: 44349,
    )
    monkeypatch.setattr(
        "bet_recorder.live.smarkets_profile_capture.AgentBrowserClient",
        FakeAgentBrowserClient,
    )

    client = PersistentSmarketsProfileClient(tmp_path / "owned-profile")

    with client as attached:
        assert attached is client

    assert captured_kwargs == {"cdp_port": 44349}
