from __future__ import annotations

from pathlib import Path
import errno
import json
import os


def ensure_watcher_run_dir(run_dir: Path) -> None:
    _ensure_private_dir(run_dir)
    _ensure_private_dir(run_dir / "screenshots")
    _touch_private_file(run_dir / "events.jsonl")
    metadata_path = run_dir / "metadata.json"
    if not metadata_path.exists():
        _write_private_text(
            metadata_path,
            json.dumps({"source": "smarkets_exchange"}) + "\n",
        )
    else:
        _set_private_mode(metadata_path, 0o600)


def acquire_watcher_process_slot(run_dir: Path) -> None:
    pid_path = run_dir / "watcher.pid"
    _ensure_private_dir(run_dir)

    while True:
        try:
            fd = os.open(pid_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        except FileExistsError:
            conflicting_pid = _find_conflicting_watcher_pid(run_dir, pid_path)
            if conflicting_pid is not None:
                raise RuntimeError(
                    f"Watcher already running for {run_dir} with pid {conflicting_pid}."
                )
            pid_path.unlink(missing_ok=True)
            continue
        except OSError as error:
            if error.errno == errno.EEXIST:
                continue
            raise
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            handle.write(f"{os.getpid()}\n")
        _set_private_mode(pid_path, 0o600)
        return


def release_watcher_process_slot(run_dir: Path) -> None:
    pid_path = run_dir / "watcher.pid"
    if not pid_path.exists():
        return
    try:
        recorded_pid = int(pid_path.read_text().strip())
    except ValueError:
        pid_path.unlink(missing_ok=True)
        return
    if recorded_pid == os.getpid():
        pid_path.unlink(missing_ok=True)


def _set_private_mode(path: Path, mode: int) -> None:
    try:
        path.chmod(mode)
    except OSError:
        pass


def _ensure_private_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    _set_private_mode(path, 0o700)


def _touch_private_file(path: Path) -> None:
    if not path.exists():
        path.touch()
    _set_private_mode(path, 0o600)


def _write_private_text(path: Path, text: str) -> None:
    if path.parent:
        _ensure_private_dir(path.parent)
    path.write_text(text)
    _set_private_mode(path, 0o600)


def _find_conflicting_watcher_pid(run_dir: Path, pid_path: Path) -> int | None:
    if not pid_path.exists():
        return None
    try:
        recorded_pid = int(pid_path.read_text().strip())
    except ValueError:
        return None
    if recorded_pid == os.getpid():
        return None
    if not _process_is_alive(recorded_pid):
        return None
    if not _process_matches_run_dir(recorded_pid, run_dir):
        return None
    return recorded_pid


def _process_matches_run_dir(pid: int, run_dir: Path) -> bool:
    cmdline_path = Path(f"/proc/{pid}/cmdline")
    if not cmdline_path.exists():
        return False
    try:
        command = (
            cmdline_path.read_bytes().replace(b"\x00", b" ").decode("utf-8", "ignore")
        )
    except Exception:
        return False
    return "watch-smarkets-session" in command and str(run_dir) in command


def _process_is_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True
