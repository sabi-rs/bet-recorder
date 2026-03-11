from datetime import UTC, datetime
from pathlib import Path
import json
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.watcher import WatcherConfig, run_smarkets_watcher  # noqa: E402


def test_run_smarkets_watcher_captures_and_writes_latest_state(
  tmp_path: Path,
) -> None:
  run_dir = tmp_path / "smarkets-run"

  calls = {"capture": 0, "sleep": 0}

  def fake_capture(config: WatcherConfig, captured_at: datetime) -> dict:
    calls["capture"] += 1
    payload = {
      "captured_at": captured_at.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
      "source": "smarkets_exchange",
      "kind": "positions_snapshot",
      "page": "open_positions",
      "url": "https://smarkets.com/open-positions",
      "document_title": "Open positions",
      "body_text": (
        "Available balance £150.00 Exposure £23.29 Unrealized P/L £2.10 "
        "Open Bets Back Arsenal Full-time result 2.12 £5.00 Open "
        "Lazio vs Sassuolo "
        "Sell 1 - 1 Correct score 7.2 £2.55 £15.81 £18.36 £2.46 £1.20 (3.53%) Order filled Trade out "
        "Sell Draw Full-time result 3.35 £9.91 £23.29 £33.20 £9.60 -£1.31 (3.13%) Order filled Trade out"
      ),
      "interactive_snapshot": [],
      "links": [],
      "inputs": {},
      "visible_actions": ["Trade out"],
      "resource_hosts": ["smarkets.com"],
      "local_storage_keys": [],
      "screenshot_path": None,
      "notes": ["watcher-loop"],
    }
    with (run_dir / "events.jsonl").open("a", encoding="utf-8") as handle:
      handle.write(json.dumps(payload) + "\n")
    return payload

  def fake_sleep(_: float) -> None:
    calls["sleep"] += 1

  state = run_smarkets_watcher(
    WatcherConfig(
      run_dir=run_dir,
      session="helium-copy",
      interval_seconds=5.0,
      commission_rate=0.0,
      target_profit=1.0,
      stop_loss=1.0,
    ),
    capture_once=fake_capture,
    sleep=fake_sleep,
    now=lambda: datetime(2026, 3, 11, 12, 5, tzinfo=UTC),
    max_iterations=1,
  )

  assert calls["capture"] == 1
  assert calls["sleep"] == 0
  assert state["decision_count"] == 2
  assert state["decisions"][0]["status"] == "take_profit_ready"
  assert state["decisions"][1]["status"] == "stop_loss_ready"
  persisted = json.loads((run_dir / "watcher-state.json").read_text())
  assert persisted["run_dir"] == str(run_dir)
  assert persisted["decisions"][0]["contract"] == "1 - 1"
  assert (run_dir / "events.jsonl").exists()
  assert (run_dir / "metadata.json").exists()
  assert (run_dir / "screenshots").is_dir()
