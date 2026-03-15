from pathlib import Path
import sys
import json

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.cli import app  # noqa: E402


def test_watch_smarkets_session_runs_one_iteration_and_emits_state(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    run_dir = tmp_path / "smarkets-run"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text("")
    (run_dir / "metadata.json").write_text("{}")
    (run_dir / "screenshots").mkdir()

    def fake_run_watcher(
        config, *, capture_once=None, sleep=None, now=None, max_iterations=None
    ):
        assert config.session == "helium-copy"
        assert config.profile_path is None
        assert config.run_dir == run_dir
        assert max_iterations == 1
        return {
            "source": "smarkets_exchange",
            "run_dir": str(run_dir),
            "decision_count": 1,
            "decisions": [{"contract": "Draw", "status": "hold"}],
        }

    monkeypatch.setattr("bet_recorder.cli.run_smarkets_watcher", fake_run_watcher)

    result = runner.invoke(
        app,
        [
            "watch-smarkets-session",
            "--run-dir",
            str(run_dir),
            "--session",
            "helium-copy",
            "--interval-seconds",
            "5",
            "--commission-rate",
            "0",
            "--target-profit",
            "1",
            "--stop-loss",
            "1",
            "--max-iterations",
            "1",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["decision_count"] == 1
    assert payload["decisions"][0]["contract"] == "Draw"


def test_watch_smarkets_session_accepts_explicit_profile_path(
    tmp_path: Path,
    monkeypatch,
) -> None:
    runner = CliRunner()
    run_dir = tmp_path / "smarkets-run"
    profile_path = tmp_path / "owned-profile"
    run_dir.mkdir()
    (run_dir / "events.jsonl").write_text("")
    (run_dir / "metadata.json").write_text("{}")
    (run_dir / "screenshots").mkdir()

    def fake_run_watcher(
        config, *, capture_once=None, sleep=None, now=None, max_iterations=None
    ):
        assert config.session == "helium-copy"
        assert config.profile_path == profile_path
        assert config.run_dir == run_dir
        assert max_iterations == 1
        return {
            "source": "smarkets_exchange",
            "run_dir": str(run_dir),
            "decision_count": 0,
            "decisions": [],
        }

    monkeypatch.setattr("bet_recorder.cli.run_smarkets_watcher", fake_run_watcher)

    result = runner.invoke(
        app,
        [
            "watch-smarkets-session",
            "--run-dir",
            str(run_dir),
            "--session",
            "helium-copy",
            "--profile-path",
            str(profile_path),
            "--interval-seconds",
            "5",
            "--commission-rate",
            "0",
            "--target-profit",
            "1",
            "--stop-loss",
            "1",
            "--max-iterations",
            "1",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["decision_count"] == 0
