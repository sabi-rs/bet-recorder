from pathlib import Path
import json
import subprocess
import sys

from typer.testing import CliRunner

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from bet_recorder.blackjack_assistant import (  # noqa: E402
    BlackjackAdvice,
    BlackjackSnapshot,
    build_blackjack_rule_options,
    extract_blackjack_snapshot,
    format_blackjack_notification,
    render_blackjack_screen,
    recommend_blackjack_move,
)
from bet_recorder.cli import app  # noqa: E402


def test_extract_blackjack_snapshot_from_server_response() -> None:
    snapshot = extract_blackjack_snapshot(
        {
            "game_state": "Decision",
            "target_url": "https://games.example.com/container.html",
            "frame_url": "https://games.example.com/blackjack/index.html",
            "server_response": {
                "payout": {
                    "activeHand": {
                        "handPosition": 1,
                        "cards": ["8S", "8D"],
                        "availableActions": ["hit", "stand", "split", "double"],
                        "minSum": 16,
                        "sum": 16,
                        "hard": True,
                        "hasBeenSplit": False,
                        "hasBlackjack": False,
                    },
                    "dealerHand": {
                        "cards": ["AC"],
                        "sum": 11,
                    },
                },
            },
        },
    )

    assert snapshot == BlackjackSnapshot(
        target_url="https://games.example.com/container.html",
        frame_url="https://games.example.com/blackjack/index.html",
        game_state="Decision",
        hand_position=1,
        player_card_codes=["8S", "8D"],
        dealer_card_codes=["AC"],
        player_cards=[8, 8],
        dealer_up_card=1,
        hand_min_sum=16,
        hand_sum=16,
        hand_is_hard=True,
        hand_is_split=False,
        hand_has_blackjack=False,
        available_actions=["hit", "stand", "split", "double"],
    )


def test_extract_blackjack_snapshot_returns_none_without_active_hand() -> None:
    snapshot = extract_blackjack_snapshot(
        {
            "game_state": "Ready",
            "target_url": "https://games.example.com/container.html",
            "frame_url": "https://games.example.com/blackjack/index.html",
            "server_response": None,
        },
    )

    assert snapshot is None


def test_recommend_blackjack_move_uses_local_blackjack_strategy(
    monkeypatch,
) -> None:
    calls: list[list[str]] = []

    def fake_run(
        args: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        assert check is True
        assert capture_output is True
        assert text is True
        calls.append(args)
        return subprocess.CompletedProcess(
            args=args, returncode=0, stdout='{"action":"split"}\n'
        )

    monkeypatch.setattr("bet_recorder.blackjack_assistant.subprocess.run", fake_run)

    advice = recommend_blackjack_move(
        BlackjackSnapshot(
            target_url="https://games.example.com/container.html",
            frame_url="https://games.example.com/blackjack/index.html",
            game_state="Decision",
            hand_position=1,
            player_card_codes=["8S", "8D"],
            dealer_card_codes=["AC"],
            player_cards=[8, 8],
            dealer_up_card=1,
            hand_min_sum=16,
            hand_sum=16,
            hand_is_hard=True,
            hand_is_split=False,
            hand_has_blackjack=False,
            available_actions=["hit", "stand", "split", "double"],
        ),
        blackjack_strategy_root=Path("/tmp/blackjack-strategy"),
    )

    assert advice == BlackjackAdvice(
        action="split",
        player_cards=[8, 8],
        dealer_up_card=1,
        player_card_codes=["8S", "8D"],
        dealer_card_codes=["AC"],
    )

    expected_script = """const blackjackStrategy = require(process.argv[1]);
const playerCards = JSON.parse(process.argv[2]);
const dealerCard = Number(process.argv[3]);
const rules = JSON.parse(process.argv[6]);
const action = blackjackStrategy.GetRecommendedPlayerAction(
  playerCards,
  dealerCard,
  Number(process.argv[4]),
  true,
  { ...rules, strategyComplexity: process.argv[5] },
);
process.stdout.write(JSON.stringify({ action }));"""

    assert calls == [
        [
            "node",
            "-e",
            expected_script,
            "/tmp/blackjack-strategy",
            "[8,8]",
            "1",
            "1",
            "advanced",
            "{}",
        ],
    ]


def test_recommend_blackjack_move_forwards_rule_options(monkeypatch) -> None:
    calls: list[list[str]] = []

    def fake_run(
        args: list[str],
        *,
        check: bool,
        capture_output: bool,
        text: bool,
    ) -> subprocess.CompletedProcess[str]:
        calls.append(args)
        return subprocess.CompletedProcess(
            args=args, returncode=0, stdout='{"action":"stand"}\n'
        )

    monkeypatch.setattr("bet_recorder.blackjack_assistant.subprocess.run", fake_run)

    recommend_blackjack_move(
        BlackjackSnapshot(
            target_url="https://games.example.com/container.html",
            frame_url="https://games.example.com/blackjack/index.html",
            game_state="Decision",
            hand_position=1,
            player_card_codes=["TS", "6D"],
            dealer_card_codes=["TC"],
            player_cards=[10, 6],
            dealer_up_card=10,
            hand_min_sum=16,
            hand_sum=16,
            hand_is_hard=True,
            hand_is_split=False,
            hand_has_blackjack=False,
            available_actions=["hit", "stand"],
        ),
        blackjack_strategy_root=Path("/tmp/blackjack-strategy"),
        rules=build_blackjack_rule_options(
            number_of_decks=8,
            hit_soft_17=False,
            surrender="none",
            double="9or10or11",
            double_after_split=False,
            resplit_aces=True,
            offer_insurance=False,
            max_split_hands=3,
        ),
    )

    assert json.loads(calls[0][-1]) == {
        "numberOfDecks": 8,
        "hitSoft17": False,
        "surrender": "none",
        "double": "9or10or11",
        "doubleAfterSplit": False,
        "resplitAces": True,
        "offerInsurance": False,
        "maxSplitHands": 3,
    }


def test_render_screen_and_notification_include_actions() -> None:
    snapshot = BlackjackSnapshot(
        target_url="https://games.example.com/container.html",
        frame_url="https://games.example.com/blackjack/index.html",
        game_state="Decision",
        hand_position=1,
        player_card_codes=["AS", "7D"],
        dealer_card_codes=["6C"],
        player_cards=[1, 7],
        dealer_up_card=6,
        hand_min_sum=8,
        hand_sum=18,
        hand_is_hard=False,
        hand_is_split=False,
        hand_has_blackjack=False,
        available_actions=["hit", "stand", "double"],
    )
    advice = BlackjackAdvice(
        action="double",
        player_cards=[1, 7],
        dealer_up_card=6,
        player_card_codes=["AS", "7D"],
        dealer_card_codes=["6C"],
    )

    screen = render_blackjack_screen(
        snapshot=snapshot,
        advice=advice,
        debug_base_url="http://127.0.0.1:9222",
    )

    assert "Player: AS 7D (soft 18 / min 8)" in screen
    assert "Actions: HIT, STAND, DOUBLE" in screen
    assert "Best move: DOUBLE" in screen
    assert (
        format_blackjack_notification(snapshot=snapshot, advice=advice)
        == "DOUBLE - player AS 7D vs dealer 6C"
    )


def test_blackjack_assist_command_outputs_json(monkeypatch) -> None:
    runner = CliRunner()

    monkeypatch.setattr(
        "bet_recorder.cli.capture_blackjack_advice",
        lambda **_: (
            BlackjackSnapshot(
                target_url="https://games.example.com/container.html",
                frame_url="https://games.example.com/blackjack/index.html",
                game_state="Decision",
                hand_position=1,
                player_card_codes=["TS", "6D"],
                dealer_card_codes=["TC"],
                player_cards=[10, 6],
                dealer_up_card=10,
                hand_min_sum=16,
                hand_sum=16,
                hand_is_hard=True,
                hand_is_split=False,
                hand_has_blackjack=False,
                available_actions=["hit", "stand"],
            ),
            BlackjackAdvice(
                action="stand",
                player_cards=[10, 6],
                dealer_up_card=10,
                player_card_codes=["TS", "6D"],
                dealer_card_codes=["TC"],
            ),
        ),
    )

    result = runner.invoke(app, ["blackjack-assist", "--json"])

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload == {
        "active_hand": True,
        "action": "stand",
        "player_cards": [10, 6],
        "dealer_up_card": 10,
        "player_card_codes": ["TS", "6D"],
        "dealer_card_codes": ["TC"],
        "available_actions": ["hit", "stand"],
        "game_state": "Decision",
        "target_url": "https://games.example.com/container.html",
        "frame_url": "https://games.example.com/blackjack/index.html",
    }