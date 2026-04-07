from __future__ import annotations

from dataclasses import asdict, dataclass
from pathlib import Path
import json
import subprocess

from bet_recorder.browser.cdp import (
    DEFAULT_DEBUG_BASE_URL,
    evaluate_debug_target_main_world_value,
    list_debug_targets,
)


DEFAULT_BLACKJACK_STRATEGY_ROOT = (
    Path(__file__).resolve().parents[3] / "blackjack-strategy"
)
DEFAULT_STRATEGY_COMPLEXITY = "advanced"
FACE_DOWN_CARD_CODE = "001"
DEFAULT_BLACKJACK_FRAME_FRAGMENT = "blackjack"
BLACKJACK_TARGET_KEYWORDS = (
    "blackjack",
    "cayetano",
    "casinoapp.prod.ggn-uki.com",
)
BLACKJACK_TARGET_BLOCKLIST = (
    "github.com",
    "reddit.com",
)


@dataclass(frozen=True)
class BlackjackSnapshot:
    target_url: str
    frame_url: str
    game_state: str | None
    hand_position: int
    player_card_codes: list[str]
    dealer_card_codes: list[str]
    player_cards: list[int]
    dealer_up_card: int
    hand_min_sum: int
    hand_sum: int
    hand_is_hard: bool
    hand_is_split: bool
    hand_has_blackjack: bool
    available_actions: list[str]

    def hand_count(self) -> int:
        return 2 if self.hand_is_split else 1


@dataclass(frozen=True)
class BlackjackAdvice:
    action: str
    player_cards: list[int]
    dealer_up_card: int
    player_card_codes: list[str]
    dealer_card_codes: list[str]

    def to_payload(self) -> dict:
        return asdict(self)


@dataclass(frozen=True)
class BrowserTarget:
    target_id: str
    target_type: str
    title: str
    url: str
    websocket_debugger_url: str


def extract_blackjack_snapshot(runtime_state: dict) -> BlackjackSnapshot | None:
    server_response = runtime_state.get("server_response")
    if not isinstance(server_response, dict):
        return None
    payout = server_response.get("payout")
    if not isinstance(payout, dict):
        return None
    active_hand = payout.get("activeHand")
    if not isinstance(active_hand, dict):
        return None

    player_card_codes = _visible_card_codes(active_hand.get("cards"))
    if not player_card_codes:
        return None

    dealer_hand = payout.get("dealerHand")
    dealer_card_codes = _visible_card_codes(
        dealer_hand.get("cards") if isinstance(dealer_hand, dict) else None,
    )
    if not dealer_card_codes:
        return None

    return BlackjackSnapshot(
        target_url=str(runtime_state.get("target_url", "") or ""),
        frame_url=str(runtime_state.get("frame_url", "") or ""),
        game_state=_optional_string(runtime_state.get("game_state")),
        hand_position=int(active_hand.get("handPosition", 1) or 1),
        player_card_codes=player_card_codes,
        dealer_card_codes=dealer_card_codes,
        player_cards=[
            _card_code_to_blackjack_value(code) for code in player_card_codes
        ],
        dealer_up_card=_card_code_to_blackjack_value(dealer_card_codes[0]),
        hand_min_sum=int(active_hand.get("minSum", 0) or 0),
        hand_sum=int(active_hand.get("sum", 0) or 0),
        hand_is_hard=bool(active_hand.get("hard", False)),
        hand_is_split=bool(active_hand.get("hasBeenSplit", False)),
        hand_has_blackjack=bool(active_hand.get("hasBlackjack", False)),
        available_actions=_normalize_available_actions(
            active_hand.get("availableActions"),
        ),
    )


ACTION_HIERARCHY = ("double", "split", "hit", "stand")


def _downgrade_action(action: str, valid_actions: list[str]) -> str:
    for higher_action in ACTION_HIERARCHY:
        if action == higher_action and higher_action in valid_actions:
            return higher_action
    for lower_action in reversed(ACTION_HIERARCHY):
        if lower_action in valid_actions:
            return lower_action
    return "stand"


def recommend_blackjack_move(
    snapshot: BlackjackSnapshot,
    *,
    blackjack_strategy_root: Path = DEFAULT_BLACKJACK_STRATEGY_ROOT,
    strategy_complexity: str = DEFAULT_STRATEGY_COMPLEXITY,
    rules: dict | None = None,
) -> BlackjackAdvice:
    script = """
const blackjackStrategy = require(process.argv[1]);
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
process.stdout.write(JSON.stringify({ action }));
""".strip()
    completed = subprocess.run(
        [
            "node",
            "-e",
            script,
            str(blackjack_strategy_root),
            json.dumps(snapshot.player_cards, separators=(",", ":")),
            str(snapshot.dealer_up_card),
            str(snapshot.hand_count()),
            strategy_complexity,
            json.dumps(rules or {}, separators=(",", ":")),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout.strip() or "{}")
    action = str(payload.get("action", "") or "").strip().lower()
    if not action:
        raise ValueError("blackjack-strategy did not return an action")
    valid_actions = [a.lower() for a in snapshot.available_actions]
    if valid_actions and action not in valid_actions:
        action = _downgrade_action(action, valid_actions)
    return BlackjackAdvice(
        action=action,
        player_cards=list(snapshot.player_cards),
        dealer_up_card=snapshot.dealer_up_card,
        player_card_codes=list(snapshot.player_card_codes),
        dealer_card_codes=list(snapshot.dealer_card_codes),
    )


def capture_blackjack_advice(
    *,
    debug_base_url: str = DEFAULT_DEBUG_BASE_URL,
    blackjack_strategy_root: Path = DEFAULT_BLACKJACK_STRATEGY_ROOT,
    strategy_complexity: str = DEFAULT_STRATEGY_COMPLEXITY,
    target_url_fragment: str | None = None,
    frame_url_fragment: str = DEFAULT_BLACKJACK_FRAME_FRAGMENT,
    rules: dict | None = None,
) -> tuple[BlackjackSnapshot | None, BlackjackAdvice | None]:
    snapshot = capture_blackjack_snapshot(
        debug_base_url=debug_base_url,
        target_url_fragment=target_url_fragment,
        frame_url_fragment=frame_url_fragment,
    )
    if snapshot is None:
        return None, None
    return snapshot, recommend_blackjack_move(
        snapshot,
        blackjack_strategy_root=blackjack_strategy_root,
        strategy_complexity=strategy_complexity,
        rules=rules,
    )


def capture_blackjack_snapshot(
    *,
    debug_base_url: str = DEFAULT_DEBUG_BASE_URL,
    target_url_fragment: str | None = None,
    frame_url_fragment: str = DEFAULT_BLACKJACK_FRAME_FRAGMENT,
) -> BlackjackSnapshot | None:
    runtime_state = capture_blackjack_runtime_state(
        debug_base_url=debug_base_url,
        target_url_fragment=target_url_fragment,
        frame_url_fragment=frame_url_fragment,
    )
    return extract_blackjack_snapshot(runtime_state)


def capture_blackjack_runtime_state(
    *,
    debug_base_url: str = DEFAULT_DEBUG_BASE_URL,
    target_url_fragment: str | None = None,
    frame_url_fragment: str = DEFAULT_BLACKJACK_FRAME_FRAGMENT,
) -> dict:
    target = _select_blackjack_target(
        _list_browser_targets(debug_base_url=debug_base_url),
        target_url_fragment=target_url_fragment,
    )
    frame_fragments = None
    if frame_url_fragment:
        lowered_target_url = target.url.lower()
        target_already_has_frame = frame_url_fragment.lower() in lowered_target_url
        is_iframe_target = target.target_type == "iframe"
        if not target_already_has_frame and not is_iframe_target:
            frame_fragments = [frame_url_fragment]
    runtime_state = evaluate_debug_target_main_world_value(
        websocket_debugger_url=target.websocket_debugger_url,
        expression=_blackjack_runtime_expression(),
        frame_url_fragments=frame_fragments,
    )
    if not isinstance(runtime_state, dict):
        raise ValueError("Blackjack runtime capture did not return an object")
    runtime_state["target_url"] = target.url
    if not runtime_state.get("frame_url"):
        runtime_state["frame_url"] = target.url
    return runtime_state


def render_blackjack_screen(
    *,
    snapshot: BlackjackSnapshot | None,
    advice: BlackjackAdvice | None,
    debug_base_url: str,
) -> str:
    lines = [
        "Blackjack Assistant",
        f"CDP: {debug_base_url}",
    ]
    if snapshot is None or advice is None:
        lines.extend(
            [
                "",
                "No active blackjack hand detected.",
                "Keep the table open and wait for a decision point.",
            ],
        )
        return "\n".join(lines)
    lines.extend(
        [
            "",
            f"State: {snapshot.game_state or 'unknown'}",
            f"Player: {' '.join(snapshot.player_card_codes)} ({_format_hand_total(snapshot)})",
            f"Dealer: {' '.join(snapshot.dealer_card_codes)}",
            f"Actions: {_format_available_actions(snapshot.available_actions)}",
            f"Best move: {advice.action.upper()}",
            "",
            f"Frame: {snapshot.frame_url}",
        ],
    )
    return "\n".join(lines)


def format_blackjack_notification(
    *,
    snapshot: BlackjackSnapshot,
    advice: BlackjackAdvice,
) -> str:
    return (
        f"{advice.action.upper()} - player {' '.join(snapshot.player_card_codes)} "
        f"vs dealer {' '.join(snapshot.dealer_card_codes)}"
    )


def build_blackjack_rule_options(
    *,
    number_of_decks: int = 6,
    hit_soft_17: bool = True,
    surrender: str = "late",
    double: str = "any",
    double_after_split: bool = True,
    resplit_aces: bool = False,
    offer_insurance: bool = True,
    max_split_hands: int = 4,
) -> dict:
    return {
        "numberOfDecks": number_of_decks,
        "hitSoft17": hit_soft_17,
        "surrender": surrender,
        "double": double,
        "doubleAfterSplit": double_after_split,
        "resplitAces": resplit_aces,
        "offerInsurance": offer_insurance,
        "maxSplitHands": max_split_hands,
    }


def _visible_card_codes(value) -> list[str]:
    if not isinstance(value, list):
        return []
    return [
        str(item)
        for item in value
        if isinstance(item, str)
        and item
        and item != FACE_DOWN_CARD_CODE
        and item[0] != "0"
    ]


def _card_code_to_blackjack_value(card_code: str) -> int:
    rank = (card_code or "")[0].upper()
    if rank == "A":
        return 1
    if rank in {"K", "Q", "J", "T"}:
        return 10
    if rank.isdigit():
        return int(rank)
    raise ValueError(f"Unsupported blackjack card code: {card_code}")


def _optional_string(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _normalize_available_actions(value) -> list[str]:
    if not isinstance(value, list):
        return []
    actions: list[str] = []
    for item in value:
        if not isinstance(item, str):
            continue
        action = item.strip().lower()
        if action and action not in actions:
            actions.append(action)
    return actions


def _format_hand_total(snapshot: BlackjackSnapshot) -> str:
    if snapshot.hand_is_hard:
        return f"hard {snapshot.hand_sum}"
    if snapshot.hand_min_sum and snapshot.hand_min_sum != snapshot.hand_sum:
        return f"soft {snapshot.hand_sum} / min {snapshot.hand_min_sum}"
    return str(snapshot.hand_sum)


def _format_available_actions(actions: list[str]) -> str:
    if not actions:
        return "unknown"
    return ", ".join(action.upper() for action in actions)


def _list_browser_targets(*, debug_base_url: str) -> list[BrowserTarget]:
    return [
        BrowserTarget(
            target_id=target.target_id,
            target_type=target.target_type,
            title=target.title,
            url=target.url,
            websocket_debugger_url=target.websocket_debugger_url,
        )
        for target in list_debug_targets(
            debug_base_url=debug_base_url,
            target_types=("page", "iframe"),
        )
    ]


def _select_blackjack_target(
    targets: list[BrowserTarget],
    *,
    target_url_fragment: str | None,
) -> BrowserTarget:
    if target_url_fragment:
        lowered_fragment = target_url_fragment.lower()
        for target in targets:
            haystack = f"{target.title} {target.url}".lower()
            if lowered_fragment in haystack:
                return target
        raise ValueError(
            f"Could not find a target matching fragment: {target_url_fragment}"
        )

    candidates: list[tuple[int, BrowserTarget]] = []
    for target in targets:
        lowered_url = target.url.lower()
        lowered_title = target.title.lower()
        if any(blocked in lowered_url for blocked in BLACKJACK_TARGET_BLOCKLIST):
            continue
        score = 0
        if "cayetanopremiumblackjack" in lowered_url:
            score += 1000
        for keyword in BLACKJACK_TARGET_KEYWORDS:
            if keyword in lowered_url or keyword in lowered_title:
                score += 100
        if target.target_type == "iframe":
            score += 10
        if score > 0:
            candidates.append((score, target))
    if not candidates:
        raise ValueError(
            "Could not find a live blackjack target in the current Chrome session"
        )
    candidates.sort(key=lambda item: (item[0], item[1].url), reverse=True)
    return candidates[0][1]


def _blackjack_runtime_expression() -> str:
    return r"""
(() => {
  const manager = window.com?.cayetano?.manager;
  const clone = (value) => {
    if (value == null) {
      return null;
    }
    try {
      return JSON.parse(JSON.stringify(value));
    } catch (error) {
      return null;
    }
  };
  const response = clone(
    manager?.serverResponse
      || manager?.platformProxy?.topbar?.server?.responseEvent?._autoData
      || null
  );
  return {
    game_state: manager?.proxy?._currentGameState ?? null,
    frame_url: location.href,
    server_response: response,
  };
})()
""".strip()
