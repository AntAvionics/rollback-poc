import logging
import threading
from copy import deepcopy
from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional

from flask import Flask, jsonify, request

# ---------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aircraft.config")

app = Flask(__name__)

# ---------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------

@dataclass
class Patch:
    prev: int
    changes: Dict[str, Any]


@dataclass
class AircraftState:
    lock: threading.RLock = field(default_factory=threading.RLock)
    # Last Known Good (from last FULL)
    lkg: Dict[str, Any] = field(default_factory=dict)

    # Currently active config
    active: Dict[str, Any] = field(default_factory=dict)

    # Last Version Applied (authoritative)
    lva: Optional[int] = None

    # Applied patches in arrival order
    applied_patches: List[Patch] = field(default_factory=list)

    # Queued patches waiting for prev == lva
    queued_patches: List[Patch] = field(default_factory=list)

    # LVA snapshot history for staged rollback (version → config)
    lva_history: "OrderedDict[int, Dict[str, Any]]" = field(default_factory=OrderedDict)
    history_limit: int = 20

    # Failure simulation
    simulate_failure: bool = False
    failure_mode: str = "none"  # "validation_error", "apply_error", "rollback_error"


state = AircraftState()

# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def validate_targeting(target):
    return None

def validate_config(cfg: Dict[str, Any]) -> Optional[str]:
    return None


def apply_patch(cfg: Dict[str, Any], patch: Patch) -> Dict[str, Any]:
    """
    Apply patch by doing a deep merge of changes into config.
    Nested dicts are merged recursively, not replaced.
    """
    new_cfg = deepcopy(cfg)
    
    def deep_merge(target: Dict, updates: Dict) -> None:
        """Recursively merge updates into target."""
        for key, value in updates.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                # Both are dicts: recurse
                deep_merge(target[key], value)
            else:
                # Scalar or new key: replace/add
                target[key] = value
    
    deep_merge(new_cfg, patch.changes)
    return new_cfg


def try_apply_queued():
    """
    Apply queued patches if their prev now matches LVA.
    FIFO by arrival order.
    """
    progressed = True
    while progressed:
        progressed = False
        for p in list(state.queued_patches):
            if p.prev == state.lva:
                logger.info("Applying queued PATCH prev=%s", p.prev)
                state.queued_patches.remove(p)
                _apply_patch_internal(p)
                progressed = True
                break


def _apply_patch_internal(patch: Patch):
    candidate = apply_patch(state.active, patch)
    err = validate_config(candidate)
    if err:
        raise ValueError(err)

    state.active = candidate
    state.applied_patches.append(patch)

    # Advance version by exactly 1
    state.lva = state.lva + 1 if state.lva is not None else None

    if state.lva is not None:
        _save_snapshot(state.lva, state.active)

    logger.info("PATCH applied → new LVA=%s", state.lva)

# ---------------------------------------------------------------------
# Core handlers
# ---------------------------------------------------------------------

def _save_snapshot(version: int, cfg: Dict[str, Any]) -> None:
    state.lva_history[version] = deepcopy(cfg)

    # enforce bounded history
    if len(state.lva_history) > state.history_limit:
        excess = len(state.lva_history) - state.history_limit
        for _ in range(excess):
            state.lva_history.popitem(last=False)

def handle_full(payload: Dict[str, Any]):
    version = payload.get("version")
    data = payload.get("data")

    if not isinstance(version, int) or not isinstance(data, dict):
        raise ValueError("FULL requires { version: int, data: object }")

    err = validate_config(data)
    if err:
        raise ValueError(err)

    # Store a copy of data with version metadata
    data_with_version = deepcopy(data)
    data_with_version["_version"] = version

    state.lkg = data_with_version
    state.active = deepcopy(data_with_version)
    state.lva = version
    state.applied_patches.clear()
    state.queued_patches.clear()

    state.lva_history.clear()
    _save_snapshot(version, state.active)

    logger.info("FULL applied version=%s", version)


def handle_patch(payload: Dict[str, Any]) -> str:
    prev = payload.get("prev")
    changes = payload.get("changes")

    if not isinstance(prev, int) or not isinstance(changes, dict):
        raise ValueError("PATCH requires { prev: int, changes: object }")

    patch = Patch(prev=prev, changes=changes)

    if state.lva is None:
        raise ValueError("PATCH received before FULL")

    # Simulate validation failure
    if state.simulate_failure and state.failure_mode == "validation_error":
        raise ValueError("Simulated validation error in PATCH")

    if patch.prev < state.lva:
        logger.info(
            "PATCH discarded (prev=%s) because current LVA=%s is higher",
            patch.prev,
            state.lva,
        )
        return "discarded"
    if patch.prev != state.lva:
        logger.warning(
            "PATCH queued (prev=%s, lva=%s)", patch.prev, state.lva
        )
        state.queued_patches.append(patch)
        return "queued"
    _apply_patch_internal(patch)
    try_apply_queued()
    return "applied"


def rollback_staged(max_steps: int = 5) -> bool:
    if state.lva is None:
        return False
    current = state.lva
    steps = 0
    # Walk backward through history, up to max_steps
    for i in range(1, max_steps + 1):
        target = current - i
        if target not in state.lva_history:
            break
        candidate = deepcopy(state.lva_history[target])
        err = validate_config(candidate)
        if not err:
            # Found first valid snapshot, restore and stop
            state.active = candidate
            state.lva = target
            state.applied_patches.clear()
            state.queued_patches.clear()
            _save_snapshot(state.lva, state.active)
            logger.info("Staged rollback: restored to LVA=%s after %d step(s)", target, i)
            return True
        else:
            logger.info("Staged rollback: LVA=%s invalid, continuing", target)
    return False



def handle_rollback(_: Dict[str, Any], max_steps: int = 5) -> str:
    """
    Three-tier rollback:
      0) Attempt staged rollback using LVA history snapshots (up to max_steps)
      1) Simple reset to LKG (Last Known Good)
      2) If validation fails, hard reset to empty state
    """
    logger.warning("ROLLBACK requested")

    # Simulate rollback failure mode
    if state.simulate_failure and state.failure_mode == "rollback_error":
        logger.error("Simulated rollback failure")
        raise ValueError("Simulated rollback processing error")
    
    # Tier 0 - Try staged rollback first
    try:
        if rollback_staged(max_steps=max_steps):
            return "rollback_staged"
    except Exception as e:
        logger.error("Staged rollback failed: %s", e)

    # Tier 1 — reset to LKG
    try:
        reset_state = deepcopy(state.lkg)
        original_version = reset_state.pop("_version", 1)  # Extract and remove version metadata
        state.lva_history.clear()
        _save_snapshot(state.lva, state.active)
        
        err = validate_config(reset_state)
        if err:
            raise ValueError(err)

        state.active = reset_state
        state.lva = original_version
        state.applied_patches.clear()
        state.queued_patches.clear()

        logger.info("Rollback to LKG successful, reset to version=%s", state.lva)
        return "rollback_lkg"

    except Exception as e:
        logger.error("Rollback to LKG failed: %s", e)

    # Tier 2 — hard reset (nuclear option)
    state.active = {}
    state.lkg = {}
    state.applied_patches.clear()
    state.queued_patches.clear()
    state.lva_history.clear()
    state.lva = None

    logger.error("Hard reset executed - all state cleared")
    return "rollback_hard_reset"

# ---------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------

@app.route("/receive", methods=["POST"])
def receive():
    msg = request.get_json(force=True)
    msg_type = msg.get("type")
    payload = msg.get("payload")

    if not msg_type or not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "invalid message"}), 400

    with state.lock:
        try:
            if msg_type == "FULL":
                handle_full(payload)
                return jsonify({"ok": True, "lva": state.lva}), 200

            if msg_type == "PATCH":
                status = handle_patch(payload)
                return jsonify({"ok": True, "status": status, "lva": state.lva}), 200

            if msg_type == "ROLLBACK":
                status = handle_rollback(payload)
                return jsonify({"ok": True, "status": status, "lva": state.lva}), 200

            return jsonify({"ok": False, "error": "unknown type"}), 400

        except Exception as e:
            logger.exception("Processing failed")
            return jsonify({
                "ok": False,
                "error": str(e),
                "lva": state.lva
            }), 422


@app.route("/aircraft_state", methods=["GET"])
def aircraft_state():
    with state.lock:
        return jsonify({
            "lkg": state.lkg,
            "active": state.active,
            "lva": state.lva,
            "applied_patches": len(state.applied_patches),
            "queued_patches": len(state.queued_patches),
        })


@app.route("/ping", methods=["GET"])
def ping():
    return jsonify({
        "ok": True,
        "version": state.lva,
        "time": datetime.utcnow().isoformat() + "Z"
    })


@app.route("/inject_fault", methods=["POST"])
def inject_fault():
    """
    Control failure simulation via POST.
    Body: {
      "enabled": bool,
      "mode": "validation_error" | "apply_error" | "rollback_error" | "none"
    }
    """
    data = request.get_json(force=True)
    enabled = data.get("enabled", False)
    mode = data.get("mode", "none")

    valid_modes = ["none", "validation_error", "apply_error", "rollback_error"]
    if mode not in valid_modes:
        return jsonify({"ok": False, "error": f"Invalid mode. Must be one of {valid_modes}"}), 400

    with state.lock:
        state.simulate_failure = enabled
        state.failure_mode = mode if enabled else "none"
        logger.info("Fault injection: enabled=%s mode=%s", state.simulate_failure, state.failure_mode)

    return jsonify({
        "ok": True,
        "simulate_failure": state.simulate_failure,
        "failure_mode": state.failure_mode
    }), 200


@app.route("/fault_status", methods=["GET"])
def fault_status():
    """Get current fault injection status."""
    return jsonify({
        "simulate_failure": state.simulate_failure,
        "failure_mode": state.failure_mode
    }), 200


@app.route("/reset", methods=["POST"])
def reset():
    """Reset aircraft state to clean slate for demo purposes."""
    with state.lock:
        state.lkg = {}
        state.active = {}
        state.lva = None
        state.applied_patches = []
        state.queued_patches = []
        state.lva_history.clear() 
        state.simulate_failure = False
        state.failure_mode = "none"
        logger.info("Aircraft state reset to clean slate")
    
    return jsonify({
        "ok": True,
        "message": "Aircraft state reset to clean slate"
    }), 200


# ---------------------------------------------------------------------

if __name__ == "__main__":
    logger.info("Starting aircraft config service on :9000")
    app.run(host="0.0.0.0", port=9000, debug=True)
