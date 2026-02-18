"""
This is mostly here for backwards compat with previous code we had
"""
from datetime import datetime

from flask import Flask, jsonify, request

from rollback.service import RollbackService
from rollback.models import logger

# ---------------------------------------------------------------------
# Single-aircraft Flask app wiring RollbackService to HTTP
# ---------------------------------------------------------------------

app = Flask(__name__)

service = RollbackService("single-aircraft")


@app.route("/receive", methods=["POST"])
def receive():
    """
    Primary entrypoint for config messages.

    Body:
      {
        "type": "FULL" | "PATCH" | "ROLLBACK",
        "payload": { ... }
      }
    """
    msg = request.get_json(force=True)
    msg_type = msg.get("type")
    payload = msg.get("payload")

    if not msg_type or not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "invalid message"}), 400

    s = service.state
    with s.lock:
        try:
            if msg_type == "FULL":
                service.handle_full(payload)
                return jsonify({"ok": True, "lva": s.lva}), 200

            if msg_type == "PATCH":
                status = service.handle_patch(payload)
                return jsonify({"ok": True, "status": status, "lva": s.lva}), 200

            if msg_type == "ROLLBACK":
                status = service.handle_rollback(payload)
                return jsonify({"ok": True, "status": status, "lva": s.lva}), 200

            return jsonify({"ok": False, "error": "unknown type"}), 400

        except Exception as e:
            # On any processing error, ensure Active is consistent with last validated LVA
            try:
                service.reconstruct_active_for_lva(s.lva)
            except Exception:
                logger.exception("Failed to reconstruct active after error")

            logger.exception("Processing failed")
            return jsonify(
                {
                    "ok": False,
                    "error": str(e),
                    "lva": s.lva,
                }
            ), 422


@app.route("/aircraft_state", methods=["GET"])
def aircraft_state():
    """
    Detailed state for this single aircraft instance.
    """
    s = service.state
    with s.lock:
        return jsonify(service.state_summary()), 200


@app.route("/ping", methods=["GET"])
def ping():
    """
    Simple health check.
    """
    s = service.state
    return (
        jsonify(
            {
                "ok": True,
                "version": s.lva,
                "time": datetime.utcnow().isoformat() + "Z",
            }
        ),
        200,
    )


@app.route("/inject_fault", methods=["POST"])
def inject_fault():
    """
    Control failure simulation via POST.
    Body:
      {
        "enabled": bool,
        "mode": "validation_error" | "apply_error" | "rollback_error" | "none"
      }
    """
    data = request.get_json(force=True)
    enabled = data.get("enabled", False)
    mode = data.get("mode", "none")

    valid_modes = ["none", "validation_error", "apply_error", "rollback_error"]
    if mode not in valid_modes:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": f"Invalid mode. Must be one of {valid_modes}",
                }
            ),
            400,
        )

    with service.state.lock:
        service.inject_fault(enabled, mode)

    return (
        jsonify(
            {
                "ok": True,
                "simulate_failure": service.state.simulate_failure,
                "failure_mode": service.state.failure_mode,
            }
        ),
        200,
    )


@app.route("/promote_lkg", methods=["POST"])
def promote_lkg():
    """
    Promote the current active configuration to become the new LKG.

    Operational hook for:
      - Rollback to LVA (Last Version Active) policy: once the current
        configuration is deemed good, Ground can explicitly mark it
        as the new baseline (LKG).
    """
    with service.state.lock:
        try:
            service.promote_active_to_lkg()
            return (
                jsonify(
                    {
                        "ok": True,
                        "lva": service.state.lva,
                        "message": "Active configuration promoted to LKG",
                    }
                ),
                200,
            )
        except Exception as e:
            logger.exception("Failed to promote active to LKG")
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": str(e),
                        "lva": service.state.lva,
                    }
                ),
                422,
            )


@app.route("/fault_status", methods=["GET"])
def fault_status():
    """
    Get current fault injection status.
    """
    s = service.state
    return (
        jsonify(
            {
                "simulate_failure": s.simulate_failure,
                "failure_mode": s.failure_mode,
            }
        ),
        200,
    )


@app.route("/reset", methods=["POST"])
def reset():
    """
    Reset aircraft state to a clean slate for demo/testing purposes.
    """
    with service.state.lock:
        service.reset()

    return (
        jsonify(
            {
                "ok": True,
                "message": "Aircraft state reset to clean slate",
            }
        ),
        200,
    )


if __name__ == "__main__":
    logger.info("Starting single-aircraft rollback service on :9000")
    app.run(host="0.0.0.0", port=9000, debug=True)
