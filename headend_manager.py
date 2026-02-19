from flask import Flask, jsonify, request
from typing import Any, Dict, List, Optional
from rollback.service import RollbackService
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("headend.manager")

app = Flask(__name__)

services: Dict[str, RollbackService] = {}


def get_or_create_service(aircraft_id: str) -> RollbackService:
    if aircraft_id not in services:
        services[aircraft_id] = RollbackService(aircraft_id)
        logger.info("Created RollbackService for aircraft_id=%s", aircraft_id)
    return services[aircraft_id]


@app.route("/aircraft", methods=["GET"])
def list_aircraft():
    """
    List known aircraft and minimal info, including LVA and LKG version.
    """
    aircraft_view: Dict[str, Dict[str, Any]] = {}

    for aid, svc in services.items():
        s = svc.state
        with s.lock:
            summary = svc.state_summary()
            aircraft_view[aid] = {
                "id": aid,
                "lva": summary.get("lva"),
                "lkg": summary.get("lkg_version"),
            }

    return jsonify(
        {
            "ok": True,
            "aircraft": aircraft_view,
        }
    ), 200



@app.route("/aircraft/<aircraft_id>/receive", methods=["POST"])
def receive_multi(aircraft_id: str):
    """
    Unified receive endpoint:
    /aircraft/<id>/receive

    Body:
      {
        "type": "FULL" | "PATCH" | "ROLLBACK",
        "payload": { ... }
      }
    """
    svc = get_or_create_service(aircraft_id)

    msg = request.get_json(force=True)
    msg_type = msg.get("type")
    payload = msg.get("payload")

    if not msg_type or not isinstance(payload, dict):
        return jsonify({"ok": False, "error": "invalid message"}), 400

    s = svc.state
    with s.lock:
        try:
            if msg_type == "FULL":
                svc.handle_full(payload)
                return jsonify({"ok": True, "lva": s.lva}), 200

            if msg_type == "PATCH":
                status = svc.handle_patch(payload)
                return jsonify({"ok": True, "status": status, "lva": s.lva}), 200

            if msg_type == "ROLLBACK":
                status = svc.handle_rollback(payload)
                return jsonify({"ok": True, "status": status, "lva": s.lva}), 200

            return jsonify({"ok": False, "error": "unknown type"}), 400

        except Exception as e:
            logger.exception("Processing failed for aircraft_id=%s", aircraft_id)
            return jsonify({"ok": False, "error": str(e), "lva": s.lva}), 422


@app.route("/aircraft/<aircraft_id>/state", methods=["GET"])
def aircraft_state_multi(aircraft_id: str):
    svc = get_or_create_service(aircraft_id)
    with svc.state.lock:
        return (
            jsonify(
                {
                    "ok": True,
                    "aircraft_id": aircraft_id,
                    "state": svc.state_summary(),
                }
            ),
            200,
        )


@app.route("/aircraft/<aircraft_id>/ping", methods=["GET"])
def aircraft_ping_multi(aircraft_id: str):
    svc = get_or_create_service(aircraft_id)
    return jsonify(svc.ping_payload()), 200


@app.route("/aircraft/<aircraft_id>/inject_fault", methods=["POST"])
def inject_fault_multi(aircraft_id: str):
    svc = get_or_create_service(aircraft_id)
    data = request.get_json(force=True)
    enabled = data.get("enabled", False)
    mode = data.get("mode", "none")

    try:
        with svc.state.lock:
            svc.inject_fault(enabled, mode)
        return (
            jsonify(
                {
                    "ok": True,
                    "aircraft_id": aircraft_id,
                    "simulate_failure": svc.state.simulate_failure,
                    "failure_mode": svc.state.failure_mode,
                }
            ),
            200,
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/aircraft/<aircraft_id>/reset", methods=["POST"])
def reset_multi(aircraft_id: str):
    svc = get_or_create_service(aircraft_id)
    with svc.state.lock:
        svc.reset()
    return (
        jsonify(
            {
                "ok": True,
                "aircraft_id": aircraft_id,
                "message": "Aircraft state reset to clean slate",
            }
        ),
        200,
    )


@app.route("/ping", methods=["GET"])
def headend_ping():
    return jsonify(
        {
            "ok": True,
            "time": datetime.utcnow().isoformat() + "Z",
            "aircraft_count": len(services),
        }
    ), 200


if __name__ == "__main__":
    logger.info("Starting multi-aircraft rollback service on :9000")
    for sample_id in ["AC001", "AC002", "AC003"]:
        services[sample_id] = RollbackService(sample_id)
        logger.info("Initialized sample RollbackService for aircraft_id=%s", sample_id)
    app.run(host="0.0.0.0", port=9000, debug=True)
