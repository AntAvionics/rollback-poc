"""
rollback_service.py
Compatibility shim for the refactored rollback modules.
- rollback/models.py   -> core dataclasses (Patch, AircraftState)
- rollback/service.py  -> RollbackService (pure rollback logic)
- rollback/app.py      -> single-aircraft Flask app wiring RollbackService to HTTP (LEGACY)
"""
# This is here for backwards compatability, run headend_manager.py for multi-headend support
from rollback.service import RollbackService
__all__ = ["RollbackService"]
