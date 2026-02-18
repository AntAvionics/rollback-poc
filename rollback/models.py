import logging
import threading
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("aircraft.config")


@dataclass
class Patch:
    """
    Represents a PATCH message:

      prev:    the version number this patch expects as its base
      changes: arbitrary nested dict of changes to apply on top of the base config
    """
    prev: int
    changes: Dict[str, Any]


@dataclass
class AircraftState:
    """
    All per-aircraft mutable state.

    This state object is protected by `lock` to ensure that operations on a
    single aircraft are serialized and internally consistent.
    """
    # Lock guarding all access/mutation to this aircraft's state
    lock: threading.RLock = field(default_factory=threading.RLock)

    # Last Known Good (from last FULL) — stored as a full snapshot with metadata.
    # When populated by FULL or promotion, this dict will include:
    #   "_version": int
    # along with the actual configuration fields.
    lkg: Dict[str, Any] = field(default_factory=dict)

    # Currently active config (typically reconstructed from LKG + applied_patches up to LVA)
    active: Dict[str, Any] = field(default_factory=dict)

    # Last Version Applied (authoritative integer version, or None if no FULL yet)
    lva: Optional[int] = None

    # Applied patches in arrival order after LKG.
    # Convention: if LKG._version == N, then applied_patches[0] corresponds to version N+1, etc.
    applied_patches: List[Patch] = field(default_factory=list)

    # Queued patches whose prev does not yet match LVA (out-of-order arrivals).
    # These are retried when LVA advances.
    queued_patches: List[Patch] = field(default_factory=list)

    # Failure simulation flags for testing / fault injection
    simulate_failure: bool = False
    failure_mode: str = "none"  # one of: "none", "validation_error", "apply_error", "rollback_error"
