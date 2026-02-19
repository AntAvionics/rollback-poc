#!/usr/bin/env python3

import logging
from copy import deepcopy
from datetime import datetime
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional
import threading
from rollback.models import Patch, AircraftState



logger = logging.getLogger("aircraft.config")



class RollbackService:
    def __init__(self, aircraft_id: str) -> None:
        self.state = AircraftState()
        self.aircraft_id = aircraft_id
        self.log = logging.getLogger(f"aircraft.{aircraft_id}")

    # ---------------------------
    # Validation placeholders
    # ---------------------------

    def validate_targeting(self, target: Any) -> Optional[str]:
        # Placeholder for targeting validation
        return None

    def validate_config(self, cfg: Dict[str, Any]) -> Optional[str]:
        # Placeholder for config validation. Return None if OK, otherwise a string error.
        return None

    # ---------------------------
    # Patch application
    # ---------------------------

    def apply_patch_to_cfg(self, cfg: Dict[str, Any], patch: Patch) -> Dict[str, Any]:
        """
        Apply a patch by deep-merging changes into cfg, returning a new dict.
        This does not mutate the original cfg.
        """
        new_cfg = deepcopy(cfg)

        def deep_merge(target: Dict[str, Any], updates: Dict[str, Any]) -> None:
            for key, value in updates.items():
                if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                    deep_merge(target[key], value)
                else:
                    target[key] = value

        deep_merge(new_cfg, patch.changes)
        return new_cfg

    # ---------------------------
    # Reconstruction helpers
    # ---------------------------

    def _lkg_version(self) -> Optional[int]:
        """
        Return the version recorded in LKG (if available).
        """
        try:
            return int(self.state.lkg.get("_version")) if "_version" in self.state.lkg else None
        except Exception:
            return None

    def reconstruct_active_for_lva(self, target_lva: Optional[int]) -> None:
        """
        Reconstruct `state.active` by replaying applied_patches on top of LKG
        up to the target_lva (inclusive). If target_lva is None or LKG missing,
        clear active accordingly.

        After reconstruction, truncate applied_patches to match the reconstructed LVA.
        """
        s = self.state
        if target_lva is None:
            # No LVA -> clear active
            s.active = {}
            s.applied_patches = []
            return

        lkg_version = self._lkg_version()
        if lkg_version is None:
            # No LKG to reconstruct from; set to empty
            s.active = {}
            s.applied_patches = []
            s.lva = None
            return

        # Determine how many patches we need to apply to reach target_lva.
        # If target_lva < lkg_version: it's an invalid target; treat as "reset to LKG"
        if target_lva < lkg_version:
            s.active = deepcopy(s.lkg)
            s.active.pop("_version", None)
            s.applied_patches = []
            s.lva = lkg_version
            return

        needed_count = target_lva - lkg_version  # how many patches to apply
        # Truncate applied_patches to at most what we have
        available_count = len(s.applied_patches)
        if needed_count > available_count:
            # We don't have enough patches to reach target; reconstruct as far as possible
            needed_count = available_count

        # Start from LKG baseline (excluding metadata)
        baseline = deepcopy(s.lkg)
        baseline.pop("_version", None)

        # Apply first `needed_count` patches in order
        candidate = baseline
        for i in range(needed_count):
            patch = s.applied_patches[i]
            candidate = self.apply_patch_to_cfg(candidate, patch)

        # Commit reconstruction
        s.active = candidate
        # Truncate applied_patches to the ones we've applied (if any extras existed beyond target_lva, they are discarded)
        s.applied_patches = s.applied_patches[:needed_count]
        # Set LVA pointer accordingly
        s.lva = lkg_version + needed_count

    # ---------------------------
    # Patch application flow
    # ---------------------------

    def _apply_patch_internal(self, patch: Patch) -> None:
        """
        Attempt to apply a patch on top of current active config.
        On success:
          - append patch to applied_patches
          - advance LVA by 1
          - update active to the candidate config
        On failure (validation/apply error):
          - do NOT modify applied_patches
          - restore active to last validated state by reconstructing from LKG + applied_patches up to current LVA
          - raise an exception
        """
        s = self.state

        # Candidate is what active would become after applying the patch
        candidate = self.apply_patch_to_cfg(s.active, patch)

        # Simulate an "apply" failure if requested (for testing)
        if s.simulate_failure and s.failure_mode == "apply_error":
            raise ValueError("Simulated apply error in PATCH")

        # Validate candidate
        err = self.validate_config(candidate)
        if err:
            # On validation failure, reconstruct active to last validated LVA and raise
            self.log.warning("Validation failed for candidate after applying patch: %s", err)
            # Ensure active is consistent with current authoritative LVA
            self.reconstruct_active_for_lva(s.lva)
            raise ValueError(err)

        # All good -> commit
        s.applied_patches.append(patch)

        # Advance LVA by exactly 1
        s.lva = s.lva + 1 if s.lva is not None else None

        # Update active to candidate
        s.active = candidate

        self.log.info("PATCH applied → new LVA=%s", s.lva)

    def try_apply_queued(self) -> None:
        """
        Apply queued patches if their prev now matches LVA.
        FIFO by arrival order.
        """
        s = self.state
        progressed = True
        while progressed:
            progressed = False
            for p in list(s.queued_patches):
                if p.prev == s.lva:
                    self.log.info("Applying queued PATCH prev=%s", p.prev)
                    s.queued_patches.remove(p)
                    try:
                        self._apply_patch_internal(p)
                    except Exception:
                        # On failure applying a queued patch, stop processing further queued patches.
                        # Active will have been reconstructed inside _apply_patch_internal on failure.
                        raise
                    progressed = True
                    break

    # ---------------------------
    # Core handlers
    # ---------------------------

    def handle_full(self, payload: Dict[str, Any]) -> None:
        """
        FULL payload: { version: int, data: dict }
        Stores LKG snapshot and resets applied/queued patches.
        """
        s = self.state
        version = payload.get("version")
        data = payload.get("data")

        if not isinstance(version, int) or not isinstance(data, dict):
            raise ValueError("FULL requires { version: int, data: object }")

        err = self.validate_config(data)
        if err:
            raise ValueError(err)

        data_with_version = deepcopy(data)
        data_with_version["_version"] = version

        s.lkg = data_with_version
        # Active is the LKG baseline immediately
        s.active = deepcopy(data_with_version)
        s.active.pop("_version", None)
        # LVA points to the FULL version (no patches applied yet)
        s.lva = version
        # Clear patch history and queue — subsequent PATCHes will append to applied_patches
        s.applied_patches.clear()
        s.queued_patches.clear()

        self.log.info("FULL applied version=%s", version)

    def handle_patch(self, payload: Dict[str, Any]) -> str:
        """
        PATCH payload: { prev: int, changes: dict }
        Returns "applied", "queued", or "discarded".
        """
        s = self.state
        prev = payload.get("prev")
        changes = payload.get("changes")

        if not isinstance(prev, int) or not isinstance(changes, dict):
            raise ValueError("PATCH requires { prev: int, changes: object }")

        patch = Patch(prev=prev, changes=changes)

        if s.lva is None:
            raise ValueError("PATCH received before FULL")

        # Simulate validation failure before attempting apply (testing)
        if s.simulate_failure and s.failure_mode == "validation_error":
            # Simulate as if the validation of candidate would fail
            raise ValueError("Simulated validation error in PATCH")

        # If patch.prev < s.lva: it's an old patch; discard
        if patch.prev < s.lva:
            self.log.info(
                "PATCH discarded (prev=%s) because current LVA=%s is higher",
                patch.prev,
                s.lva,
            )
            return "discarded"

        # If patch.prev != s.lva: out-of-order => queue
        if patch.prev != s.lva:
            self.log.warning(
                "PATCH queued (prev=%s, lva=%s)", patch.prev, s.lva
            )
            s.queued_patches.append(patch)
            return "queued"

        # Else prev == lva -> attempt to apply
        try:
            self._apply_patch_internal(patch)
            # If applied successfully, try to apply any queued patches that now match
            self.try_apply_queued()
            return "applied"
        except Exception as e:
            # On failure, ensure active is consistent with last validated LVA,
            # and discard any applied_patches beyond LVA pointer (reconstruction does truncation).
            self.log.exception("PATCH application failed: %s", e)
            self.reconstruct_active_for_lva(s.lva)
            # After failure, any queued patches with prev > lva remain queued (they may be discarded externally)
            # Report the error to the caller via exception
            raise

    def rollback_staged(self, max_steps: int = 5) -> bool:
        """
        Attempt staged rollback by walking backward from current LVA up to max_steps.
        For each candidate target, reconstruct Active from LKG + patches up to that target and validate.
        On first valid snapshot, restore and truncate applied_patches to match the restored LVA.
        """
        s = self.state
        if s.lva is None:
            return False
        current = s.lva
        for i in range(1, max_steps + 1):
            target = current - i
            if target is None:
                break
            # Attempt to reconstruct candidate state for this target
            lkg_version = self._lkg_version()
            if lkg_version is None:
                break
            # If we don't have enough applied patches to reach target, stop
            needed = target - lkg_version
            if needed < 0:
                continue
            if needed > len(s.applied_patches):
                # Can't reconstruct this far back because we don't have older patches
                # (this can happen if applied_patches were truncated earlier)
                continue
            # Build candidate by replaying first `needed` patches
            baseline = deepcopy(s.lkg)
            baseline.pop("_version", None)
            candidate = baseline
            for idx in range(needed):
                candidate = self.apply_patch_to_cfg(candidate, s.applied_patches[idx])
            # Validate candidate
            err = self.validate_config(candidate)
            if not err:
                # Success: commit restoration
                s.active = candidate
                s.lva = target
                s.applied_patches = s.applied_patches[:needed]
                s.queued_patches.clear()
                self.log.info("Staged rollback: restored to LVA=%s after %d step(s)", target, i)
                return True
            else:
                self.log.info("Staged rollback: candidate for LVA=%s invalid, continuing", target)
        return False

    def handle_rollback(self, payload: Dict[str, Any], max_steps: int = 5) -> str:
        """
        Three-tier rollback, with client-controlled target:

          target in payload:
            - "auto" (default): 2-tier logic
                1) Attempt staged rollback using patch replay up to max_steps
                2) Reset to LKG (Last Known Good)
            - "lkg": force rollback directly to LKG (skip staged rollback)
            - "lva": no-op (already at LVA) unless extended to take explicit version

        The client sends:
          {
            "target": "auto" | "lkg" | "lva"   # optional, default "auto"
          }
        """
        s = self.state
        target_mode = (payload or {}).get("target", "auto")
        if target_mode not in ("auto", "lkg", "lva"):
            raise ValueError(f"Invalid rollback target '{target_mode}', must be one of: auto, lkg, lva")

        self.log.warning("ROLLBACK requested target=%s", target_mode)

        # Simulate rollback failure mode
        if s.simulate_failure and s.failure_mode == "rollback_error":
            self.log.error("Simulated rollback failure")
            raise ValueError("Simulated rollback processing error")

        # Tier 1 - Try staged rollback first (only when target=auto)
        if target_mode == "auto":
            try:
                if self.rollback_staged(max_steps=max_steps):
                    return "rollback_staged"
            except Exception as e:
                self.log.error("Staged rollback failed: %s", e)

        # Tier 2 — reset to LKG (for target=auto or target=lkg)
        if target_mode in ("auto", "lkg"):
            try:
                reset_state = deepcopy(s.lkg)
                original_version = reset_state.pop("_version", 1)
                # Reconstruct active from LKG baseline (no applied patches)
                err = self.validate_config(reset_state)
                if err:
                    raise ValueError(err)

                s.active = reset_state
                s.lva = original_version
                s.applied_patches.clear()
                s.queued_patches.clear()

                self.log.info("Rollback to LKG successful, reset to version=%s", s.lva)
                return "rollback_lkg"

            except Exception as e:
                self.log.error("Rollback to LKG failed: %s", e)

        # If target is explicitly "lva", we do not hard reset; we are already at the current LVA.
        if target_mode == "lva":
            self.log.info("Rollback target 'lva' requested - no state change performed")
            return "rollback_lva_noop"

    # ---------------------------
    # Utilities
    # ---------------------------

    def inject_fault(self, enabled: bool, mode: str) -> None:
        valid_modes = ["none", "validation_error", "apply_error", "rollback_error"]
        if mode not in valid_modes:
            raise ValueError(f"Invalid mode. Must be one of {valid_modes}")
        s = self.state
        s.simulate_failure = enabled
        s.failure_mode = mode if enabled else "none"
        self.log.info("Fault injection: enabled=%s mode=%s", s.simulate_failure, s.failure_mode)

    def reset(self) -> None:
        s = self.state
        s.lkg = {}
        s.active = {}
        s.lva = None
        s.applied_patches = []
        s.queued_patches = []
        s.simulate_failure = False
        s.failure_mode = "none"
        self.log.info("Aircraft state reset to clean slate")

    def promote_active_to_lkg(self) -> None:
        """
        Promote the current active configuration to be the new LKG.

        - Copies active into lkg and sets the LKG _version to the current LVA.
        - Clears applied_patches because active now represents the new baseline.
        - Queued patches are preserved; callers can decide whether to drop them.
        """
        s = self.state
        if s.lva is None:
            raise ValueError("Cannot promote to LKG: no LVA is set")

        if not isinstance(s.active, dict):
            raise ValueError("Cannot promote to LKG: active config is not a dict")

        # New LKG snapshot is the current active plus version metadata
        new_lkg = deepcopy(s.active)
        new_lkg["_version"] = s.lva
        s.lkg = new_lkg

        # Active remains the same logical config (without version metadata)
        s.active = deepcopy(s.active)

        # Since LKG now reflects the current LVA, patch history is no longer needed
        s.applied_patches = []

        self.log.info("Promoted active configuration to new LKG at version=%s", s.lva)

    def state_summary(self) -> Dict[str, Any]:
        s = self.state
        # LKG version is stored in the "_version" field when present
        lkg_version = None
        if isinstance(s.lkg, dict):
            lkg_version = s.lkg.get("_version")

        return {
            "lkg": s.lkg,
            "active": s.active,
            "lva": s.lva,
            "lkg_version": lkg_version,
            "has_active": bool(s.active),
            "applied_patches": len(s.applied_patches),
            "queued_patches": len(s.queued_patches),
        }

    def ping_payload(self) -> Dict[str, Any]:
        return {
            "ok": True,
            "version": self.state.lva,
            "time": datetime.utcnow().isoformat() + "Z",
        }
