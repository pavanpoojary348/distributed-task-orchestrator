"""Cluster Auto-Scaler for Distributed Task Orchestrator.

Monitors queue backlog and worker utilization:
- Scale-Up: Spawns new dynamic workers when pending queue depth exceeds threshold.
- Scale-Down: Gracefully shuts down and removes idle dynamic workers when queue is drained.
"""

import time
from typing import Dict, Optional, Callable
from core.models import WorkerStatus
from core.coordinator import Coordinator
from core.worker import WorkerNode


class AutoScaler:
    """Autonomous autoscaler that adjusts the worker fleet based on queue pressure."""

    def __init__(
        self,
        min_workers: int = 3,
        max_workers: int = 8,
        scale_up_threshold: int = 3,
        scale_down_idle_seconds: float = 6.0,
    ):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_idle_seconds = scale_down_idle_seconds
        self.last_scale_action: float = time.time()
        self.scale_cooldown: float = 2.0  # seconds between scale operations

    def evaluate(
        self,
        coordinator: Coordinator,
        worker_nodes: Dict[str, WorkerNode],
        spawn_worker_fn: Callable[[bool], str],
        retire_worker_fn: Callable[[str], None],
    ) -> Optional[str]:
        """Evaluate cluster metrics and trigger scale-up or scale-down if needed."""
        now = time.time()
        if now - self.last_scale_action < self.scale_cooldown:
            return None

        should_scale_up = False
        retire_id: Optional[str] = None
        retire_idle_time = 0.0

        with coordinator._lock:
            # Active and healthy workers
            active_workers = [
                w for w in coordinator.workers.values()
                if w.status == WorkerStatus.ACTIVE
            ]
            current_worker_count = len(active_workers)
            pending_count = len(coordinator.task_queue)

            if pending_count >= self.scale_up_threshold and current_worker_count < self.max_workers:
                should_scale_up = True
            elif pending_count == 0 and current_worker_count > self.min_workers:
                for w in active_workers:
                    if w.is_dynamic and w.current_task_id is None:
                        idle_time = (now - w.idle_since) if w.idle_since else 0.0
                        if idle_time >= self.scale_down_idle_seconds:
                            retire_id = w.id
                            retire_idle_time = idle_time
                            break

        if should_scale_up:
            new_worker_id = spawn_worker_fn(True)  # True = is_dynamic
            self.last_scale_action = now
            msg = (
                f"High queue pressure ({pending_count} pending). "
                f"Scaled UP: spawned '{new_worker_id}' (Fleet: {current_worker_count + 1}/{self.max_workers})"
            )
            coordinator._log_event("AUTOSCALER", msg, level="WARNING")
            return msg

        if retire_id:
            retire_worker_fn(retire_id)
            self.last_scale_action = now
            msg = (
                f"Zero queue pressure & idle for {retire_idle_time:.1f}s. "
                f"Scaled DOWN: retired '{retire_id}' (Fleet: {current_worker_count - 1}/{self.max_workers})"
            )
            coordinator._log_event("AUTOSCALER", msg, level="INFO")
            return msg

        return None
