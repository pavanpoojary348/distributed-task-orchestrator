"""Worker Node implementation for the Distributed Task Orchestrator.

Responsibilities:
- Registers with the coordinator node.
- Sends periodic heartbeat pings (default: every 2 seconds).
- Executes background tasks defined in the task registry.
- Supports chaos testing (crash/kill simulation and recovery).
"""

import time
import threading
from typing import Optional, Any
from core.models import Task, WorkerStatus
from tasks.definitions import TASK_REGISTRY


class WorkerNode:
    """A worker process that consumes and executes tasks."""

    def __init__(
        self,
        worker_id: str,
        coordinator=None,
        heartbeat_interval: float = 2.0,
    ):
        self.worker_id = worker_id
        self.coordinator = coordinator
        self.heartbeat_interval = heartbeat_interval
        self.is_running = True
        self.current_task: Optional[Task] = None

        self._heartbeat_thread: Optional[threading.Thread] = None
        self._stop_event = threading.Event()

    @property
    def active_coordinator(self):
        """Dynamically resolve active coordinator node (supports HA cluster failover)."""
        if self.coordinator is None:
            return None
        return getattr(self.coordinator, "active_coordinator", self.coordinator)

    def start(self):
        """Start worker background threads (heartbeats)."""
        self.is_running = True
        self._stop_event.clear()

        # Register with coordinator
        if self.active_coordinator:
            self.active_coordinator.register_worker(self.worker_id)

        # Start heartbeat background thread
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,
            name=f"heartbeat-{self.worker_id}"
        )
        self._heartbeat_thread.start()

    def stop(self):
        """Gracefully stop worker."""
        self.is_running = False
        self._stop_event.set()
        if self.active_coordinator:
            self.active_coordinator.unregister_worker(self.worker_id)

    def simulate_crash(self):
        """Simulate a sudden crash/hardware failure.

        Stops heartbeats immediately WITHOUT unregistering, causing the
        coordinator to detect the failure via timeout and reassign tasks.
        """
        self.is_running = False
        self._stop_event.set()

    def recover(self):
        """Simulate a crashed node rebooting and rejoining the cluster."""
        self.start()

    def send_heartbeat(self) -> bool:
        """Send a single heartbeat ping to the coordinator."""
        if not self.is_running:
            return False
        if self.active_coordinator:
            return self.active_coordinator.record_heartbeat(self.worker_id)
        return True

    def _heartbeat_loop(self):
        """Loop running in background sending heartbeats at regular intervals."""
        while not self._stop_event.is_set():
            if self.is_running:
                self.send_heartbeat()
            self._stop_event.wait(self.heartbeat_interval)

    def execute_task(self, task: Task) -> Any:
        """Execute a single task using the task registry."""
        if not self.is_running:
            raise RuntimeError(f"Cannot execute task '{task.id}': Worker is offline/dead")

        self.current_task = task
        start_time = time.time()

        try:
            handler = TASK_REGISTRY.get(task.name)
            if not handler:
                raise ValueError(f"Unknown task handler: '{task.name}'")

            # Execute the function
            result = handler(task.payload)
            duration = time.time() - start_time

            # Report success back to coordinator
            if self.active_coordinator:
                self.active_coordinator.complete_task(task.id, result=result, execution_time=duration)

            return result

        except Exception as exc:
            duration = time.time() - start_time
            # Report failure back to coordinator
            if self.active_coordinator:
                self.active_coordinator.fail_task(task.id, error_message=str(exc))
            raise

        finally:
            self.current_task = None
