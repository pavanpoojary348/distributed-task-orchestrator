"""Coordinator (Leader) Node for the Distributed Task Orchestrator.

Responsibilities:
- Manages cluster topology and worker health via heartbeats.
- Detects worker crashes (timeouts) and reassigns orphaned tasks.
- Manages the task lifecycle, task queue, and Dead Letter Queue (DLQ).
- Dispatches pending tasks to available healthy workers.
- Thread-safe for concurrent operations.
"""

import time
import threading
import random
from typing import Dict, List, Optional, Any
from core.models import Task, TaskPriority, TaskStatus, Worker, WorkerStatus, CoordinatorRole


def sanitize_error_message(text: str) -> str:
    """Sanitize error messages and logs to prevent leaking filesystem paths or PII."""
    if not text:
        return text
    import re
    # Mask Windows C:\Users\xxx and Linux /home/xxx paths
    text = re.sub(r'[A-Za-z]:\\[Uu]sers\\[^\\]+', r'[REDACTED_PATH]', text)
    text = re.sub(r'/home/[^/]+', r'[REDACTED_PATH]', text)
    # Mask authorization token patterns if present
    text = re.sub(r'(bearer\s+)[A-Za-z0-9\-_\.]+', r'\1[REDACTED]', text, flags=re.IGNORECASE)
    return text


class ClusterState:
    """Thread-safe centralized state store shared across HA coordinator nodes."""
    def __init__(self):
        self.lock = threading.RLock()
        self.workers: Dict[str, Worker] = {}
        self.tasks: Dict[str, Task] = {}
        self.task_queue: List[str] = []         # IDs of PENDING tasks
        self.dead_letter_queue: List[str] = []   # IDs of DEAD_LETTER tasks
        self.event_log: List[Dict[str, Any]] = []


class Coordinator:
    """The central leader node orchestrating cluster workers and tasks."""

    def __init__(
        self,
        cluster_id: str = "cluster-01",
        node_id: str = "coordinator-primary",
        role: CoordinatorRole = CoordinatorRole.LEADER,
        heartbeat_timeout: float = 6.0,
        suspected_timeout: float = 3.5,
        base_retry_delay: float = 2.0,
        retry_jitter: float = 0.5,
        shared_state: Optional[ClusterState] = None,
        metrics_collector: Optional[Any] = None,
        max_task_history: int = 1000,
    ):
        self.cluster_id = cluster_id
        self.node_id = node_id
        self.role = role
        self.is_alive = True
        self.heartbeat_timeout = heartbeat_timeout
        self.suspected_timeout = suspected_timeout
        self.base_retry_delay = base_retry_delay
        self.retry_jitter = retry_jitter
        self.metrics_collector = metrics_collector
        self.max_task_history = max_task_history

        # Thread-safe storage (either shared with HA standby or locally initialized)
        if shared_state is not None:
            self.shared_state = shared_state
            self._lock = shared_state.lock
            self.workers = shared_state.workers
            self.tasks = shared_state.tasks
            self.task_queue = shared_state.task_queue
            self.dead_letter_queue = shared_state.dead_letter_queue
            self.event_log = shared_state.event_log
        else:
            self.shared_state = None
            self._lock = threading.RLock()
            self.workers = {}
            self.tasks = {}
            self.task_queue = []
            self.dead_letter_queue = []
            self.event_log = []

        self._log_event(self.node_id, f"Coordinator '{self.node_id}' initialized in role [{self.role.value}]")

    @property
    def active_coordinator(self):
        """Self-resolving property for unified interface compatibility."""
        return self

    def _prune_task_history(self):
        """Prune old terminal tasks (COMPLETED or DEAD_LETTER) to prevent memory leaks."""
        if len(self.tasks) <= self.max_task_history:
            return
        # Find terminal tasks sorted by created_at ascending
        terminal_tasks = [
            t for t in self.tasks.values()
            if t.status in (TaskStatus.COMPLETED, TaskStatus.DEAD_LETTER, TaskStatus.CANCELLED)
        ]
        terminal_tasks.sort(key=lambda x: x.created_at)
        excess = len(self.tasks) - self.max_task_history
        for t in terminal_tasks[:excess]:
            self.tasks.pop(t.id, None)
            if t.id in self.dead_letter_queue:
                try:
                    self.dead_letter_queue.remove(t.id)
                except ValueError:
                    pass

    def _log_event(self, source: str, message: str, level: str = "INFO"):
        """Append an event to the internal audit log with sanitized contents."""
        clean_msg = sanitize_error_message(message)
        event = {
            "timestamp": time.time(),
            "time_str": time.strftime("%H:%M:%S"),
            "source": source,
            "message": clean_msg,
            "level": level,
        }
        self.event_log.append(event)
        # Keep log trimmed to last 200 events
        if len(self.event_log) > 200:
            del self.event_log[:-200]

    # -------------------------------------------------------------------------
    # Worker Management & Heartbeats
    # -------------------------------------------------------------------------

    def register_worker(self, worker_id: str, is_dynamic: bool = False) -> Worker:
        """Register a new worker node in the cluster."""
        with self._lock:
            if worker_id in self.workers:
                worker = self.workers[worker_id]
                worker.status = WorkerStatus.ACTIVE
                worker.last_heartbeat = time.time()
                worker.is_dynamic = is_dynamic
                self._log_event(worker_id, f"Worker '{worker_id}' reconnected")
                return worker

            worker = Worker(
                id=worker_id,
                last_heartbeat=time.time(),
                is_dynamic=is_dynamic,
                idle_since=time.time()
            )
            self.workers[worker_id] = worker
            self._log_event(
                worker_id,
                f"Worker '{worker_id}' registered successfully{' [AUTOSCALED]' if is_dynamic else ''}"
            )
            return worker

    def unregister_worker(self, worker_id: str):
        """Unregister a worker gracefully."""
        with self._lock:
            if worker_id in self.workers:
                worker = self.workers[worker_id]
                # If worker was holding a task, put it back in queue
                if worker.current_task_id:
                    self._reclaim_orphaned_task(worker.current_task_id, reason="Worker graceful shutdown")
                del self.workers[worker_id]
                self._log_event(worker_id, f"Worker '{worker_id}' gracefully removed")

    def record_heartbeat(self, worker_id: str) -> bool:
        """Record a heartbeat ping from an active worker."""
        with self._lock:
            if worker_id not in self.workers:
                # Auto-register if not known
                self.register_worker(worker_id)

            worker = self.workers[worker_id]
            was_dead = worker.status == WorkerStatus.DEAD
            worker.last_heartbeat = time.time()
            worker.status = WorkerStatus.ACTIVE

            if was_dead:
                self._log_event(worker_id, f"Worker '{worker_id}' recovered from DEAD state", level="WARNING")
            return True

    def detect_failures(self) -> List[str]:
        """Scan all workers and detect timed-out / dead nodes. Reassign tasks."""
        now = time.time()
        dead_workers: List[str] = []

        with self._lock:
            for worker_id, worker in self.workers.items():
                elapsed = now - worker.last_heartbeat

                # Complete failure timeout
                if elapsed > self.heartbeat_timeout:
                    if worker.status != WorkerStatus.DEAD:
                        worker.status = WorkerStatus.DEAD
                        dead_workers.append(worker_id)
                        self._log_event(
                            worker_id,
                            f"Worker '{worker_id}' DEAD (no heartbeat for {elapsed:.1f}s)",
                            level="ERROR"
                        )
                        # Reassign any task it was working on
                        if worker.current_task_id:
                            self._reclaim_orphaned_task(
                                worker.current_task_id,
                                reason=f"Worker '{worker_id}' crashed/died"
                            )
                            worker.current_task_id = None

                # Suspected failure threshold
                elif elapsed > self.suspected_timeout:
                    if worker.status == WorkerStatus.ACTIVE:
                        worker.status = WorkerStatus.SUSPECTED_DEAD
                        self._log_event(
                            worker_id,
                            f"Worker '{worker_id}' SUSPECTED DEAD (missed ping, {elapsed:.1f}s elapsed)",
                            level="WARNING"
                        )

        return dead_workers

    # -------------------------------------------------------------------------
    # Task Submission & Scheduling
    # -------------------------------------------------------------------------

    def _enqueue_task(self, task_id: str):
        """Enqueue task into priority queue sorted by (priority.value, created_at)."""
        if task_id not in self.task_queue:
            self.task_queue.append(task_id)
        # Sort queue: lowest priority integer first (e.g. CRITICAL=1), then earliest created_at (FIFO)
        self.task_queue.sort(
            key=lambda tid: (
                self.tasks[tid].priority.value if tid in self.tasks else 999,
                self.tasks[tid].created_at if tid in self.tasks else 0,
            )
        )

    def submit_task(
        self,
        name: str,
        payload: Optional[dict] = None,
        priority: TaskPriority = TaskPriority.NORMAL,
        max_retries: int = 3,
    ) -> Task:
        """Submit a new background task to the cluster queue."""
        task = Task(
            name=name,
            payload=payload or {},
            priority=priority,
            max_retries=max_retries,
            status=TaskStatus.PENDING,
        )
        with self._lock:
            self.tasks[task.id] = task
            self._enqueue_task(task.id)
            self._log_event(
                "DISPATCHER",
                f"Task '{task.name}' ({task.id[:8]}) [{task.priority.name}] queued"
            )
        return task

    def get_task(self, task_id: str) -> Optional[Task]:
        """Fetch a task by ID."""
        with self._lock:
            return self.tasks.get(task_id)

    def cancel_task(self, task_id: str) -> bool:
        """Cancel a pending task before it is dispatched to a worker."""
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                return False
            if task.status != TaskStatus.PENDING:
                # Cannot cancel tasks that are already executing, completed, or dead
                return False

            if task_id in self.task_queue:
                self.task_queue.remove(task_id)

            task.status = TaskStatus.CANCELLED
            task.next_retry_at = None
            task.updated_at = time.time()
            self._log_event(
                "OPERATOR",
                f"Task '{task.name}' ({task.id[:8]}) CANCELLED by operator",
                level="WARNING"
            )
            self._prune_task_history()
            return True

    def retry_task(self, task_id: str) -> bool:
        """Re-drive a dead-letter or failed/cancelled task back to the pending queue."""
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                return False
            if task.status not in (TaskStatus.DEAD_LETTER, TaskStatus.FAILED, TaskStatus.CANCELLED):
                return False

            # Remove from DLQ list if present
            if task_id in self.dead_letter_queue:
                self.dead_letter_queue.remove(task_id)

            # Reset task state for re-execution
            task.status = TaskStatus.PENDING
            task.retry_count = 0
            task.assigned_worker = None
            task.next_retry_at = None
            task.error = None
            task.result = None
            task.execution_time = None
            task.updated_at = time.time()

            self._enqueue_task(task.id)
            self._log_event(
                "OPERATOR",
                f"Task '{task.name}' ({task.id[:8]}) re-driven to queue [PENDING]",
                level="INFO"
            )
            return True

    def retry_all_dlq(self) -> int:
        """Re-drive all tasks currently parked in the Dead Letter Queue."""
        with self._lock:
            dlq_ids = list(self.dead_letter_queue)
            count = 0
            for task_id in dlq_ids:
                if self.retry_task(task_id):
                    count += 1
            if count > 0:
                self._log_event(
                    "OPERATOR",
                    f"Re-driven {count} tasks from DLQ to queue",
                    level="INFO"
                )
            return count

    def dispatch_tasks(self) -> List[Dict[str, str]]:
        """Dispatch pending tasks to available idle workers, respecting backoff timers."""
        assignments: List[Dict[str, str]] = []
        now = time.time()

        with self._lock:
            # Find all healthy workers that are not currently executing a task
            idle_workers = [
                w for w in self.workers.values()
                if w.status == WorkerStatus.ACTIVE and w.current_task_id is None
            ]

            i = 0
            while idle_workers and i < len(self.task_queue):
                task_id = self.task_queue[i]
                task = self.tasks.get(task_id)

                if not task:
                    self.task_queue.pop(i)
                    continue

                # If task is cooling down under exponential backoff, skip it
                if task.next_retry_at and now < task.next_retry_at:
                    i += 1
                    continue

                # Eligible task found! Pop it from queue
                self.task_queue.pop(i)
                worker = idle_workers.pop(0)
                task.status = TaskStatus.IN_PROGRESS
                task.assigned_worker = worker.id
                task.updated_at = now

                worker.current_task_id = task.id
                worker.idle_since = None

                assignments.append({"task_id": task.id, "worker_id": worker.id})
                self._log_event(
                    "DISPATCHER",
                    f"Assigned task '{task.name}' ({task.id[:8]}) to '{worker.id}'"
                )

        return assignments

    # -------------------------------------------------------------------------
    # Task Completion & Failure Handlers
    # -------------------------------------------------------------------------

    def complete_task(self, task_id: str, result: Any = None, execution_time: Optional[float] = None) -> bool:
        """Called when a worker finishes a task successfully."""
        now = time.time()
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                return False

            task.status = TaskStatus.COMPLETED
            task.result = result
            task.execution_time = execution_time
            task.next_retry_at = None
            task.updated_at = now

            # Free up the worker
            if task.assigned_worker and task.assigned_worker in self.workers:
                worker = self.workers[task.assigned_worker]
                if worker.current_task_id == task_id:
                    worker.current_task_id = None
                    worker.idle_since = now
                worker.completed_tasks += 1

            self._log_event(
                task.assigned_worker or "UNKNOWN",
                f"Task '{task.name}' ({task.id[:8]}) COMPLETED in {execution_time or 0:.2f}s",
                level="SUCCESS"
            )
            if self.metrics_collector:
                self.metrics_collector.record_completion(execution_time=execution_time)
            self._prune_task_history()
            return True

    def fail_task(self, task_id: str, error_message: str) -> bool:
        """Called when a task execution raises an error."""
        now = time.time()
        with self._lock:
            task = self.tasks.get(task_id)
            if not task:
                return False

            if self.metrics_collector:
                self.metrics_collector.record_failure()

            clean_error = sanitize_error_message(error_message)
            task.error = clean_error

            task.updated_at = now
            worker_id = task.assigned_worker

            # Free up the worker
            if worker_id and worker_id in self.workers:
                worker = self.workers[worker_id]
                if worker.current_task_id == task_id:
                    worker.current_task_id = None
                    worker.idle_since = now
                worker.failed_tasks += 1

            # Check retry logic
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.PENDING
                task.assigned_worker = None

                # Exponential backoff formula with jitter: base * 2^(retry-1) + jitter
                backoff = (self.base_retry_delay * (2 ** (task.retry_count - 1))) + random.uniform(0, self.retry_jitter)
                task.next_retry_at = time.time() + backoff

                # Push back to priority queue for retry
                self._enqueue_task(task.id)
                self._log_event(
                    worker_id or "COORDINATOR",
                    f"Task '{task.name}' ({task.id[:8]}) FAILED. Backing off for {backoff:.1f}s before retry ({task.retry_count}/{task.max_retries}): {clean_error}",
                    level="WARNING"
                )
            else:
                # Retries exhausted -> route to Dead Letter Queue
                task.status = TaskStatus.DEAD_LETTER
                task.next_retry_at = None
                if task.id not in self.dead_letter_queue:
                    self.dead_letter_queue.append(task.id)
                self._log_event(
                    worker_id or "COORDINATOR",
                    f"Task '{task.name}' ({task.id[:8]}) moved to DEAD LETTER QUEUE (exhausted {task.max_retries} retries)",
                    level="ERROR"
                )

            self._prune_task_history()
            return True

    def _reclaim_orphaned_task(self, task_id: str, reason: str):
        """Reassigns an unfinished task left behind by a failed worker."""
        task = self.tasks.get(task_id)
        if not task:
            return

        if task.status == TaskStatus.IN_PROGRESS:
            if task.retry_count < task.max_retries:
                task.retry_count += 1
                task.status = TaskStatus.PENDING
                task.assigned_worker = None
                task.next_retry_at = None  # Reset backoff so reclaimed tasks are dispatched immediately
                task.updated_at = time.time()
                # Prioritize reassignment in priority queue
                self._enqueue_task(task.id)
                self._log_event(
                    "COORDINATOR",
                    f"Reclaimed orphaned task '{task.name}' ({task.id[:8]}) -> Re-queued ({reason})",
                    level="WARNING"
                )
            else:
                task.status = TaskStatus.DEAD_LETTER
                task.next_retry_at = None
                if task.id not in self.dead_letter_queue:
                    self.dead_letter_queue.append(task.id)
                self._log_event(
                    "COORDINATOR",
                    f"Orphaned task '{task.name}' ({task.id[:8]}) sent to DLQ (retries exhausted)",
                    level="ERROR"
                )

    # -------------------------------------------------------------------------
    # Cluster Telemetry & Stats
    # -------------------------------------------------------------------------

    def get_cluster_stats(self) -> Dict[str, Any]:
        """Return a snapshot of cluster health and throughput metrics."""
        with self._lock:
            total_tasks = len(self.tasks)
            completed_tasks = sum(1 for t in self.tasks.values() if t.status == TaskStatus.COMPLETED)
            failed_tasks = sum(1 for t in self.tasks.values() if t.status == TaskStatus.FAILED)
            in_progress_tasks = sum(1 for t in self.tasks.values() if t.status == TaskStatus.IN_PROGRESS)
            dlq_count = len(self.dead_letter_queue)
            pending_count = len(self.task_queue)

            active_workers = sum(1 for w in self.workers.values() if w.status == WorkerStatus.ACTIVE)
            suspected_workers = sum(1 for w in self.workers.values() if w.status == WorkerStatus.SUSPECTED_DEAD)
            dead_workers = sum(1 for w in self.workers.values() if w.status == WorkerStatus.DEAD)

            return {
                "cluster_id": self.cluster_id,
                "workers": {
                    "total": len(self.workers),
                    "active": active_workers,
                    "suspected": suspected_workers,
                    "dead": dead_workers,
                    "list": [w.model_dump() for w in self.workers.values()]
                },
                "tasks": {
                    "total": total_tasks,
                    "pending": pending_count,
                    "in_progress": in_progress_tasks,
                    "completed": completed_tasks,
                    "failed": failed_tasks,
                    "dlq": dlq_count,
                },
                "recent_events": list(reversed(self.event_log[-20:])),
            }
