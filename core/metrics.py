"""Real-Time Performance Metrics & Throughput Analytics.

Collects rolling time-window telemetry:
- Real-time throughput (Tasks per second / Tasks per minute)
- Task execution latency (Average & P95 in milliseconds)
- Cluster worker utilization percentage
- Task success vs failure rates
- Rolling 30-second time-series history for interactive dashboard charts
"""

import time
import threading
from collections import deque
from typing import Dict, List, Optional, Any


class MetricsCollector:
    """Thread-safe performance metrics and throughput time-series collector."""

    def __init__(self, history_seconds: int = 30):
        self.history_seconds = history_seconds
        self._lock = threading.RLock()

        # Rolling circular buffers
        self.completed_events: deque = deque(maxlen=1000)   # timestamps of completed tasks
        self.failed_events: deque = deque(maxlen=1000)      # timestamps of failed tasks
        self.latencies: deque = deque(maxlen=200)           # execution durations in seconds

        # Time-series rolling history for charts (last N intervals)
        self.history: deque = deque(maxlen=history_seconds)

    def record_completion(self, execution_time: Optional[float] = None):
        """Record a successful task completion with execution duration."""
        now = time.time()
        with self._lock:
            self.completed_events.append(now)
            if execution_time is not None and execution_time > 0:
                self.latencies.append(execution_time)

    def record_failure(self):
        """Record a failed task execution."""
        now = time.time()
        with self._lock:
            self.failed_events.append(now)

    def sample(self, coordinator, worker_nodes: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Compute instantaneous metrics snapshot and append to rolling history."""
        now = time.time()
        with self._lock:
            # 1. Throughput calculations
            completed_5s = sum(1 for t in self.completed_events if now - t <= 5.0)
            throughput_tps = round(completed_5s / 5.0, 2)

            completed_60s = sum(1 for t in self.completed_events if now - t <= 60.0)
            throughput_tpm = completed_60s

            # 2. Latency calculations (in milliseconds)
            if self.latencies:
                avg_latency_ms = round((sum(self.latencies) / len(self.latencies)) * 1000.0, 1)
                sorted_lats = sorted(self.latencies)
                p95_idx = min(int(len(sorted_lats) * 0.95), len(sorted_lats) - 1)
                p95_latency_ms = round(sorted_lats[p95_idx] * 1000.0, 1)
            else:
                avg_latency_ms = 0.0
                p95_latency_ms = 0.0

            # 3. Success Rate
            total_done = len(self.completed_events)
            total_failed = len(self.failed_events)
            total_outcomes = total_done + total_failed
            if total_outcomes > 0:
                success_rate_pct = round((total_done / total_outcomes) * 100.0, 1)
            else:
                success_rate_pct = 100.0

            # 4. Worker Utilization
            total_workers = 0
            busy_workers = 0
            if worker_nodes:
                total_workers = len(worker_nodes)
                busy_workers = sum(1 for w in worker_nodes.values() if getattr(w, "current_task", None) is not None)
            elif hasattr(coordinator, "workers"):
                total_workers = len(coordinator.workers)
                busy_workers = sum(1 for w in coordinator.workers.values() if w.current_task_id is not None)

            utilization_pct = round((busy_workers / total_workers * 100.0), 1) if total_workers > 0 else 0.0

            # 5. Queue depth & in-flight count
            pending_count = len(getattr(coordinator, "task_queue", []))
            tasks_dict = getattr(coordinator, "tasks", {})
            in_progress_count = sum(
                1 for t in tasks_dict.values()
                if getattr(t, "status", None) == "IN_PROGRESS" or (isinstance(t, dict) and t.get("status") == "IN_PROGRESS")
            )

            snapshot = {
                "timestamp": now,
                "time_str": time.strftime("%H:%M:%S", time.localtime(now)),
                "throughput_tps": throughput_tps,
                "throughput_tpm": throughput_tpm,
                "queue_depth": pending_count,
                "in_progress": in_progress_count,
                "avg_latency_ms": avg_latency_ms,
                "p95_latency_ms": p95_latency_ms,
                "utilization_pct": utilization_pct,
                "success_rate_pct": success_rate_pct,
                "busy_workers": busy_workers,
                "total_workers": total_workers,
            }

            self.history.append(snapshot)

            return {
                "summary": snapshot,
                "history": list(self.history),
            }

    def get_metrics(self) -> Dict[str, Any]:
        """Return current summary metrics and rolling history."""
        with self._lock:
            latest = self.history[-1] if self.history else {
                "timestamp": time.time(),
                "time_str": time.strftime("%H:%M:%S"),
                "throughput_tps": 0.0,
                "throughput_tpm": 0,
                "queue_depth": 0,
                "in_progress": 0,
                "avg_latency_ms": 0.0,
                "p95_latency_ms": 0.0,
                "utilization_pct": 0.0,
                "success_rate_pct": 100.0,
                "busy_workers": 0,
                "total_workers": 0,
            }
            return {
                "summary": latest,
                "history": list(self.history),
            }
