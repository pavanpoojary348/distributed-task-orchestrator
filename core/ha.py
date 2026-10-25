"""High Availability & Leader Election (Active-Standby Coordinator).

Zero Single Point of Failure (Zero SPOF) for the Distributed Task Orchestrator.
- Manages an Active Leader Coordinator and a Hot Standby Coordinator.
- Implements a heartbeat lease election mechanism (similar to Raft/etcd/ZooKeeper).
- Standby continuously monitors the Leader lease; if the Leader misses heartbeats,
  Standby executes automatic failover and takes over orchestration seamlessly.
- Thread-safe shared state store ensures zero task or worker loss during failover.
"""

import time
import threading
from typing import Dict, List, Optional, Any
from core.models import CoordinatorRole, Task, Worker
from core.coordinator import Coordinator, ClusterState
from core.metrics import MetricsCollector


class LeaderElection:

    """Distributed heartbeat lease and leader election controller."""

    def __init__(self, lease_duration: float = 3.0, current_leader_id: str = "coordinator-primary"):
        self.lease_duration = lease_duration
        self.term: int = 1
        self.current_leader_id: str = current_leader_id
        self.last_renewed_at: float = time.time()
        self.lease_expires_at: float = time.time() + lease_duration
        self._lock = threading.RLock()

    def renew_lease(self, leader_id: str) -> bool:
        """Called periodically by the active leader to renew its lease."""
        with self._lock:
            if leader_id == self.current_leader_id:
                now = time.time()
                self.last_renewed_at = now
                self.lease_expires_at = now + self.lease_duration
                return True
            return False

    def check_lease_and_elect(self, candidate_id: str) -> bool:
        """Standby checks if the incumbent leader's lease expired. If so, elect candidate."""
        with self._lock:
            now = time.time()
            if now > self.lease_expires_at:
                # Incumbent leader lease expired! Trigger failover election
                self.term += 1
                self.current_leader_id = candidate_id
                self.last_renewed_at = now
                self.lease_expires_at = now + self.lease_duration
                return True
            return False

    def force_failover(self, target_leader_id: str) -> bool:
        """Manually trigger immediate graceful failover to the target coordinator."""
        with self._lock:
            now = time.time()
            self.term += 1
            self.current_leader_id = target_leader_id
            self.last_renewed_at = now
            self.lease_expires_at = now + self.lease_duration
            return True


class HACoordinator:
    """High Availability Coordinator Group managing Primary and Hot Standby nodes."""

    def __init__(
        self,
        cluster_id: str = "cluster-production-01",
        lease_duration: float = 3.0,
        heartbeat_timeout: float = 6.0,
        suspected_timeout: float = 3.5,
        base_retry_delay: float = 2.0,
        retry_jitter: float = 0.5,
    ):
        self.cluster_id = cluster_id
        self.lease_duration = lease_duration
        self._lock = threading.RLock()

        # Centralized shared state store across both coordinators
        self.shared_state = ClusterState()

        # Centralized performance metrics & throughput time-series collector
        self.metrics_collector = MetricsCollector(history_seconds=30)

        # Primary and Standby Coordinator instances sharing state
        self.primary = Coordinator(
            cluster_id=cluster_id,
            node_id="coordinator-primary",
            role=CoordinatorRole.LEADER,
            heartbeat_timeout=heartbeat_timeout,
            suspected_timeout=suspected_timeout,
            base_retry_delay=base_retry_delay,
            retry_jitter=retry_jitter,
            shared_state=self.shared_state,
            metrics_collector=self.metrics_collector,
        )

        self.standby = Coordinator(
            cluster_id=cluster_id,
            node_id="coordinator-standby",
            role=CoordinatorRole.STANDBY,
            heartbeat_timeout=heartbeat_timeout,
            suspected_timeout=suspected_timeout,
            base_retry_delay=base_retry_delay,
            retry_jitter=retry_jitter,
            shared_state=self.shared_state,
            metrics_collector=self.metrics_collector,
        )


        self.elector = LeaderElection(
            lease_duration=lease_duration,
            current_leader_id="coordinator-primary"
        )

        self._nodes: Dict[str, Coordinator] = {
            "coordinator-primary": self.primary,
            "coordinator-standby": self.standby,
        }

    # -------------------------------------------------------------------------
    # Node & Role Resolution
    # -------------------------------------------------------------------------

    @property
    def active_coordinator(self) -> Coordinator:
        """Resolve the currently elected active leader coordinator."""
        with self._lock:
            leader_id = self.elector.current_leader_id
            return self._nodes.get(leader_id, self.primary)

    @property
    def standby_coordinator(self) -> Coordinator:
        """Resolve the current standby coordinator."""
        with self._lock:
            leader_id = self.elector.current_leader_id
            if leader_id == "coordinator-primary":
                return self.standby
            return self.primary

    # -------------------------------------------------------------------------
    # Shared State Passthroughs (For Drop-in Compatibility)
    # -------------------------------------------------------------------------

    @property
    def tasks(self) -> Dict[str, Task]:
        return self.shared_state.tasks

    @property
    def task_queue(self) -> List[str]:
        return self.shared_state.task_queue

    @property
    def dead_letter_queue(self) -> List[str]:
        return self.shared_state.dead_letter_queue

    @property
    def workers(self) -> Dict[str, Worker]:
        return self.shared_state.workers

    @property
    def event_log(self) -> List[Dict[str, Any]]:
        return self.shared_state.event_log

    # -------------------------------------------------------------------------
    # HA Heartbeat Lease Evaluation & Failover
    # -------------------------------------------------------------------------

    def evaluate_leadership(self) -> Optional[str]:
        """Called every tick in the background orchestrator loop.

        1. Active leader renews its lease if healthy.
        2. If active leader missed heartbeats / crashed, standby detects expiry
           and self-promotes to active leader.
        """
        with self._lock:
            active = self.active_coordinator
            standby = self.standby_coordinator

            # 1. If active leader is alive, renew lease
            if active.is_alive and active.role == CoordinatorRole.LEADER:
                self.elector.renew_lease(active.node_id)
                return None

            # 2. If active leader is dead / stopped renewing, standby evaluates lease
            if standby.is_alive:
                elected = self.elector.check_lease_and_elect(standby.node_id)
                if elected:
                    standby.role = CoordinatorRole.LEADER
                    if not active.is_alive:
                        active.role = CoordinatorRole.DEAD
                    else:
                        active.role = CoordinatorRole.STANDBY

                    msg = (
                        f"👑 LEADER PROMOTION: Leader lease expired for '{active.node_id}'. "
                        f"'{standby.node_id}' elected as new ACTIVE LEADER (Term #{self.elector.term})!"
                    )
                    standby._log_event(
                        standby.node_id,
                        msg,
                        level="WARNING"
                    )
                    return msg

            return None

    # -------------------------------------------------------------------------
    # Chaos Simulation & Manual Control
    # -------------------------------------------------------------------------

    def simulate_crash_leader(self) -> str:
        """Simulate an abrupt hardware/process crash of the active leader."""
        with self._lock:
            active = self.active_coordinator
            active.is_alive = False
            active.role = CoordinatorRole.DEAD
            # Expire lease immediately so standby detects failure instantly
            self.elector.lease_expires_at = time.time() - 0.1

            msg = f"💀 Leader '{active.node_id}' CRASHED! Heartbeat lease stopped."
            self.shared_state.event_log.append({
                "timestamp": time.time(),
                "time_str": time.strftime("%H:%M:%S"),
                "source": "CHAOS",
                "message": msg,
                "level": "ERROR",
            })
            return msg

    def recover_coordinator(self, node_id: Optional[str] = None) -> str:
        """Reboot a crashed coordinator node. Reconnects as STANDBY (Zero Split-Brain)."""
        with self._lock:
            if not node_id:
                # Find first dead node
                for nid, node in self._nodes.items():
                    if not node.is_alive or node.role == CoordinatorRole.DEAD:
                        node_id = nid
                        break

            if not node_id or node_id not in self._nodes:
                return "No crashed coordinator to recover."

            node = self._nodes[node_id]
            node.is_alive = True
            # Rejoin safely as STANDBY if the other node is currently LEADER
            if self.elector.current_leader_id != node_id:
                node.role = CoordinatorRole.STANDBY
            else:
                node.role = CoordinatorRole.LEADER

            msg = f"🔄 Coordinator '{node_id}' rebooted and rejoined cluster as [{node.role.value}]."
            node._log_event(node_id, msg, level="INFO")
            return msg

    def trigger_manual_failover(self) -> str:
        """Manually trigger a graceful failover to the standby coordinator."""
        with self._lock:
            current = self.active_coordinator
            target = self.standby_coordinator

            if not target.is_alive:
                raise RuntimeError(f"Cannot failover to '{target.node_id}': Standby node is dead/offline.")

            self.elector.force_failover(target.node_id)
            target.role = CoordinatorRole.LEADER
            if current.is_alive:
                current.role = CoordinatorRole.STANDBY

            msg = (
                f"🔀 MANUAL FAILOVER: Orchestrator leadership shifted gracefully from "
                f"'{current.node_id}' to '{target.node_id}' (Term #{self.elector.term})."
            )
            target._log_event(target.node_id, msg, level="INFO")
            return msg

    def get_ha_stats(self) -> Dict[str, Any]:
        """Telemetry snapshot of high availability and leader election state."""
        with self._lock:
            now = time.time()
            remaining = max(0.0, round(self.elector.lease_expires_at - now, 1))
            return {
                "leader_id": self.elector.current_leader_id,
                "term": self.elector.term,
                "lease_remaining": remaining,
                "lease_duration": self.elector.lease_duration,
                "primary": {
                    "node_id": self.primary.node_id,
                    "role": self.primary.role.value,
                    "is_alive": self.primary.is_alive,
                },
                "standby": {
                    "node_id": self.standby.node_id,
                    "role": self.standby.role.value,
                    "is_alive": self.standby.is_alive,
                }
            }

    # -------------------------------------------------------------------------
    # Delegated Coordinator API Methods
    # -------------------------------------------------------------------------

    def submit_task(self, *args, **kwargs) -> Task:
        return self.active_coordinator.submit_task(*args, **kwargs)

    def get_task(self, task_id: str) -> Optional[Task]:
        return self.active_coordinator.get_task(task_id)

    def dispatch_tasks(self) -> List[Dict[str, str]]:
        return self.active_coordinator.dispatch_tasks()

    def detect_failures(self) -> List[str]:
        return self.active_coordinator.detect_failures()

    def register_worker(self, *args, **kwargs) -> Worker:
        return self.active_coordinator.register_worker(*args, **kwargs)

    def unregister_worker(self, *args, **kwargs):
        return self.active_coordinator.unregister_worker(*args, **kwargs)

    def record_heartbeat(self, *args, **kwargs) -> bool:
        return self.active_coordinator.record_heartbeat(*args, **kwargs)

    def complete_task(self, *args, **kwargs) -> bool:
        return self.active_coordinator.complete_task(*args, **kwargs)

    def fail_task(self, *args, **kwargs) -> bool:
        return self.active_coordinator.fail_task(*args, **kwargs)

    def get_cluster_stats(self) -> Dict[str, Any]:
        stats = self.active_coordinator.get_cluster_stats()
        stats["ha"] = self.get_ha_stats()
        stats["metrics"] = self.metrics_collector.get_metrics()
        return stats

