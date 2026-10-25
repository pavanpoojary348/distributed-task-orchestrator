"""Unit tests for High Availability, Standby Coordinator, and Leader Election."""

import time
import pytest
from core.models import CoordinatorRole, TaskPriority, TaskStatus
from core.ha import HACoordinator, LeaderElection
from core.worker import WorkerNode


def test_leader_election_lease_renew():
    """Verify that the incumbent leader can renew its lease before expiry."""
    elector = LeaderElection(lease_duration=2.0, current_leader_id="coordinator-primary")
    assert elector.term == 1
    assert elector.current_leader_id == "coordinator-primary"

    # Active leader renews lease
    renewed = elector.renew_lease("coordinator-primary")
    assert renewed is True
    assert elector.lease_expires_at > time.time()

    # Non-leader cannot renew
    assert elector.renew_lease("coordinator-standby") is False


def test_leader_election_failover_on_expired_lease():
    """Verify that standby wins election when active leader lease expires."""
    elector = LeaderElection(lease_duration=0.2, current_leader_id="coordinator-primary")
    
    # Not expired yet
    assert elector.check_lease_and_elect("coordinator-standby") is False

    # Wait for lease to expire
    time.sleep(0.25)

    # Standby checks and claims election
    won = elector.check_lease_and_elect("coordinator-standby")
    assert won is True
    assert elector.term == 2
    assert elector.current_leader_id == "coordinator-standby"


def test_ha_coordinator_initial_state():
    """Verify initial state of HA cluster has Primary as LEADER and Standby as STANDBY."""
    ha = HACoordinator(cluster_id="test-ha-cluster", lease_duration=2.0)
    assert ha.active_coordinator.node_id == "coordinator-primary"
    assert ha.active_coordinator.role == CoordinatorRole.LEADER
    assert ha.standby_coordinator.node_id == "coordinator-standby"
    assert ha.standby_coordinator.role == CoordinatorRole.STANDBY
    assert ha.elector.term == 1


def test_ha_automatic_failover_when_leader_crashes():
    """Verify that crashing primary causes standby to take over as LEADER."""
    ha = HACoordinator(cluster_id="test-ha-cluster", lease_duration=0.5)

    # Crash the primary
    ha.simulate_crash_leader()
    assert ha.primary.is_alive is False
    assert ha.primary.role == CoordinatorRole.DEAD

    # Standby evaluates leadership lease
    announcement = ha.evaluate_leadership()
    assert announcement is not None
    assert "elected as new ACTIVE LEADER" in announcement

    # Standby is now the active leader
    assert ha.active_coordinator.node_id == "coordinator-standby"
    assert ha.active_coordinator.role == CoordinatorRole.LEADER
    assert ha.elector.term == 2


def test_ha_recovering_node_rejoins_as_standby():
    """Verify that rebooting a dead primary joins as STANDBY (prevents split-brain)."""
    ha = HACoordinator(cluster_id="test-ha-cluster", lease_duration=0.5)
    ha.simulate_crash_leader()
    ha.evaluate_leadership()

    assert ha.active_coordinator.node_id == "coordinator-standby"

    # Recover the primary
    msg = ha.recover_coordinator("coordinator-primary")
    assert "STANDBY" in msg
    assert ha.primary.is_alive is True
    assert ha.primary.role == CoordinatorRole.STANDBY

    # Incumbent leader remains standby coordinator
    assert ha.active_coordinator.node_id == "coordinator-standby"


def test_ha_zero_task_loss_during_failover():
    """Verify that tasks submitted before leader crash are preserved and dispatched by standby."""
    ha = HACoordinator(cluster_id="test-ha-cluster", lease_duration=0.5)

    # Submit task to primary leader
    task = ha.submit_task("resize_image", {"filename": "hero.png"}, priority=TaskPriority.CRITICAL)
    assert task.id in ha.task_queue

    # Register a worker with HA cluster
    worker = WorkerNode(worker_id="worker-ha-01", coordinator=ha)
    ha.register_worker("worker-ha-01")

    # Crash the primary leader
    ha.simulate_crash_leader()
    ha.evaluate_leadership()

    # Verify standby has assumed leadership
    assert ha.active_coordinator.node_id == "coordinator-standby"

    # Standby dispatches the task that was originally queued by the primary!
    assignments = ha.dispatch_tasks()
    assert len(assignments) == 1
    assert assignments[0]["task_id"] == task.id
    assert assignments[0]["worker_id"] == "worker-ha-01"

    # Worker executes task and reports to HA cluster
    worker.execute_task(task)

    # Task is completed without any data loss!
    finished_task = ha.get_task(task.id)
    assert finished_task.status == TaskStatus.COMPLETED


def test_ha_manual_failover():
    """Verify operator-triggered graceful failover."""
    ha = HACoordinator(cluster_id="test-ha-cluster", lease_duration=2.0)
    assert ha.active_coordinator.node_id == "coordinator-primary"

    msg = ha.trigger_manual_failover()
    assert "MANUAL FAILOVER" in msg
    assert ha.active_coordinator.node_id == "coordinator-standby"
    assert ha.standby_coordinator.node_id == "coordinator-primary"
    assert ha.elector.term == 2
