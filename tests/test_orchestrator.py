"""Comprehensive unit and integration tests for the Distributed Task Orchestrator."""

import time
import pytest
from core.models import Task, TaskPriority, TaskStatus, Worker, WorkerStatus
from core.coordinator import Coordinator
from core.worker import WorkerNode


def test_task_initialization():
    """Verify default fields and lifecycle values of Task."""
    task = Task(name="test_job", payload={"key": "val"})
    assert task.status == TaskStatus.PENDING
    assert task.retry_count == 0
    assert task.max_retries == 3
    assert task.assigned_worker is None
    assert task.result is None


def test_worker_initialization():
    """Verify default fields and statuses of Worker."""
    worker = Worker()
    assert worker.status == WorkerStatus.ACTIVE
    assert worker.is_leader is False
    assert worker.current_task_id is None
    assert worker.completed_tasks == 0


def test_coordinator_worker_registration_and_heartbeat():
    """Verify worker registration and heartbeat updates."""
    coord = Coordinator()
    w1 = coord.register_worker("worker-1")
    assert w1.id == "worker-1"
    assert w1.status == WorkerStatus.ACTIVE

    stats = coord.get_cluster_stats()
    assert stats["workers"]["total"] == 1
    assert stats["workers"]["active"] == 1

    first_heartbeat = w1.last_heartbeat
    time.sleep(0.05)
    coord.record_heartbeat("worker-1")
    assert coord.workers["worker-1"].last_heartbeat > first_heartbeat


def test_task_submission_and_dispatch():
    """Verify submitting tasks and dispatching to an idle worker."""
    coord = Coordinator()
    coord.register_worker("worker-1")

    task = coord.submit_task("resize_image", {"filename": "pic.jpg"})
    assert task.status == TaskStatus.PENDING
    assert len(coord.task_queue) == 1

    # Dispatch tasks
    assignments = coord.dispatch_tasks()
    assert len(assignments) == 1
    assert assignments[0]["task_id"] == task.id
    assert assignments[0]["worker_id"] == "worker-1"

    # Task is now in progress and assigned
    updated_task = coord.get_task(task.id)
    assert updated_task.status == TaskStatus.IN_PROGRESS
    assert updated_task.assigned_worker == "worker-1"
    assert coord.workers["worker-1"].current_task_id == task.id


def test_worker_execution_success():
    """Verify end-to-end task execution by WorkerNode."""
    coord = Coordinator()
    worker_node = WorkerNode("worker-exec-1", coordinator=coord)
    worker_node.start()

    task = coord.submit_task("resize_image", {"filename": "avatar.png", "delay": 0.01})
    coord.dispatch_tasks()

    # Execute task
    result = worker_node.execute_task(task)
    assert result["status"] == "success"
    assert result["file"] == "avatar.png"

    # Verify coordinator state updated
    updated_task = coord.get_task(task.id)
    assert updated_task.status == TaskStatus.COMPLETED
    assert updated_task.result == result
    assert updated_task.execution_time is not None
    assert coord.workers["worker-exec-1"].current_task_id is None
    assert coord.workers["worker-exec-1"].completed_tasks == 1

    worker_node.stop()


def test_failure_detection_and_task_reassignment():
    """Verify that when a worker crashes, its task is recovered and reassigned."""
    coord = Coordinator(heartbeat_timeout=0.1, suspected_timeout=0.05)
    coord.register_worker("worker-crash")
    coord.register_worker("worker-backup")

    task = coord.submit_task("resize_image", {"filename": "data.raw"})

    # Dispatch to worker-crash
    coord.dispatch_tasks()
    assigned_worker_id = task.assigned_worker
    assert assigned_worker_id is not None

    # Force heartbeat of worker-crash into the past to simulate missed pings
    coord.workers[assigned_worker_id].last_heartbeat = time.time() - 1.0

    # Trigger failure detection
    dead = coord.detect_failures()
    assert assigned_worker_id in dead
    assert coord.workers[assigned_worker_id].status == WorkerStatus.DEAD

    # Task must have been reclaimed into the queue with retry_count incremented
    reclaimed_task = coord.get_task(task.id)
    assert reclaimed_task.status == TaskStatus.PENDING
    assert reclaimed_task.retry_count == 1
    assert reclaimed_task.assigned_worker is None

    # Now dispatch should give it to the backup worker
    new_assignments = coord.dispatch_tasks()
    assert len(new_assignments) == 1
    assert new_assignments[0]["worker_id"] != assigned_worker_id


def test_dead_letter_queue_on_max_retries():
    """Verify that poison pill tasks reaching max retries are safely quarantined in the DLQ."""
    coord = Coordinator()
    coord.register_worker("worker-dlq")
    worker = WorkerNode("worker-dlq", coordinator=coord)

    task = coord.submit_task("poison_pill", {}, max_retries=2)

    for _ in range(3):
        try:
            worker.execute_task(task)
        except ValueError:
            pass

    # Verify task is quarantined in DLQ
    dlq_task = coord.get_task(task.id)
    assert dlq_task.status == TaskStatus.DEAD_LETTER
    assert task.id in coord.dead_letter_queue
    stats = coord.get_cluster_stats()
    assert stats["tasks"]["dlq"] == 1


def test_task_priority_scheduling():
    """Verify that CRITICAL and HIGH priority tasks jump ahead of LOW and NORMAL tasks."""
    coord = Coordinator()
    # Register 1 worker so only 1 task gets dispatched at a time
    coord.register_worker("worker-single")

    # Submit 3 LOW priority tasks first
    low1 = coord.submit_task("resize_image", {"id": "low1"}, priority=TaskPriority.LOW)
    low2 = coord.submit_task("resize_image", {"id": "low2"}, priority=TaskPriority.LOW)
    low3 = coord.submit_task("resize_image", {"id": "low3"}, priority=TaskPriority.LOW)

    # Submit 1 CRITICAL task after the LOW tasks
    critical = coord.submit_task("resize_image", {"id": "crit"}, priority=TaskPriority.CRITICAL)

    # Submit 1 HIGH task after the CRITICAL task
    high = coord.submit_task("resize_image", {"id": "high"}, priority=TaskPriority.HIGH)

    # Dispatch to the single worker -> CRITICAL must be chosen first!
    assignments_1 = coord.dispatch_tasks()
    assert len(assignments_1) == 1
    assert assignments_1[0]["task_id"] == critical.id

    # Complete the task to free the worker
    coord.complete_task(critical.id, result={"status": "ok"})

    # Next dispatch -> HIGH must be chosen next!
    assignments_2 = coord.dispatch_tasks()
    assert len(assignments_2) == 1
    assert assignments_2[0]["task_id"] == high.id

    # Complete to free worker
    coord.complete_task(high.id, result={"status": "ok"})

    # Next dispatch -> First LOW task (low1) must be chosen (FIFO within same priority)
    assignments_3 = coord.dispatch_tasks()
    assert len(assignments_3) == 1
    assert assignments_3[0]["task_id"] == low1.id


def test_exponential_backoff_and_jitter():
    """Verify that failed tasks back off exponentially and are not dispatched prematurely."""
    coord = Coordinator(base_retry_delay=2.0, retry_jitter=0.2)
    coord.register_worker("worker-backoff")

    task = coord.submit_task("flaky_task")
    # First dispatch
    coord.dispatch_tasks()
    assert task.assigned_worker == "worker-backoff"

    # Fail the task (attempt 1)
    coord.fail_task(task.id, error_message="Network glitch")
    assert task.retry_count == 1
    assert task.status == TaskStatus.PENDING
    assert task.assigned_worker is None
    # Verify next_retry_at is set ~2.0s into the future
    assert task.next_retry_at is not None
    assert task.next_retry_at >= time.time() + 1.8

    # Attempt to dispatch immediately: must NOT dispatch because task is backing off
    immediate_assignments = coord.dispatch_tasks()
    assert len(immediate_assignments) == 0

    # Simulate backoff timer elapsed
    task.next_retry_at = time.time() - 0.1

    # Now dispatch should pick it up!
    retry_assignments = coord.dispatch_tasks()
    assert len(retry_assignments) == 1
    assert retry_assignments[0]["task_id"] == task.id

    # Fail again (attempt 2): backoff should be around 2.0 * (2^1) = ~4.0s
    coord.fail_task(task.id, error_message="Network glitch again")
    assert task.retry_count == 2
    assert task.next_retry_at >= time.time() + 3.8


