"""Tests for Dead Letter Queue (DLQ) Re-drive and Task Cancellation APIs."""

import pytest
from fastapi.testclient import TestClient

from core.coordinator import Coordinator
from core.ha import HACoordinator
from core.models import Task, TaskPriority, TaskStatus
from dashboard.app import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_coordinator_cancel_pending_task():
    """Verify that a pending task can be cancelled and removed from the queue."""
    coord = Coordinator()
    task = coord.submit_task(name="resize_image", payload={"filename": "test.jpg"})
    assert task.id in coord.task_queue
    assert task.status == TaskStatus.PENDING

    success = coord.cancel_task(task.id)
    assert success is True
    assert task.status == TaskStatus.CANCELLED
    assert task.id not in coord.task_queue


def test_coordinator_cancel_invalid_status_fails():
    """Verify that completed or in-progress tasks cannot be cancelled."""
    coord = Coordinator()
    task = coord.submit_task(name="resize_image", payload={})
    task.status = TaskStatus.COMPLETED

    assert coord.cancel_task(task.id) is False
    assert task.status == TaskStatus.COMPLETED


def test_coordinator_retry_dlq_task():
    """Verify that re-driving a dead-letter task resets retries and re-enqueues it."""
    coord = Coordinator()
    task = coord.submit_task(name="flaky_task", payload={})
    task.status = TaskStatus.DEAD_LETTER
    task.retry_count = 3
    task.error = "Persistent failure"
    coord.dead_letter_queue.append(task.id)
    if task.id in coord.task_queue:
        coord.task_queue.remove(task.id)

    success = coord.retry_task(task.id)
    assert success is True
    assert task.status == TaskStatus.PENDING
    assert task.retry_count == 0
    assert task.error is None
    assert task.id not in coord.dead_letter_queue
    assert task.id in coord.task_queue


def test_coordinator_retry_all_dlq():
    """Verify that retry_all_dlq re-drives all tasks in the DLQ."""
    coord = Coordinator()
    t1 = coord.submit_task(name="task1", payload={})
    t2 = coord.submit_task(name="task2", payload={})

    t1.status = TaskStatus.DEAD_LETTER
    t2.status = TaskStatus.DEAD_LETTER
    coord.dead_letter_queue = [t1.id, t2.id]
    coord.task_queue.clear()

    count = coord.retry_all_dlq()
    assert count == 2
    assert len(coord.dead_letter_queue) == 0
    assert len(coord.task_queue) == 2
    assert t1.status == TaskStatus.PENDING
    assert t2.status == TaskStatus.PENDING


def test_api_cancel_task(client):
    """Verify POST /api/tasks/{task_id}/cancel cancels a queued task."""
    post_res = client.post("/api/tasks", json={
        "name": "generate_pdf_report",
        "payload": {"delay": 5.0}  # Large delay so we can cancel before worker completes
    })
    assert post_res.status_code == 200
    task_id = post_res.json()["task"]["id"]

    cancel_res = client.post(f"/api/tasks/{task_id}/cancel")
    # If the task was already grabbed by a fast worker, 400 is expected; otherwise 200
    if cancel_res.status_code == 200:
        assert "cancelled successfully" in cancel_res.json()["message"]
    else:
        assert cancel_res.status_code == 400


def test_api_retry_dlq_endpoints(client):
    """Verify POST /api/tasks/{task_id}/retry and POST /api/dlq/retry-all."""
    # Submit and artificially move a task to DLQ
    from dashboard.app import coordinator
    task = coordinator.submit_task(name="poison_pill_task", payload={})
    with coordinator._lock:
        task.status = TaskStatus.DEAD_LETTER
        task.retry_count = 3
        task.error = "Unrecoverable error"
        coordinator.dead_letter_queue.append(task.id)
        if task.id in coordinator.task_queue:
            coordinator.task_queue.remove(task.id)

    # Test single retry endpoint
    retry_res = client.post(f"/api/tasks/{task.id}/retry")
    assert retry_res.status_code == 200
    assert task.status == TaskStatus.PENDING
    assert task.id not in coordinator.dead_letter_queue

    # Artificially add 2 tasks to DLQ for retry-all endpoint test
    with coordinator._lock:
        t_a = coordinator.submit_task(name="task_a", payload={})
        t_b = coordinator.submit_task(name="task_b", payload={})
        t_a.status = TaskStatus.DEAD_LETTER
        t_b.status = TaskStatus.DEAD_LETTER
        coordinator.dead_letter_queue.extend([t_a.id, t_b.id])

    retry_all_res = client.post("/api/dlq/retry-all")
    assert retry_all_res.status_code == 200
    data = retry_all_res.json()
    assert data["re_driven_count"] >= 2


def test_retry_pending_or_running_task_rejected():
    """Verify that attempting to re-drive tasks in PENDING, IN_PROGRESS, or COMPLETED state is rejected."""
    coord = Coordinator()
    task = coord.submit_task(name="resize_image", payload={})
    assert task.status == TaskStatus.PENDING

    # Attempting to retry a task that is already PENDING must be rejected
    assert coord.retry_task(task.id) is False
    assert coord.task_queue.count(task.id) == 1  # No duplicate enqueue

    task.status = TaskStatus.IN_PROGRESS
    assert coord.retry_task(task.id) is False

    task.status = TaskStatus.COMPLETED
    assert coord.retry_task(task.id) is False


def test_api_retry_active_task_returns_400(client):
    """Verify API rejects retry requests for tasks that are currently PENDING or COMPLETED."""
    from dashboard.app import coordinator
    task = coordinator.submit_task(name="resize_image", payload={})
    with coordinator._lock:
        task.status = TaskStatus.COMPLETED

    retry_res = client.post(f"/api/tasks/{task.id}/retry")
    assert retry_res.status_code == 400
    assert "cannot be retried" in retry_res.json()["detail"]


def test_cancelled_task_history_pruning():
    """Verify that CANCELLED tasks are properly pruned to prevent memory leaks."""
    coord = Coordinator()
    coord.max_task_history = 3

    t1 = coord.submit_task("t1", {})
    t2 = coord.submit_task("t2", {})
    t3 = coord.submit_task("t3", {})

    coord.cancel_task(t1.id)
    coord.cancel_task(t2.id)

    # Submit 4th task to trigger pruning beyond max_task_history
    t4 = coord.submit_task("t4", {})
    coord.cancel_task(t4.id)

    # Only max_task_history tasks should remain
    assert len(coord.tasks) <= coord.max_task_history
