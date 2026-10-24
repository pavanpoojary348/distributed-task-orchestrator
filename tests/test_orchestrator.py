"Unit tests for Distributed Task Orchestrator"
import pytest
from core.models import Task, TaskStatus, Worker, WorkerStatus

def test_task_initialization():
    task = Task(name=test_job, payload={key: val})
    assert task.status == TaskStatus.PENDING
    assert task.retry_count == 0
    assert task.max_retries == 3

def test_worker_initialization():
    worker = Worker()
    assert worker.status == WorkerStatus.ACTIVE
    assert worker.is_leader is False
