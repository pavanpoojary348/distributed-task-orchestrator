"""Security and Privacy Test Suite.

Verifies:
- HTTP Security Headers (X-Content-Type-Options, X-Frame-Options, CSP, etc.)
- Privacy / PII / Credential Masking in telemetry & task inspection
- DoS prevention via oversized payload & input length limits
- Path Traversal prevention in task execution handlers
- Information Disclosure / Stack trace & user path sanitization
- Stale retry backoff reset on worker failure
- History memory-leak bounds in coordinator
"""

import time
import pytest
from fastapi.testclient import TestClient

from core.coordinator import Coordinator, sanitize_error_message
from core.models import Task, TaskPriority, TaskStatus
from dashboard.app import app, mask_sensitive_payload
from tasks.definitions import resize_image


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_security_headers_present(client):
    """Verify standard security headers are enforced on responses."""
    response = client.get("/")
    assert response.status_code == 200
    headers = response.headers

    assert headers.get("x-content-type-options") == "nosniff"
    assert headers.get("x-frame-options") == "DENY"
    assert "1; mode=block" in headers.get("x-xss-protection", "")
    assert headers.get("referrer-policy") == "strict-origin-when-cross-origin"
    assert "default-src 'self'" in headers.get("content-security-policy", "")
    assert "geolocation=()" in headers.get("permissions-policy", "")


def test_privacy_sensitive_payload_masking(client):
    """Verify that credentials and tokens in task payloads are masked in API responses."""
    sensitive_payload = {
        "filename": "document.pdf",
        "api_key": "sk-proj-super-secret-12345",
        "password": "SuperSecretPassword123!",
        "access_token": "bearer-eyJhbGciOi...",
        "user_profile": {
            "ssn": "123-45-6789",
            "username": "alice",
            "client_secret": "my-client-secret"
        }
    }

    # Test the masking utility directly
    masked = mask_sensitive_payload(sensitive_payload)
    assert masked["filename"] == "document.pdf"
    assert masked["api_key"] == "********"
    assert masked["password"] == "********"
    assert masked["access_token"] == "********"
    assert masked["user_profile"]["ssn"] == "********"
    assert masked["user_profile"]["client_secret"] == "********"
    assert masked["user_profile"]["username"] == "alice"

    # Test via API submission and fetch
    post_res = client.post("/api/tasks", json={
        "name": "resize_image",
        "payload": sensitive_payload
    })
    assert post_res.status_code == 200
    task_id = post_res.json()["task"]["id"]

    tasks_res = client.get("/api/tasks")
    assert tasks_res.status_code == 200
    all_tasks = tasks_res.json()
    matched = [t for t in all_tasks if t["id"] == task_id]
    assert len(matched) == 1
    t = matched[0]
    assert t["payload"]["api_key"] == "********"
    assert t["payload"]["password"] == "********"
    assert t["payload"]["user_profile"]["ssn"] == "********"
    assert t["payload"]["user_profile"]["username"] == "alice"


def test_dos_oversized_payload_rejected(client):
    """Verify that payloads exceeding the 64KB threshold are rejected with HTTP 413."""
    huge_payload = {"data": "A" * (65 * 1024)}
    response = client.post("/api/tasks", json={
        "name": "resize_image",
        "payload": huge_payload
    })
    assert response.status_code == 413
    assert "exceeds maximum allowed size" in response.json()["detail"]


def test_input_validation_name_length(client):
    """Verify that oversized task names are rejected by schema validation."""
    long_name = "a" * 129
    response = client.post("/api/tasks", json={
        "name": long_name,
        "payload": {}
    })
    assert response.status_code == 422


def test_path_traversal_sanitization():
    """Verify that directory traversal attempts in image resizing are neutralized."""
    # Test Unix-style traversal
    res_unix = resize_image({"filename": "../../../../etc/shadow"})
    assert res_unix["file"] == "shadow"
    assert ".." not in res_unix["file"]
    assert "/" not in res_unix["file"]

    # Test Windows-style traversal
    res_win = resize_image({"filename": "..\\..\\Windows\\System32\\config\\SAM"})
    assert res_win["file"] == "SAM"
    assert ".." not in res_win["file"]
    assert "\\" not in res_win["file"]


def test_error_path_and_token_redaction():
    """Verify that file paths and tokens are redacted from error messages."""
    raw_error = "Failed processing at C:\\Users\\Alice\\Documents\\repo\\secret.py line 42 with Bearer eyJhbGciOi..."
    sanitized = sanitize_error_message(raw_error)
    assert "[REDACTED_PATH]" in sanitized
    assert "Alice" not in sanitized
    assert "[REDACTED]" in sanitized
    assert "eyJhbGci" not in sanitized

    # Test Linux path redaction
    linux_error = "FileNotFoundError in /home/bob/secret/key.pem"
    sanitized_linux = sanitize_error_message(linux_error)
    assert "[REDACTED_PATH]" in sanitized_linux
    assert "/home/bob" not in sanitized_linux


def test_stale_backoff_cleared_on_orphan_reclaim():
    """Verify that when a task is reclaimed from a crashed worker, stale backoff is cleared."""
    coord = Coordinator(max_task_history=50)
    task = Task(name="test_task", payload={})
    task.next_retry_at = time.time() + 300.0  # Stale 5 minute backoff
    task.assigned_worker = "worker-crash"
    task.status = TaskStatus.IN_PROGRESS
    coord.tasks[task.id] = task

    coord._reclaim_orphaned_task(task.id, reason="Worker crashed")

    assert task.status == TaskStatus.PENDING
    assert task.assigned_worker is None
    assert task.next_retry_at is None  # Must be cleared so it can dispatch immediately


def test_coordinator_task_history_pruning():
    """Verify that completed tasks do not accumulate unboundedly in memory."""
    coord = Coordinator(max_task_history=20)

    # Add 40 completed tasks
    for i in range(40):
        t = Task(name=f"task_{i}", payload={})
        t.status = TaskStatus.COMPLETED
        coord.tasks[t.id] = t

    coord._prune_task_history()

    completed_count = sum(1 for t in coord.tasks.values() if t.status in (TaskStatus.COMPLETED, TaskStatus.DEAD_LETTER))
    assert completed_count <= 20
