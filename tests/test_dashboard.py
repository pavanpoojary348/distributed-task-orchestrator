"""API tests for the Telemetry Dashboard endpoints."""

import pytest
from fastapi.testclient import TestClient
from dashboard.app import app


@pytest.fixture(scope="module")
def client():
    with TestClient(app) as test_client:
        yield test_client


def test_dashboard_html_page(client):
    """Verify that the home page renders the HTML dashboard."""
    response = client.get("/")
    assert response.status_code == 200
    assert "Distributed Task Orchestrator" in response.text
    assert "Worker Fleet" in response.text


def test_api_stats(client):
    """Verify cluster stats API returns active workers and queue counters."""
    response = client.get("/api/stats")
    assert response.status_code == 200
    data = response.json()
    assert "cluster_id" in data
    assert "workers" in data
    assert "tasks" in data
    assert data["workers"]["total"] >= 3


def test_api_submit_task(client):
    """Verify submitting a valid task returns 200 and queuing confirmation."""
    response = client.post("/api/tasks", json={
        "name": "resize_image",
        "payload": {"filename": "test.jpg"},
        "max_retries": 3
    })
    assert response.status_code == 200
    data = response.json()
    assert data["task"]["name"] == "resize_image"
    assert data["task"]["status"] in ["PENDING", "IN_PROGRESS", "COMPLETED"]


def test_api_submit_task_with_priority(client):
    """Verify submitting a task with CRITICAL priority (1) returns priority in response."""
    response = client.post("/api/tasks", json={
        "name": "resize_image",
        "payload": {"filename": "urgent.png"},
        "priority": 1
    })
    assert response.status_code == 200
    data = response.json()
    assert data["task"]["priority"] == 1



def test_api_submit_invalid_task(client):
    """Verify submitting an unknown task handler returns 400 Bad Request."""
    response = client.post("/api/tasks", json={
        "name": "non_existent_function",
        "payload": {}
    })
    assert response.status_code == 400


def test_api_chaos_burst(client):
    """Verify submitting a burst of tasks returns task IDs."""
    response = client.post("/api/chaos/burst")
    assert response.status_code == 200
    data = response.json()
    assert len(data["task_ids"]) == 6


def test_api_chaos_surge(client):
    """Verify submitting a surge wave returns 10 task IDs and triggers autoscaler."""
    response = client.post("/api/chaos/surge")
    assert response.status_code == 200
    data = response.json()
    assert len(data["task_ids"]) == 10



def test_api_worker_crash_and_recover(client):
    """Verify crashing and recovering a worker via API."""
    stats = client.get("/api/stats").json()
    worker_id = stats["workers"]["list"][0]["id"]

    # Kill worker
    kill_res = client.post(f"/api/workers/{worker_id}/kill")
    assert kill_res.status_code == 200

    # Recover worker
    rec_res = client.post(f"/api/workers/{worker_id}/recover")
    assert rec_res.status_code == 200


def test_websocket_stream(client):
    """Verify that connecting to /ws yields an INITIAL_SNAPSHOT with stats and tasks."""
    with client.websocket_connect("/ws") as websocket:
        data = websocket.receive_json()
        assert data["type"] == "INITIAL_SNAPSHOT"
        assert "stats" in data
        assert "tasks" in data
        assert "workers" in data["stats"]


def test_api_ha_endpoints(client):
    """Verify HA status, leader killing, standby promotion, and recovery via API."""
    # 1. Fetch initial HA status
    res = client.get("/api/ha/status")
    assert res.status_code == 200
    ha_data = res.json()
    assert "leader_id" in ha_data
    assert "term" in ha_data
    assert ha_data["term"] >= 1

    # 2. Trigger manual failover
    res_fail = client.post("/api/ha/failover")
    assert res_fail.status_code == 200
    fail_data = res_fail.json()
    assert "ha" in fail_data

    # 3. Simulate killing the leader
    res_kill = client.post("/api/ha/kill_leader")
    assert res_kill.status_code == 200

    # 4. Recover the crashed coordinator
    res_rec = client.post("/api/ha/recover")
    assert res_rec.status_code == 200
    rec_data = res_rec.json()
    assert "ha" in rec_data


def test_api_metrics_endpoint(client):
    """Verify that GET /api/metrics returns live throughput and history."""
    res = client.get("/api/metrics")
    assert res.status_code == 200
    data = res.json()
    assert "summary" in data
    assert "history" in data
    assert "throughput_tps" in data["summary"]
    assert "avg_latency_ms" in data["summary"]



