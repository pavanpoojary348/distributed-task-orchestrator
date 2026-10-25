"""Unit tests for the Cluster AutoScaler."""

import time
import pytest
from core.models import WorkerStatus
from core.coordinator import Coordinator
from core.worker import WorkerNode
from core.autoscaler import AutoScaler


def test_autoscaler_scale_up_on_queue_pressure():
    """Verify that autoscaler spawns an additional dynamic worker when queue depth exceeds threshold."""
    coord = Coordinator()
    for name in ["w1", "w2", "w3"]:
        coord.register_worker(name, is_dynamic=False)

    worker_nodes = {}
    spawned = []
    retired = []

    def mock_spawn(is_dynamic: bool = True):
        new_id = f"worker-auto-{len(spawned) + 1}"
        spawned.append(new_id)
        coord.register_worker(new_id, is_dynamic=is_dynamic)
        return new_id

    def mock_retire(worker_id: str):
        retired.append(worker_id)
        coord.unregister_worker(worker_id)

    scaler = AutoScaler(min_workers=3, max_workers=5, scale_up_threshold=3)
    scaler.last_scale_action = time.time() - 10.0  # bypass cooldown

    # Submit 4 tasks (exceeds threshold 3)
    for i in range(4):
        coord.submit_task("resize_image", {"id": i})

    # Trigger evaluation
    action = scaler.evaluate(coord, worker_nodes, mock_spawn, mock_retire)
    assert action is not None
    assert "Scaled UP" in action
    assert len(spawned) == 1
    assert "worker-auto-1" in coord.workers
    assert coord.workers["worker-auto-1"].is_dynamic is True


def test_autoscaler_respects_max_workers():
    """Verify that autoscaler never exceeds max_workers limit even under extreme queue backlog."""
    coord = Coordinator()
    for name in ["w1", "w2", "w3", "w4", "w5"]:
        coord.register_worker(name)

    spawned = []

    def mock_spawn(is_dynamic: bool = True):
        spawned.append("extra")
        return "extra"

    scaler = AutoScaler(min_workers=3, max_workers=5, scale_up_threshold=2)
    scaler.last_scale_action = time.time() - 10.0

    # Queue 20 tasks
    for i in range(20):
        coord.submit_task("resize_image", {"id": i})

    action = scaler.evaluate(coord, {}, mock_spawn, lambda wid: None)
    assert action is None  # Cannot scale up because max_workers (5) reached
    assert len(spawned) == 0


def test_autoscaler_scale_down_on_idle():
    """Verify that idle dynamic workers are retired when queue depth is zero."""
    coord = Coordinator()
    # 3 static + 1 dynamic
    for name in ["w1", "w2", "w3"]:
        coord.register_worker(name, is_dynamic=False)

    dyn_worker = coord.register_worker("worker-dyn-1", is_dynamic=True)
    dyn_worker.idle_since = time.time() - 20.0  # idle for 20 seconds

    retired = []

    def mock_retire(worker_id: str):
        retired.append(worker_id)
        coord.unregister_worker(worker_id)

    scaler = AutoScaler(min_workers=3, max_workers=6, scale_down_idle_seconds=5.0)
    scaler.last_scale_action = time.time() - 10.0

    # Queue is completely empty
    action = scaler.evaluate(coord, {}, lambda is_dyn: "new", mock_retire)
    assert action is not None
    assert "Scaled DOWN" in action
    assert len(retired) == 1
    assert retired[0] == "worker-dyn-1"
    assert "worker-dyn-1" not in coord.workers


def test_autoscaler_never_scales_below_min_workers():
    """Verify that static/base fleet workers are never retired below min_workers."""
    coord = Coordinator()
    for name in ["w1", "w2", "w3"]:
        coord.register_worker(name, is_dynamic=False)

    retired = []

    def mock_retire(worker_id: str):
        retired.append(worker_id)
        coord.unregister_worker(worker_id)

    scaler = AutoScaler(min_workers=3, max_workers=6, scale_down_idle_seconds=5.0)
    scaler.last_scale_action = time.time() - 10.0

    action = scaler.evaluate(coord, {}, lambda is_dyn: "new", mock_retire)
    assert action is None
    assert len(retired) == 0
    assert len(coord.workers) == 3
