"""Unit tests for Performance Metrics & Throughput Analytics."""

import time
from core.metrics import MetricsCollector


def test_metrics_collector_record_completion():
    """Verify recording task completion calculates throughput and latency."""
    collector = MetricsCollector(history_seconds=10)
    
    # Record two task executions
    collector.record_completion(execution_time=0.150)
    collector.record_completion(execution_time=0.250)

    # Mock coordinator with empty queue
    class MockCoordinator:
        task_queue = []
        tasks = {}
        workers = {}

    result = collector.sample(MockCoordinator())
    summary = result["summary"]
    
    assert summary["throughput_tps"] > 0
    assert summary["throughput_tpm"] == 2
    assert summary["avg_latency_ms"] == 200.0   # (150ms + 250ms) / 2
    assert summary["success_rate_pct"] == 100.0


def test_metrics_collector_failure_and_success_rate():
    """Verify recording failures calculates correct success percentage."""
    collector = MetricsCollector(history_seconds=10)
    
    collector.record_completion(execution_time=0.100)
    collector.record_failure()

    class MockCoordinator:
        task_queue = []
        tasks = {}
        workers = {}

    result = collector.sample(MockCoordinator())
    summary = result["summary"]

    assert summary["success_rate_pct"] == 50.0  # 1 success out of 2 total


def test_metrics_collector_rolling_history_capping():
    """Verify that history length is strictly bounded by history_seconds."""
    collector = MetricsCollector(history_seconds=5)

    class MockCoordinator:
        task_queue = ["t1", "t2"]
        tasks = {}
        workers = {}

    # Sample 10 times
    for _ in range(10):
        collector.sample(MockCoordinator())

    assert len(collector.history) == 5
    metrics = collector.get_metrics()
    assert len(metrics["history"]) == 5
    assert metrics["summary"]["queue_depth"] == 2
