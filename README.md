# ? Distributed Task Orchestrator

A lightweight, fault-tolerant distributed background task orchestrator featuring automated leader election, heartbeat-driven failure detection, and automatic task reassignment with dead-letter queue (DLQ) support.

Designed as an educational and production-grade demonstration of distributed systems fundamentals (concurrency, split-brain prevention, and failure resilience).

---

## ??? System Architecture

`	ext
                     +-------------------------------+
                     ¦       Client / REST API       ¦
                     +-------------------------------+
                                     ¦ Submit Task
                                     ?
                     +-------------------------------+
                     ¦    Task Queue (FIFO/Redis)    ¦
                     +-------------------------------+
                                     ¦ Pull Tasks
                                     ?
                     +-------------------------------+
                     ¦      ?? COORDINATOR NODE      ¦
                     ¦  (Leader Election & Failover) ¦
                     +-------------------------------+
                                     ¦
         +---------------------------+---------------------------+
         ? Heartbeats (2s)           ? Heartbeats (2s)           ? Heartbeats (2s)
  +--------------+            +--------------+            +--------------+
  ¦   Worker 1   ¦            ¦   Worker 2   ¦            ¦   Worker 3   ¦
  ¦  [Status OK] ¦            ¦  [Status OK] ¦            ¦  [Status OK] ¦
  +--------------+            +--------------+            +--------------+
`

---

## ?? Key Features

1. **Heartbeat Protocol & Crash Recovery**:
   - Each worker periodically emits a heartbeat ping (default: every 2 seconds).
   - If a worker fails to ping within the timeout threshold (e.g., 6 seconds), the coordinator marks it as DEAD.
   - Any uncommitted tasks assigned to that worker are automatically reclaimed and reassigned to healthy workers.

2. **Automated Leader Election**:
   - The coordinator cluster maintains high availability. If the primary coordinator fails, a standby worker node assumes leadership to prevent a Single Point of Failure (SPOF).

3. **Dead Letter Queue (DLQ) & Exponential Backoff**:
   - Poison-pill tasks (tasks that throw unhandled exceptions or crash workers) are retried up to $ times with exponential backoff & jitter.
   - Upon exhausting maximum retries, tasks are safely routed to the Dead Letter Queue for post-mortem inspection.

4. **Live Telemetry & Chaos Simulation Dashboard**:
   - A real-time web interface displaying live cluster status, task throughput, and worker states.
   - Includes interactive buttons to kill individual workers to observe automatic failover in real-time.

---

## ??? Tech Stack

- **Language**: Python 3.10+
- **Coordination & Storage**: Redis / In-Memory State Store
- **API & Dashboard**: FastAPI, Uvicorn, WebSockets
- **Testing**: PyTest

---

## ?? Repository Structure

`	ext
distributed-task-orchestrator/
+-- core/
¦   +-- coordinator.py     # Leader election & failover logic
¦   +-- worker.py          # Worker execution loop & heartbeats
¦   +-- models.py          # Task, Worker, and Cluster data schemas
¦   +-- queue.py           # Task queue management & DLQ routing
+-- tasks/
¦   +-- definitions.py     # Sample CPU-bound & I/O-bound jobs
+-- dashboard/
¦   +-- app.py             # FastAPI telemetry server & UI
¦   +-- static/            # Live cluster visualizer
+-- tests/
¦   +-- test_orchestrator.py # Unit tests for crash recovery
+-- requirements.txt       # Dependencies
+-- README.md              # Project documentation
`

---

## ?? Interview Talking Points

- **Concurrency vs Parallelism**: How worker processes execute tasks independently without shared state collisions.
- **Handling Network Partitions**: Mitigating the Split-Brain problem during coordinator failover.
- **At-Least-Once Delivery**: Ensuring zero tasks are lost when nodes crash unexpectedly.
