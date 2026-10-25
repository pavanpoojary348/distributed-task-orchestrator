# ⚡ Distributed Task Orchestrator

A production-grade, fault-tolerant distributed background task orchestrator written in Python (FastAPI, Pydantic, WebSockets, PyTest). Features active-standby leader election, autonomous auto-scaling, priority-based scheduling, exponential backoff with jitter, dead-letter queuing, and a real-time command center telemetry dashboard with live performance charts.

---

## 🏛️ System Architecture

```text
                      +---------------------------------------+
                      |         Client / Dashboard            |
                      +---------------------------------------+
                                      │  (WebSockets / HTTP)
                                      ▼
                      +---------------------------------------+
                      |       HIGH AVAILABILITY LEASE         |
                      |          (3.0s Auto-Renewing)         |
                      +---------------------------------------+
                                      │
                 ┌────────────────────┴────────────────────┐
                 ▼                                         ▼
   +-----------------------------+           +-----------------------------+
   |    coordinator-primary      |           |     coordinator-standby     |
   |      [👑 ACTIVE LEADER]     |           |       [💤 HOT STANDBY]      |
   +-----------------------------+           +-----------------------------+
                 │                                         │
                 │ (Automatic Failover upon lease expiry)   │
                 └───────────────────┬─────────────────────┘
                                     │
                     +───────────────┴───────────────+
                     |    Priority Task Queue (FIFO)  |
                     |  [CRITICAL, HIGH, NORMAL, LOW]|
                     +───────────────┬───────────────+
                                     │
                     +───────────────┴───────────────+
                     |    Autonomous Auto-Scaler     |
                     | (Surge Scale-up / Idle Drain) |
                     +───────────────┬───────────────+
                                     │
     ┌──────────────────┬────────────┴───────┬──────────────────┐
     ▼                  ▼                    ▼                  ▼
+--------------+   +--------------+   +--------------+   +-------------------+
| worker-alpha |   |  worker-beta |   | worker-gamma |   | worker-auto-XXXX  |
|  [Status OK] |   |  [Status OK] |   |  [Status OK] |   | [⚡ ELASTIC NODE]  |
+--------------+   +--------------+   +--------------+   +-------------------+
```

---

## ✨ 6 Production Features Implemented

### 1. 🎟️ Task Priority Queuing ("VIP Fast Pass")
- Tasks support 4 priority tiers: `CRITICAL (1)`, `HIGH (2)`, `NORMAL (3)`, and `LOW (4)`.
- Dispatching uses a multi-tier comparator: lower integer priority values jump ahead in the queue, with FIFO ordering applied as a tie-breaker.

### 2. ⏳ Exponential Backoff with Jitter ("Smart Breathing Room")
- Failed tasks calculate dynamic retry delays: `delay = base * 2^(retry-1) + uniform(0, jitter)`.
- Prevents the **Thundering Herd** problem on downstream services.
- Cooling-down tasks are non-blocking: ready tasks bypass cooling tasks smoothly.

### 3. 🌊 Autonomous Cluster Auto-Scaler ("Elastic Surge Staffing")
- Continuously monitors pending queue pressure.
- Scales up additional dynamic worker nodes when pending tasks $\ge 3$ (up to 8 nodes).
- Scales down idle dynamic workers after 6 seconds of inactivity (down to minimum 3 core nodes).

### 4. ⚡ Real-Time WebSockets Push ("Instant Zero-Lag Telemetry")
- Replaced 1-second HTTP polling with bi-directional streaming via `@app.websocket("/ws")`.
- Dispatches, completions, failures, and worker status changes are broadcast with zero latency to all connected browsers.

### 5. 🛡️ Standby Coordinator & Leader Election ("Zero SPOF")
- Eliminates Single Points of Failure through Active-Standby clustering (`coordinator-primary` and `coordinator-standby`).
- Heartbeat lease consensus mechanism: Active leader renews lease every second.
- If the active leader fails or crashes, the standby coordinator detects lease expiration and executes **instant automatic promotion** (`👑 Term #2`).
- A recovering node automatically rejoins as `STANDBY`, preventing split-brain conflicts.

### 6. 📈 Live Performance Charts & Throughput Analytics
- Dual time-series Canvas charts tracking a rolling 30-second sliding window:
  - **Throughput & Queue Pressure:** Tasks per second (TPS), Pending Queue Depth, and In-Flight executing tasks.
  - **Worker Fleet Utilization & Latency:** Fleet busy percentage and average execution latency (ms) with P95 metrics.
- Instant KPI badges: Live throughput (tasks/min), Average latency, Success rate, and Active utilization.

---

## 🚀 Quick Start Guide

### 1. Prerequisites
- Python 3.10+ installed on your system.

### 2. Activate the Virtual Environment
On Windows (PowerShell):
```powershell
.\.venv\Scripts\Activate.ps1
```
*(Or invoke `.venv\Scripts\python` and `.venv\Scripts\pytest` directly)*

### 3. Launch the Web Telemetry Dashboard
Run the dashboard server with Uvicorn:
```powershell
.venv\Scripts\uvicorn dashboard.app:app --reload --port 8000
```

Open your browser and navigate to:
👉 **[http://localhost:8000](http://localhost:8000)**

### 4. Run the Automated Test Suite
Execute all 34 unit, integration, and chaos tests with PyTest:
```powershell
.venv\Scripts\pytest -v
```

---

## 🧪 Interactive Chaos Simulation Walkthrough

1. Open **[http://localhost:8000](http://localhost:8000)** in your browser.
2. Observe the top header badge: **`⚡ WebSocket LIVE`**.
3. **Test Priority Queuing:**
   - Select `generate_pdf_report` with `LOW` priority, submit it.
   - Immediately select `resize_image` with `CRITICAL` priority, submit it.
   - Watch the `CRITICAL` task jump straight to the front of the queue!
4. **Test Auto-Scaling:**
   - Click **"🌊 Surge (10 Tasks -> Auto-Scale)"**.
   - Notice the Auto-Scaler detect queue pressure and spawn `worker-auto-xxxx` with the `⚡ ELASTIC` badge!
   - Once queue pressure drains, watch the elastic worker retire cleanly after 6s of idle time.
5. **Test High Availability & Leader Election:**
   - Under the **High Availability** panel, click **"💥 Kill Leader"**.
   - Watch `coordinator-primary` turn red (`💀 CRASHED`), while `coordinator-standby` self-promotes to `👑 ACTIVE LEADER`!
   - Click **"⚡ Queue Burst"** to verify tasks continue to process without cluster downtime!
   - Click **"🔄 Reboot Node"** to watch the primary rejoin safely as `STANDBY`.
6. **Watch Real-Time Analytics:**
   - Watch the live Canvas charts plot throughput surges (Cyan), queue depth spikes (Violet), and worker fleet utilization (Green) in real time!

---

## 📁 Repository Structure

```text
distributed-task-orchestrator/
├── core/
│   ├── models.py          # Pydantic schemas (Task, Worker, TaskPriority, CoordinatorRole)
│   ├── coordinator.py     # Centralized orchestrator node & shared ClusterState
│   ├── ha.py              # Active-Standby leader election & heartbeat lease manager
│   ├── worker.py          # Worker node process with auto-reconnect & heartbeats
│   ├── autoscaler.py      # Autonomous queue pressure auto-scaler
│   └── metrics.py         # Performance analytics, throughput TPS & latency tracker
├── dashboard/
│   └── app.py             # FastAPI telemetry server, WebSockets stream & Command Center UI
├── tasks/
│   └── definitions.py     # Registry of executable tasks (image thumbnailing, PDF, etc.)
├── tests/
│   ├── test_orchestrator.py # Core task dispatch, heartbeats, priority & DLQ tests
│   ├── test_autoscaler.py   # Auto-scaler scale up/down pressure tests
│   ├── test_ha.py           # Leader election, failover, lease renewal tests
│   ├── test_metrics.py      # Throughput, latency, and rolling history tests
│   └── test_dashboard.py    # REST APIs, WebSockets, and HA chaos endpoints
├── pytest.ini             # PyTest configuration
└── requirements.txt       # Frozen dependencies
```
