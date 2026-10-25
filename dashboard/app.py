"""Web Telemetry & Chaos Simulation Dashboard (FastAPI).

Features:
- Live cluster telemetry and heartbeat monitoring
- Interactive task submission & chaos engineering (worker kill/recover)
- Automated coordinator background loop for failure detection & dispatching
- Single-page command-center UI with dark mode, glowing status indicators, and live updates
"""

import time
import threading
import uuid
import asyncio
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

from core.models import Task, TaskPriority, TaskStatus, Worker, WorkerStatus, CoordinatorRole
from core.coordinator import Coordinator
from core.ha import HACoordinator
from core.worker import WorkerNode
from core.autoscaler import AutoScaler
from tasks.definitions import TASK_REGISTRY


# Global cluster state with High Availability (Primary & Hot Standby Leader Election)
coordinator = HACoordinator(
    cluster_id="cluster-production-01",
    lease_duration=3.0,
    heartbeat_timeout=6.0,
    suspected_timeout=3.5,
    base_retry_delay=2.0,
    retry_jitter=0.5
)


# Active worker nodes dictionary {worker_id: WorkerNode}
worker_nodes: Dict[str, WorkerNode] = {}
_background_loop_active = True
_lock = threading.Lock()

# Autonomous cluster auto-scaler
autoscaler = AutoScaler(
    min_workers=3,
    max_workers=8,
    scale_up_threshold=3,
    scale_down_idle_seconds=6.0
)


class ConnectionManager:
    """Manages active real-time WebSocket client connections."""
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self._lock = threading.Lock()

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        with self._lock:
            self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        with self._lock:
            if websocket in self.active_connections:
                self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        with self._lock:
            connections = list(self.active_connections)

        for connection in connections:
            try:
                await connection.send_json(message)
            except Exception:
                self.disconnect(connection)


manager = ConnectionManager()
_main_loop: Optional[asyncio.AbstractEventLoop] = None


def _get_current_stats_snapshot() -> Dict[str, Any]:
    """Capture a snapshot of cluster stats augmented with worker state."""
    stats = coordinator.get_cluster_stats()
    now = time.time()
    for w in stats["workers"]["list"]:
        wn = worker_nodes.get(w["id"])
        w["is_simulated_killed"] = not wn.is_running if wn else False
        w["seconds_since_heartbeat"] = round(now - w["last_heartbeat"], 1)

    stats["autoscaler"] = {
        "min": autoscaler.min_workers,
        "max": autoscaler.max_workers,
        "threshold": autoscaler.scale_up_threshold,
        "cooldown": autoscaler.scale_down_idle_seconds,
    }
    return stats


def _get_current_tasks_snapshot() -> List[Dict[str, Any]]:
    """Capture newest 50 tasks for dashboard rendering."""
    with coordinator._lock:
        all_tasks = [t.model_dump() for t in coordinator.tasks.values()]
        all_tasks.sort(key=lambda x: x["created_at"], reverse=True)
        return all_tasks[:50]


def broadcast_cluster_state():
    """Thread-safe trigger to broadcast latest telemetry to all WebSocket clients."""
    global _main_loop
    if _main_loop and _main_loop.is_running() and manager.active_connections:
        payload = {
            "type": "TELEMETRY_UPDATE",
            "stats": _get_current_stats_snapshot(),
            "tasks": _get_current_tasks_snapshot(),
        }
        asyncio.run_coroutine_threadsafe(manager.broadcast(payload), _main_loop)


def spawn_dynamic_worker(is_dynamic: bool = True) -> str:
    """Spawn a new worker node and register it with the coordinator."""
    with _lock:
        new_id = f"worker-auto-{uuid.uuid4().hex[:4]}"
        node = WorkerNode(worker_id=new_id, coordinator=coordinator, heartbeat_interval=2.0)
        worker_nodes[new_id] = node
        node.start()
        coordinator.register_worker(new_id, is_dynamic=is_dynamic)
        return new_id


def retire_dynamic_worker(worker_id: str):
    """Gracefully retire and remove an idle dynamic worker node."""
    with _lock:
        node = worker_nodes.get(worker_id)
        if node:
            node.stop()
            del worker_nodes[worker_id]


def _orchestrator_background_loop():
    """Background loop that continuously drives failure detection, task dispatching, and autoscaling."""
    global _background_loop_active
    while _background_loop_active:
        try:
            # 0. High Availability Leader Lease evaluation & automatic failover
            coordinator.evaluate_leadership()

            # 1. Detect any worker that timed out (run on active leader)
            if coordinator.active_coordinator.is_alive:
                coordinator.detect_failures()

                # 2. Dispatch pending tasks to idle workers
                assignments = coordinator.dispatch_tasks()

                # 3. For any assignments made, execute them on the assigned worker
                for assignment in assignments:
                    task_id = assignment["task_id"]
                    worker_id = assignment["worker_id"]
                    task = coordinator.get_task(task_id)
                    worker_node = worker_nodes.get(worker_id)

                    if task and worker_node and worker_node.is_running:
                        # Spawn task execution in a separate daemon thread so worker doesn't block coordinator loop
                        def run_worker_task(wn=worker_node, t=task):
                            try:
                                wn.execute_task(t)
                            except Exception:
                                # Already handled in worker.execute_task (logged to coordinator)
                                pass

                        t_thread = threading.Thread(target=run_worker_task, daemon=True)
                        t_thread.start()

                # 4. Evaluate Autoscaler rules (scale-up under high pressure, scale-down on idle)
                autoscaler.evaluate(
                    coordinator=coordinator.active_coordinator,
                    worker_nodes=worker_nodes,
                    spawn_worker_fn=spawn_dynamic_worker,
                    retire_worker_fn=retire_dynamic_worker
                )

            # 4.5. Sample real-time performance & throughput metrics
            if hasattr(coordinator, "metrics_collector") and coordinator.metrics_collector:
                coordinator.metrics_collector.sample(coordinator=coordinator, worker_nodes=worker_nodes)

            # 5. Broadcast instant state update via WebSockets to connected browsers
            broadcast_cluster_state()


        except Exception as e:
            print(f"[Orchestrator Loop Error] {e}")

        time.sleep(1.0)



def initialize_default_cluster():
    """Create default workers (Alpha, Beta, Gamma) and start their heartbeats."""
    with _lock:
        for name in ["worker-alpha", "worker-beta", "worker-gamma"]:
            if name not in worker_nodes:
                node = WorkerNode(worker_id=name, coordinator=coordinator, heartbeat_interval=2.0)
                worker_nodes[name] = node
                node.start()


# Initialize default worker fleet on startup
initialize_default_cluster()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    global _main_loop
    try:
        _main_loop = asyncio.get_running_loop()
    except RuntimeError:
        pass

    initialize_default_cluster()

    # Start the coordinator orchestrator loop in background thread
    loop_thread = threading.Thread(target=_orchestrator_background_loop, daemon=True)
    loop_thread.start()

    yield

    # Clean shutdown
    global _background_loop_active
    _background_loop_active = False
    for node in worker_nodes.values():
        node.stop()



app = FastAPI(
    title="Distributed Task Orchestrator Dashboard",
    description="Real-time telemetry and chaos engineering interface",
    lifespan=lifespan
)


# Request schemas
class SubmitTaskRequest(BaseModel):
    name: str
    payload: Dict[str, Any] = {}
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = 3


# -----------------------------------------------------------------------------
# REST API Endpoints
# -----------------------------------------------------------------------------

@app.get("/api/stats")
def get_stats():
    """Return cluster telemetry snapshot."""
    stats = coordinator.get_cluster_stats()
    # Augment worker list with is_running state from WorkerNode instance
    now = time.time()
    for w in stats["workers"]["list"]:
        wn = worker_nodes.get(w["id"])
        w["is_simulated_killed"] = not wn.is_running if wn else False
        w["seconds_since_heartbeat"] = round(now - w["last_heartbeat"], 1)

    stats["autoscaler"] = {
        "min": autoscaler.min_workers,
        "max": autoscaler.max_workers,
        "threshold": autoscaler.scale_up_threshold,
        "cooldown": autoscaler.scale_down_idle_seconds,
    }
    return stats


@app.get("/api/tasks")
def get_tasks():
    """Return all tasks ordered from newest to oldest."""
    with coordinator._lock:
        all_tasks = [t.model_dump() for t in coordinator.tasks.values()]
        all_tasks.sort(key=lambda x: x["created_at"], reverse=True)
        return all_tasks[:50]


@app.post("/api/tasks")
def submit_task(req: SubmitTaskRequest):
    """Enqueue a new background task with priority."""
    if req.name not in TASK_REGISTRY:
        raise HTTPException(status_code=400, detail=f"Invalid task name. Options: {list(TASK_REGISTRY.keys())}")

    task = coordinator.submit_task(
        name=req.name,
        payload=req.payload,
        priority=req.priority,
        max_retries=req.max_retries
    )
    broadcast_cluster_state()
    return {"message": "Task queued successfully", "task": task.model_dump()}


@app.post("/api/chaos/burst")
def trigger_burst():
    """Enqueues a burst of assorted tasks with varied priorities to demonstrate priority scheduling."""
    import random
    task_types = ["resize_image", "generate_pdf_report", "flaky_task", "resize_image"]
    priorities = [TaskPriority.LOW, TaskPriority.NORMAL, TaskPriority.HIGH, TaskPriority.CRITICAL]
    created_ids = []

    for i in range(6):
        chosen = random.choice(task_types)
        chosen_prio = random.choice(priorities)
        payload = {}
        if chosen == "resize_image":
            payload = {"filename": f"photo_{random.randint(100, 999)}.jpg", "delay": 1.2}
        elif chosen == "generate_pdf_report":
            payload = {"title": f"Report #{random.randint(10, 99)}", "delay": 1.5, "pages": random.randint(5, 30)}
        elif chosen == "flaky_task":
            payload = {"failure_rate": 0.5}

        t = coordinator.submit_task(name=chosen, payload=payload, priority=chosen_prio)
        created_ids.append(t.id)

    broadcast_cluster_state()
    return {"message": f"Queued burst of {len(created_ids)} tasks", "task_ids": created_ids}


@app.post("/api/chaos/surge")
def trigger_surge():
    """Enqueue a heavy surge wave of 10 tasks to trigger the AutoScaler in real-time."""
    created_ids = []
    for i in range(10):
        t = coordinator.submit_task(
            name="resize_image",
            payload={"filename": f"surge_photo_{i+1}.jpg", "delay": 1.8},
            priority=TaskPriority.NORMAL
        )
        created_ids.append(t.id)

    broadcast_cluster_state()
    return {"message": f"Queued surge wave of {len(created_ids)} tasks! Auto-Scaler triggered.", "task_ids": created_ids}


@app.post("/api/workers/{worker_id}/kill")
def kill_worker(worker_id: str):
    """Simulate a sudden worker node crash (stops heartbeats immediately)."""
    with _lock:
        node = worker_nodes.get(worker_id)
        if not node:
            raise HTTPException(status_code=404, detail=f"Worker '{worker_id}' not found")

        node.simulate_crash()
        broadcast_cluster_state()
        return {"message": f"Worker '{worker_id}' crashed. Heartbeats stopped."}


@app.post("/api/workers/{worker_id}/recover")
def recover_worker(worker_id: str):
    """Recover a crashed worker node (resumes heartbeats)."""
    with _lock:
        node = worker_nodes.get(worker_id)
        if not node:
            raise HTTPException(status_code=404, detail=f"Worker '{worker_id}' not found")

        node.recover()
        broadcast_cluster_state()
        return {"message": f"Worker '{worker_id}' rebooted and rejoined the cluster."}


@app.post("/api/workers/spawn")
def spawn_worker():
    """Dynamically spawn an additional worker node."""
    new_id = spawn_dynamic_worker(is_dynamic=False)
    broadcast_cluster_state()
    return {"message": f"Spawned new worker node '{new_id}'", "worker_id": new_id}


# -----------------------------------------------------------------------------
# High Availability & Leader Election Endpoints
# -----------------------------------------------------------------------------

@app.get("/api/ha/status")
def get_ha_status():
    """Retrieve HA cluster leader election status and node roles."""
    return coordinator.get_ha_stats()


@app.post("/api/ha/kill_leader")
def kill_leader_coordinator():
    """Chaos engineering: Simulate fatal failure of the active Leader Coordinator."""
    msg = coordinator.simulate_crash_leader()
    broadcast_cluster_state()
    return {"message": msg, "ha": coordinator.get_ha_stats()}


@app.post("/api/ha/recover")
def recover_crashed_coordinator(node_id: Optional[str] = None):
    """Reboot a crashed coordinator node, which safely rejoins as STANDBY."""
    msg = coordinator.recover_coordinator(node_id)
    broadcast_cluster_state()
    return {"message": msg, "ha": coordinator.get_ha_stats()}


@app.post("/api/ha/failover")
def manual_coordinator_failover():
    """Gracefully transition leadership from active coordinator to standby coordinator."""
    try:
        msg = coordinator.trigger_manual_failover()
        broadcast_cluster_state()
        return {"message": msg, "ha": coordinator.get_ha_stats()}
    except RuntimeError as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/api/metrics")
def get_performance_metrics():
    """Retrieve real-time performance analytics, throughput history, and latency stats."""
    return coordinator.metrics_collector.get_metrics()


@app.websocket("/ws")


async def websocket_telemetry(websocket: WebSocket):
    """Real-time bi-directional streaming telemetry and state push over WebSocket."""
    await manager.connect(websocket)
    try:
        # Push initial snapshot immediately upon handshake
        await websocket.send_json({
            "type": "INITIAL_SNAPSHOT",
            "stats": _get_current_stats_snapshot(),
            "tasks": _get_current_tasks_snapshot(),
        })
        while True:
            # Keep socket open and receive client heartbeats/pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception:
        manager.disconnect(websocket)


# -----------------------------------------------------------------------------
# Frontend Dashboard (HTML + CSS + JavaScript)
# -----------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index_page():
    return HTML_CONTENT


HTML_CONTENT = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Distributed Task Orchestrator | Command Center</title>
  <link rel="preconnect" href="https://fonts.googleapis.com">
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
  <link href="https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500;700&display=swap" rel="stylesheet">
  <style>
    :root {
      --bg-base: #090d16;
      --bg-surface: #111827;
      --bg-card: rgba(17, 24, 39, 0.7);
      --border-color: rgba(255, 255, 255, 0.08);
      --border-highlight: rgba(99, 102, 241, 0.3);
      --text-main: #f3f4f6;
      --text-muted: #9ca3af;
      --primary: #6366f1;
      --primary-hover: #4f46e5;
      --accent-cyan: #06b6d4;
      --status-active: #10b981;
      --status-suspected: #f59e0b;
      --status-dead: #ef4444;
      --status-pending: #8b5cf6;
      --glass-blur: blur(12px);
    }

    * {
      box-sizing: border-box;
      margin: 0;
      padding: 0;
    }

    body {
      font-family: 'Plus Jakarta Sans', -apple-system, BlinkMacSystemFont, sans-serif;
      background-color: var(--bg-base);
      color: var(--text-main);
      min-height: 100vh;
      overflow-x: hidden;
      background-image: 
        radial-gradient(circle at 10% 10%, rgba(99, 102, 241, 0.08) 0%, transparent 40%),
        radial-gradient(circle at 90% 80%, rgba(6, 182, 212, 0.06) 0%, transparent 40%);
    }

    .container {
      max-width: 1400px;
      margin: 0 auto;
      padding: 24px;
    }

    /* Header */
    header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding-bottom: 24px;
      border-bottom: 1px solid var(--border-color);
      margin-bottom: 28px;
    }

    .logo-area {
      display: flex;
      align-items: center;
      gap: 14px;
    }

    .logo-badge {
      width: 44px;
      height: 44px;
      border-radius: 12px;
      background: linear-gradient(135deg, #6366f1, #06b6d4);
      display: flex;
      align-items: center;
      justify-content: center;
      font-size: 22px;
      box-shadow: 0 4px 20px rgba(99, 102, 241, 0.35);
    }

    h1 {
      font-size: 22px;
      font-weight: 700;
      letter-spacing: -0.5px;
    }

    .subtitle {
      font-size: 13px;
      color: var(--text-muted);
    }

    .cluster-status-pill {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 6px 14px;
      border-radius: 9999px;
      background: rgba(16, 185, 129, 0.12);
      border: 1px solid rgba(16, 185, 129, 0.25);
      color: #34d399;
      font-size: 12px;
      font-weight: 600;
    }

    .pulse-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background-color: var(--status-active);
      animation: pulse 2s infinite;
    }

    @keyframes pulse {
      0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0.7); }
      70% { transform: scale(1); box-shadow: 0 0 0 8px rgba(16, 185, 129, 0); }
      100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(16, 185, 129, 0); }
    }

    /* Metric Cards Grid */
    .metrics-grid {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
      gap: 16px;
      margin-bottom: 28px;
    }

    .metric-card {
      background: var(--bg-card);
      backdrop-filter: var(--glass-blur);
      border: 1px solid var(--border-color);
      border-radius: 16px;
      padding: 20px;
      position: relative;
      overflow: hidden;
      transition: transform 0.2s ease, border-color 0.2s ease;
    }

    .metric-card:hover {
      transform: translateY(-2px);
      border-color: var(--border-highlight);
    }

    .metric-label {
      font-size: 13px;
      font-weight: 500;
      color: var(--text-muted);
      margin-bottom: 8px;
      display: flex;
      justify-content: space-between;
      align-items: center;
    }

    .metric-value {
      font-size: 32px;
      font-weight: 800;
      letter-spacing: -1px;
      color: #fff;
    }

    .metric-sub {
      font-size: 12px;
      margin-top: 6px;
      color: var(--text-muted);
    }

    /* Main Two-Column Layout */
    .dashboard-layout {
      display: grid;
      grid-template-columns: 1.8fr 1.2fr;
      gap: 24px;
      margin-bottom: 28px;
    }

    @media (max-width: 1024px) {
      .dashboard-layout {
        grid-template-columns: 1fr;
      }
    }

    .panel {
      background: var(--bg-card);
      backdrop-filter: var(--glass-blur);
      border: 1px solid var(--border-color);
      border-radius: 18px;
      padding: 22px;
    }

    .panel-header {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 18px;
    }

    .panel-title {
      font-size: 16px;
      font-weight: 700;
      display: flex;
      align-items: center;
      gap: 8px;
    }

    /* Worker Fleet Cards */
    .workers-grid {
      display: grid;
      grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
      gap: 16px;
    }

    .worker-card {
      background: rgba(255, 255, 255, 0.02);
      border: 1px solid var(--border-color);
      border-radius: 14px;
      padding: 16px;
      transition: all 0.2s ease;
      position: relative;
    }

    .worker-card.status-active {
      border-left: 4px solid var(--status-active);
    }
    .worker-card.status-suspected {
      border-left: 4px solid var(--status-suspected);
    }
    .worker-card.status-dead {
      border-left: 4px solid var(--status-dead);
      opacity: 0.75;
      background: rgba(239, 68, 68, 0.03);
    }

    .worker-top {
      display: flex;
      justify-content: space-between;
      align-items: center;
      margin-bottom: 12px;
    }

    .worker-id {
      font-family: 'JetBrains Mono', monospace;
      font-size: 13px;
      font-weight: 700;
    }

    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      text-transform: uppercase;
      letter-spacing: 0.5px;
    }

    .badge-active { background: rgba(16, 185, 129, 0.2); color: #34d399; }
    .badge-suspected { background: rgba(245, 158, 11, 0.2); color: #fbbf24; }
    .badge-dead { background: rgba(239, 68, 68, 0.2); color: #f87171; }

    .worker-meta {
      font-size: 12px;
      color: var(--text-muted);
      line-height: 1.6;
      margin-bottom: 14px;
    }

    .worker-task-info {
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      color: var(--accent-cyan);
      background: rgba(6, 182, 212, 0.08);
      padding: 4px 8px;
      border-radius: 6px;
      margin-bottom: 14px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .worker-actions {
      display: flex;
      gap: 8px;
    }

    .btn {
      cursor: pointer;
      border: none;
      border-radius: 8px;
      padding: 7px 12px;
      font-size: 12px;
      font-weight: 600;
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      transition: all 0.15s ease;
      text-decoration: none;
    }

    .btn:active {
      transform: scale(0.97);
    }

    .btn-primary {
      background: var(--primary);
      color: #fff;
    }
    .btn-primary:hover { background: var(--primary-hover); }

    .btn-danger {
      background: rgba(239, 68, 68, 0.15);
      border: 1px solid rgba(239, 68, 68, 0.3);
      color: #f87171;
    }
    .btn-danger:hover {
      background: rgba(239, 68, 68, 0.3);
    }

    .btn-success {
      background: rgba(16, 185, 129, 0.15);
      border: 1px solid rgba(16, 185, 129, 0.3);
      color: #34d399;
    }
    .btn-success:hover {
      background: rgba(16, 185, 129, 0.3);
    }

    .btn-outline {
      background: rgba(255, 255, 255, 0.04);
      border: 1px solid var(--border-color);
      color: var(--text-main);
    }
    .btn-outline:hover {
      background: rgba(255, 255, 255, 0.08);
    }

    /* Action Deck Panel */
    .form-group {
      margin-bottom: 14px;
    }

    .form-label {
      display: block;
      font-size: 12px;
      color: var(--text-muted);
      margin-bottom: 6px;
      font-weight: 500;
    }

    .form-select, .form-input {
      width: 100%;
      background: rgba(0, 0, 0, 0.25);
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 8px 12px;
      color: #fff;
      font-family: inherit;
      font-size: 13px;
      outline: none;
    }

    .form-select:focus, .form-input:focus {
      border-color: var(--primary);
    }

    .chaos-deck {
      margin-top: 20px;
      padding-top: 18px;
      border-top: 1px solid var(--border-color);
    }

    /* Live Event Logs */
    .log-box {
      background: #06090e;
      border: 1px solid var(--border-color);
      border-radius: 12px;
      padding: 12px;
      height: 240px;
      overflow-y: auto;
      font-family: 'JetBrains Mono', monospace;
      font-size: 11px;
      line-height: 1.5;
    }

    .log-entry {
      padding: 3px 0;
      border-bottom: 1px solid rgba(255, 255, 255, 0.02);
      display: flex;
      gap: 8px;
    }

    .log-time { color: #6b7280; }
    .log-source { color: #818cf8; font-weight: 600; }
    .log-msg { color: #d1d5db; word-break: break-all; }
    .level-ERROR .log-msg { color: #f87171; }
    .level-WARNING .log-msg { color: #fbbf24; }
    .level-SUCCESS .log-msg { color: #34d399; }

    /* Task Table */
    .table-container {
      overflow-x: auto;
      margin-top: 14px;
    }

    table {
      width: 100%;
      border-collapse: collapse;
      text-align: left;
      font-size: 12px;
    }

    th {
      padding: 10px 12px;
      background: rgba(255, 255, 255, 0.02);
      color: var(--text-muted);
      font-weight: 600;
      border-bottom: 1px solid var(--border-color);
    }

    td {
      padding: 10px 12px;
      border-bottom: 1px solid var(--border-color);
    }

    .task-status-pill {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 10px;
      font-weight: 700;
      text-transform: uppercase;
    }

    .status-PENDING { background: rgba(139, 92, 246, 0.2); color: #a78bfa; }
    .status-IN_PROGRESS { background: rgba(6, 182, 212, 0.2); color: #38bdf8; }
    .status-COMPLETED { background: rgba(16, 185, 129, 0.2); color: #34d399; }
    .status-FAILED { background: rgba(239, 68, 68, 0.2); color: #f87171; }
    .status-DEAD_LETTER { background: rgba(244, 63, 94, 0.25); color: #fb7185; border: 1px solid rgba(244, 63, 94, 0.4); }

    .priority-pill {
      display: inline-block;
      padding: 2px 7px;
      border-radius: 5px;
      font-size: 10px;
      font-weight: 700;
      letter-spacing: 0.5px;
    }
    .priority-1 { background: rgba(239, 68, 68, 0.25); color: #f87171; border: 1px solid rgba(239, 68, 68, 0.4); }
    .priority-2 { background: rgba(245, 158, 11, 0.25); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4); }
    .priority-3 { background: rgba(99, 102, 241, 0.2); color: #a5b4fc; }
    .priority-4 { background: rgba(156, 163, 175, 0.15); color: #9ca3af; }

    .toast {
      position: fixed;
      bottom: 24px;
      right: 24px;
      background: #1f2937;
      color: #fff;
      border: 1px solid var(--border-color);
      border-radius: 8px;
      padding: 12px 18px;
      font-size: 13px;
      box-shadow: 0 10px 30px rgba(0,0,0,0.5);
      transform: translateY(100px);
      opacity: 0;
      transition: all 0.3s ease;
      z-index: 999;
    }
    .toast.show {
      transform: translateY(0);
      opacity: 1;
    }
  </style>
</head>
<body>

  <div class="container">
    <!-- Header -->
    <header>
      <div class="logo-area">
        <div class="logo-badge">⚡</div>
        <div>
          <h1>Distributed Task Orchestrator</h1>
          <div class="subtitle">Fault-Tolerant Leader & Worker Fleet Control Center</div>
        </div>
      </div>
      <div style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
        <div class="cluster-status-pill" id="ws-status-pill" style="border-color: rgba(16, 185, 129, 0.4); background: rgba(16, 185, 129, 0.12); color: #34d399;">
          <div class="pulse-dot" id="ws-pulse-dot" style="background-color: #10b981;"></div>
          <span id="ws-status-text">⚡ WebSocket Live</span>
        </div>
        <div class="cluster-status-pill" style="border-color: rgba(99, 102, 241, 0.4); background: rgba(99, 102, 241, 0.12); color: #a5b4fc;">
          <div class="pulse-dot" style="background-color: #818cf8;"></div>
          <span>Auto-Scaler: <b>ACTIVE</b> (3–8 nodes)</span>
        </div>
        <div class="cluster-status-pill">
          <div class="pulse-dot"></div>
          <span id="cluster-name">cluster-production-01</span>
        </div>
      </div>
    </header>

    <!-- Stat Metric Cards -->
    <div class="metrics-grid">
      <div class="metric-card">
        <div class="metric-label">Active Workers <span>🟢</span></div>
        <div class="metric-value" id="val-active-workers">-</div>
        <div class="metric-sub" id="val-total-workers">of - total nodes</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Queue Depth (Pending) <span>⏳</span></div>
        <div class="metric-value" id="val-pending-tasks" style="color: #a78bfa;">-</div>
        <div class="metric-sub">waiting in FIFO buffer</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">In-Progress Tasks <span>⚡</span></div>
        <div class="metric-value" id="val-in-progress-tasks" style="color: #38bdf8;">-</div>
        <div class="metric-sub">currently running on workers</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Tasks Completed <span>✅</span></div>
        <div class="metric-value" id="val-completed-tasks" style="color: #34d399;">-</div>
        <div class="metric-sub" id="val-total-tasks">of - submitted</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">Dead-Letter Queue (DLQ) <span>⚠️</span></div>
        <div class="metric-value" id="val-dlq-tasks" style="color: #fb7185;">-</div>
        <div class="metric-sub">quarantined bad tasks</div>
      </div>
    </div>

    <!-- High Availability & Leader Election (Step 5) -->
    <div class="panel" style="margin-bottom: 24px; border: 1px solid rgba(245, 158, 11, 0.35); background: linear-gradient(180deg, rgba(30, 27, 75, 0.35) 0%, rgba(17, 24, 39, 0.7) 100%);">
      <div class="panel-header" style="flex-wrap: wrap; gap: 12px;">
        <div class="panel-title" style="display: flex; align-items: center; gap: 10px;">
          <span style="font-size: 20px;">🛡️</span>
          <div>
            <div style="font-size: 15px; font-weight: 700;">High Availability & Standby Coordinator (Zero SPOF)</div>
            <div style="font-size: 12px; color: var(--text-muted);">Heartbeat Lease Consensus • Automatic Failover • Zero Task Loss</div>
          </div>
        </div>
        <div style="display: flex; align-items: center; gap: 10px; flex-wrap: wrap;">
          <div class="cluster-status-pill" id="ha-term-pill" style="border-color: rgba(245, 158, 11, 0.4); background: rgba(245, 158, 11, 0.12); color: #fbbf24;">
            <span>👑 Term: <b id="ha-term-val">#1</b></span>
          </div>
          <div class="cluster-status-pill" id="ha-lease-pill" style="border-color: rgba(6, 182, 212, 0.4); background: rgba(6, 182, 212, 0.12); color: #38bdf8;">
            <span>⏱️ Leader Lease: <b id="ha-lease-val">3.0s</b> remaining</span>
          </div>
        </div>
      </div>

      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 16px; margin-top: 14px;">
        <!-- Primary Coordinator Card -->
        <div class="ha-node-card" id="card-coord-primary" style="background: rgba(0,0,0,0.3); border: 1px solid var(--border-color); border-radius: 12px; padding: 16px; transition: all 0.3s ease;">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
            <div style="font-weight: 700; font-size: 14px; display: flex; align-items: center; gap: 8px;">
              <span>🏛️</span> coordinator-primary
            </div>
            <span class="ha-role-badge" id="badge-coord-primary" style="padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 800;">LEADER</span>
          </div>
          <div style="font-size: 12px; color: var(--text-muted); line-height: 1.4;" id="desc-coord-primary">
            Active leader driving heartbeat detection, queue scheduling, and autoscaling.
          </div>
        </div>

        <!-- Standby Coordinator Card -->
        <div class="ha-node-card" id="card-coord-standby" style="background: rgba(0,0,0,0.3); border: 1px solid var(--border-color); border-radius: 12px; padding: 16px; transition: all 0.3s ease;">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 10px;">
            <div style="font-weight: 700; font-size: 14px; display: flex; align-items: center; gap: 8px;">
              <span>🛡️</span> coordinator-standby
            </div>
            <span class="ha-role-badge" id="badge-coord-standby" style="padding: 4px 10px; border-radius: 6px; font-size: 11px; font-weight: 800;">STANDBY</span>
          </div>
          <div style="font-size: 12px; color: var(--text-muted); line-height: 1.4;" id="desc-coord-standby">
            Hot standby watchdog. Ready for immediate promotion upon lease expiration.
          </div>
        </div>

        <!-- HA Chaos Actions -->
        <div style="background: rgba(0,0,0,0.3); border: 1px solid var(--border-color); border-radius: 12px; padding: 16px; display: flex; flex-direction: column; justify-content: center; gap: 10px;">
          <div style="font-size: 11px; font-weight: 700; color: #f59e0b; text-transform: uppercase; letter-spacing: 0.5px;">⚡ HA Chaos Testing</div>
          <div style="display: flex; gap: 8px; flex-wrap: wrap;">
            <button class="btn btn-outline" style="border-color: rgba(239, 68, 68, 0.5); color: #f87171; flex: 1; padding: 6px 10px; font-size: 12px;" onclick="killActiveLeader()">
              💥 Kill Leader
            </button>
            <button class="btn btn-outline" style="border-color: rgba(16, 185, 129, 0.5); color: #34d399; flex: 1; padding: 6px 10px; font-size: 12px;" onclick="recoverCrashedCoordinator()">
              🔄 Reboot Node
            </button>
            <button class="btn btn-outline" style="border-color: rgba(99, 102, 241, 0.5); color: #a5b4fc; flex: 1; padding: 6px 10px; font-size: 12px;" onclick="triggerFailover()">
              🔀 Failover
            </button>
          </div>
        </div>
      </div>
    </div>

    <!-- Step 6: Live Performance Charts & Throughput Analytics -->
    <div class="panel" style="margin-bottom: 24px; border: 1px solid rgba(6, 182, 212, 0.35); background: linear-gradient(180deg, rgba(8, 30, 48, 0.4) 0%, rgba(17, 24, 39, 0.7) 100%);">
      <div class="panel-header" style="flex-wrap: wrap; gap: 12px;">
        <div class="panel-title" style="display: flex; align-items: center; gap: 10px;">
          <span style="font-size: 20px;">📈</span>
          <div>
            <div style="font-size: 15px; font-weight: 700;">Live Performance Charts & Cluster Analytics</div>
            <div style="font-size: 12px; color: var(--text-muted);">Real-Time Throughput • Execution Latency (P95) • Worker Fleet Utilization</div>
          </div>
        </div>
        <div style="display: flex; gap: 10px; align-items: center; flex-wrap: wrap;">
          <div class="cluster-status-pill" style="border-color: rgba(6, 182, 212, 0.4); background: rgba(6, 182, 212, 0.12); color: #38bdf8;">
            <span>🚀 Throughput: <b id="kpi-tps">0.0</b>/s (<b id="kpi-tpm">0</b>/min)</span>
          </div>
          <div class="cluster-status-pill" style="border-color: rgba(245, 158, 11, 0.4); background: rgba(245, 158, 11, 0.12); color: #fbbf24;">
            <span>⏱️ Avg Latency: <b id="kpi-latency">0 ms</b></span>
          </div>
          <div class="cluster-status-pill" style="border-color: rgba(16, 185, 129, 0.4); background: rgba(16, 185, 129, 0.12); color: #34d399;">
            <span>🎯 Success Rate: <b id="kpi-success">100%</b></span>
          </div>
          <div class="cluster-status-pill" style="border-color: rgba(139, 92, 246, 0.4); background: rgba(139, 92, 246, 0.12); color: #a78bfa;">
            <span>📊 Fleet Busy: <b id="kpi-utilization">0%</b></span>
          </div>
        </div>
      </div>

      <!-- Dual Canvas Charts Grid -->
      <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(420px, 1fr)); gap: 18px; margin-top: 14px;">
        <!-- Chart 1: Throughput & Queue Pressure -->
        <div style="background: rgba(0,0,0,0.35); border: 1px solid var(--border-color); border-radius: 12px; padding: 16px;">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">
            <div style="font-size: 13px; font-weight: 700; color: #38bdf8;">
              ⚡ Throughput & Queue Pressure (Last 30s)
            </div>
            <div style="display: flex; gap: 12px; font-size: 11px;">
              <span style="color: #06b6d4; font-weight: 600;">● Throughput (TPS)</span>
              <span style="color: #a78bfa; font-weight: 600;">● Queue Depth</span>
              <span style="color: #60a5fa; font-weight: 600;">● In-Flight</span>
            </div>
          </div>
          <div style="position: relative; height: 160px; width: 100%;">
            <canvas id="throughputCanvas" style="width: 100%; height: 160px; display: block;"></canvas>
          </div>
        </div>

        <!-- Chart 2: Worker Utilization & Latency -->
        <div style="background: rgba(0,0,0,0.35); border: 1px solid var(--border-color); border-radius: 12px; padding: 16px;">
          <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 12px; flex-wrap: wrap; gap: 8px;">
            <div style="font-size: 13px; font-weight: 700; color: #34d399;">
              📊 Worker Utilization & Latency Response (Last 30s)
            </div>
            <div style="display: flex; gap: 12px; font-size: 11px;">
              <span style="color: #10b981; font-weight: 600;">● Fleet Busy (%)</span>
              <span style="color: #fbbf24; font-weight: 600;">● Latency (ms)</span>
            </div>
          </div>
          <div style="position: relative; height: 160px; width: 100%;">
            <canvas id="utilizationCanvas" style="width: 100%; height: 160px; display: block;"></canvas>
          </div>
        </div>
      </div>
    </div>

    <!-- Main Two-Column Layout -->
    <div class="dashboard-layout">

      <!-- Left: Worker Fleet Grid -->
      <div class="panel">
        <div class="panel-header">
          <div class="panel-title">
            <span>🖥️</span> Worker Fleet & Heartbeats
          </div>
          <button class="btn btn-outline" onclick="spawnNewWorker()">➕ Add Worker Node</button>
        </div>
        
        <div class="workers-grid" id="workers-container">
          <!-- Worker cards populated dynamically -->
        </div>
      </div>

      <!-- Right: Control Deck & Chaos Engineering -->
      <div class="panel">
        <div class="panel-header">
          <div class="panel-title">
            <span>🎮</span> Task Dispatcher & Chaos Simulator
          </div>
        </div>

        <div class="form-group">
          <label class="form-label">Task Type to Submit</label>
          <select class="form-select" id="task-select">
            <option value="resize_image">🖼️ resize_image (Fast image thumbnailing)</option>
            <option value="generate_pdf_report">📄 generate_pdf_report (PDF report compilation)</option>
            <option value="flaky_task">🎲 flaky_task (Simulates random network timeout)</option>
            <option value="poison_pill">☠️ poison_pill (Guaranteed failure -> tests DLQ)</option>
          </select>
        </div>

        <div class="form-group">
          <label class="form-label">Scheduling Priority (VIP Fast-Pass)</label>
          <select class="form-select" id="priority-select">
            <option value="1">🔴 CRITICAL (Jumps ahead of all tasks)</option>
            <option value="2">🟠 HIGH (High priority)</option>
            <option value="3" selected>🔵 NORMAL (Standard scheduling)</option>
            <option value="4">⚪ LOW (Background batch)</option>
          </select>
        </div>

        <div style="display: flex; flex-direction: column; gap: 8px;">
          <button class="btn btn-primary" onclick="submitSingleTask()">
            🚀 Submit Single Task
          </button>
          <div style="display: flex; gap: 8px;">
            <button class="btn btn-outline" style="flex: 1;" onclick="submitBurst()">
              ⚡ Queue Burst (6 Tasks)
            </button>
            <button class="btn btn-outline" style="flex: 1; border-color: rgba(6, 182, 212, 0.4); color: #38bdf8;" onclick="submitSurge()">
              🌊 Surge (10 Tasks -> Auto-Scale)
            </button>
          </div>
        </div>

        <!-- Chaos Instructions -->
        <div class="chaos-deck">
          <div style="font-size: 13px; font-weight: 700; margin-bottom: 6px; color: #f59e0b;">
            💥 How to test Fault Tolerance:
          </div>
          <p style="font-size: 12px; color: var(--text-muted); line-height: 1.5; margin-bottom: 12px;">
            1. Click <b>"Queue Burst"</b> to send tasks.<br>
            2. Immediately click <b>"💥 Crash"</b> on a busy worker.<br>
            3. Watch the Coordinator notice the missing heartbeats, declare the node <b>DEAD</b>, and automatically reassign the task to a healthy worker!
          </p>
        </div>

        <!-- Live Cluster Events Log -->
        <div style="margin-top: 18px;">
          <div style="font-size: 13px; font-weight: 600; margin-bottom: 8px;">📡 Live Coordinator Audit Log</div>
          <div class="log-box" id="event-log">
            <!-- Event entries inserted here -->
          </div>
        </div>
      </div>
    </div>

    <!-- Bottom: Task Pipeline Stream Table -->
    <div class="panel">
      <div class="panel-header">
        <div class="panel-title">
          <span>📋</span> Recent Task Pipeline
        </div>
        <div style="font-size: 12px; color: var(--text-muted);">Auto-refreshes live every 1 second</div>
      </div>

      <div class="table-container">
        <table>
          <thead>
            <tr>
              <th>Task ID</th>
              <th>Handler</th>
              <th>Priority</th>
              <th>Status</th>
              <th>Worker</th>
              <th>Retries</th>
              <th>Execution Time</th>
              <th>Result / Diagnostic Error</th>
            </tr>
          </thead>
          <tbody id="task-table-body">
            <!-- Tasks populated dynamically -->
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="toast" id="toast">Task dispatched</div>

  <script>
    const prioNames = { 1: 'CRITICAL', 2: 'HIGH', 3: 'NORMAL', 4: 'LOW' };
    let ws = null;
    let wsReconnectTimer = null;

    function showToast(msg) {
      const toast = document.getElementById('toast');
      toast.innerText = msg;
      toast.classList.add('show');
      setTimeout(() => toast.classList.remove('show'), 2800);
    }

    function renderStats(data) {
      // Update Stat Cards
      document.getElementById('val-active-workers').innerText = data.workers.active;
      document.getElementById('val-total-workers').innerText = `of ${data.workers.total} total nodes`;
      document.getElementById('val-pending-tasks').innerText = data.tasks.pending;
      document.getElementById('val-in-progress-tasks').innerText = data.tasks.in_progress;
      document.getElementById('val-completed-tasks').innerText = data.tasks.completed;
      document.getElementById('val-total-tasks').innerText = `of ${data.tasks.total} submitted`;
      document.getElementById('val-dlq-tasks').innerText = data.tasks.dlq;
      document.getElementById('cluster-name').innerText = data.cluster_id;

      // Render Workers
      renderWorkers(data.workers.list);

      // Render Event Logs
      renderLogs(data.recent_events);

      // Render High Availability & Leader Election
      if (data.ha) {
        renderHA(data.ha);
      }

      // Render Performance & Throughput Analytics
      if (data.metrics) {
        renderMetrics(data.metrics);
      }
    }

    function renderMetrics(metrics) {
      if (!metrics) return;
      if (metrics.summary) {
        const s = metrics.summary;
        const tpsEl = document.getElementById('kpi-tps');
        const tpmEl = document.getElementById('kpi-tpm');
        const latEl = document.getElementById('kpi-latency');
        const succEl = document.getElementById('kpi-success');
        const utilEl = document.getElementById('kpi-utilization');

        if (tpsEl) tpsEl.innerText = s.throughput_tps.toFixed(1);
        if (tpmEl) tpmEl.innerText = s.throughput_tpm;
        if (latEl) latEl.innerText = `${s.avg_latency_ms.toFixed(0)} ms`;
        if (succEl) succEl.innerText = `${s.success_rate_pct.toFixed(0)}%`;
        if (utilEl) utilEl.innerText = `${s.utilization_pct.toFixed(0)}%`;
      }

      if (metrics.history && metrics.history.length > 0) {
        let hist = metrics.history;
        if (hist.length < 15) {
          const padCount = 15 - hist.length;
          const dummy = [];
          for (let i = 0; i < padCount; i++) {
            dummy.push({ throughput_tps: 0, queue_depth: 0, in_progress: 0, utilization_pct: 0, avg_latency_ms: 0 });
          }
          hist = dummy.concat(hist);
        }

        const tpsData = hist.map(p => p.throughput_tps);
        const queueData = hist.map(p => p.queue_depth);
        const inFlightData = hist.map(p => p.in_progress);

        drawChart('throughputCanvas', [
          { name: 'Throughput', data: tpsData, color: '#06b6d4', fillColor: 'rgba(6, 182, 212, 0.2)' },
          { name: 'Queue Depth', data: queueData, color: '#a78bfa' },
          { name: 'In-Flight', data: inFlightData, color: '#60a5fa' }
        ]);

        const utilData = hist.map(p => p.utilization_pct);
        const latData = hist.map(p => p.avg_latency_ms);

        drawChart('utilizationCanvas', [
          { name: 'Utilization', data: utilData, color: '#10b981', fillColor: 'rgba(16, 185, 129, 0.18)' },
          { name: 'Latency', data: latData, color: '#fbbf24' }
        ], 100);
      }
    }

    function drawChart(canvasId, seriesList, maxValOverride = null) {
      const canvas = document.getElementById(canvasId);
      if (!canvas) return;
      const ctx = canvas.getContext('2d');
      const dpr = window.devicePixelRatio || 1;
      const rect = canvas.getBoundingClientRect();

      if (rect.width === 0 || rect.height === 0) return;

      if (canvas.width !== Math.floor(rect.width * dpr) || canvas.height !== Math.floor(rect.height * dpr)) {
        canvas.width = Math.floor(rect.width * dpr);
        canvas.height = Math.floor(rect.height * dpr);
      }
      ctx.resetTransform();
      ctx.scale(dpr, dpr);
      const w = rect.width;
      const h = rect.height;

      ctx.clearRect(0, 0, w, h);

      // Grid Lines
      ctx.strokeStyle = 'rgba(255, 255, 255, 0.05)';
      ctx.lineWidth = 1;
      for (let i = 1; i <= 3; i++) {
        const y = (h / 4) * i;
        ctx.beginPath();
        ctx.moveTo(32, y);
        ctx.lineTo(w, y);
        ctx.stroke();
      }

      // Max value scaling
      let maxVal = maxValOverride || 1;
      seriesList.forEach(s => {
        s.data.forEach(v => { if (v > maxVal) maxVal = v; });
      });
      maxVal = Math.ceil(maxVal * 1.15) || 1;

      // Axis label text
      ctx.fillStyle = '#6b7280';
      ctx.font = '10px JetBrains Mono, monospace';
      ctx.textAlign = 'right';
      ctx.fillText(maxVal >= 1000 ? (maxVal/1000).toFixed(1) + 'k' : maxVal, 28, 12);
      ctx.fillText(Math.round(maxVal / 2), 28, h / 2 + 3);
      ctx.fillText('0', 28, h - 4);

      const leftPad = 34;
      const chartW = w - leftPad - 8;
      const chartH = h - 20;

      seriesList.forEach(series => {
        const pts = series.data;
        if (!pts || pts.length < 2) return;
        const stepX = chartW / (pts.length - 1);

        // Filled area
        if (series.fillColor) {
          ctx.beginPath();
          ctx.moveTo(leftPad, h - 4);
          pts.forEach((v, i) => {
            const x = leftPad + i * stepX;
            const y = (h - 4) - (Math.min(v, maxVal) / maxVal) * chartH;
            ctx.lineTo(x, y);
          });
          ctx.lineTo(leftPad + (pts.length - 1) * stepX, h - 4);
          ctx.closePath();
          const grad = ctx.createLinearGradient(0, 0, 0, h);
          grad.addColorStop(0, series.fillColor);
          grad.addColorStop(1, 'rgba(0,0,0,0)');
          ctx.fillStyle = grad;
          ctx.fill();
        }

        // Line curve
        ctx.beginPath();
        ctx.strokeStyle = series.color;
        ctx.lineWidth = 2.2;
        ctx.shadowBlur = 8;
        ctx.shadowColor = series.color;

        pts.forEach((v, i) => {
          const x = leftPad + i * stepX;
          const y = (h - 4) - (Math.min(v, maxVal) / maxVal) * chartH;
          if (i === 0) ctx.moveTo(x, y);
          else ctx.lineTo(x, y);
        });
        ctx.stroke();
        ctx.shadowBlur = 0;

        // Pulse dot on latest point
        if (pts.length > 0) {
          const lastIdx = pts.length - 1;
          const lastX = leftPad + lastIdx * stepX;
          const lastY = (h - 4) - (Math.min(pts[lastIdx], maxVal) / maxVal) * chartH;
          ctx.beginPath();
          ctx.arc(lastX, lastY, 3.5, 0, Math.PI * 2);
          ctx.fillStyle = series.color;
          ctx.fill();
        }
      });
    }


    function renderHA(ha) {
      if (!ha) return;
      document.getElementById('ha-term-val').innerText = `#${ha.term}`;
      document.getElementById('ha-lease-val').innerText = `${ha.lease_remaining.toFixed(1)}s`;

      function updateNodeCard(prefix, node) {
        const card = document.getElementById(`card-coord-${prefix}`);
        const badge = document.getElementById(`badge-coord-${prefix}`);
        const desc = document.getElementById(`desc-coord-${prefix}`);
        if (!card || !badge || !desc) return;

        if (node.role === 'LEADER') {
          card.style.borderColor = 'rgba(245, 158, 11, 0.7)';
          card.style.boxShadow = '0 0 20px rgba(245, 158, 11, 0.15)';
          card.style.background = 'rgba(245, 158, 11, 0.05)';
          badge.style.background = 'rgba(245, 158, 11, 0.2)';
          badge.style.color = '#fbbf24';
          badge.style.border = '1px solid rgba(245, 158, 11, 0.4)';
          badge.innerText = '👑 ACTIVE LEADER';
          desc.innerText = 'Currently driving heartbeat failure detection, task dispatching, and auto-scaler.';
        } else if (node.role === 'STANDBY') {
          card.style.borderColor = 'rgba(99, 102, 241, 0.3)';
          card.style.boxShadow = 'none';
          card.style.background = 'rgba(0, 0, 0, 0.25)';
          badge.style.background = 'rgba(99, 102, 241, 0.2)';
          badge.style.color = '#a5b4fc';
          badge.style.border = '1px solid rgba(99, 102, 241, 0.3)';
          badge.innerText = '💤 HOT STANDBY';
          desc.innerText = 'Watchdog monitoring active leader lease. Instantly assumes leadership if leader fails.';
        } else {
          card.style.borderColor = 'rgba(239, 68, 68, 0.6)';
          card.style.boxShadow = '0 0 15px rgba(239, 68, 68, 0.2)';
          card.style.background = 'rgba(239, 68, 68, 0.05)';
          badge.style.background = 'rgba(239, 68, 68, 0.25)';
          badge.style.color = '#f87171';
          badge.style.border = '1px solid rgba(239, 68, 68, 0.4)';
          badge.innerText = '💀 CRASHED / DEAD';
          desc.innerText = 'Node is offline/unresponsive. Heartbeat lease expired. Failover triggered.';
        }
      }

      updateNodeCard('primary', ha.primary);
      updateNodeCard('standby', ha.standby);
    }


    function renderTasks(tasks) {
      const tbody = document.getElementById('task-table-body');
      if (!tasks || tasks.length === 0) {
        tbody.innerHTML = `<tr><td colspan="8" style="text-align: center; color: var(--text-muted); padding: 24px;">No tasks queued yet. Submit one above to begin!</td></tr>`;
        return;
      }

      const nowSec = Date.now() / 1000;
      tbody.innerHTML = tasks.map(t => {
        let diagnostic = "-";
        if (t.error) {
          diagnostic = `<span style="color: #f87171; font-family: monospace;">${t.error}</span>`;
        } else if (t.result) {
          diagnostic = `<span style="color: #34d399; font-family: monospace;">${JSON.stringify(t.result)}</span>`;
        }

        const prioName = prioNames[t.priority] || 'NORMAL';
        const prioBadge = `<span class="priority-pill priority-${t.priority}">${prioName}</span>`;
        const duration = t.execution_time ? `${t.execution_time.toFixed(2)}s` : '-';
        const worker = t.assigned_worker ? `<code style="color: #818cf8;">${t.assigned_worker}</code>` : `<span style="color: var(--text-muted);">Unassigned</span>`;

        let statusBadge = `<span class="task-status-pill status-${t.status}">${t.status}</span>`;
        if (t.status === 'PENDING' && t.next_retry_at && t.next_retry_at > nowSec) {
          const remaining = (t.next_retry_at - nowSec).toFixed(1);
          statusBadge = `<span class="task-status-pill" style="background: rgba(245, 158, 11, 0.2); color: #fbbf24; border: 1px solid rgba(245, 158, 11, 0.4);">⏳ Backoff (${remaining}s)</span>`;
        }

        return `
          <tr>
            <td><code style="font-size: 11px;">${t.id.substring(0, 8)}</code></td>
            <td><b>${t.name}</b></td>
            <td>${prioBadge}</td>
            <td>${statusBadge}</td>
            <td>${worker}</td>
            <td>${t.retry_count} / ${t.max_retries}</td>
            <td>${duration}</td>
            <td>${diagnostic}</td>
          </tr>
        `;
      }).join('');
    }

    async function fetchStats() {
      try {
        const res = await fetch('/api/stats');
        const data = await res.json();
        renderStats(data);
      } catch (e) {
        console.error("Telemetry error:", e);
      }
    }

    async function fetchTasks() {
      try {
        const res = await fetch('/api/tasks');
        const tasks = await res.json();
        renderTasks(tasks);
      } catch (e) {
        console.error("Task fetch error:", e);
      }
    }

    function renderWorkers(workers) {
      const container = document.getElementById('workers-container');
      container.innerHTML = workers.map(w => {
        const isDead = w.status === 'DEAD' || w.is_simulated_killed;
        const statusClass = w.status === 'ACTIVE' ? 'status-active' : (w.status === 'SUSPECTED_DEAD' ? 'status-suspected' : 'status-dead');
        const badgeClass = w.status === 'ACTIVE' ? 'badge-active' : (w.status === 'SUSPECTED_DEAD' ? 'badge-suspected' : 'badge-dead');
        const currentTask = w.current_task_id ? `⚙️ Working: ${w.current_task_id.substring(0, 8)}...` : '💤 Idle (Waiting for tasks)';
        const dynamicBadge = w.is_dynamic ? `<span class="badge" style="background: rgba(99, 102, 241, 0.25); color: #a5b4fc; border: 1px solid rgba(99, 102, 241, 0.4); margin-right: 4px;">⚡ ELASTIC</span>` : '';

        return `
          <div class="worker-card ${statusClass}">
            <div class="worker-top">
              <span class="worker-id">${w.id}</span>
              <div>
                ${dynamicBadge}
                <span class="badge ${badgeClass}">${w.status}</span>
              </div>
            </div>
            <div class="worker-task-info">${currentTask}</div>
            <div class="worker-meta">
              <div>Last Ping: <b>${w.seconds_since_heartbeat}s ago</b></div>
              <div>Completed: <b>${w.completed_tasks} tasks</b> | Failed: <b>${w.failed_tasks}</b></div>
            </div>
            <div class="worker-actions">
              ${w.is_simulated_killed ? `
                <button class="btn btn-success" style="flex: 1;" onclick="recoverWorker('${w.id}')">
                  🔄 Reboot Node
                </button>
              ` : `
                <button class="btn btn-danger" style="flex: 1;" onclick="killWorker('${w.id}')">
                  💥 Crash / Kill
                </button>
              `}
            </div>
          </div>
        `;
      }).join('');
    }

    function renderLogs(logs) {
      const logBox = document.getElementById('event-log');
      logBox.innerHTML = logs.map(entry => `
        <div class="log-entry level-${entry.level}">
          <span class="log-time">[${entry.time_str}]</span>
          <span class="log-source">${entry.source}:</span>
          <span class="log-msg">${entry.message}</span>
        </div>
      `).join('');
    }

    function connectWebSocket() {
      const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
      const wsUrl = `${protocol}//${window.location.host}/ws`;
      const wsPill = document.getElementById('ws-status-pill');
      const wsDot = document.getElementById('ws-pulse-dot');
      const wsText = document.getElementById('ws-status-text');

      try {
        ws = new WebSocket(wsUrl);

        ws.onopen = () => {
          wsPill.style.borderColor = 'rgba(16, 185, 129, 0.4)';
          wsPill.style.background = 'rgba(16, 185, 129, 0.12)';
          wsPill.style.color = '#34d399';
          wsDot.style.backgroundColor = '#10b981';
          wsText.innerHTML = '⚡ WebSocket <b>LIVE</b>';
          if (wsReconnectTimer) {
            clearTimeout(wsReconnectTimer);
            wsReconnectTimer = null;
          }
        };

        ws.onmessage = (event) => {
          try {
            const payload = JSON.parse(event.data);
            if (payload.stats) renderStats(payload.stats);
            if (payload.tasks) renderTasks(payload.tasks);
          } catch (err) {
            console.error("WS message parse error:", err);
          }
        };

        ws.onclose = () => {
          wsPill.style.borderColor = 'rgba(245, 158, 11, 0.4)';
          wsPill.style.background = 'rgba(245, 158, 11, 0.12)';
          wsPill.style.color = '#fbbf24';
          wsDot.style.backgroundColor = '#f59e0b';
          wsText.innerHTML = '⏳ WS Reconnecting...';

          if (!wsReconnectTimer) {
            wsReconnectTimer = setTimeout(connectWebSocket, 2000);
          }
        };

        ws.onerror = () => {
          if (ws) ws.close();
        };
      } catch (err) {
        if (!wsReconnectTimer) {
          wsReconnectTimer = setTimeout(connectWebSocket, 2000);
        }
      }
    }

    async function submitSingleTask() {
      const name = document.getElementById('task-select').value;
      const priority = parseInt(document.getElementById('priority-select').value);
      const res = await fetch('/api/tasks', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name: name, priority: priority, payload: {} })
      });
      const data = await res.json();
      showToast(`Task '${name}' [${prioNames[priority] || 'NORMAL'}] queued`);
      fetchStats();
      fetchTasks();
    }

    async function submitBurst() {
      const res = await fetch('/api/chaos/burst', { method: 'POST' });
      const data = await res.json();
      showToast(data.message);
      fetchStats();
      fetchTasks();
    }

    async function submitSurge() {
      const res = await fetch('/api/chaos/surge', { method: 'POST' });
      const data = await res.json();
      showToast(data.message);
      fetchStats();
      fetchTasks();
    }

    async function killWorker(workerId) {
      await fetch(`/api/workers/${workerId}/kill`, { method: 'POST' });
      showToast(`Simulated crash for ${workerId}`);
      fetchStats();
    }

    async function recoverWorker(workerId) {
      await fetch(`/api/workers/${workerId}/recover`, { method: 'POST' });
      showToast(`Worker ${workerId} rebooted`);
      fetchStats();
    }

    async function spawnNewWorker() {
      const res = await fetch('/api/workers/spawn', { method: 'POST' });
      const data = await res.json();
      showToast(data.message);
      fetchStats();
    }

    async function killActiveLeader() {
      try {
        const res = await fetch('/api/ha/kill_leader', { method: 'POST' });
        const data = await res.json();
        showToast(data.message);
        fetchStats();
      } catch (e) {
        showToast('Error killing leader: ' + e);
      }
    }

    async function recoverCrashedCoordinator() {
      try {
        const res = await fetch('/api/ha/recover', { method: 'POST' });
        const data = await res.json();
        showToast(data.message);
        fetchStats();
      } catch (e) {
        showToast('Error recovering coordinator: ' + e);
      }
    }

    async function triggerFailover() {
      try {
        const res = await fetch('/api/ha/failover', { method: 'POST' });
        const data = await res.json();
        showToast(data.message);
        fetchStats();
      } catch (e) {
        showToast('Failover error: ' + e);
      }
    }

    // Connect WebSocket stream on load

    connectWebSocket();

    // Fallback polling loop (runs every 3000ms as a safety net if WS reconnects)
    setInterval(() => {
      if (!ws || ws.readyState !== WebSocket.OPEN) {
        fetchStats();
        fetchTasks();
      }
    }, 3000);

    // Initial load fetch
    fetchStats();
    fetchTasks();
  </script>
</body>
</html>
"""
