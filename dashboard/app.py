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
from pathlib import Path
from typing import Dict, List, Optional, Any
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field
import json

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


def mask_sensitive_payload(payload: Any) -> Any:
    """Mask sensitive values (passwords, tokens, API keys, credentials) in task payloads for privacy."""
    if isinstance(payload, dict):
        sensitive_keys = {
            "password", "passwd", "pwd", "secret", "token", "api_key",
            "apikey", "access_key", "auth", "authorization", "private_key",
            "credential", "ssn", "credit_card", "card_number"
        }
        masked = {}
        for k, v in payload.items():
            if any(s in str(k).lower() for s in sensitive_keys):
                masked[k] = "********"
            elif isinstance(v, (dict, list)):
                masked[k] = mask_sensitive_payload(v)
            else:
                masked[k] = v
        return masked
    elif isinstance(payload, list):
        return [mask_sensitive_payload(item) for item in payload]
    return payload


class ConnectionManager:
    """Manages active real-time WebSocket client connections with connection limits."""
    def __init__(self, max_connections: int = 50):
        self.active_connections: List[WebSocket] = []
        self._lock = threading.Lock()
        self.max_connections = max_connections

    async def connect(self, websocket: WebSocket) -> bool:
        with self._lock:
            if len(self.active_connections) >= self.max_connections:
                await websocket.close(code=1008, reason="Max concurrent connections reached")
                return False
            self.active_connections.append(websocket)
        await websocket.accept()
        return True

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


manager = ConnectionManager(max_connections=50)
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
    """Capture newest 50 tasks for dashboard rendering with privacy-masked payloads."""
    with coordinator._lock:
        all_tasks = []
        for t in coordinator.tasks.values():
            dump = t.model_dump()
            if "payload" in dump and dump["payload"]:
                dump["payload"] = mask_sensitive_payload(dump["payload"])
            all_tasks.append(dump)
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
        node = worker_nodes.pop(worker_id, None)
        if node:
            node.stop()
    coordinator.unregister_worker(worker_id)


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


@app.middleware("http")
async def add_security_headers(request, call_next):
    """Enforce strict security and privacy headers on all HTTP responses."""
    response = await call_next(request)
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-XSS-Protection"] = "1; mode=block"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "geolocation=(), microphone=(), camera=()"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "font-src 'self' https://fonts.gstatic.com; "
        "connect-src 'self' ws: wss:; "
        "img-src 'self' data:;"
    )
    return response


# Request schemas
class SubmitTaskRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    max_retries: int = Field(default=3, ge=0, le=10)


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
    """Return all tasks ordered from newest to oldest with privacy-masked payloads."""
    with coordinator._lock:
        all_tasks = []
        for t in coordinator.tasks.values():
            dump = t.model_dump()
            if "payload" in dump and dump["payload"]:
                dump["payload"] = mask_sensitive_payload(dump["payload"])
            all_tasks.append(dump)
        all_tasks.sort(key=lambda x: x["created_at"], reverse=True)
        return all_tasks[:50]


@app.post("/api/tasks")
def submit_task(req: SubmitTaskRequest):
    """Enqueue a new background task with priority and payload safety validation."""
    if req.name not in TASK_REGISTRY:
        raise HTTPException(status_code=400, detail=f"Invalid task name. Options: {list(TASK_REGISTRY.keys())}")

    # Enforce maximum payload size to prevent memory exhaustion DoS attacks
    try:
        serialized = json.dumps(req.payload)
        if len(serialized) > 65536:
            raise HTTPException(status_code=413, detail="Payload exceeds maximum allowed size limit of 64KB")
    except (TypeError, OverflowError):
        raise HTTPException(status_code=400, detail="Payload contains non-serializable data")

    task = coordinator.submit_task(
        name=req.name,
        payload=req.payload,
        priority=req.priority,
        max_retries=req.max_retries
    )
    broadcast_cluster_state()
    return {"message": "Task queued successfully", "task": task.model_dump()}


@app.post("/api/tasks/{task_id}/cancel")
def cancel_task_endpoint(task_id: str):
    """Cancel a pending task before it is picked up by a worker."""
    success = coordinator.cancel_task(task_id)
    if not success:
        raise HTTPException(
            status_code=400,
            detail=f"Cannot cancel task '{task_id}'. Task not found or not in PENDING state."
        )
    broadcast_cluster_state()
    return {"message": f"Task '{task_id}' cancelled successfully"}


@app.post("/api/tasks/{task_id}/retry")
def retry_task_endpoint(task_id: str):
    """Re-drive a dead-letter or failed task back into the dispatch queue."""
    with coordinator._lock:
        task = coordinator.get_task(task_id)
        if not task:
            raise HTTPException(status_code=404, detail=f"Task '{task_id}' not found")
        if task.status not in (TaskStatus.DEAD_LETTER, TaskStatus.FAILED, TaskStatus.CANCELLED):
            raise HTTPException(
                status_code=400,
                detail=f"Task '{task_id}' is in status '{task.status.value}' and cannot be retried. Only DEAD_LETTER, FAILED, or CANCELLED tasks can be re-driven."
            )
    success = coordinator.retry_task(task_id)
    if not success:
        raise HTTPException(
            status_code=400,
            detail=f"Could not retry task '{task_id}'"
        )
    broadcast_cluster_state()
    return {"message": f"Task '{task_id}' re-driven to pending queue"}


@app.post("/api/dlq/retry-all")
def retry_all_dlq_endpoint():
    """Re-drive all tasks currently parked in the Dead Letter Queue."""
    count = coordinator.retry_all_dlq()
    broadcast_cluster_state()
    return {
        "message": f"Re-driven {count} tasks from DLQ to queue",
        "re_driven_count": count
    }


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
    connected = await manager.connect(websocket)
    if not connected:
        return
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

TEMPLATE_PATH = Path(__file__).parent / "templates" / "index.html"


def load_template() -> str:
    """Load the latest dashboard HTML template."""
    if TEMPLATE_PATH.exists():
        return TEMPLATE_PATH.read_text(encoding="utf-8")
    return "<h1>Template not found</h1>"


HTML_CONTENT = load_template()


@app.get("/", response_class=HTMLResponse)
def index_page():
    return load_template()
