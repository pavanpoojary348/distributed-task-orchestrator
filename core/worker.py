"
Worker Node
- Connects to coordinator
- Emits periodic heartbeats (ping every 2s)
- Consumes assigned tasks, executes handlers, and reports completion/failure
"
class WorkerNode:
    def __init__(self, worker_id: str):
        self.worker_id = worker_id
        self.is_running = True

    def send_heartbeat(self):
        pass

    def execute_task(self, task):
        pass
