"
Web Telemetry Dashboard (FastAPI)
Exposes endpoints to view cluster health, queue size, and simulate node crashes.
"
from fastapi import FastAPI

app = FastAPI(title=Distributed Task Orchestrator Dashboard)

@app.get(/)
def read_root():
    return {system: Distributed Task Orchestrator, status: online}
