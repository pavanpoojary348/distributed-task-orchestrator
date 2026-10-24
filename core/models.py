from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
import time
import uuid

class TaskStatus(str, Enum):
    PENDING = PENDING
    IN_PROGRESS = IN_PROGRESS
    COMPLETED = COMPLETED
    FAILED = FAILED
    DEAD_LETTER = DEAD_LETTER

class WorkerStatus(str, Enum):
    ACTIVE = ACTIVE
    SUSPECTED_DEAD = SUSPECTED_DEAD
    DEAD = DEAD

class Task(BaseModel):
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    assigned_worker: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)

class Worker(BaseModel):
    id: str = Field(default_factory=lambda: fworker-{uuid.uuid4().hex[:6]})
    status: WorkerStatus = WorkerStatus.ACTIVE
    is_leader: bool = False
    last_heartbeat: float = Field(default_factory=time.time)
    current_task_id: Optional[str] = None
