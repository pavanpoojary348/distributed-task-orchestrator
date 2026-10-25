"""Data models for Distributed Task Orchestrator.

Defines the structure and statuses for Tasks and Workers using Pydantic.
"""

from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field
import time
import uuid


class TaskStatus(str, Enum):
    """The lifecycle states of a task in the cluster."""
    PENDING = "PENDING"              # Waiting in queue to be picked up
    IN_PROGRESS = "IN_PROGRESS"      # Currently being executed by a worker
    COMPLETED = "COMPLETED"          # Finished successfully
    FAILED = "FAILED"                # Failed execution, might be retried
    DEAD_LETTER = "DEAD_LETTER"      # Failed too many times; parked in DLQ


class TaskPriority(int, Enum):
    """Priority tiers for tasks. Lower integer = higher scheduling priority."""
    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4


class WorkerStatus(str, Enum):
    """The health status of a worker node."""
    ACTIVE = "ACTIVE"                # Healthy and sending heartbeats
    SUSPECTED_DEAD = "SUSPECTED_DEAD"# Missed 1-2 heartbeats; under observation
    DEAD = "DEAD"                    # Missed heartbeats beyond timeout threshold


class CoordinatorRole(str, Enum):
    """The high-availability operational role of a coordinator node."""
    LEADER = "LEADER"                # Active cluster leader handling scheduling & dispatch
    STANDBY = "STANDBY"              # Hot standby watching leader lease & ready for instant failover
    DEAD = "DEAD"                    # Crashed/unresponsive node



class Task(BaseModel):
    """Represents a job to be processed by a worker node."""
    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    name: str
    payload: Dict[str, Any] = Field(default_factory=dict)
    priority: TaskPriority = TaskPriority.NORMAL
    status: TaskStatus = TaskStatus.PENDING
    retry_count: int = 0
    max_retries: int = 3
    assigned_worker: Optional[str] = None
    result: Optional[Any] = None
    error: Optional[str] = None
    created_at: float = Field(default_factory=time.time)
    updated_at: float = Field(default_factory=time.time)
    execution_time: Optional[float] = None
    next_retry_at: Optional[float] = None


class Worker(BaseModel):
    """Represents a worker node that registers with the coordinator."""
    id: str = Field(default_factory=lambda: f"worker-{uuid.uuid4().hex[:6]}")
    status: WorkerStatus = WorkerStatus.ACTIVE
    is_leader: bool = False
    last_heartbeat: float = Field(default_factory=time.time)
    current_task_id: Optional[str] = None
    completed_tasks: int = 0
    failed_tasks: int = 0
    started_at: float = Field(default_factory=time.time)
    is_dynamic: bool = False
    idle_since: Optional[float] = Field(default_factory=time.time)
