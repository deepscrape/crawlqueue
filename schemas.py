from enum import Enum
from typing import Optional
from pydantic import BaseModel

class TaskStatus(str, Enum):
    READY = "Ready"
    STARTED = "Start"
    SCHEDULED = "Scheduled"
    IN_PROGRESS = "In Progress"
    CANCELED = "Canceled"
    COMPLETED = "Completed"
    FAILED = "Failed"
    
class OperationResult(BaseModel):
    operation_id: str
    machine_id: str
    duration: float
    start_time: Optional[float] = None
    end_time: Optional[float] = None
    peak_memory: Optional[float] = None
    memory_used: Optional[float] = None
    status: str
    urls_processed: Optional[int] = None
    error: Optional[str] = None

class SystemStats(BaseModel):
    cpu_usage: float
    memory_usage: float
    queue_length: int
    error_rate: float

class ResourceStatus(BaseModel):
    memory_ok: bool
    cpu_ok: bool
    memory_usage: float
    cpu_usage: float
    per_core_usage: list[float]

class CoreMetrics(BaseModel):
    core_id: int
    cpu_usage: float
    memory_usage: float

# This class likely represents a product entity and inherits from a base model class.
class Product(BaseModel):
    name: str
    price: str

class UserAbortException(Exception):
    """Raised when a user aborts the operation."""
    pass