from enum import Enum


class ExecutionPlaneSyncStatus(str, Enum):
    DELETING = "deleting"
    ERROR = "error"
    PENDING = "pending"
    READY = "ready"

    def __str__(self) -> str:
        return str(self.value)
