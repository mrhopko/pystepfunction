from dataclasses import dataclass, field
from enum import Enum
from typing import List


class IncompleteTask(Exception):
    pass


class JsonPathNoMatch(Exception):
    pass


# Error Constants
ERROR_STATE_ALL = "States.ALL"
ERROR_STATE_BRANCH_FAILED = "States.BranchFailed"
ERROR_STATE_DATA_LIMIT_EXCEEDED = "States.DataLimitExceeded"
ERROR_STATE_EXCEED_TOLERATED_FAILIURE_THRESHOLD = (
    "States.ExceedToleratedFailiureThreshold"
)
ERROR_STATE_HEARTBEAT_TIMEOUT = "States.HeartbeatTimeout"
ERROR_STATE_INTRINSIC_FAILURE = "States.IntrinsicFailure"
ERROR_STATE_ITEM_READER_FAILED = "States.ItemReaderFailed"
ERROR_STATE_NO_CHOICE_MATCHED = "States.NoChoiceMatched"
ERROR_STATE_PARAMETER_PATH_FAILURE = "States.ParameterPathFailure"
ERROR_STATE_PERMISSIONS = "States.Permissions"
ERROR_STATE_RESULT_PATH_MATCH_FAILURE = "States.ResultPathMatchFailure"
ERROR_STATE_RESULT_WRITER_FAILED = "States.ResultWriterFailed"
ERROR_STATE_RUNTIME = "States.Runtime"
ERROR_STATE_TASK_FAILED = "States.TaskFailed"
ERROR_STATE_TIMEOUT = "States.Timeout"


@dataclass
class Retry:
    """Retry configuration for a task"""

    error_equals: List[str]
    """List of error states to retry on"""
    interval_seconds: int
    """Interval in seconds between retries"""
    max_attempts: int
    """Maximum number of retries"""
    backoff_rate: float = 1.0
    """Backoff rate for retries"""

    def to_asl(self) -> dict:
        """Convert to ASL"""
        return {
            "ErrorEquals": self.error_equals,
            "IntervalSeconds": self.interval_seconds,
            "MaxAttempts": self.max_attempts,
            "BackoffRate": self.backoff_rate,
        }


@dataclass
class Catcher:
    """Catcher configuration for a task"""

    error_equals: List[str]
    """List of error states to catch"""
    next: str
    """Next task to execute"""

    def to_asl(self) -> dict:
        """Convert to ASL"""
        return {
            "ErrorEquals": self.error_equals,
            "Next": self.next,
        }


@dataclass
class ErrorHandler:
    """Handle State Errors"""

    retries: List[Retry] = field(default_factory=list)
    """Retry configuration for the task"""
    catcher: List[Catcher] = field(default_factory=list)
