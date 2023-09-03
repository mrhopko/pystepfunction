"""Exceptions and aws stepfunction error constants"""


class IncompleteTaskException(Exception):
    """Exception - raised when a task is missing a required field"""

    pass


class JsonPathNoMatchException(Exception):
    """Exception - raised when a json path does not match a value"""

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
