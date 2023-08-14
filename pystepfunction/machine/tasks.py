"""
Tasks for stepfunction machine

Build a stepfunction machine by creating tasks and connecting them with `and_then` or `>>` operator.

Example:
    >>> pystepfunction.machine.tasks import *
    retry = [Retry(error_equals=["States.ALL"], interval_seconds=1, max_attempts=3)]
    branch1 = Branch(Task("1") >> Task("2"))
    branch2 = Branch(Task("3").retry(retry) >> PassTask("pass") >> Task("4"))
    branch_parallel = Branch(
        Task("start") >> ParallelTask("par", [branch1, branch2]) >> Task("end").is_end()
    )
    asl = branch_parallel.to_asl()
    logger.info(asl)
"""
import types
from itertools import chain
from typing import Any, Optional, List, Dict, Callable
from dataclasses import dataclass
from abc import ABC
from enum import Enum


class ErrorStates(Enum):
    """Stepfunction error states - these should probably be constants"""

    ALL = "States.ALL"
    BRANCH_FAILED = "States.BranchFailed"
    DATA_LIMIT_EXCEEDED = "States.DataLimitExceeded"
    EXCEED_TOLERATED_FAILIURE_THRESHOLD = "States.ExceedToleratedFailiureThreshold"
    HEARTBEAT_TIMEOUT = "States.HeartbeatTimeout"
    INTRINSIC_FAILURE = "States.IntrinsicFailure"
    ITEM_READER_FAILED = "States.ItemReaderFailed"
    NO_CHOICE_MATCHED = "States.NoChoiceMatched"
    PARAMETER_PATH_FAILURE = "States.ParameterPathFailure"
    PERMISSIONS = "States.Permissions"
    RESULT_PATH_MATCH_FAILURE = "States.ResultPathMatchFailure"
    RESULT_WRITER_FAILED = "States.ResultWriterFailed"
    RUNTIME = "States.Runtime"
    TASK_FAILED = "States.TaskFailed"
    TIMEOUT = "States.Timeout"


class IncompleteTask(Exception):
    pass


@dataclass
class State:
    """representation of the internal stepfunction state"""

    value: dict
    context: dict


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


class Task(ABC):
    """Base class for all tasks"""

    task_type = "Task"
    """Task type for ASL"""
    resource = ""
    """Task resource for ASL"""

    def __init__(self, name: str) -> None:
        """Initialize a task

        Args:
            name (str): Name of the task
        """
        self._next: Optional[List["Task"]] = None
        """Next task in the stepfunction machine"""
        self.on_error: Optional["Task"] = None
        """Task to execute on error"""
        self.name = str(name)
        """Name of the task"""
        self.end: bool = False
        """Is the task the end of the stepfunction branch"""
        self.payload: dict = {}
        """Payload for the task - used with parameters"""
        self.parameters: dict = {}
        """Parameters for the task"""
        self.retries: List[Retry] = []
        """Retry configuration for the task"""
        self.catcher: List[Catcher] = []
        """Catcher configuration for the task"""

    def exec(state: State) -> State:
        return State

    def next(self) -> Optional["Task"]:
        """Get the next task in the stepfunction machine"""
        if self._next is None:
            return None
        if len(self._next) == 0:
            return None
        return self._next[0]

    def set_next(self, task: "Task") -> "Task":
        """Set the next task in the stepfunction machine

        Overrides the immediate next task if it exists

        Args:
            task (Task): Next task in the stepfunction machine"""
        self._next = [task]
        return self

    def and_then(self, task: "Task") -> "Task":
        """Set the next task in the stepfunction machine

        Appends the task to the end of the current task chain

        Args:
            task (Task): Next task in the stepfunction machine"""
        return self.__rshift__(task)

    def __rshift__(self, task: "Task") -> "Task":
        """Set the next task in the stepfunction machine

        Appends the task to the end of the current task chain

        Args:
            task (Task): Next task in the stepfunction machine"""
        if self.next() is None:
            self._next = [task]
        else:
            self.next().__rshift__(task)
        return self

    def set_payload(self, state_keys: Dict[str, List[str]], fixed_keys: dict):
        """Set the payload for the task

        Used in Lambdas and glue tasks to send data to a job of function

        Args:
            state_keys (Dict[str, List[str]]): Keys to extract from the state
            fixed_keys (dict): Fixed keys to add to the payload
        """
        self.payload = {f"{k}.$": f"$.{'.'.join(v)}" for k, v in state_keys.items()}
        self.payload.update(fixed_keys)
        self.parameters.update(
            {
                "Payload": self.payload,
            }
        )

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {
            "Type": self.task_type,
            "Resource": self.resource,
            "End": self.end,
        }
        if len(self.parameters.items()) > 0:
            asl.update({"Parameters": self.parameters})
        if self.next() is not None:
            asl.update({"Next": self.next().name})
        if len(self.retries) > 0:
            asl.update({"Retry": [retry.to_asl() for retry in self.retries]})
        if len(self.catcher) > 0:
            asl.update({"Catch": self.catcher})

        return {self.name: asl}

    def is_end(self) -> "Task":
        """Set the task as the end of the stepfunction branch"""
        self.end = True
        return self

    def retry(self, retries: List[Retry]) -> "Task":
        """Set the retry configuration for the task

        Args:
            retries (List[Retry]): Retry configuration for the task

        Returns:
            Task: The task"""
        self.retries = retries
        return self

    def catch_task(self, catchers: List[Catcher]) -> "Task":
        """Set the catcher configuration for the task

        Args:
            catchers (List[Catcher]): Catcher configuration for the task

        Returns:
            Task: The task"""
        self.catcher = catchers
        return self


class PassTask(Task):
    """Pass task for stepfunction machine"""

    task_type = "Pass"
    """Task type for AS = Pass"""

    def __init__(self, name: str, result: dict = {}) -> None:
        """Initialize a pass task

        Args:
            name (str): Name of the task
            result (dict, optional): Result of the task. Defaults to {}. result is the payload of the next task.
        """
        super().__init__(name)
        self.result = result
        """Result of the task. result is the payload of the next task."""

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {"Type": self.task_type, "End": self.end}
        if len(self.result.items()) > 0:
            asl.update({"Result": self.result})
        return {self.name: asl}


class LambdaTask(Task):
    """Lambda task for stepfunction machine

    Properties:
        function_arn (str): ARN of the lambda function
    """

    resource: str = "arn:aws:states:::lambda:invoke"
    """Task resource for ASL = Lambda:invoke"""

    def __init__(self, name: str, function_arn: str) -> None:
        """Initialize a lambda task

        Args:
            name (str): Name of the task
            function_arn (str): ARN of the lambda function"""
        super().__init__(name)
        self.function_arn = function_arn
        """ARN of the lambda function"""
        self.parameters.update({"FunctionName": self.function_arn})

    def with_payload(
        self, state_keys: Dict[str, List[str]], fixed_keys: dict
    ) -> "LambdaTask":
        """Set the payload for the task

        Used in Lambdas to send data to a function

        Args:
            state_keys (Dict[str, List[str]]): Keys to extract from the state
            fixed_keys (dict): Fixed keys to add to the payload

        Returns:
            LambdaTask: The task"""
        self.set_payload(state_keys=state_keys, fixed_keys=fixed_keys)
        return self


class GlueTask(Task):
    """Glue task for stepfunction machine"""

    resource: str = "arn:aws:states:::glue:startJobRun.sync"

    def __init__(self, name: str, job_name: str) -> None:
        """Initialize a glue task

        Args:
            name (str): Name of the task
            job_name (str): Name of the glue job"""
        super().__init__(name)
        self.job_name = job_name
        """Name of the glue job"""
        self.parameters.update({"JobName": self.job_name})

    def with_payload(
        self, state_keys: Dict[str, List[str]], fixed_keys: dict
    ) -> "GlueTask":
        """Set the payload for the task

        Used in glue jobs to send data to a job

        Args:
            state_keys (Dict[str, List[str]]): Keys to extract from the state
            fixed_keys (dict): Fixed keys to add to the payload"""
        self.set_payload(state_keys=state_keys, fixed_keys=fixed_keys)
        return self


class WaitTask(Task):
    """Wait task for stepfunction machine"""

    task_type = "Wait"

    def __init__(self, name) -> None:
        """Initialize a wait task

        Args:
            name (str): Name of the task
        """
        super().__init__(name)
        self.seconds = 0
        """Number of seconds to wait"""
        self.timestamp = ""
        """Timestamp to wait until"""
        self.seconds_keys: List[str] = []
        """List of keys to extract the number of seconds to wait from the state"""
        self.timestamp_keys: List[str] = []
        """List of keys to extract the timestamp to wait until from the state"""

    def wait_seconds(self, seconds: int = 0, seconds_keys: List[str] = []) -> "Task":
        """Set the number of seconds to wait

        Args:
            seconds (int, optional): Number of seconds to wait. Defaults to 0.
            seconds_keys (List[str], optional): List of keys to extract the number of seconds to wait from the state. Defaults to [].

        Returns:
            Task: The task"""
        self.seconds = seconds
        self.seconds_keys = seconds_keys
        return self

    def wait_timestamp(
        self, timestamp: str = "", timestamp_keys: List[str] = []
    ) -> "Task":
        """Set the timestamp to wait until

        Args:
            timestamp (str, optional): Timestamp to wait until. Defaults to "".
            timestamp_keys (List[str], optional): List of keys to extract the timestamp to wait until from the state. Defaults to [].

        Returns:
            Task: The task"""
        self.timestamp = timestamp
        self.timestamp_keys = timestamp_keys
        return self

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {"Type": self.task_type}
        if self.next() is not None:
            asl["Next"] = self.next()
        if self.seconds > 0:
            asl["Seconds"] = self.seconds
        elif self.timestamp != "":
            asl["Timestamp"] = self.timestamp
        elif len(self.seconds_keys) > 0:
            asl["SecondsPath"] = f"$.{'.'.join(self.seconds_keys)}"
        elif len(self.timestamp_keys) > 0:
            asl["TimestampPath"] = f"$.{'.'.join(self.timestamp_keys)}"
        return asl


class SucceedTask(Task):
    """Succeed task for stepfunction machine"""

    task_type = "Succeed"

    def __init__(self, name: str) -> None:
        super().__init__(name)


class FailTask(Task):
    """Fail task for stepfunction machine"""

    task_type = "Fail"

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.cause = ""
        """Cause of the failure"""
        self.error = ""
        """Error message of the failure"""

    def set_cause(self, cause: str, error: str) -> "FailTask":
        """Set the cause for the failure

        Args:
            cause (str): Cause of the failure
            error (str): Error message of the failure"""
        self.cause = cause
        self.error = error

    def to_asl(self) -> dict:
        """Convert to ASL"""
        return {
            self.name: {
                "Type": self.task_type,
                "Cause": self.cause,
                "Error": self.error,
            }
        }


class DmsTask(Task):
    def exec(state: State) -> State:
        return super().exec()


class SdkTask(Task):
    def exec(state: State) -> State:
        return super().exec()


class InputTask(Task):
    def exec(state: State) -> State:
        return super().exec()


class ChooseBranch:
    def exec(state: State) -> State:
        return super().exec()


class FailTask(Task):
    def exec(state: State) -> State:
        return super().exec()


class ChoiceRule:
    """Choice rule for stepfunction machine"""

    def __init__(
        self,
        variable: str,
        condition: str,
        value: Optional[int | bool | str | float] = None,
        next: Optional[Task] = None,
    ) -> None:
        """Initialize a choice rule

        Args:
            variable (str): Variable to check
            condition (str): Condition to check
            value (int | bool | str | float, optional): Value to check. Defaults to None.
            next (Task, optional): Next task to execute. Defaults to None.
        """
        self.variable: str = f"$.{variable}"
        """Variable to check"""
        self.condition: str = condition
        """Condition to check"""
        self.value = value
        """Value to compare to"""
        self.next = next
        """Next task to execute"""
        self._and_rules: List["ChoiceRule"] = []
        self._or_rules: List["ChoiceRule"] = []
        self._is_not: bool = False

    def __not__(self) -> "ChoiceRule":
        self._is_not = True
        return self

    def __and__(self, rule: "ChoiceRule") -> "ChoiceRule":
        self._and_rules.append(rule)
        return self

    def __or__(self, rule: "ChoiceRule") -> "ChoiceRule":
        self._or_rules.append(rule)
        return self

    def _short_asl(self) -> dict:
        short_asl = {"Variable": self.variable, "Condition": self.condition}
        if self.value is not None:
            short_asl["Value"] = self.value
        if self._is_not:
            return {"Not": short_asl}
        return short_asl

    def to_asl(self) -> dict:
        """Convert to ASL"""
        if len(self._and_rules) > 0:
            rule_list = [self._short_asl()] + [
                rule._short_asl() for rule in self._and_rules
            ]
            return {"And": rule_list, "Next": self.next.name}

        if len(self._or_rules) > 0:
            rule_list = [self._short_asl()] + [
                rule._short_asl() for rule in self._or_rules
            ]
            return {"Or": rule_list, "Next": self.next.name}

        asl = self._short_asl()
        asl["Next"] = self.next.name
        return asl


class ChoiceTask(Task):
    """Choice task for stepfunction machine"""

    task_type = "Choice"

    def __init__(self, name: str, choices: List[ChoiceRule], default: Task) -> None:
        """Initialize a choice task

        Args:
            name (str): Name of the task
            choices (List[ChoiceRule]): List of choice rules
            default (Task): Default task to execute
        """
        super().__init__(name)
        self.choices = choices
        """List of choice rules"""
        self.default = default
        """Default task to execute"""
        self._next = []
        for choice in choices:
            self._get_next(choice.next)

    def _get_next(self, task: Optional[Task]):
        if task is None:
            return
        self._next.append(task)
        if task.next() is None:
            return
        for t in task._next:
            self._get_next(t)

    def to_asl(self) -> dict:
        """Convert to ASL"""
        return {
            self.name: {
                "Type": self.task_type,
                "Choices": [choice.to_asl() for choice in self.choices],
                "Default": self.default.name,
            }
        }


class Branch:
    """A sequence of tasks to execute in a stepfunction machine"""

    def __init__(self, start_task: Task, comment: str = "") -> None:
        self.start_task = start_task
        """The first task to execute"""
        self.comment = comment
        """Comment for the stepfunction machine"""
        self.task_dict: dict = {}
        """Dictionary of all tasks in the stepfunction machine"""
        self.task_list: List[Task] = []
        """List of all tasks in the stepfunction machine"""

    def build_task_list(self, task: Task):
        """Build the list of tasks in the stepfunction machine

        Recursively checks task.next()

        Args:
            task (Task): The task to add to the list
        """
        if task.name in self.task_dict:
            return
        self.task_dict[task.name] = task
        self.task_list.append(task)
        if task.next() is None:
            return
        for t in task._next:
            self.build_task_list(t)

    def to_asl(self) -> dict:
        """Convert to ASL"""
        self.build_task_list(self.start_task)
        asl = {}
        for task in self.task_list:
            asl.update(task.to_asl())
        return {"Comment": self.comment, "StartAt": self.start_task.name, "States": asl}


class ParallelTask(Task):
    """Parallel task for stepfunction machine

    Execute several branches in parallel

    Properties:
        branches (List[Branch]): List of branches to execute
    """

    task_type = "Parallel"

    def __init__(self, name: str, branches: List[Branch]) -> None:
        """Initialize a parallel task

        Args:
            name (str): Name of the task
            branches (List[Branch]): List of branches to execute
        """
        super().__init__(name)
        self.branches = branches

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {
            "Type": self.task_type,
            "End": self.end,
            "Branches": [branch.to_asl() for branch in self.branches],
        }
        if self.next() is not None:
            asl["Next"] = self.next().name
        return {self.name: asl}
