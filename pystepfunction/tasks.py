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
from pystepfunction.errors import ErrorHandler, Retry
from pystepfunction.state import InputState, OutputState, State


class Task(ABC):
    """Base class for all tasks"""

    task_type = "Task"
    """Task type for ASL"""
    resource = ""
    """Task resource for ASL"""
    resource_return: dict = {}
    """Shape of return data from the task resource"""
    input_state: Optional[State] = None
    """Used for testing inputs/outputs"""
    output_state: Optional[State] = None
    """used for testing inputs/outputs"""

    def __init__(self, name: str) -> None:
        """Initialize a task

        Args:
            name (str): Name of the task
        """
        self._next: Optional[List["Task"]] = None
        """Next task in the stepfunction machine"""
        self._on_error: Optional["Task"] = None
        """Task to execute on error"""
        self.name = str(name)
        """Name of the task"""
        self.end: bool = False
        """Is the task the end of the stepfunction branch"""
        self.error_handler: Optional[ErrorHandler] = None
        """Error handling for the task"""
        self.input_state: Optional[InputState] = None
        """Manipulate the input state for the task"""
        self.output_state: Optional[OutputState] = None
        """Manipulate the output state for the task"""

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

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {
            "Type": self.task_type,
            "Resource": self.resource,
            "End": self.end,
        }
        if self.next() is not None:
            asl.update({"Next": self.next().name})
        if self.input_state is not None:
            asl.update(self.input_state.to_asl())
        if self.output_state is not None:
            asl.update(self.output_state.to_asl())

        return {self.name: asl}

    def is_end(self) -> "Task":
        """Set the task as the end of the stepfunction branch"""
        self.end = True
        return self

    def with_input(self, input_state: InputState) -> "Task":
        """Set the input state for the task

        Args:
            input_state (InputState): Input state for the task"""
        if self.has_input_state():
            self.input_state = self.input_state.merge_state(input_state)
        else:
            self.input_state = input_state
        return self

    def with_output(self, output_state: OutputState) -> "Task":
        """Set the output state for the task

        Args:
            output_state (OutputState): Output state for the task"""
        self.output_state = output_state
        return self

    def with_error_handler(self, error_handler: ErrorHandler) -> "Task":
        """Set the error handler for the task

        Args:
            error_handler (ErrorHandler): Error handler for the task"""
        self.error_handler = error_handler
        return self

    def has_error_handler(self) -> bool:
        self.error_handler is not None

    def has_input_state(self) -> bool:
        self.input_state is not None

    def has_output_state(self) -> bool:
        self.output_state is not None

    def has_next(self) -> bool:
        if self._next is None:
            return False
        return len(self._next) > 0

    @classmethod
    def _get_task_class_name(cls) -> str:
        return cls.__class__.__name__


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
        self.input_state = InputState(parameters={"FunctionName": self.function_arn})

    def with_payload(self, payload: dict) -> "LambdaTask":
        """Set the payload for the task

        Args:
            payload (dict): Payload for the task"""
        self.input_state.with_parameter("Payload", payload)
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
        self.input_state = InputState(parameters={"JobName": job_name})
        self.job_name = job_name

    def with_payload(self, payload: dict) -> "LambdaTask":
        """Set the payload for the task

        Args:
            payload (dict): Payload for the task"""
        self.input_state.with_parameter("Payload", payload)
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
