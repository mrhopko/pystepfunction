"""
A Task is a step in a stepfunction machine.

Build a stepfunction machine by creating tasks and connecting them with `and_then` or `>>` operator.  
Use `pystepfunction.branch.Branch` to create a stepfunction machine from a connected task.

Example:
```python
    >>> pystepfunction.machine.tasks import *
    retry = [Retry(error_equals=["States.ALL"], interval_seconds=1, max_attempts=3)]
    branch1 = Branch(Task("1") >> Task("2"))
    branch2 = Branch(Task("3").retry(retry) >> PassTask("pass") >> Task("4"))
    branch_parallel = Branch(
        Task("start") >> ParallelTask("par", [branch1, branch2]) >> Task("end").is_end()
    )
    asl = branch_parallel.to_asl()  
    logger.info(asl)
```
 
See https://docs.aws.amazon.com/step-functions/latest/dg/concepts-input-output-filtering.html  

`InputState` is used to mutate state data before it is passed to a task resource (eg lambda function) by setting:
- `InputState.input_path` - Behaves like a filter. a single jsonpath to select a subset of the state value
- `InputState.parameters` - Used to create a dictionary of parameters to pass to the task resource. 
`InputState.input_path` is applied first, so all jsonpath parameters are relative to the `InputState.input_path` result.

`OutputState` is used to mutate state data after a task has completed, before being passed to the next task.
- `OutputState.result_selector` - Used to create a dictionary of parameters from the raw resource returned results. jsonpath is relative to the raw result.
- `OutputState.result_path` - Where results are to be inserted into the state value. jsonpath is relative to the state value. If not set, the root of the state value is used ($.) overwriting the entire state value.
- `OutputState.output_path` - Behaves like a filter. a single jsonpath to select a subset of the state value to pass to the next task. It is applied after `OutputState.result_path` is applied.

Example:
```python
# Add state manipulation to a task
from pystepfunction.tasks import LambdaTask, InputState, OutputState

input_state = InputState(
    parameters={"Input1.$": "$.Input1", "Input2.$": "$.Input2"}, 
    input_path="$.Inputs"
)

output_state = OutputState(
    result_path="$.TaskResult",
    output_path="$.Outputs",
    result_selector={"Output1.$": "$.output1", "Output2.$": "$.output2"},
)

lambda_task = (
    LambdaTask(name="LambdaTaskName", function_arn="my-lambda-arn")
    .with_input(input_state)
    .with_output(output_state)
)
lambda_task.to_asl()
``` 
"""
from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import Any, Optional, List, Dict, Tuple
from abc import ABC


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

    def __str__(self) -> str:
        return ",".join(self.error_equals)


@dataclass
class TaskOutputState:
    result_selector: dict = field(default_factory=dict)
    """define a custom object from the task resource output"""
    result_path: str = "$"
    """Select where the resulting data will be inserted into the stat - defaults to $"""
    output_path: str = ""
    """Select what output is passed to the next task"""
    logger: Logger = getLogger(__name__)

    def has_result_selector(self) -> bool:
        """Check if the result selector is set"""
        return len(self.result_selector.items()) > 0

    def has_result_path(self) -> bool:
        """Check if the result path is set"""
        return len(self.result_path) > 0

    def has_output_path(self) -> bool:
        """Check if the output path is set"""
        return len(self.output_path) > 0

    def with_result_key(self, key: str, value: str):
        """Add a parameter to the result_selector"""
        self.result_selector[key] = value
        return self

    def with_result_selector(self, result_selector: dict):
        """Update the parameters"""
        self.result_selector.update(result_selector)
        return self

    def with_logger(self, logger: Logger) -> "TaskOutputState":
        self.logger = logger
        return self

    def merge_state(self, other: "TaskOutputState") -> "TaskOutputState":
        """Merge two output states"""
        if other.has_result_path():
            self.result_path = other.result_path
        if other.has_output_path():
            self.output_path = other.output_path
        if other.has_result_selector():
            self.with_result_selector(other.result_selector)
        return self

    def __add__(self, other: "TaskOutputState") -> "TaskOutputState":
        """Merge two input states"""
        return self.merge_state(other)

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {}
        if self.has_result_path():
            asl.update({"ResultPath": self.result_path})
        if self.has_result_selector():
            asl.update({"ResultSelector": self.result_selector})
        if self.has_output_path():
            asl.update({"OutputPath": self.output_path})
        return asl


@dataclass
class TaskInputState:
    input_path: str = ""
    """Filter what input is passed to the task"""
    parameters: dict = field(default_factory=dict)
    """define a custom object to pass as input to the task resource"""
    logger: Logger = getLogger(__name__)

    def with_logger(self, logger: Logger) -> "TaskInputState":
        self.logger = logger
        return self

    def has_input_path(self) -> bool:
        """Check if the input path is set"""
        return len(self.input_path) > 0

    def has_parameters(self) -> bool:
        """Check if the parameters are set"""
        return len(self.parameters.items()) > 0

    def with_parameter(self, key: str, value: str):
        """Add a parameter to the input"""
        self.parameters[key] = value
        return self

    def with_parameters(self, parameters: dict):
        """Update the parameters"""
        self.parameters.update(parameters)
        return self

    def merge_state(self, other: "TaskInputState") -> "TaskInputState":
        """Merge two input states"""
        if other.has_input_path():
            self.input_path = other.input_path
        if other.has_parameters():
            self.with_parameters(other.parameters)
        return self

    def __add__(self, other: "TaskInputState") -> "TaskInputState":
        """Merge two input states"""
        return self.merge_state(other)

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {}
        if self.has_input_path():
            asl.update({"InputPath": self.input_path})
        if self.has_parameters():
            asl.update({"Parameters": self.parameters})
        return asl


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
        self._on_error: Optional["Task"] = None
        """Task to execute on error"""
        self.name = str(name)
        """Name of the task"""
        self.end: bool = False
        """Is the task the end of the stepfunction branch"""
        self.retries: Optional[List[Retry]] = None
        """Retry configuration for the task"""
        self.catcher: Optional[List[Tuple[List[str], "Task"]]] = None
        """Catcher error handling"""
        self.input_state: Optional[TaskInputState] = None
        """Manipulate the input state for the task"""
        self.output_state: Optional[TaskOutputState] = None
        """Manipulate the output state for the task"""
        self.resource_result: dict = {}
        """Shape of return data from the task resource"""

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
        if self.has_retries():
            asl.update({"Retry": [retry.to_asl() for retry in self.retries]})
        if self.has_catcher():
            catch = [{"ErrorEquals": e, "Next": t.name} for e, t in self.catcher]
            asl.update({"Catch": catch})
        return {self.name: asl}

    def is_end(self) -> "Task":
        """Set the task as the end of the stepfunction branch"""
        self.end = True
        return self

    def with_input_state(self, input_state: TaskInputState) -> "Task":
        """Set the input state for the task

        Args:
            input_state (InputState): Input state for the task"""
        if self.has_input_state():
            self.input_state = self.input_state.merge_state(input_state)
        else:
            self.input_state = input_state
        return self

    def with_input(self, input_path: str = "", parameters: dict = {}) -> "Task":
        """Set the input state for the task

        Args:
            input_path (str, optional): Select a single path from the input state using jsonpath. Defaults to "$".
            parameters (dict, optional): Create a set of key/values from the input state (after input_path) to pass to the resource.
        """
        input_state = TaskInputState(input_path=input_path, parameters=parameters)
        return self.with_input_state(input_state)

    def with_output_state(self, output_state: TaskOutputState) -> "Task":
        """Set the output state for the task

        Args:
            output_state (OutputState): Output state for the task"""
        self.output_state = output_state
        return self

    def with_output(
        self, result_selector: dict = {}, result_path: str = "$", output_path: str = ""
    ) -> "Task":
        """Set the output state for the task

        Args:
            result_selector (dict, optional): generate a set of key value pairs from the resource_result.
            result_path (str, optional): Insert the task result into current state at the result_path. Defaults to "$".
            output_path (str, optional): Select a single path from the output of this task. applied after result_path and result_selector.
        """
        output_state = TaskOutputState(
            result_selector=result_selector,
            result_path=result_path,
            output_path=output_path,
        )
        return self.with_output_state(output_state)

    def with_retry(
        self,
        error_equals: List[str],
        interval_seconds: int,
        max_attempts: int,
        backoff_rate: float = 1.0,
    ) -> "Task":
        """add a retry configuration for the task

        Appends a retry configuration to existing retries

        Args:
            error_equals (List[str]): List of error states to retry on
            interval_seconds (int): Interval in seconds between retries
            max_attempts (int): Maximum number of retries
            backoff_rate (float, optional): Backoff rate for retries. Defaults to 1.0.
        """
        if self.retries is None:
            self.retries = []
        self.retries.append(
            Retry(error_equals, interval_seconds, max_attempts, backoff_rate)
        )
        return self

    def with_retries(self, retries: List[Retry]) -> "Task":
        """Set the retry configuration for the task

        Replaces existing retries

        Args:
            retries (List[Retry]): Retry configuration for the task"""
        self.retries = retries
        return self

    def with_catcher(self, error: List[str], task: "Task") -> "Task":
        """add a catcher mapping from errors to task

        mapping is appended to existing catchers

        Args:
            error (List[str]): List of errors to catch
            task (Task): Task to execute on error"""
        if self.catcher is None:
            self.catcher = []
        self.catcher.append((error, task))
        return self

    def with_catchers(self, catchers: List[Tuple[List[str], "Task"]]) -> "Task":
        """add a list of catcher mappings from errors to task

        replaces existing catchers

        Args:
            catchers (List[Tuple[List[str], Task]]): List of catcher mappings from errors to task
        """
        self.catcher = catchers
        return self

    def with_resource_result(self, resource_result: dict) -> "Task":
        """Set the resource return for the task

        Args:
            resource_return (dict): Resource return for the task"""
        self.resource_result = resource_result
        return self

    def has_catcher(self) -> bool:
        if self.catcher is None:
            return False
        if len(self.catcher) == 0:
            return False
        return True

    def has_retries(self) -> bool:
        if self.retries is None:
            return False
        if len(self.retries) == 0:
            return False
        return True

    def has_input_state(self) -> bool:
        return self.input_state is not None

    def has_output_state(self) -> bool:
        return self.output_state is not None

    def has_next(self) -> bool:
        if self._next is None:
            return False
        return len(self._next) > 0

    def has_resource_result(self) -> bool:
        return len(self.resource_result.items()) > 0

    @classmethod
    def _get_task_class_name(cls) -> str:
        return cls.__name__


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
        self.input_state = TaskInputState(
            parameters={"FunctionName": self.function_arn}
        )

    def with_payload(self, payload: dict) -> "LambdaTask":
        """Set the payload for the task

        Args:
            payload (dict): Payload for the task"""
        self.input_state.with_parameter("Payload", payload)
        return self


class GlueTask(Task):
    """Glue task for stepfunction machine"""

    resource: str = "arn:aws:states:::glue:startJobRun.sync"

    def __init__(
        self, name: str, job_name: str, job_args: Optional[dict] = None
    ) -> None:
        """Initialize a glue task

        Args:
            name (str): Name of the task
            job_name (str): Name of the glue job - gets included as "JobName" in the task input parameters
            job_args (Optional[dict], optional): set of arguments to pass to the glue job. Gets appended to the Task input paramters. Defaults to None.
        """
        super().__init__(name)
        self.job_args = job_args
        self.job_name = job_name
        self.input_state = TaskInputState(parameters={"JobName": job_name})
        if job_args is not None:
            self._set_job_args(job_args)

    def with_job_args(self, job_args: dict) -> "LambdaTask":
        """Set the payload for the task

        Args:
            job_args dict: set of arguments to pass to the glue job. Gets appended to the Task input paramters.
        """
        self._set_job_args(job_args)
        return self

    def _set_job_args(self, job_args: dict):
        args = {}
        for k, v in job_args.items():
            if str(k).startswith("--"):
                args[k] = v
            else:
                args[f"--{k}"] = v
        self.input_state.with_parameter(f"Arguments", args)


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
        self.end = True

    def to_asl(self) -> dict:
        """Convert to ASL"""
        return {self.name: {"Type": self.task_type}}


class FailTask(Task):
    """Fail task for stepfunction machine"""

    task_type = "Fail"

    def __init__(self, name: str, cause: str = "", error: str = "") -> None:
        super().__init__(name)
        self.cause = cause
        """Cause of the failure"""
        self.error = error
        """Error message of the failure"""
        self.end = True

    def with_cause(self, cause: str, error: str) -> "FailTask":
        """Set the cause for the failure

        Args:
            cause (str): Cause of the failure
            error (str): Error message of the failure"""
        self.cause = cause
        self.error = error
        return self

    def to_asl(self) -> dict:
        """Convert to ASL"""
        return {
            self.name: {
                "Type": self.task_type,
                "Cause": self.cause,
                "Error": self.error,
            }
        }


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

    def __str__(self) -> str:
        return str({"Variable": self.variable, "Condition": self.condition})


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
