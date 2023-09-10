"""StateMachine is used to test the flow of data manipulations between tasks

In pystepfunction, state is represented by a `State` object. It can also be used to test the flow of data manipulations.  
See https://docs.aws.amazon.com/step-functions/latest/dg/concepts-input-output-filtering.html

jsonpath_ng is used to parse and evaluate jsonpath expressions.  

glom is used to assign values to a dict using a path.  

Example:  

```python
from logging import getLogger
from pystepfunction.state import StateMachine
from pystepfunction.tasks import Task

# initialise a StateMachine with a logger and state
logger = getLogger(__name__)
init_state = {
    "init_key1": "init_value1",
    "init_key2": {"init_key22": "init_value22"},
}
sm = StateMachine(state=init_state).with_logger(logger)

# create a task with input and output states and an expected resource result
# select init_key2 and assign its value to new_key
# assign a reource_result representing the data returned from the task resource
# select resource2 from resource_result and assign its value to selected_result
# insert selected_result into the state at path task1_result
task = (
    Task("task1")
    .with_input(input_path="$.init_key2", parameters={"new_key.$": "$.init_key22"})
    .with_resource_result({"resource1": "value1", "resource2": "value2"})
    .with_output(
        result_selector={"selected_result.$": "$.resource2"},
        result_path="$.task1_result",
    )
)

# apply the task data manipulation to the statemachine
sm.apply_task(task)
logger.info(sm.state)

# check the state is as expected
expected_state = {
    "new_key": "init_value22",
    "task1_result": {"selected_result": "value2"},
}

assert sm.state == expected_state
```
"""
from dataclasses import dataclass, field
import json
from logging import Logger, getLogger
from typing import Any, List, Optional
import jsonpath_ng  # type: ignore
import glom  # type: ignore
from pystepfunction.branch import Branch
from pystepfunction.errors import JsonPathNoMatchException
from pystepfunction.tasks import Task


@dataclass
class StateMachine:
    """Test the flow of data manipulations between tasks"""

    state: dict = field(default_factory=dict)
    """Current state value"""

    state_log: List[dict] = field(default_factory=list)
    """A log of state values mutated over time with a __msg__ key"""

    context: dict = field(default_factory=dict)
    """The context of the state - provided by the stepfunction execution"""

    logger: Logger = getLogger(__name__)

    def has_value(self) -> bool:
        """Check if the state has a value"""
        return len(self.state.items()) > 0

    def has_context(self) -> bool:
        """Check if the state has a context"""
        return len(self.context.items()) > 0

    def is_path_value(self, v: str) -> bool:
        """Check if a string is a path"""
        return v.startswith("$.")

    def is_path_key(self, k: str) -> bool:
        return k.endswith("$")

    def insert_value_at_path(self, insert, path: str):
        if path == "$":
            self.state = insert
        key = path.replace("$.", "")
        glom.assign(self.state, key, insert)

    def get_path_value(self, path: str, from_state: Optional[dict] = None):
        """Select the value by path"""
        jsonpath_expr = jsonpath_ng.parse(path)
        if from_state is None:
            from_state = self.state
        values = [match.value for match in jsonpath_expr.find(from_state)]
        if len(values) == 0:
            self.logger.error(f"No Match for jsonpath {path}")
            self.logger.error(self.__repr__())
            raise JsonPathNoMatchException(f"No Match for jsonpath {path}")
        self.logger.info(values[0])
        return values[0]

    def get_path_state(self, path: str) -> Any:
        """Filter state by path"""
        new_state = self.get_path_value(path)
        self.logger.info(new_state)
        return new_state

    def get_paramaters(self, selector: dict, from_value: Optional[dict] = None) -> dict:
        """Select the values using a selector

        Keys that end with .$ are treated as paths. .$ is removed from key

        Args:
            selector (dict): The selector to use (a dict of absolute k,v pairs or k.&, $.v)

        Returns:
            dict: The selected values with paths evaluated against the state value
        """
        result = {}
        for k, v in selector.items():
            if self.is_path_key(k):
                new_key = k.replace(".$", "")
                result[new_key] = self.get_path_value(v, from_state=from_value)
            else:
                result[k] = v
        self.logger.info(result)
        return result

    def with_logger(self, logger: Logger) -> "StateMachine":
        self.logger = logger
        return self

    def update_state(self, state: dict, msg: str):
        """Update the state value"""
        self.logger.info(msg)
        self.logger.info(state)
        self.state = state
        if isinstance(state, dict):
            log_state = state.copy()
            log_state["__msg__"] = msg
        else:
            log_state = {"__msg__": msg, "__state__": state}
        self.state_log.append(log_state)

    def apply_input_parameters(self, task: Task):
        """Apply input parameters to the state"""
        assert task.input_state is not None
        if task.input_state.has_parameters():
            msg = f"apply input parameters for task {task.name}"
            self.logger.info("apply_parameters")
            new_state = self.get_paramaters(task.input_state.parameters)
            self.update_state(new_state, msg)

    def apply_input_path(self, task: Task):
        """Apply the input path to the state"""
        assert task.input_state is not None
        if task.input_state.has_input_path():
            msg = f"apply input path for task {task.name}"
            self.logger.info(msg)
            new_state = self.get_path_state(task.input_state.input_path)
            self.update_state(new_state, msg)

    def apply_task_input(self, task: Task):
        """Apply the task input state to the state"""
        if task.has_input_state():
            self.logger.info(f"Apply input state for task {task.name}")
            self.apply_input_path(task)
            self.apply_input_parameters(task)
        else:
            self.logger.info(f"No input_state for task {task.name}")

    def get_result_selector(self, task: Task) -> dict:
        """Apply the result selector to the state

        state.resource_result is overriden with selected results"""
        if not task.has_resource_result():
            return {}
        resource_result = task.resource_result
        assert task.output_state is not None
        if task.output_state.has_result_selector():
            result_selector = task.output_state.result_selector
            result = self.get_paramaters(result_selector, dict(resource_result))
            return result
        return dict(resource_result)

    def apply_result_path(self, task: Task):
        """Move resource_result to state.value given a path"""
        result = self.get_result_selector(task)
        msg = f"apply_result_path for task {task.name}"
        assert task.output_state is not None
        if task.output_state.has_result_path():
            self.logger.info(msg)
            result_path = task.output_state.result_path
            self.insert_value_at_path(insert=result, path=result_path)
            self.update_state(self.state, msg)
        else:
            self.update_state(result, msg)

    def apply_output_path(self, task: Task):
        """Apply the output path to the state"""
        assert task.output_state is not None
        if task.output_state.has_output_path():
            msg = f"apply_output_path for task {task.name}"
            self.logger.info(msg)
            new_state = self.get_path_value(task.output_state.output_path)
            self.update_state(new_state, msg)

    def apply_task_output(self, task: Task):
        """Apply the task input state to the state"""
        if task.has_output_state():
            self.apply_result_path(task)
            self.apply_output_path(task)

    def apply_task(self, task: Task):
        """Apply the task input state to the state"""
        self.logger.info(f"apply task {task.name}")
        self.apply_task_input(task)
        self.apply_task_output(task)

    def apply_branch(
        self, branch: Branch, init_state: Optional[dict] = None, max_steps=100
    ):
        """Apply a branch to the state

        Args:
            branch (Branch): The branch to apply
            init_state (Optional[dict], optional): The initial state. If None then existing state is used. Defaults to None.
            max_steps (int, optional): The maximum number of steps to apply. Defaults to 100. (in case of loops)
        """
        self.logger.info(f"apply branch")
        if init_state is not None:
            self.state = init_state
        current_task = branch.start_task
        current_step = 1
        self.apply_task(current_task)
        while current_task.has_next() and current_step < max_steps:
            next_task = current_task.next()
            assert next_task is not None
            current_task = next_task
            self.apply_task(current_task)
            current_step += 1

    def __repr__(self) -> str:
        sep = "\n----------------\n"
        return sep.join([json.dumps(log, indent=2) for log in self.state_log])

    def show_logs(self):
        print(self.__repr__())
