from dataclasses import dataclass, field
from logging import Logger, getLogger
from typing import Optional
import jsonpath_ng
import glom
from pystepfunction.errors import JsonPathNoMatch


@dataclass
class State:
    """representation of the internal stepfunction state"""

    value: dict = field(default_factory=dict)
    """The value of the state"""

    context: dict = field(default_factory=dict)
    """The context of the state - provided by the stepfunction execution"""

    resource_result: dict = field(default_factory=dict)
    """The result of the resource - used for testing OutputState"""

    logger: Logger = getLogger(__name__)

    def has_value(self) -> bool:
        """Check if the state has a value"""
        return len(self.value.items()) > 0

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
            self.value = insert
        key = path.replace("$.", "")
        glom.assign(self.value, key, insert)

    def get_path_value(self, path: str, from_value: Optional[dict] = None):
        """Select the value by path"""
        jsonpath_expr = jsonpath_ng.parse(path)
        if from_value is None:
            from_value = self.value
        values = [match.value for match in jsonpath_expr.find(from_value)]
        if len(values) == 0:
            raise JsonPathNoMatch("No Match for jsonpath %s", path)
        self.logger.info(values[0])
        return values[0]

    def get_path_state(self, path: str) -> "State":
        """Filter the value by path"""
        new_value = self.get_path_value(path)
        self.logger.info(new_value)
        return State(value=new_value, context=self.context)

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
                result[new_key] = self.get_path_value(v, from_value=from_value)
            else:
                result[k] = v
        self.logger.info(result)
        return result

    def with_resource_result(self, result: dict) -> "State":
        """Set the resource result returned from a task resource - usually for testing"""
        self.resource_result = result
        return self

    def with_logger(self, logger: Logger) -> "State":
        self.logger = logger
        return self


@dataclass
class OutputState:
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

    def with_logger(self, logger: Logger) -> "OutputState":
        self.logger = logger
        return self

    def merge_state(self, other: "OutputState") -> "OutputState":
        """Merge two output states"""
        if other.has_result_path():
            self.result_path = other.result_path
        if other.has_output_path():
            self.output_path = other.output_path
        if other.has_result_selector():
            self.with_result_selector(other.result_selector)
        return self

    def __add__(self, other: "OutputState") -> "OutputState":
        """Merge two input states"""
        return self.merge_state(other)

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {}
        if self.has_result_path():
            asl.update({"ResultPath": f"$.{'.'.join(self.result_path)}"})
        if self.has_result_selector():
            asl.update({"ResultSelector": self.result_selector})
        if self.has_output_path():
            asl.update({"OutputPath": f"$.{'.'.join(self.output_path)}"})
        return asl

    def apply_result_selector(self, state: State) -> State:
        """Apply the result selector to the state

        state.resource_result is overriden with selected results"""
        if self.has_result_selector():
            self.logger.info("apply_result_selector")
            result = state.get_paramaters(self.result_selector, state.resource_result)
            return State(
                value=state.value, context=state.context, resource_result=result
            )
        return state

    def apply_result_path(self, state: State) -> State:
        """Move resource_result to state.value given a path"""
        # how do i insert values given a jsonpath?
        if self.has_result_path():
            self.logger.info("apply_result_path")
            state.insert_value_at_path(
                insert=state.resource_result, path=self.result_path
            )
        return state

    def apply_output_path(self, state: State) -> State:
        """Apply the output path to the state"""
        if self.has_output_path():
            self.logger.info("apply_output_path")
            result = state.get_path_value(self.output_path)
            return State(value=result, context=state.context)
        return state

    def apply_to_state(self, state: State) -> State:
        """Process the output state"""
        self.logger.info("apply_to_state")
        result_selector_result = self.apply_result_selector(state)
        result_path_result = self.apply_result_path(result_selector_result)
        output_path_result = self.apply_output_path(result_path_result)
        return output_path_result


@dataclass
class InputState:
    input_path: str = ""
    """Filter what input is passed to the task"""
    parameters: dict = field(default_factory=dict)
    """define a custom object to pass as input to the task resource"""
    logger: Logger = getLogger(__name__)

    def with_logger(self, logger: Logger) -> "InputState":
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

    def merge_state(self, other: "InputState") -> "InputState":
        """Merge two input states"""
        if other.has_input_path():
            self.input_path = other.input_path
        if other.has_parameters():
            self.with_parameters(other.parameters)
        return self

    def __add__(self, other: "InputState") -> "InputState":
        """Merge two input states"""
        return self.merge_state(other)

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {}
        if self.has_input_path():
            asl.update({"InputPath": f"$.{'.'.join(self.input_path)}"})
        if self.has_parameters():
            asl.update({"Parameters": self.parameters})
        return asl

    def apply_input_path(self, state: State) -> State:
        """Apply the input path to the state"""
        if self.has_input_path():
            self.logger.info("apply_input_path")
            return State(
                value=state.get_path_value(self.input_path), context=state.context
            )
        return state

    def apply_parameters(self, state: State) -> State:
        """Apply the parameters to the state"""
        if self.has_parameters():
            self.logger.info("apply_parameters")
            return State(
                value=state.get_paramaters(self.parameters), context=state.context
            )
        return state

    def apply_to_state(self, state: State, logger: Optional[Logger] = None) -> State:
        """Process the input state"""
        self.logger.info("InputState.apply_to_state")
        input_result = self.apply_input_path(state)
        parameter_result = self.apply_parameters(input_result)
        return parameter_result
