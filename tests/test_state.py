from logging import getLogger
from pystepfunction.state import InputState, OutputState, State

logger = getLogger(__name__)


def test_input_state():
    expect_output = "val22"
    test_value = {"key1": "val1", "key2": {"key22": expect_output}}
    test_state = State(value=test_value).with_logger(logger)
    input_path = "$.key2.key22"
    parameters = {"new_key22.$": input_path, "fixed_key": "fixed_value"}

    parameter_value = test_state.get_paramaters(parameters)
    assert parameter_value == {"new_key22": expect_output, "fixed_key": "fixed_value"}
    new_state = test_state.get_path_state(input_path)
    assert new_state.value == expect_output

    input_state = InputState(
        input_path="$.key2", parameters={"new_key.$": "$.key22"}
    ).with_logger(logger)
    result_state = input_state.apply_to_state(test_state, logger)
    assert result_state.value == {"new_key": expect_output}


def test_output_state():
    expect_output = {"key222": "val222"}
    test_value = {"key1": "val1", "key2": {"key22": expect_output}}
    test_resource_result = {"resource2": "value2", "resource1": "value1"}
    test_state = State(value=test_value).with_resource_result(test_resource_result)

    result_selector = {"selected_result.$": "$.resource2", "fixed_key": "fixed_value"}
    output_state = OutputState(result_selector=result_selector, result_path="$.key3")

    output_after_result_selector = output_state.apply_result_selector(test_state)
    assert output_after_result_selector.resource_result == {
        "selected_result": "value2",
        "fixed_key": "fixed_value",
    }

    output_after_result_path = output_state.apply_result_path(
        output_after_result_selector
    )
    test_value.update({"key3": output_after_result_selector.resource_result})
    assert output_after_result_path.value == test_value

    assert output_state.apply_to_state(test_state).value == test_value
