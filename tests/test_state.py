from logging import getLogger
from pystepfunction.tasks import Task, TaskInputState, TaskOutputState
from pystepfunction.state import StateMachine

logger = getLogger(__name__)


def test_input_state():
    expect_output = "val22"
    init_state = {"key1": "val1", "key2": {"key22": expect_output}}
    input_path = "$.key2.key22"
    expect_input_path = expect_output

    parameters = {"new_key22.$": "$", "fixed_key": "fixed_value"}
    expect_parameters = {"new_key22": expect_output, "fixed_key": "fixed_value"}

    task = Task("task1").with_input(input_path=input_path, parameters=parameters)

    test_state = StateMachine(state=init_state).with_logger(logger)

    test_state.apply_input_path(task)
    assert test_state.state == expect_input_path

    test_state.apply_input_parameters(task)
    assert test_state.state == expect_parameters


def test_output_state():
    expect_output = {"key222": "val222"}
    init_state = {"key1": "val1", "key2": {"key22": expect_output}}
    test_resource_result = {"resource2": "value2", "resource1": "value1"}
    result_selector = {"selected_result.$": "$.resource2", "fixed_key": "fixed_value"}
    result_path = "$.key3"
    output_path = "$.key2.key22"

    expect_result_path = init_state.copy()
    expect_result_path.update(
        {"key3": {"selected_result": "value2", "fixed_key": "fixed_value"}}
    )
    expected_output_path = expect_output

    task = (
        Task("task1")
        .with_resource_result(test_resource_result)
        .with_output(
            result_selector=result_selector,
            result_path=result_path,
            output_path=output_path,
        )
    )
    state_machine = StateMachine(state=init_state).with_logger(logger)
    state_machine.apply_result_path(task)

    assert state_machine.state == expect_result_path

    state_machine.apply_output_path(task)
    assert state_machine.state == expected_output_path


def test_input_output_state():
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
