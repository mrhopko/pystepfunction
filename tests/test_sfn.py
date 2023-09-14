from pystepfunction import sfn


def test_start_execution_task():
    task = sfn.SfnStartExecutionTask(
        name="StartExecution",
        arn="test_arn",
        sfn_input={"input1": "value1"},
    )
    assert task.name == "StartExecution"
    assert task.arn == "test_arn"
    asl = task.to_asl()
    assert (
        asl["StartExecution"]["Resource"]
        == "arn:aws:states:::states:startExecution.sync:2"
    )
    assert asl["StartExecution"]["Parameters"]["StateMachineArn"] == "test_arn"
    assert asl["StartExecution"]["Parameters"]["Input"] == {
        "AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$": "$$.Execution.Id",
        "input1": "value1",
    }
