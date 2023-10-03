import logging
from pystepfunction.branch import Branch, MapTask, ItemProcessor, ProcessorConfig
from pystepfunction.errors import ERROR_STATE_ALL
from pystepfunction.lambda_function import LambdaTaskInvoke
from pystepfunction.tasks import SucceedTask, FailTask

logger = logging.getLogger(__name__)

expected = {
    "InputPath": "$.input_object",
    "ItemsPath": "$.items_in_input",
    "ItemProcessor": {
        "ProcessorConfig": {"Mode": "INLINE"},
        "Comment": "sub_branch",
        "StartAt": "lambda_task",
        "States": {
            "lambda_task": {
                "Catch": [
                    {
                        "ErrorEquals": ["States.ALL"],
                        "Next": "lambda_task_fail",
                        "ResultPath": "$.LambdaTaskResult",
                    }
                ],
                "Next": "succeeded",
                "Parameters": {"FunctionName": "aws::my-lambda-arn"},
                "Resource": "arn:aws:states:::lambda:invoke",
                "ResultPath": "$.LambdaTaskResult",
                "ResultSelector": {"SelectThis.$": "$.Payload.Result"},
                "Retry": [
                    {
                        "BackoffRate": 1.0,
                        "ErrorEquals": ["States.ALL"],
                        "IntervalSeconds": 1,
                        "MaxAttempts": 3,
                    }
                ],
                "Type": "Task",
            },
            "lambda_task_fail": {
                "Cause": "lambda task " "failed",
                "Error": "MyLambdaError",
                "Type": "Fail",
            },
            "succeeded": {"Type": "Succeed"},
        },
    },
    "MaxConcurrency": 0,
    "Type": "Map",
}


def test_map():
    # a task for the map
    lambda_task = (
        LambdaTaskInvoke("lambda_task", function_arn="aws::my-lambda-arn")
        .with_retry(error_equals=[ERROR_STATE_ALL], interval_seconds=1, max_attempts=3)
        .with_catcher(
            error=[ERROR_STATE_ALL],
            task=FailTask(
                "lambda_task_fail", cause="lambda task failed", error="MyLambdaError"
            ),
        )
        .with_resource_result({"Payload": {"Result": "LambdaResult"}})
        .with_output(
            result_path="$.LambdaTaskResult",
            result_selector={"SelectThis.$": "$.Payload.Result"},
        )
    )

    # sub-branch
    lambda_task = lambda_task >> SucceedTask("succeeded")
    sub_branch = Branch(comment="sub_branch", start_task=lambda_task)

    item_processor = ItemProcessor(sub_branch, ProcessorConfig(), "$.items_in_input")

    map_task = MapTask("map", item_processor).with_input(input_path="$.input_object")

    observed = map_task.to_asl()
    logger.info(str(observed))

    assert observed["map"] == expected
