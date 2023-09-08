from pystepfunction.branch import Branch
from pystepfunction.errors import ERROR_STATE_ALL
from pystepfunction.tasks import SucceedTask, FailTask, MapTask, LambdaTask

expected = {'InputPath': '$.input_object',
            'ItemsPath': '$.items_in_input',
            'ItemProcessor': {'Comment': 'sub_branch',
                               'StartAt': 'lambda_task',
                               'States': {'lambda_task': {'Catch': [{'ErrorEquals': ['States.ALL'],
                                                                     'Next': 'lambda_task_fail'}],
                                                          'End': False,
                                                          'Next': 'succeeded',
                                                          'Parameters': {'FunctionName': 'aws::my-lambda-arn'},
                                                          'Resource': 'arn:aws:states:::lambda:invoke',
                                                          'ResultPath': '$.LambdaTaskResult',
                                                          'ResultSelector': {'SelectThis.$': '$.Payload.Result'},
                                                          'Retry': [{'BackoffRate': 1.0,
                                                                     'ErrorEquals': ['States.ALL'],
                                                                     'IntervalSeconds': 1,
                                                                     'MaxAttempts': 3}],
                                                          'Type': 'Task'},
                                          'lambda_task_fail': {'Cause': 'lambda task '
                                                                        'failed',
                                                               'Error': 'MyLambdaError',
                                                               'Type': 'Fail'},
                                          'succeeded': {'Type': 'Succeed'}}},
            'MaxConcurrency': 10,
            'Type': 'Map'}


def test_map():
    # a task for the map
    lambda_task = (
        LambdaTask("lambda_task", function_arn="aws::my-lambda-arn")
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

    map_task = MapTask("map", "$.input_object", sub_branch) \
        .with_max_concurrency(10) \
        .with_items_path("$.items_in_input")

    observed = map_task.to_asl()

    assert observed == expected
