def test_example_asl():
    from logging import getLogger
    from pystepfunction.tasks import (
        PassTask,
        SucceedTask,
        FailTask,
    )
    from pystepfunction.branch import Branch
    from pystepfunction.viz import BranchViz
    from pystepfunction.state import StateMachine
    from pystepfunction.errors import ERROR_STATE_ALL
    from pystepfunction.glue import GlueTaskStartJobRun
    from pystepfunction.lambda_function import LambdaTaskInvoke

    logger = getLogger(__name__)

    # create a lambda task
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

    # create a glue task
    glue_task = (
        GlueTaskStartJobRun(
            "glue_task",
            job_name="my-glue-job-name",
            job_args={"job_input_arg.$": "$.LambdaTaskResult.SelectThis"},
        )
        .with_catcher(
            error=[ERROR_STATE_ALL],
            task=FailTask(
                "glue_task_fail", cause="glue task failed", error="MyGlueError"
            ),
        )
        .with_resource_result({"JobResult": "GlueResult"})
        .with_output(result_path="$.GlueTaskResult")
    )

    # chain them together and create a branch
    lambda_task = lambda_task >> glue_task >> SucceedTask("succeeded")
    branch = Branch(comment="Lambda and Glue", start_task=lambda_task)

    # view the asl
    print(branch)

    # asl as a dict
    asl = branch.to_asl()

    # write the asl to a file
    branch.write_asl("my_asl_file.asl.json")

    # visualise the asl
    BranchViz(branch).show()

    # create a state machine
    sm = StateMachine(state={"Input1": "Input1Value"}, logger=logger)
    sm.apply_branch(branch)
    sm.show_logs()

    assert True
