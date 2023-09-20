from pystepfunction import lambda_function as lf


def test_lambda():
    lambda_task = lf.LambdaTaskInvoke("test", "arn").with_output(result_path="$.result")
    assert lambda_task.get_return_path() == "$.result.Payload"
