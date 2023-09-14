from pystepfunction import ssm
from pystepfunction import tasks


def test_json_path_append():
    path = "$."
    append = ".Parameter"
    assert tasks.json_path_append(path, append) == "$.Parameter"
    path = "."
    append = "Parameter"
    assert tasks.json_path_append(path, append) == "$.Parameter"


def test_ssm():
    task = ssm.SsmTaskGetParameter("GetParameter", "my-parameter")
    assert task.get_return_path() == "$.Parameter.Value"
    assert task.resource == "arn:aws:states:::aws-sdk:ssm:getParameter"
    assert task.parameters == {"Name": "my-parameter"}
    task.with_output(result_path="$.result")
    assert task.get_return_path() == "$.result.Parameter.Value"
