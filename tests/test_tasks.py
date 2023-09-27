from pystepfunction.tasks import PassTask, Task


# Test that PassTask creates correct ASL
def test_pass_task():
    result = {"result_key": "result_value"}

    task = PassTask("PassTask").and_then(Task("NextTask"))
    assert task.to_asl() == {
        "PassTask": {"Type": "Pass", "Next": "NextTask", "ResultPath": "$"}
    }

    task = PassTask("PassTask", result=result)
    assert task.to_asl() == {
        "PassTask": {"Type": "Pass", "Result": result, "ResultPath": "$"}
    }

    task = PassTask("PassTask", result_path="$.result")
    assert task.to_asl() == {
        "PassTask": {
            "Type": "Pass",
            "ResultPath": "$.result",
        }
    }

    task = PassTask("PassTask", result=result, result_path="$.result")
    assert task.to_asl() == {
        "PassTask": {
            "Type": "Pass",
            "Result": result,
            "ResultPath": "$.result",
        }
    }
