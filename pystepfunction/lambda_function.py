from pystepfunction.tasks import Task, TaskInputState


class LambdaTaskInvoke(Task):
    """Lambda task for stepfunction machine

    Properties:
        function_arn (str): ARN of the lambda function
    """

    resource: str = "arn:aws:states:::lambda:invoke"
    """Task resource for ASL = Lambda:invoke"""
    return_path_append = "Payload"

    def __init__(self, name: str, function_arn: str) -> None:
        """Initialize a lambda task

        Args:
            name (str): Name of the task
            function_arn (str): ARN of the lambda function"""
        super().__init__(name)
        self.function_arn = function_arn
        self.input_state = TaskInputState(
            parameters={"FunctionName": self.function_arn}
        )

    def with_payload(self, payload: dict) -> "LambdaTaskInvoke":
        """Set the payload for the task

        Args:
            payload (dict): Payload for the task"""
        if self.has_input_state():
            assert self.input_state is not None
            self.input_state.with_parameter("Payload", payload)
        return self
