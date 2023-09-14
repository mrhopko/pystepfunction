from typing import Optional
from pystepfunction.tasks import Task, TaskInputState


class SfnStartExecutionTask(Task):
    resource = "arn:aws:states:::states:startExecution.sync:2"
    execution_id = {"AWS_STEP_FUNCTIONS_STARTED_BY_EXECUTION_ID.$": "$$.Execution.Id"}

    def __init__(self, name, arn: str, sfn_input: Optional[dict] = None):
        super().__init__(name)
        self.arn = arn
        self.input_state = TaskInputState(parameters={"StateMachineArn": self.arn})
        self.execution_input = self.execution_id.copy()
        if sfn_input:
            self.update_execution_input(sfn_input)

    def update_execution_input(self, sfn_input: dict) -> None:
        self.execution_input.update(sfn_input)
        assert self.input_state is not None
        self.input_state.with_parameter("Input", self.execution_input)

    def with_execution_input(self, sfn_input: dict) -> "SfnStartExecutionTask":
        self.update_execution_input(sfn_input)
        return self
