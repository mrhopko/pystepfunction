from pystepfunction.tasks import Task


class SsmTaskGetParameter(Task):
    resource = "arn:aws:states:::aws-sdk:ssm:getParameter"
    return_path_append = "Parameter.Value"

    def __init__(self, name: str, parameter_name: str) -> None:
        super().__init__(name)
        self.parameter_name = parameter_name
        self.parameters = {"Name": parameter_name}
