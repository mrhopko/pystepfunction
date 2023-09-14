from typing import Optional
from pystepfunction.tasks import Task, TaskInputState


class GlueTaskStartJobRun(Task):
    """Glue task for stepfunction machine"""

    resource: str = "arn:aws:states:::glue:startJobRun.sync"

    return_path_append = "JobRunId"

    def __init__(
        self, name: str, job_name: str, job_args: Optional[dict] = None
    ) -> None:
        """Initialize a glue task

        Args:
            name (str): Name of the task
            job_name (str): Name of the glue job - gets included as "JobName" in the task input parameters
            job_args (Optional[dict], optional): set of arguments to pass to the glue job. Gets appended to
            the Task input paramters. Defaults to None.
        """
        super().__init__(name)
        self.job_args = job_args
        self.job_name = job_name
        self.input_state = TaskInputState(parameters={"JobName": job_name})
        if job_args is not None:
            self._set_job_args(job_args)

    def with_job_args(self, job_args: dict) -> "GlueTaskStartJobRun":
        """Set the payload for the task

        Args:
            job_args dict: set of arguments to pass to the glue job.
            Gets appended to the Task input paramters.
        """
        self._set_job_args(job_args)
        return self

    def _set_job_args(self, job_args: dict):
        args = {}
        for k, v in job_args.items():
            if str(k).startswith("--"):
                args[k] = v
            else:
                args[f"--{k}"] = v
        assert self.input_state is not None
        self.input_state.with_parameter("Arguments", args)
