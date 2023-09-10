"""
A branch is a sequence of tasks in a stepfunction machine.

`Branch` - a sequence of tasks in a stepfunction.  
`ParallelTask` - a list of branches to execute in parallel.  

Examples:
```python
from pystepfunction.tasks import LambdaTask, GlueTask, SucceedTask
from pystepfunction.branch import Branch


# create a simple chain of tasks
lambda_task = (
    LambdaTask(name="LambdaTaskName", function_arn="my-lambda-arn")
    .and_then(GlueTask(name="GlueTaskName", job_name="my-glue-job-name"))
    .and_then(SucceedTask(name="SucceedTaskName"))
)
# or
lambda_task = (
    LambdaTask(name="LambdaTaskName", function_arn="my-lambda-arn") 
    >> GlueTask(name="GlueTaskName", job_name="my-glue-job-name")
    >> SucceedTask(name="SucceedTaskName")
)

# create a branch
easy_branch = Branch(comment="This is an easy branch", start_task=lambda_task)
# view the asl as a dict
asl = easy_branch.to_asl()
asl

# write the asl to a file
asl.write_asl("my_asl_file.asl.json")
# create a parallel task
branch1 = Branch(Task("1") >> Task("2"))
branch2 = Branch(Task("3") >> Task("4"))
parallel = ParallelTask("par", [branch1, branch2])  
branch_with_parallel = Branch(comment="This is a full branch", start_task=(
   Task("start") >> parallel >> Task("end")
))
branch_with_parallel.to_asl()
```
"""

import json
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple
from pystepfunction.tasks import ChoiceRule, ChoiceTask, Task


@dataclass
class _TaskEdge:
    """Used to visualize the edges between tasks in a stepfunction machine"""

    source: Task
    """Source task"""
    target: Task
    """Target task"""
    choice_rule: Optional[ChoiceRule] = None
    """Choice rule for the edge"""
    catcher: Optional[Tuple[List[str], Task]] = None
    """Error handler for the edge"""

    def has_choice_rule(self) -> bool:
        return self.choice_rule is not None

    def has_catcher(self) -> bool:
        return self.catcher is not None


def _get_task_edges(task: Task) -> List[_TaskEdge]:
    """Get the task edge for a task

    Args:
        task (Task): The task to get the edge for

    Returns:
        TaskEdge: The task edge for the task
    """
    edges = []
    match task._get_task_class_name():
        case ChoiceTask.__name__:
            assert isinstance(task, ChoiceTask)
            for choice in task.choices:
                edges.append(
                    _TaskEdge(
                        source=task,
                        target=choice.next,
                        choice_rule=choice,
                    )
                )
        case _:
            if task.has_next():
                assert task._next is not None
                for next_task in task._next:
                    edges.append(
                        _TaskEdge(
                            source=task,
                            target=next_task,
                        )
                    )
    if task.has_catcher():
        assert task.catcher is not None
        for k, v in task.catcher:
            edges.append(
                _TaskEdge(
                    source=task,
                    target=v,
                    catcher=(k, v),
                )
            )

    return edges


class Branch:
    """A sequence of tasks to execute in a stepfunction machine

    Example:
    ```python
    from pystepfunction.tasks import LambdaTask, GlueTask, SucceedTask
    from pystepfunction.branch import Branch

    # create a simple chain of tasks
    lambda_task = (
        LambdaTask(name="LambdaTaskName", function_arn="my-lambda-arn")
        >> GlueTask(name="GlueTaskName", job_name="my-glue-job-name")
        >> SucceedTask(name="SucceedTaskName")
    )

    # create a branch
    easy_branch = Branch(comment="This is an easy branch", start_task=lambda_task)
    # view the asl as a dict
    asl = easy_branch.to_asl()
    asl

    # write the asl to a file
    asl.write_asl("my_asl_file.asl.json")
    ```
    """

    def __init__(self, start_task: Task, comment: str = "") -> None:
        self.start_task = start_task
        """The first task to execute in a chain of tasks"""
        self.comment = comment
        """Comment for the stepfunction machine"""
        self.task_dict: dict = {}
        """Dictionary of all tasks in the branch"""
        self.task_list: List[Task] = []
        """List of all tasks in the branch"""
        self.task_edges: List[_TaskEdge] = []
        self._build_task_list(self.start_task)

    def _build_task_list(self, task: Task):
        """Build the list of tasks in the stepfunction machine

        Recursively checks task.next()

        Args:
            task (Task): The task to add to the list
        """
        if task.name in self.task_dict:
            return
        self.task_dict[task.name] = task
        self.task_list.append(task)
        edges = _get_task_edges(task)
        self.task_edges.extend(edges)
        if task.has_next():
            assert task._next is not None
            for t in task._next:
                self._build_task_list(t)
        if task.has_catcher():
            assert task.catcher is not None
            for k, v in task.catcher:
                self._build_task_list(v)

    def to_asl(self) -> dict:
        """Convert to ASL"""
        self._build_task_list(self.start_task)
        asl = {}
        for task in self.task_list:
            asl.update(task.to_asl())
        return {"Comment": self.comment, "StartAt": self.start_task.name, "States": asl}

    def write_asl(self, output_to: str) -> None:
        """Write the ASL to a file

        Args:
            output_to (str): The file to write to
        """

        with open(output_to, "w") as f:
            json.dump(self.to_asl(), f, indent=4)

    def __repr__(self) -> str:
        asl = self.to_asl()
        return json.dumps(asl, indent=2)


class ParallelTask(Task):
    """Parallel task for stepfunction machine

    Execute several branches in parallel

    Properties:
        branches (List[Branch]): List of branches to execute
    """

    task_type = "Parallel"

    def __init__(self, name: str, branches: List[Branch]) -> None:
        """Initialize a parallel task

        Args:
            name (str): Name of the task
            branches (List[Branch]): List of branches to execute
        """
        super().__init__(name)
        self.branches = branches

    def to_asl(self) -> dict:
        """Convert to ASL"""
        asl = {
            "Type": self.task_type,
            "End": self.end,
            "Branches": [branch.to_asl() for branch in self.branches],
        }
        next = self.next()
        if next is not None:
            asl["Next"] = next.name
        return {self.name: asl}
