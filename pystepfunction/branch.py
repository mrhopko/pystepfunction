from dataclasses import dataclass
from typing import List, Optional
from pystepfunction.errors import ErrorHandler
from pystepfunction.tasks import ChoiceRule, ChoiceTask, Task


@dataclass
class TaskEdge:
    """Edge between two tasks in a stepfunction machine"""

    source: Task
    """Source task"""
    target: Task
    """Target task"""
    choice_rule: Optional[ChoiceRule] = None
    """Choice rule for the edge"""
    error_handler: Optional[ErrorHandler] = None
    """Error handler for the edge"""

    def has_choice_rule(self) -> bool:
        return self.choice_rule is not None

    def has_error_handler(self) -> bool:
        return self.error_handler is not None


def get_task_edges(task: Task) -> List[TaskEdge]:
    """Get the task edge for a task

    Args:
        task (Task): The task to get the edge for

    Returns:
        TaskEdge: The task edge for the task
    """
    edges = []
    match task._get_task_class_name():
        case ChoiceTask.__name__:
            for choice in task.choices:
                edges.append(
                    TaskEdge(
                        source=task,
                        target=choice.next,
                        choice_rule=choice,
                    )
                )
        case _:
            if task.has_next():
                for next_task in task._next:
                    edges.append(
                        TaskEdge(
                            source=task,
                            target=next_task,
                        )
                    )
    if task.has_error_handler():
        edges.append(
            TaskEdge(
                source=task,
                target=task.error_handler.next,
                error_handler=task.error_handler,
            )
        )

    return edges


class Branch:
    """A sequence of tasks to execute in a stepfunction machine"""

    def __init__(self, start_task: Task, comment: str = "") -> None:
        self.start_task = start_task
        """The first task to execute"""
        self.comment = comment
        """Comment for the stepfunction machine"""
        self.task_dict: dict = {}
        """Dictionary of all tasks in the stepfunction machine"""
        self.task_list: List[Task] = []
        """List of all tasks in the stepfunction machine"""
        self.task_edges: List[TaskEdge] = []

    def build_task_list(self, task: Task):
        """Build the list of tasks in the stepfunction machine

        Recursively checks task.next()

        Args:
            task (Task): The task to add to the list
        """
        if task.name in self.task_dict:
            return
        self.task_dict[task.name] = task
        self.task_list.append(task)
        edges = get_task_edges(task)
        self.task_edges.append(edges)
        if task.has_next():
            for t in task._next:
                self.build_task_list(t)

    def to_asl(self) -> dict:
        """Convert to ASL"""
        self.build_task_list(self.start_task)
        asl = {}
        for task in self.task_list:
            asl.update(task.to_asl())
        return {"Comment": self.comment, "StartAt": self.start_task.name, "States": asl}


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
        if self.next() is not None:
            asl["Next"] = self.next().name
        return {self.name: asl}
