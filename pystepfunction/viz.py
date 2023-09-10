"""Visualization of a branch using pyvis

Example:
```python
branch_c1 = Task("Task-c1")
branch_c2 = Task("Task-c2") >> Task("Task-c3")
choice_1 = ChoiceRule("var1", "cond1", next=branch_c1)
choice_2 = ChoiceRule("var2", "cond2", next=branch_c2)
fail = FailTask("Fail")

branch_n1 = Branch(
    Task("Task1")
    .with_retry(error_equals=["States.ALL"], interval_seconds=1, max_attempts=3)
    .with_catcher(["States.ALL"], fail)
    >> Task("Task2")
    >> Task("Task3")
    >> ChoiceTask("ChoiceTask1", [choice_1, choice_2], branch_c1)
)

branch_viz = BranchViz(branch_n1)
assert isinstance(branch_viz, BranchViz)
assert len(branch_viz.branch_nodes) == len(branch_n1.task_list)
assert len(branch_viz.branch_edges) == len(branch_n1.task_edges)
logger.info(branch_viz.net)
branch_viz.show()
```
"""
from dataclasses import dataclass
from typing import List
from pyvis.network import Network  # type: ignore
from pystepfunction.branch import Branch, _TaskEdge
from pystepfunction.tasks import Task


@dataclass
class BranchNode:
    n_id: str
    label: str
    group: str = "task"
    level: int = 0
    shape: str = "box"
    title: str = ""
    physics: bool = False

    @classmethod
    def from_task(cls, task: Task) -> "BranchNode":
        """Convert to a node for visualization"""
        return cls(
            n_id=task.name,
            label=task.name,
            group=task.task_type,
            title=str(task.to_asl()),
        )


@dataclass
class BranchEdge:
    from_node: str
    to_node: str
    name: str
    title: str = ""
    physics: bool = False

    @classmethod
    def from_task_edge(cls, task_edge: _TaskEdge) -> "BranchEdge":
        """Convert to a branch edge for visualization"""

        source_node = BranchNode.from_task(task_edge.source)
        target_node = BranchNode.from_task(task_edge.target)
        if task_edge.has_choice_rule():
            assert task_edge.choice_rule is not None
            return cls(
                from_node=source_node.n_id,
                to_node=target_node.n_id,
                name=str(task_edge.choice_rule),
                title=str(task_edge.choice_rule.to_asl()),
            )
        elif task_edge.has_catcher():
            assert task_edge.catcher is not None
            e, t = task_edge.catcher
            return cls(
                from_node=source_node.n_id,
                to_node=target_node.n_id,
                name=str(e),
                title=str(e),
                physics=False,
            )
        else:
            return cls(
                from_node=source_node.n_id,
                to_node=target_node.n_id,
                name="",
                title=f"Next: {task_edge.target.name}",
                physics=False,
            )


class BranchViz:
    def __init__(self, branch: Branch):
        self.branch = branch
        self.task_list = branch.task_list
        self.task_edges = branch.task_edges
        self.branch_nodes: List[BranchNode] = self.get_nodes(self.task_list)
        self.branch_edges: List[BranchEdge] = self.get_edges(self.task_edges)
        self.net = Network(directed=True, notebook=False)
        self.net.toggle_physics(False)
        self.add_nodes()
        self.add_edges()

    def add_nodes(self):
        for node in self.branch_nodes:
            self.net.add_node(
                node.n_id,
                label=node.label,
                group=node.group,
                level=node.level,
                shape=node.shape,
                title=node.title,
                physics=node.physics,
            )

    def add_edges(self):
        for edge in self.branch_edges:
            self.net.add_edge(
                edge.from_node,
                edge.to_node,
                label=edge.name,
                title=edge.title,
                physics=edge.physics,
            )

    def get_nodes(self, task_list: List[Task]) -> List[BranchNode]:
        nodes = []
        for task in task_list:
            nodes.append(BranchNode.from_task(task))
        return nodes

    def get_edges(self, task_edges: List[_TaskEdge]) -> List[BranchEdge]:
        edges = []
        for edge in task_edges:
            edges.append(BranchEdge.from_task_edge(edge))
        return edges

    def show(self, filename: str = "branch.html", in_notebook: bool = False):
        self.net.show(filename, notebook=in_notebook)
