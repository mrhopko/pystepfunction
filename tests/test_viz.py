from logging import getLogger
from pystepfunction.tasks import ChoiceRule, ChoiceTask, FailTask, Task, Retry
from pystepfunction.branch import Branch
from pystepfunction.viz import BranchNode, BranchEdge, BranchViz

logger = getLogger(__name__)


def test_branch_node_from_task():
    task = Task("Task")
    branch_node = BranchNode.from_task(task)
    assert isinstance(branch_node, BranchNode)


def test_branch_edge_from_task_edge():
    branch = Branch(Task("Task1") >> Task("Task2"))
    logger.info(branch.task_edges)
    branch_edge = BranchEdge.from_task_edge(branch.task_edges[0])
    logger.info(branch_edge)
    assert isinstance(branch_edge, BranchEdge)


def test_branch_viz():
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
    assert True
