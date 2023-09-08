from pystepfunction.tasks import (
    ChoiceRule,
    ChoiceTask,
    GlueTask,
    PassTask,
    Task,
    LambdaTask,
    Retry,
)
from pystepfunction.branch import Branch, ParallelTask
from logging import getLogger

logger = getLogger(__name__)


def test_task():
    a = Task("a") >> Task("b") >> Task("c") >> Task("d")
    assert a.name == "a"
    assert a.next().name == "b"
    assert a.next().next().name == "c"
    assert a.next().next().next().name == "d"


def test_lambda_task():
    l = LambdaTask("test_lambda", "test_arn").with_payload({"key1.$": "$.value1"})
    asl = l.to_asl()
    logger.info(asl)
    assert asl["test_lambda"]["Type"] == "Task"
    assert asl["test_lambda"]["Resource"] == "arn:aws:states:::lambda:invoke"
    assert asl["test_lambda"]["Parameters"]["Payload"]["key1.$"] == "$.value1"


def test_glue_task():
    l = GlueTask("test_glue", "glue_job").with_job_args(
        {"statekey1.$": "$.key1.key2", "fixed1": "fixed"}
    )
    asl = l.to_asl()
    logger.info(asl)
    assert asl["test_glue"]["Type"] == "Task"
    assert asl["test_glue"]["Resource"] == "arn:aws:states:::glue:startJobRun.sync"
    assert asl["test_glue"]["Parameters"]["Arguments"]["--statekey1.$"] == "$.key1.key2"
    assert asl["test_glue"]["Parameters"]["Arguments"]["--fixed1"] == "fixed"
    assert asl["test_glue"]["Parameters"]["JobName"] == "glue_job"


def test_branch():
    branch = (
        LambdaTask("lambda1", "fun1")
        >> LambdaTask("lambda2", "fun2")
        >> GlueTask("glue1", "job1")
    )
    machine = Branch(comment="mmm", start_task=branch)
    asl = machine.to_asl()
    assert branch.has_next()
    logger.info(asl)
    assert asl["Comment"] == "mmm"
    assert len(asl["States"].items()) == 3


def test_choice():
    choice = ChoiceTask(
        "choices",
        choices=[
            ChoiceRule("var1", "Bigger", next=Task("choice1")),
            ChoiceRule("var2", "Bigger", value=10, next=Task("choice2")),
        ],
        default=Task("default"),
    )
    asl = choice.to_asl()
    logger.info(asl)
    assert len(asl["choices"]["Choices"]) == 2


def test_branch_with_choice():
    task1 = Task("task1")
    task2 = Task("task2") >> Task("task3")
    branch = task1 >> ChoiceTask(
        "choice",
        [
            ChoiceRule("var1", "better", next=Task("end")),
            ChoiceRule("var2", "stronger", 10, next=task2),
        ],
        default=task2,
    )
    machine = Branch(branch, "choices in this one")
    asl = machine.to_asl()
    logger.info(asl)
    assert asl["States"]["task3"]["Type"] == "Task"
    assert len(machine.task_edges) == 4


def test_paralell():
    branch1 = Branch(Task("1") >> Task("2"))
    branch2 = Branch(Task("3") >> Task("4"))
    parallel = ParallelTask("par", [branch1, branch2])
    asl = parallel.to_asl()
    logger.info(asl)
    assert asl["par"]["Branches"][0]["StartAt"] == "1"
    assert asl["par"]["Branches"][1]["StartAt"] == "3"


def test_parallel_branch():
    branch1 = Branch(Task("1") >> Task("2"))
    branch2 = Branch(
        Task("3")
        .with_retries(
            retries=[
                Retry(error_equals=["States.ALL"], interval_seconds=1, max_attempts=3)
            ]
        )
        .with_catcher(["States.ALL"], Task("4"))
        >> PassTask("pass")
        >> Task("4")
    )
    branch_parallel = Branch(
        Task("start") >> ParallelTask("par", [branch1, branch2]) >> Task("end").is_end()
    )
    asl = branch_parallel.to_asl()
    logger.info(asl)
    assert asl["States"]["par"]["Branches"][0]["StartAt"] == "1"
    assert asl["States"]["end"]["End"] == True
