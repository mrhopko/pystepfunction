import logging
from pystepfunction.dms import (
    DmsTaskDescribeReplicationTask,
    ReplicationTaskSettings,
    DmsTaskCreateReplicationTask,
)

logger = logging.getLogger(__name__)


def test_replication_settings():
    settings = ReplicationTaskSettings(
        ReplicationTaskIdentifier="test",
        ReplicationInstanceArn="testarn",
        SourceEndpointArn="arn:aws:dms:us-east-1:123456789012:endpoint:123456789012:source",
        TargetEndpointArn="$.target_endpoint_arn",
    )
    asl = settings.to_asl()
    logger.info(str(asl))
    assert asl["ReplicationTaskIdentifier"] == "test"
    assert asl["TargetEndpointArn.$"] == "$.target_endpoint_arn"


def test_create_replication_task():
    settings = ReplicationTaskSettings(
        ReplicationTaskIdentifier="test",
        ReplicationInstanceArn="$.testarn",
    )
    task = DmsTaskCreateReplicationTask("test", settings)
    asl = task.to_asl()
    logger.info(str(asl))
    assert (
        asl["test"]["Resource"]
        == "arn:aws:states:::aws-sdk:databasemigration:createReplicationTask"
    )
    assert asl["test"]["Parameters"]["ReplicationTaskIdentifier"] == "test"
    assert asl["test"]["Parameters"]["ReplicationInstanceArn.$"] == "$.testarn"


def test_describe_replication_task():
    task = DmsTaskDescribeReplicationTask("test", "task_id")
    asl = task.to_asl()
    logger.info(str(asl))
    assert (
        asl["test"]["Resource"]
        == "arn:aws:states:::aws-sdk:databasemigration:describeReplicationTasks"
    )
    assert asl["test"]["Parameters"]["Filters"] == [
        {
            "Name": "replication-task-id",
            "Values": "States.Array(task_id)",
        }
    ]
