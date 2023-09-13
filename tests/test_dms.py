import logging
from pystepfunction.dms import ReplicationTaskSettings, DmsTaskCreateReplicationTask

logger = logging.getLogger(__name__)


def test_replication_settings():
    settings = ReplicationTaskSettings(
        ReplicationTaskIdentifier="test",
        SourceEndpointArn="arn:aws:dms:us-east-1:123456789012:endpoint:123456789012:source",
    )
    asl = settings.to_asl()
    assert asl["ReplicationTaskIdentifier"] == "test"


def test_create_replication_task():
    settings = ReplicationTaskSettings(
        ReplicationTaskIdentifier="test",
        SourceEndpointArn="endpoint1234",
    )
    task = DmsTaskCreateReplicationTask("test", settings)
    asl = task.to_asl()
    logger.info(str(asl))
    assert (
        asl["test"]["Resource"]
        == "arn:aws:states:::aws-sdk:databasemigration:createReplicationTask"
    )
    assert asl["test"]["Parameters"]["ReplicationTaskIdentifier"] == "test"
