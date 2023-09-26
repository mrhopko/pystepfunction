"""DMS Tasks for AWS Step Functions"""
from dataclasses import dataclass
import datetime
from typing import List, Optional
from mypy_boto3_dms import type_defs
from pystepfunction.tasks import Task, TaskInputState
from pystepfunction import tasks

RULE_ACTION_INCLUDE = "include"
RULE_ACTION_EXCLUDE = "exclude"
RULE_ACTION_EXPLICIT = "explicit"

MIGRATION_TYPE_FULL_LOAD = "full-load"
MIGRATION_TYPE_CDC = "cdc"
MIGRATION_TYPE_FULL_LOAD_AND_CDC = "full-load-and-cdc"


@dataclass
class SelectionRule:
    """DMS Selection Rule"""

    rule_id: int
    rule_name: str
    schema_name: str
    table_name: str
    rule_action: str = RULE_ACTION_EXPLICIT

    def to_asl(self):
        return {
            "rule-type": "selection",
            "rule-id": self.rule_id,
            "rule-name": self.rule_name,
            "object-locator": {
                "schema-name": self.schema_name,
                "table-name": self.table_name,
            },
            "rule-action": self.rule_action,
        }


@dataclass
class ReplicationTaskSettings:
    """DMS Replication Task Settings for creating a DMS task"""

    ReplicationInstanceArn: str
    ReplicationTaskIdentifier: str
    SourceEndpointArn: Optional[str] = None
    TargetEndpointArn: Optional[str] = None
    MigrationType: str = MIGRATION_TYPE_FULL_LOAD_AND_CDC
    CdcStartPosition: Optional[str] = None
    CdcStartTime: Optional[datetime.datetime] = None
    CdcStopPosition: Optional[str] = None
    ReplicationTaskSettings: Optional[str] = None
    ResourceIdentifier: Optional[str] = None
    TableMappings: Optional[str] = None
    TaskData: Optional[str] = None

    def to_asl(self) -> dict:
        result = {
            tasks.asl_key_path(k, v): v
            for k, v in self.__dict__.items()
            if v is not None
        }
        return result


def select_tables(
    schema_name: str, table_names: List[str], escape_char: str = ""
) -> dict:
    """Create a list of selection rules for a given schema and list of table names

    Args:
        schma_name (str): Schema name
        table_names (List[str]): List of table names

    Returns:
        dict: dictionary representation of selection rules
    """
    schema_name_escape = schema_name.replace("_", f"{escape_char}_")
    rules = []
    for i, table_name in enumerate(table_names):
        table_name_escape = table_name.replace("_", f"{escape_char}_")
        rules.append(
            SelectionRule(
                rule_id=i,
                rule_name=f"{schema_name}_{table_name}",
                schema_name=schema_name_escape,
                table_name=table_name_escape,
            ).to_asl()
        )
    return {"rules": rules}


class DmsTask(Task):
    """Base DMS Task for building specific DMS tasks"""

    resource_stub: str = "arn:aws:states:::aws-sdk:databasemigration"

    def __init__(
        self,
        name: str,
        dms_cmd: str,
        task_id: Optional[str] = None,
        task_arn: Optional[str] = None,
    ) -> None:
        """Base Task for buiding DMS tasks

        Args:
            name (str): Name of the task
            task_id (str): DMS task id (i.e name of DMS task)
            dms_cmd (str): DMS command to run (i.e startReplicationTask)
            task_arn (Optional[str], optional): DMS task arn. Defaults to None.
        """
        super().__init__(name)
        self.task_id = task_id
        self.dms_cmd = dms_cmd
        self.task_arn = task_arn
        self.resource = f"{self.resource_stub}:{self.dms_cmd}"
        parameters = {}
        if task_id is not None:
            parameters["ReplicationTaskIdentifier"] = task_id
        if task_arn is not None:
            parameters["ReplicationTaskArn"] = task_arn
        self.input_state = TaskInputState(parameters=parameters)


class DmsTaskCreateReplicationTask(DmsTask):
    dms_cmd = "createReplicationTask"

    def __init__(self, name: str, task_settings: ReplicationTaskSettings) -> None:
        """Creates a DMS replication task

        Args:
            name (str): Name of the task
            task_id (str): DMS task id (i.e name of DMS task)
        """
        self.task_settings = task_settings
        super().__init__(
            name, dms_cmd=self.dms_cmd, task_id=task_settings.ReplicationTaskIdentifier
        )
        self.input_state = TaskInputState(parameters=task_settings.to_asl())

    def with_resource_result_type(
        self, resource_result: type_defs.CreateReplicationTaskResponseTypeDef
    ) -> Task:
        """Use a mypy typed definition to set the resource result"""
        return super().with_resource_result(resource_result)


class DmsTaskDeleteReplicationTask(DmsTask):
    dms_cmd = "deleteReplicationTask"

    def __init__(self, name: str, task_arn: str) -> None:
        """Deletes a DMS replication task

        Args:
            name (str): Name of the task
            task_arn (str): DMS task ARN
        """
        super().__init__(name, dms_cmd=self.dms_cmd, task_arn=task_arn)

    def with_resource_result_type(
        self, resource_result: type_defs.DeleteReplicationTaskResponseTypeDef
    ) -> Task:
        """Use a mypy typed definition to set the resource result"""
        return super().with_resource_result(resource_result)


class DmsTaskDescribeReplicationTask(DmsTask):
    dms_cmd = "describeReplicationTasks"

    def __init__(self, name: str, task_id: str) -> None:
        """Descrive a DMS replication task

        Args:
            name (str): Name of the task
            task_id (str): DMS task id (i.e name of DMS task)
        """
        super().__init__(name, dms_cmd=self.dms_cmd, task_id=task_id)
        if task_id.startswith("$."):
            values = "Values.$"
        else:
            values = "Values"
        self.input_state = TaskInputState(
            parameters={
                "Filters": [
                    {
                        "Name": "replication-task-id",
                        values: f"States.Array({task_id})",
                    }
                ]
            }
        )

    def with_resource_result_type(
        self, resource_result: type_defs.DescribeReplicationTasksResponseTypeDef
    ) -> Task:
        """Use a mypy typed definition to set the resource result"""
        return super().with_resource_result(resource_result)


class DmsTaskModifyReplicationTask(DmsTask):
    dms_cmd = "modifyReplicationTask"

    def __init__(self, name: str, task_arn: str) -> None:
        """Modifies a DMS replication task

        Args:
            name (str): Name of the task
            task_arn (str): DMS task id (i.e name of DMS task)
        """
        super().__init__(name, dms_cmd=self.dms_cmd, task_arn=task_arn)

    def with_resource_result_type(
        self, resource_result: type_defs.ModifyReplicationTaskResponseTypeDef
    ) -> Task:
        """Use a mypy typed definition to set the resource result"""
        return super().with_resource_result(resource_result)


class DmsTaskReloadTables(DmsTask):
    dms_cmd = "reloadTables"

    def __init__(self, name: str, task_id: str) -> None:
        """Reloads a DMS endpoint tables

        Args:
            name (str): Name of the task
            task_id (str): DMS task id (i.e name of DMS task)
        """
        super().__init__(name, dms_cmd=self.dms_cmd, task_id=task_id)

    def with_resource_result_type(
        self, resource_result: type_defs.ReloadTablesResponseTypeDef
    ) -> Task:
        """Use a mypy typed definition to set the resource result"""
        return super().with_resource_result(resource_result)


class DmsTaskStartReplicationTask(DmsTask):
    dms_cmd = "startReplicationTask"

    def __init__(
        self,
        name: str,
        task_arn: str,
        replication_type: str = MIGRATION_TYPE_FULL_LOAD_AND_CDC,
    ) -> None:
        """Starts a DMS replication task

        Args:
            name (str): Name of the task
            task_arn (str): DMS task ARN
            task_type (str): Type of task (i.e full-load, cdc, full-load-and-cdc)
        """
        super().__init__(name, dms_cmd=self.dms_cmd, task_arn=task_arn)
        self.replication_type = replication_type
        assert self.input_state
        assert self.input_state.parameters
        self.input_state.parameters["StartReplicationTaskType"] = replication_type

    def with_resource_result_type(
        self, resource_result: type_defs.StartReplicationResponseTypeDef
    ) -> Task:
        """Use a mypy typed definition to set the resource result"""
        return super().with_resource_result(resource_result)


class DmsTaskStopReplicationTask(DmsTask):
    dms_cmd = "startReplicationTask"

    def __init__(self, name: str, task_arn: str) -> None:
        """Starts a DMS replication task

        Args:
            name (str): Name of the task
            task_arn (str): DMS task ARN
        """
        super().__init__(name, dms_cmd=self.dms_cmd, task_arn=task_arn)

    def with_resource_result_type(
        self, resource_result: type_defs.StopReplicationResponseTypeDef
    ) -> Task:
        """Use a mypy typed definition to set the resource result"""
        return super().with_resource_result(resource_result)
