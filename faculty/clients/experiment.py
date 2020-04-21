# Copyright 2018-2020 Faculty Science Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Interact with Faculty experiments.
"""


from collections import namedtuple
from enum import Enum

from marshmallow import fields, post_load, pre_dump, ValidationError
from marshmallow_enum import EnumField

from faculty._oneofschema import OneOfSchema
from faculty.clients.base import BaseSchema, BaseClient, Conflict


class LifecycleStage(Enum):
    """The lifecycle stage of an experiment or run."""

    ACTIVE = "active"
    DELETED = "deleted"


class ExperimentRunStatus(Enum):
    """The status of an experiment run."""

    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    SCHEDULED = "scheduled"
    KILLED = "killed"


Page = namedtuple("Page", ["start", "limit"])
Pagination = namedtuple("Pagination", ["start", "size", "previous", "next"])

Metric = namedtuple("Metric", ["key", "value", "timestamp", "step"])
Param = namedtuple("Param", ["key", "value"])
Tag = namedtuple("Tag", ["key", "value"])

Experiment = namedtuple(
    "Experiment",
    [
        "id",
        "name",
        "description",
        "artifact_location",
        "created_at",
        "last_updated_at",
        "deleted_at",
    ],
)


ExperimentRun = namedtuple(
    "ExperimentRun",
    [
        "id",
        "run_number",
        "experiment_id",
        "name",
        "parent_run_id",
        "artifact_location",
        "status",
        "started_at",
        "ended_at",
        "deleted_at",
        "tags",
        "params",
        "metrics",
    ],
)


class ComparisonOperator(Enum):
    DEFINED = "defined"
    EQUAL_TO = "eq"
    NOT_EQUAL_TO = "ne"
    LESS_THAN = "lt"
    LESS_THAN_OR_EQUAL_TO = "le"
    GREATER_THAN = "gt"
    GREATER_THAN_OR_EQUAL_TO = "ge"


ProjectIdFilter = namedtuple("ProjectIdFilter", ["operator", "value"])
ExperimentIdFilter = namedtuple("ExperimentIdFilter", ["operator", "value"])
RunIdFilter = namedtuple("RunIdFilter", ["operator", "value"])
RunStatusFilter = namedtuple("RunStatusFilter", ["operator", "value"])
DeletedAtFilter = namedtuple("DeletedAtFilter", ["operator", "value"])
TagFilter = namedtuple("TagFilter", ["key", "operator", "value"])
ParamFilter = namedtuple("ParamFilter", ["key", "operator", "value"])
MetricFilter = namedtuple("MetricFilter", ["key", "operator", "value"])


class LogicalOperator(Enum):
    AND = "and"
    OR = "or"


CompoundFilter = namedtuple("CompoundFilter", ["operator", "conditions"])


class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"


StartedAtSort = namedtuple("StartedAtSort", ["order"])
RunNumberSort = namedtuple("RunNumberSort", ["order"])
DurationSort = namedtuple("DurationSort", ["order"])
TagSort = namedtuple("TagSort", ["key", "order"])
ParamSort = namedtuple("ParamSort", ["key", "order"])
MetricSort = namedtuple("MetricSort", ["key", "order"])

RunQuery = namedtuple("RunQuery", ["filter", "sort", "page"])

MetricDataPoint = namedtuple("Metric", ["value", "timestamp", "step"])
MetricHistory = namedtuple(
    "MetricHistory", ["original_size", "subsampled", "key", "history"]
)

ListExperimentRunsResponse = namedtuple(
    "ListExperimentRunsResponse", ["runs", "pagination"]
)
DeleteExperimentRunsResponse = namedtuple(
    "DeleteExperimentRunsResponse", ["deleted_run_ids", "conflicted_run_ids"]
)
RestoreExperimentRunsResponse = namedtuple(
    "RestoreExperimentRunsResponse", ["restored_run_ids", "conflicted_run_ids"]
)


class ExperimentNameConflict(Exception):
    def __init__(self, name):
        tpl = "An experiment with name '{}' already exists in that project"
        message = tpl.format(name)
        super(ExperimentNameConflict, self).__init__(message)


class ParamConflict(Exception):
    def __init__(self, message, conflicting_params=None):
        super(ParamConflict, self).__init__(message)
        if conflicting_params is None:
            self.conflicting_params = []
        else:
            self.conflicting_params = conflicting_params


class ExperimentDeleted(Exception):
    def __init__(self, message, experiment_id):
        super(ExperimentDeleted, self).__init__(message)
        self.experiment_id = experiment_id


class ExperimentClient(BaseClient):
    """Client for the Faculty experiment service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("experiment")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    """

    _SERVICE_NAME = "atlas"

    def create(
        self, project_id, name, description=None, artifact_location=None
    ):
        """Create an experiment.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to create an experiment in.
        name : str
            The name of the new experiment.
        description : str, optional
            A description for the new experiment.
        artifact_location : str, optional
            The default location for artifacts of runs of this experiment. If
            not specified, artifacts will be stored in the datasets of this
            project.

        Returns
        -------
        Experiment
            The created experiment.

        Raises
        ------
        ExperimentNameConflict
            When an experiment of the provided name already exists in the
            project.
        """
        endpoint = "/project/{}/experiment".format(project_id)
        payload = {
            "name": name,
            "description": description,
            "artifactLocation": artifact_location,
        }
        try:
            return self._post(endpoint, _ExperimentSchema(), json=payload)
        except Conflict as err:
            if err.error_code == "experiment_name_conflict":
                raise ExperimentNameConflict(name)
            else:
                raise

    def get(self, project_id, experiment_id):
        """Get a specified experiment.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to get an experiment from.
        experiment_id : int
            The ID of the experiment to get.

        Returns
        -------
        Experiment
            The retrieved experiment.
        """
        endpoint = "/project/{}/experiment/{}".format(
            project_id, experiment_id
        )
        return self._get(endpoint, _ExperimentSchema())

    def list(self, project_id, lifecycle_stage=None):
        """List the experiments in a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to list experiments in.
        lifecycle_stage : LifecycleStage, optional
            To filter experiments in the given lifecycle stage only
            (ACTIVE | DELETED). By default, all experiments in the
            project are returned.

        Returns
        -------
        List[Experiment]
            The matching experiments.
        """
        query_params = {}
        if lifecycle_stage is not None:
            query_params["lifecycleStage"] = lifecycle_stage.value
        endpoint = "/project/{}/experiment".format(project_id)
        return self._get(
            endpoint, _ExperimentSchema(many=True), params=query_params
        )

    def update(self, project_id, experiment_id, name=None, description=None):
        """Update the name and/or description of an experiment.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment.
        experiment_id : int
            The ID of the experiment to update.
        name : str, optional
            The new name of the experiment. If not provided, the name will not
            be modified.
        description : str, optional
            The new description of the experiment. If not provided, the
            description will not be modified.

        Raises
        ------
        ExperimentNameConflict
            When an experiment of the provided name already exists in the
            project.
        """
        endpoint = "/project/{}/experiment/{}".format(
            project_id, experiment_id
        )
        payload = {"name": name, "description": description}
        try:
            self._patch_raw(endpoint, json=payload)
        except Conflict as err:
            if err.error_code == "experiment_name_conflict":
                raise ExperimentNameConflict(name)
            else:
                raise

    def delete(self, project_id, experiment_id):
        """Delete a specified experiment.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment.
        experiment_id : int
            The ID of the experiment to delete.
        """
        endpoint = "/project/{}/experiment/{}".format(
            project_id, experiment_id
        )
        self._delete_raw(endpoint)

    def restore(self, project_id, experiment_id):
        """Restore a specified experiment.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment.
        experiment_id : int
            The ID of the experiment to restore.
        """
        endpoint = "/project/{}/experiment/{}/restore".format(
            project_id, experiment_id
        )
        self._put_raw(endpoint)

    def create_run(
        self,
        project_id,
        experiment_id,
        name,
        started_at,
        parent_run_id=None,
        artifact_location=None,
        tags=None,
    ):
        """Create an experiment run in a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment.
        experiment_id : int
            The ID of the experiment to create a run of.
        name : str
            The name of the experiment run.
        started_at : datetime.datetime
            Time at which the run was started. If the datetime does not have a
            timezone, it will be assumed to be in UTC.
        parent_run_id : uuid.UUID, optional
            The ID of the parent run, if any.
        artifact_location: str, optional
            The location of the artifact repository to use for this run.
            If omitted, the value of `artifact_location` for the experiment
            will be used.
        tags : List[Tag]
            Tags to set on the created run.

        Returns
        -------
        ExperimentRun
            The created experiment run.

        Raises
        ------
        ExperimentDeleted
            When the run that is being updated refers to an experiment that is
            deleted
        """
        if tags is None:
            tags = []

        endpoint = "/project/{}/experiment/{}/run".format(
            project_id, experiment_id
        )
        payload = _CreateRunSchema().dump(
            {
                "name": name,
                "parent_run_id": parent_run_id,
                "started_at": started_at,
                "artifact_location": artifact_location,
                "tags": tags,
            }
        )
        try:
            return self._post(endpoint, _ExperimentRunSchema(), json=payload)
        except Conflict as err:
            if err.error_code == "experiment_deleted":
                raise ExperimentDeleted(
                    err.error, err.response.json()["experimentId"]
                )
            else:
                raise

    def get_run(self, project_id, run_id):
        """Get a specified experiment run.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment run.
        run_id : uuid.UUID
            The ID of the experiment run to get.

        Returns
        -------
        ExperimentRun
            The retrieved experiment run.
        """
        endpoint = "/project/{}/run/{}".format(project_id, run_id)
        return self._get(endpoint, _ExperimentRunSchema())

    def list_runs(
        self,
        project_id,
        experiment_ids=None,
        lifecycle_stage=None,
        start=None,
        limit=None,
    ):
        """List experiment runs.

        This method returns pages of runs. If less than the full number of runs
        for the job is returned, the ``next`` page of the returned response
        object will not be ``None``:

        >>> response = client.list_runs(project_id)
        >>> response.pagination.next
        Page(start=10, limit=10)

        Get all experiment runs by making successive calls to ``list_runs``,
        passing the ``start`` and ``limit`` of the ``next`` page each time
        until ``next`` is returned as ``None``.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment runs.
        experiment_ids : List[int], optional
            To filter runs of experiments with the given IDs only. If an empty
            list is passed, a result with an empty list of runs is returned.
            By default, runs from all experiments are returned.
        lifecycle_stage: LifecycleStage, optional
            To filter runs of experiments in a specific lifecycle stage only.
            By default, runs in any stage are returned.
        start : int, optional
            The (zero-indexed) starting point of runs to retrieve.
        limit : int, optional
            The maximum number of runs to retrieve.

        Returns
        -------
        ListExperimentRunsResponse
            The matching experiment runs.
        """

        experiment_ids_filter = None
        lifecycle_filter = None
        filter = None

        if experiment_ids is not None:
            if len(experiment_ids) == 0:
                return ListExperimentRunsResponse(
                    runs=[],
                    pagination=Pagination(
                        start=0, size=0, previous=None, next=None
                    ),
                )
            experiment_id_filters = [
                ExperimentIdFilter(ComparisonOperator.EQUAL_TO, experiment_id)
                for experiment_id in experiment_ids
            ]
            experiment_ids_filter = CompoundFilter(
                LogicalOperator.OR, experiment_id_filters
            )
        if lifecycle_stage is not None:
            lifecycle_filter = DeletedAtFilter(
                ComparisonOperator.DEFINED,
                lifecycle_stage == LifecycleStage.DELETED,
            )

        if experiment_ids_filter is not None and lifecycle_filter is not None:
            filter = CompoundFilter(
                LogicalOperator.AND, [experiment_ids_filter, lifecycle_filter]
            )
        elif experiment_ids_filter is not None:
            filter = experiment_ids_filter
        elif lifecycle_filter is not None:
            filter = lifecycle_filter

        return self.query_runs(project_id, filter, None, start, limit)

    def query_runs(
        self, project_id, filter=None, sort=None, start=None, limit=None
    ):
        """Query experiment runs.

        This method returns pages of runs. If less than the full number of runs
        for the job is returned, the ``next`` page of the returned response
        object will not be ``None``:

        >>> response = client.query_runs(project_id)
        >>> response.pagination.next
        Page(start=10, limit=10)

        Get all experiment runs by making successive calls to ``query_runs``,
        passing the ``start`` and ``limit`` of the ``next`` page each time
        until ``next`` is returned as ``None``.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment runs.
        filter: SingleFilter or CompoundFilter, optional
            To filter runs of experiments with the given filter. By default,
            runs from all experiments are returned.
        sort: List[Sort], optional
            Runs are order using the conditions in sort. The relative
            importance of each condition gradually decreases in order.
            By default, experiment runs are sorted by their startedAt value.
        start : int, optional
            The (zero-indexed) starting point of runs to retrieve.
        limit : int, optional
            The maximum number of runs to retrieve.

        Returns
        -------
        ListExperimentRunsResponse
            The matching experiment runs.
        """
        endpoint = "/project/{}/run/query".format(project_id)
        page = None
        if start is not None and limit is not None:
            page = Page(start, limit)
        payload = _RunQuerySchema().dump(RunQuery(filter, sort, page))
        return self._post(
            endpoint, _ListExperimentRunsResponseSchema(), json=payload
        )

    def log_run_data(
        self, project_id, run_id, metrics=None, params=None, tags=None
    ):
        """Update the data of a run.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment run.
        run_id : uuid.UUID
            The ID of the experiment run to update.
        metrics : List[Metric], optional
            Each metric will be inserted.
        params : List[Param], optional
            Each param will be inserted. Note that on a name conflict the
            entire operation will be rejected.
        tags : List[Tag], optional
            Each tag be upserted.

        Raises
        ------
        ParamConflict
            When a provided param already exists and has a different value than
            was specified.
        """
        if all(kwarg is None for kwarg in [metrics, params, tags]):
            return
        endpoint = "/project/{}/run/{}/data".format(project_id, run_id)
        payload = _ExperimentRunDataSchema().dump(
            {"metrics": metrics, "params": params, "tags": tags}
        )
        try:
            self._patch_raw(endpoint, json=payload)
        except Conflict as err:
            if err.error_code == "conflicting_params":
                raise ParamConflict(
                    err.error, err.response.json()["parameterKeys"]
                )
            else:
                raise

    def update_run_info(self, project_id, run_id, status=None, ended_at=None):
        """Update the status and end time of a run.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment run.
        run_id : uuid.UUID
            The ID of the experiment run to update.
        status: ExperimentRunStatus, optional
            The run status to set, if passed.
        ended_at: datetime, optional
            The run end time to set, if passed.

        Returns
        -------
        ExperimentRun
        """
        endpoint = "/project/{}/run/{}/info".format(project_id, run_id)
        payload = _ExperimentRunInfoSchema().dump(
            {"status": status, "ended_at": ended_at}
        )
        return self._patch(endpoint, _ExperimentRunSchema(), json=payload)

    def get_metric_history(self, project_id, run_id, key):
        """Get the history of a metric.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment run.
        run_id : uuid.UUID
            The ID of the experiment run to query.
        key : str
            The metric to get.

        Returns
        -------
        List[Metric]
            The history of the queried metric, ordered by timestamp and value.
        """
        endpoint = "/project/{}/run/{}/metric/{}/history".format(
            project_id, run_id, key
        )
        metric_history = self._get(endpoint, _MetricHistorySchema())
        return [
            Metric(
                key=metric_history.key,
                value=metric_data_point.value,
                timestamp=metric_data_point.timestamp,
                step=metric_data_point.step,
            )
            for metric_data_point in metric_history.history
        ]

    def delete_runs(self, project_id, run_ids=None):
        """Delete experiment runs.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment runs.
        run_ids : List[uuid.UUID], optional
            A list of run IDs to delete. If not specified, all runs in the
            project will be deleted. If an empty list is passed, no runs
            will be deleted.

        Returns
        -------
        DeleteExperimentRunsResponse
            Containing lists of successfully deleted and conflicting (already
            deleted) run IDs.
        """
        endpoint = "/project/{}/run/delete/query".format(project_id)

        if run_ids is None:
            # Delete all runs in project
            payload = {}  # No filter
        elif len(run_ids) == 0:
            return DeleteExperimentRunsResponse(
                deleted_run_ids=[], conflicted_run_ids=[]
            )
        else:
            run_id_filters = [
                RunIdFilter(ComparisonOperator.EQUAL_TO, run_id)
                for run_id in run_ids
            ]
            filter = CompoundFilter(LogicalOperator.OR, run_id_filters)
            payload = {"filter": _FilterSchema().dump(filter)}

        return self._post(
            endpoint, _DeleteExperimentRunsResponseSchema(), json=payload
        )

    def restore_runs(self, project_id, run_ids=None):
        """Restore experiment runs.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the experiment runs.
        run_ids : List[uuid.UUID], optional
            A list of run IDs to restore. If not specified, all runs in the
            project will be restored. If an empty list is passed, no runs
            will be restored.

        Returns
        -------
        RestoreExperimentRunsResponse
            Containing lists of successfully restored and conflicting (already
            active) run IDs.
        """
        endpoint = "/project/{}/run/restore/query".format(project_id)

        if run_ids is None:
            # Restore all runs in project
            payload = {}  # No filter
        elif len(run_ids) == 0:
            return RestoreExperimentRunsResponse(
                restored_run_ids=[], conflicted_run_ids=[]
            )
        else:
            run_id_filters = [
                RunIdFilter(ComparisonOperator.EQUAL_TO, run_id)
                for run_id in run_ids
            ]
            filter = CompoundFilter(LogicalOperator.OR, run_id_filters)
            payload = {"filter": _FilterSchema().dump(filter)}

        return self._post(
            endpoint, _RestoreExperimentRunsResponseSchema(), json=payload
        )


class _OptionalField(fields.Field):
    """Wrap another field, passing through Nones."""

    def __init__(self, nested, *args, **kwargs):
        self.nested = nested
        super(_OptionalField, self).__init__(*args, **kwargs)

    def _deserialize(self, value, *args, **kwargs):
        if value is None:
            return None
        else:
            return self.nested._deserialize(value, *args, **kwargs)

    def _serialize(self, value, *args, **kwargs):
        if value is None:
            return None
        else:
            return self.nested._serialize(value, *args, **kwargs)


class _OneOfSchemaWithoutType(OneOfSchema):
    def dump(self, *args, **kwargs):
        data = super(_OneOfSchemaWithoutType, self).dump(*args, **kwargs)
        # Remove the type field added by marshmallow-oneofschema
        return {k: v for k, v in data.items() if k != "type"}


class _PageSchema(BaseSchema):
    start = fields.Integer(required=True)
    limit = fields.Integer(required=True)

    @post_load
    def make_page(self, data):
        return Page(**data)


class _PaginationSchema(BaseSchema):
    start = fields.Integer(required=True)
    size = fields.Integer(required=True)
    previous = fields.Nested(_PageSchema, missing=None)
    next = fields.Nested(_PageSchema, missing=None)

    @post_load
    def make_pagination(self, data):
        return Pagination(**data)


class _MetricSchema(BaseSchema):
    key = fields.String(required=True)
    value = fields.Float(required=True)
    timestamp = fields.DateTime(required=True)
    step = fields.Integer(required=True)

    @post_load
    def make_metric(self, data):
        return Metric(**data)


class _ParamSchema(BaseSchema):
    key = fields.String(required=True)
    value = fields.String(required=True)

    @post_load
    def make_param(self, data):
        return Param(**data)


class _TagSchema(BaseSchema):
    key = fields.String(required=True)
    value = fields.String(required=True)

    @post_load
    def make_tag(self, data):
        return Tag(**data)


class _ExperimentSchema(BaseSchema):
    id = fields.Integer(data_key="experimentId", required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    artifact_location = fields.String(
        data_key="artifactLocation", required=True
    )
    created_at = fields.DateTime(data_key="createdAt", required=True)
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt", required=True)
    deleted_at = fields.DateTime(data_key="deletedAt", missing=None)

    @post_load
    def make_experiment(self, data):
        return Experiment(**data)


class _ExperimentRunSchema(BaseSchema):
    id = fields.UUID(data_key="runId", required=True)
    run_number = fields.Integer(data_key="runNumber", required=True)
    experiment_id = fields.Integer(data_key="experimentId", required=True)
    name = fields.String(required=True)
    parent_run_id = fields.UUID(data_key="parentRunId", missing=None)
    artifact_location = fields.String(
        data_key="artifactLocation", required=True
    )
    status = EnumField(ExperimentRunStatus, by_value=True, required=True)
    started_at = fields.DateTime(data_key="startedAt", required=True)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)
    deleted_at = fields.DateTime(data_key="deletedAt", missing=None)
    tags = fields.Nested(_TagSchema, many=True, required=True)
    params = fields.Nested(_ParamSchema, many=True, required=True)
    metrics = fields.Nested(_MetricSchema, many=True, required=True)

    @post_load
    def make_experiment_run(self, data):
        return ExperimentRun(**data)


# Schemas for payloads sent to API:


class _ExperimentRunDataSchema(BaseSchema):
    metrics = fields.List(fields.Nested(_MetricSchema))
    params = fields.List(fields.Nested(_ParamSchema))
    tags = fields.List(fields.Nested(_TagSchema))


class _ExperimentRunInfoSchema(BaseSchema):
    status = EnumField(ExperimentRunStatus, by_value=True, required=True)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)


class _ListExperimentRunsResponseSchema(BaseSchema):
    pagination = fields.Nested(_PaginationSchema, required=True)
    runs = fields.Nested(_ExperimentRunSchema, many=True, required=True)

    @post_load
    def make_list_runs_response_schema(self, data):
        return ListExperimentRunsResponse(**data)


class _CreateRunSchema(BaseSchema):
    name = fields.String()
    parent_run_id = fields.UUID(data_key="parentRunId")
    started_at = fields.DateTime(data_key="startedAt")
    artifact_location = fields.String(data_key="artifactLocation")
    tags = fields.Nested(_TagSchema, many=True, required=True)


class _ParamFilterValueField(fields.Field):
    """Field that passes through strings or numbers."""

    default_error_messages = {
        "unsupported_type": "Param values must be of type str, int or float."
    }

    def _serialize(self, value, attr, obj, **kwargs):
        if isinstance(value, str):
            field = fields.String()
        elif isinstance(value, int) or isinstance(value, float):
            field = fields.Number()
        else:
            self.fail("unsupported_type")
        return field._serialize(value, attr, obj, **kwargs)


class _FilterValueField(fields.Field):
    def __init__(self, other_field_type, *args, **kwargs):
        self.other_field_type = other_field_type
        super(_FilterValueField, self).__init__(*args, **kwargs)

    def _serialize(self, value, attr, obj, **kwargs):
        if obj.operator == ComparisonOperator.DEFINED:
            field = fields.Boolean()
        else:
            field = self.other_field_type
        return field._serialize(value, attr, obj, **kwargs)


def _validate_discrete(operator):
    if operator not in {
        ComparisonOperator.DEFINED,
        ComparisonOperator.EQUAL_TO,
        ComparisonOperator.NOT_EQUAL_TO,
    }:
        raise ValidationError({"operator": "Not a discrete operator."})


class _ProjectIdFilterSchema(BaseSchema):
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.UUID())
    by = fields.Constant("projectId", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        _validate_discrete(obj.operator)
        return obj


class _ExperimentIdFilterSchema(BaseSchema):
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.Integer())
    by = fields.Constant("experimentId", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        _validate_discrete(obj.operator)
        return obj


class _RunIdFilterSchema(BaseSchema):
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.UUID())
    by = fields.Constant("runId", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        _validate_discrete(obj.operator)
        return obj


class _RunStatusFilterSchema(BaseSchema):
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(EnumField(ExperimentRunStatus, by_value=True))
    by = fields.Constant("status", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        _validate_discrete(obj.operator)
        return obj


class _DeletedAtFilterSchema(BaseSchema):
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.DateTime())
    by = fields.Constant("deletedAt", dump_only=True)


class _TagFilterSchema(BaseSchema):
    key = fields.String()
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.String())
    by = fields.Constant("tag", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        _validate_discrete(obj.operator)
        return obj


class _ParamFilterSchema(BaseSchema):
    key = fields.String()
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(_ParamFilterValueField())
    by = fields.Constant("param", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        if isinstance(obj.value, str):
            _validate_discrete(obj.operator)
        return obj


class _MetricFilterSchema(BaseSchema):
    key = fields.String()
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.Float())
    by = fields.Constant("metric", dump_only=True)


class _CompoundFilterSchema(BaseSchema):
    operator = EnumField(LogicalOperator, by_value=True)
    conditions = fields.List(fields.Nested("_FilterSchema"))


class _FilterSchema(_OneOfSchemaWithoutType):
    type_schemas = {
        "ProjectIdFilter": _ProjectIdFilterSchema,
        "ExperimentIdFilter": _ExperimentIdFilterSchema,
        "RunIdFilter": _RunIdFilterSchema,
        "RunStatusFilter": _RunStatusFilterSchema,
        "DeletedAtFilter": _DeletedAtFilterSchema,
        "TagFilter": _TagFilterSchema,
        "ParamFilter": _ParamFilterSchema,
        "MetricFilter": _MetricFilterSchema,
        "CompoundFilter": _CompoundFilterSchema,
    }


class _StartedAtSortSchema(BaseSchema):
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("startedAt", dump_only=True)


class _RunNumberSortSchema(BaseSchema):
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("runNumber", dump_only=True)


class _DurationSortSchema(BaseSchema):
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("duration", dump_only=True)


class _TagSortSchema(BaseSchema):
    key = fields.String()
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("tag", dump_only=True)


class _ParamSortSchema(BaseSchema):
    key = fields.String()
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("param", dump_only=True)


class _MetricSortSchema(BaseSchema):
    key = fields.String()
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("metric", dump_only=True)


class _SortSchema(_OneOfSchemaWithoutType):
    type_schemas = {
        "StartedAtSort": _StartedAtSortSchema,
        "RunNumberSort": _RunNumberSortSchema,
        "DurationSort": _DurationSortSchema,
        "TagSort": _TagSortSchema,
        "ParamSort": _ParamSortSchema,
        "MetricSort": _MetricSortSchema,
    }


class _RunQuerySchema(BaseSchema):
    filter = _OptionalField(fields.Nested(_FilterSchema))
    sort = fields.List(fields.Nested(_SortSchema))
    page = fields.Nested(_PageSchema, missing=None)


# Schemas for responses returned from API:


class _DeleteExperimentRunsResponseSchema(BaseSchema):
    deleted_run_ids = fields.List(
        fields.UUID(), data_key="deletedRunIds", required=True
    )
    conflicted_run_ids = fields.List(
        fields.UUID(), data_key="conflictedRunIds", required=True
    )

    @post_load
    def make_delete_runs_response(self, data):
        return DeleteExperimentRunsResponse(**data)


class _RestoreExperimentRunsResponseSchema(BaseSchema):
    restored_run_ids = fields.List(
        fields.UUID(), data_key="restoredRunIds", required=True
    )
    conflicted_run_ids = fields.List(
        fields.UUID(), data_key="conflictedRunIds", required=True
    )

    @post_load
    def make_restore_runs_response(self, data):
        return RestoreExperimentRunsResponse(**data)


class _MetricDataPointSchema(BaseSchema):
    """Deserialise a data point from the metric history endpoint.

    This schema is written with the expectation that it is not used
    alongside the metric subsampling feature, which can result in null
    timestamp or step, or a non-integer step.
    """

    value = fields.Float(required=True)
    timestamp = fields.DateTime(required=True)
    step = fields.Integer(required=True)

    @post_load
    def make_metric(self, data):
        return MetricDataPoint(**data)


class _MetricHistorySchema(BaseSchema):
    original_size = fields.Integer(data_key="originalSize", required=True)
    subsampled = fields.Boolean(required=True)
    key = fields.String(required=True)
    history = fields.Nested(_MetricDataPointSchema, many=True, required=True)

    @post_load
    def make_history(self, data):
        return MetricHistory(**data)
