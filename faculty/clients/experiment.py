# Copyright 2018-2019 Faculty Science Limited
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

from collections import namedtuple
from enum import Enum

from marshmallow import fields, post_load
from marshmallow_enum import EnumField

from faculty.clients.base import BaseClient, BaseSchema, Conflict


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


class ExperimentRunStatus(Enum):
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    SCHEDULED = "scheduled"
    KILLED = "killed"


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

Metric = namedtuple("Metric", ["key", "value", "timestamp", "step"])
Param = namedtuple("Param", ["key", "value"])
Tag = namedtuple("Tag", ["key", "value"])

Page = namedtuple("Page", ["start", "limit"])
Pagination = namedtuple("Pagination", ["start", "size", "previous", "next"])
ListExperimentRunsResponse = namedtuple(
    "ListExperimentRunsResponse", ["runs", "pagination"]
)
DeleteExperimentRunsResponse = namedtuple(
    "DeleteExperimentRunsResponse", ["deleted_run_ids", "conflicted_run_ids"]
)
RestoreExperimentRunsResponse = namedtuple(
    "RestoreExperimentRunsResponse", ["restored_run_ids", "conflicted_run_ids"]
)

_SingleFilter = namedtuple("_SingleFilter", ["by", "key", "operator", "value"])


class SingleFilter(_SingleFilter):
    def __new__(cls, by, key, operator, value):
        if isinstance(by, SingleFilterBy) and by.needs_key() and key is None:
            raise ValueError(
                "key must not be none for filter type {}".format(by)
            )
        elif (
            isinstance(by, SingleFilterBy)
            and not by.needs_key()
            and key is not None
        ):
            raise ValueError("key must be none for filter type {}".format(by))
        elif (
            by == SingleFilterBy.PARAM
            and operator.is_numeric()
            and not (isinstance(value, float) or isinstance(value, int))
        ):
            raise ValueError(
                (
                    "invalid type {}. Value has to be either an int or a float"
                ).format(type(value))
            )
        return super(SingleFilter, cls).__new__(cls, by, key, operator, value)


CompoundFilter = namedtuple("CompoundFilter", ["operator", "conditions"])


class SingleFilterOperator(Enum):
    DEFINED = "defined"
    EQUAL_TO = "eq"
    NOT_EQUAL_TO = "ne"
    LESS_THAN = "lt"
    LESS_THAN_OR_EQUAL_TO = "le"
    GREATER_THAN = "gt"
    GREATER_THAN_OR_EQUAL_TO = "ge"

    def is_numeric(self):
        return self in {
            SingleFilterOperator.LESS_THAN,
            SingleFilterOperator.LESS_THAN_OR_EQUAL_TO,
            SingleFilterOperator.GREATER_THAN,
            SingleFilterOperator.GREATER_THAN_OR_EQUAL_TO,
        }


class SingleFilterBy(Enum):
    PROJECT_ID = "projectId"
    EXPERIMENT_ID = "experimentId"
    RUN_ID = "runId"
    DELETED_AT = "deletedAt"
    TAG = "tag"
    PARAM = "param"
    METRIC = "metric"

    def needs_key(self):
        return self in {
            SingleFilterBy.TAG,
            SingleFilterBy.PARAM,
            SingleFilterBy.METRIC,
        }


class CompoundFilterOperator(Enum):
    AND = "and"
    OR = "or"


_Sort = namedtuple("_Sort", ["by", "key", "order"])


class Sort(_Sort):
    def __new__(cls, by, key, order):
        if by.needs_key() and key is None:
            raise ValueError(
                "key must not be none for sort type {}".format(by)
            )
        elif not by.needs_key() and key is not None:
            raise ValueError("key must be none for sort type {}".format(by))
        return super(Sort, cls).__new__(cls, by, key, order)


class SortBy(Enum):
    STARTED_AT = "startedAt"
    RUN_NUMBER = "runNumber"
    DURATION = "duration"
    TAG = "tag"
    PARAM = "param"
    METRIC = "metric"

    def needs_key(self):
        return self in {SortBy.TAG, SortBy.PARAM, SortBy.METRIC}


class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"


QueryRuns = namedtuple("QueryRuns", ["filter", "sort", "page"])


class PageSchema(BaseSchema):
    start = fields.Integer(required=True)
    limit = fields.Integer(required=True)

    @post_load
    def make_page(self, data):
        return Page(**data)


MetricDataPoint = namedtuple("Metric", ["value", "timestamp", "step"])

MetricHistory = namedtuple(
    "MetricHistory", ["original_size", "subsampled", "key", "history"]
)


class MetricSchema(BaseSchema):
    key = fields.String(required=True)
    value = fields.Float(required=True)
    timestamp = fields.DateTime(required=True)
    step = fields.Integer(required=True)

    @post_load
    def make_metric(self, data):
        return Metric(**data)


class ParamSchema(BaseSchema):
    key = fields.String(required=True)
    value = fields.String(required=True)

    @post_load
    def make_param(self, data):
        return Param(**data)


class TagSchema(BaseSchema):
    key = fields.String(required=True)
    value = fields.String(required=True)

    @post_load
    def make_tag(self, data):
        return Tag(**data)


class LifecycleStage(Enum):
    ACTIVE = "active"
    DELETED = "deleted"


class ExperimentSchema(BaseSchema):
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


class ExperimentRunSchema(BaseSchema):
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
    tags = fields.Nested(TagSchema, many=True, required=True)
    params = fields.Nested(ParamSchema, many=True, required=True)
    metrics = fields.Nested(MetricSchema, many=True, required=True)

    @post_load
    def make_experiment_run(self, data):
        return ExperimentRun(**data)


class ExperimentRunDataSchema(BaseSchema):
    metrics = fields.List(fields.Nested(MetricSchema))
    params = fields.List(fields.Nested(ParamSchema))
    tags = fields.List(fields.Nested(TagSchema))


class ExperimentRunInfoSchema(BaseSchema):
    status = EnumField(ExperimentRunStatus, by_value=True, required=True)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)


class PaginationSchema(BaseSchema):
    start = fields.Integer(required=True)
    size = fields.Integer(required=True)
    previous = fields.Nested(PageSchema, missing=None)
    next = fields.Nested(PageSchema, missing=None)

    @post_load
    def make_pagination(self, data):
        return Pagination(**data)


class ListExperimentRunsResponseSchema(BaseSchema):
    pagination = fields.Nested(PaginationSchema, required=True)
    runs = fields.Nested(ExperimentRunSchema, many=True, required=True)

    @post_load
    def make_list_runs_response_schema(self, data):
        return ListExperimentRunsResponse(**data)


class CreateRunSchema(BaseSchema):
    name = fields.String()
    parent_run_id = fields.UUID(data_key="parentRunId")
    started_at = fields.DateTime(data_key="startedAt")
    artifact_location = fields.String(data_key="artifactLocation")
    tags = fields.Nested(TagSchema, many=True, required=True)


class DeleteExperimentRunsResponseSchema(BaseSchema):
    deleted_run_ids = fields.List(
        fields.UUID(), data_key="deletedRunIds", required=True
    )
    conflicted_run_ids = fields.List(
        fields.UUID(), data_key="conflictedRunIds", required=True
    )

    @post_load
    def make_delete_runs_response(self, data):
        return DeleteExperimentRunsResponse(**data)


class RestoreExperimentRunsResponseSchema(BaseSchema):
    restored_run_ids = fields.List(
        fields.UUID(), data_key="restoredRunIds", required=True
    )
    conflicted_run_ids = fields.List(
        fields.UUID(), data_key="conflictedRunIds", required=True
    )

    @post_load
    def make_restore_runs_response(self, data):
        return RestoreExperimentRunsResponse(**data)


class ParamFilterValueField(fields.Field):
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


class SingleFilterValueField(fields.Field):
    """
    Field that serialises/deserialises a run filter.
    """

    default_error_messages = {
        "invalid_filter_operator": "Invalid filter operator."
    }

    FILTER_BY_FIELD_MAPPING = {
        SingleFilterBy.PROJECT_ID: fields.UUID,
        SingleFilterBy.RUN_ID: fields.UUID,
        SingleFilterBy.EXPERIMENT_ID: fields.Integer,
        SingleFilterBy.DELETED_AT: fields.DateTime,
        SingleFilterBy.TAG: fields.String,
        SingleFilterBy.PARAM: ParamFilterValueField,
        SingleFilterBy.METRIC: fields.Number,
    }

    def _serialize(self, value, attr, obj, **kwargs):
        if obj.operator == SingleFilterOperator.DEFINED:
            field_cls = fields.Boolean
        else:
            try:
                field_cls = self.FILTER_BY_FIELD_MAPPING[obj.by]
            except KeyError:
                self.fail("invalid_filter_operator")
        return field_cls()._serialize(value, attr, obj, **kwargs)


class OptionalField(fields.Field):
    """Wrap another field, passing through Nones."""

    def __init__(self, nested, *args, **kwargs):
        self.nested = nested
        super().__init__(*args, **kwargs)

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


class FilterField(fields.Field):
    """
    Field that serialises/deserialises a run filter.
    """

    default_error_messages = {
        "invalid_filter_type": "Unsupported filter type."
    }

    def _serialize(self, value, attr, obj, **kwargs):
        if isinstance(value, SingleFilter):
            return SingleFilterSchema().dump(value)
        elif isinstance(value, CompoundFilter):
            return CompoundFilterSchema().dump(value)
        else:
            self.fail("invalid_filter_type")


class SingleFilterSchema(BaseSchema):
    by = EnumField(SingleFilterBy, by_value=True, required=True)
    key = fields.String()
    operator = EnumField(SingleFilterOperator, by_value=True, required=True)
    value = SingleFilterValueField(required=True)


class CompoundFilterSchema(BaseSchema):
    operator = EnumField(CompoundFilterOperator, by_value=True, required=True)
    conditions = fields.List(FilterField())


class SortSchema(BaseSchema):
    by = EnumField(SortBy, by_value=True, required=True)
    key = fields.String()
    order = EnumField(SortOrder, by_value=True, required=True)


class QueryRunsSchema(BaseSchema):
    filter = OptionalField(FilterField())
    sort = fields.List(fields.Nested(SortSchema))
    page = fields.Nested(PageSchema, missing=None)


class MetricDataPointSchema(BaseSchema):
    """Deserialise a data point from the metric history endpoint.

    This schema is written with the expectation that it is not used alongside
    the metric subsampling feature, which can result in null timestamp or step,
    or a non-integer step.
    """

    value = fields.Float(required=True)
    timestamp = fields.DateTime(required=True)
    step = fields.Integer(required=True)

    @post_load
    def make_metric(self, data):
        return MetricDataPoint(**data)


class MetricHistorySchema(BaseSchema):
    original_size = fields.Integer(data_key="originalSize", required=True)
    subsampled = fields.Boolean(required=True)
    key = fields.String(required=True)
    history = fields.Nested(MetricDataPointSchema, many=True, required=True)

    @post_load
    def make_history(self, data):
        return MetricHistory(**data)


class ExperimentClient(BaseClient):

    SERVICE_NAME = "atlas"

    def create(
        self, project_id, name, description=None, artifact_location=None
    ):
        """Create an experiment.

        Parameters
        ----------
        project_id : uuid.UUID
        name : str
        description : str, optional
        artifact_location : str, optional

        Returns
        -------
        Experiment

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
            return self._post(endpoint, ExperimentSchema(), json=payload)
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
        experiment_id : int

        Returns
        -------
        Experiment
        """
        endpoint = "/project/{}/experiment/{}".format(
            project_id, experiment_id
        )
        return self._get(endpoint, ExperimentSchema())

    def list(self, project_id, lifecycle_stage=None):
        """List the experiments in a project.

        Parameters
        ----------
        project_id : uuid.UUID
        lifecycle_stage : LifecycleStage, optional
            To filter experiments in the given lifecycle stage only
            (ACTIVE | DELETED). By default, all experiments in the
            project are returned.

        Returns
        -------
        List[Experiment]
        """
        query_params = {}
        if lifecycle_stage is not None:
            query_params["lifecycleStage"] = lifecycle_stage.value
        endpoint = "/project/{}/experiment".format(project_id)
        return self._get(
            endpoint, ExperimentSchema(many=True), params=query_params
        )

    def update(self, project_id, experiment_id, name=None, description=None):
        """Update the name and/or description of an experiment.

        Parameters
        ----------
        project_id : uuid.UUID
        experiment_id : int
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
        experiment_id : int
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
        experiment_id : int
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
        """Create a run in a project.

        Parameters
        ----------
        project_id : uuid.UUID
        experiment_id : int
        name : str
        started_at : datetime.datetime
            Time at which the run was started. If the datetime does not have a
            timezone, it will be assumed to be in UTC.
        parent_run_id : uuid.UUID, optional
            The ID of the parent run, if any.
        artifact_location: str, optional
            The location of the artifact repository to use for this run.
            If omitted, the value of `artifact_location` for the experiment
            will be used.
        tags: List[Tag]

        Returns
        -------
        ExperimentRun

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
        payload = CreateRunSchema().dump(
            {
                "name": name,
                "parent_run_id": parent_run_id,
                "started_at": started_at,
                "artifact_location": artifact_location,
                "tags": tags,
            }
        )
        try:
            return self._post(endpoint, ExperimentRunSchema(), json=payload)
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
        run_id : uuid.UUID

        Returns
        -------
        ExperimentRun
        """
        endpoint = "/project/{}/run/{}".format(project_id, run_id)
        return self._get(endpoint, ExperimentRunSchema())

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
                SingleFilter(
                    SingleFilterBy.EXPERIMENT_ID,
                    None,
                    SingleFilterOperator.EQUAL_TO,
                    experiment_id,
                )
                for experiment_id in experiment_ids
            ]
            experiment_ids_filter = CompoundFilter(
                CompoundFilterOperator.OR, experiment_id_filters
            )
        if lifecycle_stage is not None:
            lifecycle_filter = SingleFilter(
                SingleFilterBy.DELETED_AT,
                None,
                SingleFilterOperator.DEFINED,
                lifecycle_stage == LifecycleStage.DELETED,
            )

        if experiment_ids_filter is not None and lifecycle_filter is not None:
            filter = CompoundFilter(
                CompoundFilterOperator.AND,
                [experiment_ids_filter, lifecycle_filter],
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
        """
        endpoint = "/project/{}/run/query".format(project_id)
        page = None
        if start is not None and limit is not None:
            page = Page(start, limit)
        payload = QueryRunsSchema().dump(QueryRuns(filter, sort, page))
        return self._post(
            endpoint, ListExperimentRunsResponseSchema(), json=payload
        )

    def log_run_data(
        self, project_id, run_id, metrics=None, params=None, tags=None
    ):
        """Update the data of a run.

        Parameters
        ----------
        project_id : uuid.UUID
        run_id : uuid.UUID
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
        payload = ExperimentRunDataSchema().dump(
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
        run_id : uuid.UUID
        status: ExperimentRunStatus, optional
        ended_at: datetime, optional

        Returns
        -------
        ExperimentRun
        """
        endpoint = "/project/{}/run/{}/info".format(project_id, run_id)
        payload = ExperimentRunInfoSchema().dump(
            {"status": status, "ended_at": ended_at}
        )
        return self._patch(endpoint, ExperimentRunSchema(), json=payload)

    def get_metric_history(self, project_id, run_id, key):
        """Get the history of a metric.

        Parameters
        ----------
        project_id : uuid.UUID
        run_id : uuid.UUID
        key: string

        Returns
        -------
        List[Metric], ordered by timestamp and value
        """
        endpoint = "/project/{}/run/{}/metric/{}/history".format(
            project_id, run_id, key
        )
        metric_history = self._get(endpoint, MetricHistorySchema())
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
                SingleFilter(
                    SingleFilterBy.RUN_ID,
                    None,
                    SingleFilterOperator.EQUAL_TO,
                    run_id,
                )
                for run_id in run_ids
            ]
            run_ids_filter = CompoundFilter(
                CompoundFilterOperator.OR, run_id_filters
            )
            payload = {"filter": run_ids_filter}

        return self._post(
            endpoint, DeleteExperimentRunsResponseSchema(), json=payload
        )

    def restore_runs(self, project_id, run_ids=None):
        """Restore experiment runs.

        Parameters
        ----------
        project_id : uuid.UUID
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
                SingleFilter(
                    SingleFilterBy.RUN_ID,
                    None,
                    SingleFilterOperator.EQUAL_TO,
                    run_id,
                )
                for run_id in run_ids
            ]
            run_ids_filter = CompoundFilter(
                CompoundFilterOperator.OR, run_id_filters
            )
            payload = {"filter": run_ids_filter}

        return self._post(
            endpoint, RestoreExperimentRunsResponseSchema(), json=payload
        )
