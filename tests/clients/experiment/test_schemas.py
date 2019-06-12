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


from datetime import datetime
from uuid import uuid4

import pytest
from marshmallow import ValidationError
from pytz import UTC

from faculty.clients.experiment._models import (
    ComparisonOperator,
    CompoundFilter,
    DeleteExperimentRunsResponse,
    DeletedAtFilter,
    DurationSort,
    Experiment,
    ExperimentIdFilter,
    ExperimentRun,
    ExperimentRunStatus,
    ListExperimentRunsResponse,
    LogicalOperator,
    Metric,
    MetricDataPoint,
    MetricFilter,
    MetricHistory,
    MetricSort,
    Page,
    Pagination,
    Param,
    ParamFilter,
    ParamSort,
    ProjectIdFilter,
    RestoreExperimentRunsResponse,
    RunIdFilter,
    RunNumberSort,
    RunQuery,
    SortOrder,
    StartedAtSort,
    Tag,
    TagFilter,
    TagSort,
)
from faculty.clients.experiment._schemas import (
    CreateRunSchema,
    DeleteExperimentRunsResponseSchema,
    ExperimentRunDataSchema,
    ExperimentRunSchema,
    ExperimentSchema,
    FilterSchema,
    ListExperimentRunsResponseSchema,
    MetricHistorySchema,
    MetricSchema,
    PageSchema,
    PaginationSchema,
    ParamSchema,
    RestoreExperimentRunsResponseSchema,
    RunQuerySchema,
    SortSchema,
    TagSchema,
)

PROJECT_ID = uuid4()
EXPERIMENT_ID = 661
EXPERIMENT_RUN_ID = uuid4()
EXPERIMENT_RUN_NUMBER = 3
EXPERIMENT_RUN_NAME = "run name"
PARENT_RUN_ID = uuid4()
CREATED_AT = datetime(2018, 3, 10, 11, 32, 6, 247000, tzinfo=UTC)
CREATED_AT_STRING = "2018-03-10T11:32:06.247Z"
LAST_UPDATED_AT = datetime(2018, 3, 10, 11, 32, 30, 172000, tzinfo=UTC)
LAST_UPDATED_AT_STRING = "2018-03-10T11:32:30.172Z"
DELETED_AT = datetime(2018, 3, 10, 11, 37, 42, 482000, tzinfo=UTC)
DELETED_AT_STRING = "2018-03-10T11:37:42.482Z"
DELETED_AT_STRING_PYTHON = "2018-03-10T11:37:42.482000+00:00"

EXPERIMENT = Experiment(
    id=EXPERIMENT_ID,
    name="experiment name",
    description="experiment description",
    artifact_location="https://example.com",
    created_at=CREATED_AT,
    last_updated_at=LAST_UPDATED_AT,
    deleted_at=DELETED_AT,
)
EXPERIMENT_BODY = {
    "experimentId": EXPERIMENT_ID,
    "name": EXPERIMENT.name,
    "description": EXPERIMENT.description,
    "artifactLocation": EXPERIMENT.artifact_location,
    "createdAt": CREATED_AT_STRING,
    "lastUpdatedAt": LAST_UPDATED_AT_STRING,
    "deletedAt": DELETED_AT_STRING,
}

RUN_ID = uuid4()
RUN_STARTED_AT = datetime(2018, 3, 10, 11, 39, 12, 110000, tzinfo=UTC)
RUN_STARTED_AT_NO_TIMEZONE = datetime(2018, 3, 10, 11, 39, 12, 110000)
RUN_STARTED_AT_STRING_PYTHON = "2018-03-10T11:39:12.110000+00:00"
RUN_STARTED_AT_STRING_JAVA = "2018-03-10T11:39:12.11Z"
RUN_ENDED_AT = datetime(2018, 3, 10, 11, 39, 15, 110000, tzinfo=UTC)
RUN_ENDED_AT_STRING = "2018-03-10T11:39:15.11Z"

TAG = Tag(key="tag-key", value="tag-value")
TAG_BODY = {"key": "tag-key", "value": "tag-value"}

OTHER_TAG = Tag(key="other-tag-key", value="other-tag-value")
OTHER_TAG_BODY = {"key": "other-tag-key", "value": "other-tag-value"}

PARAM = Param(key="param-key", value="param-value")
PARAM_BODY = {"key": "param-key", "value": "param-value"}

METRIC_KEY = "metric-key"
METRIC = Metric(
    key=METRIC_KEY,
    value=123.0,
    timestamp=datetime(2018, 3, 12, 16, 20, 22, 122000, tzinfo=UTC),
    step=0,
)
METRIC_BODY = {
    "key": METRIC.key,
    "value": METRIC.value,
    "timestamp": "2018-03-12T16:20:22.122000+00:00",
    "step": METRIC.step,
}

METRIC_DATA_POINT = MetricDataPoint(
    value=METRIC.value, timestamp=METRIC.timestamp, step=METRIC.step
)
METRIC_DATA_POINT_BODY = {
    "value": METRIC_BODY["value"],
    "timestamp": METRIC_BODY["timestamp"],
    "step": METRIC_BODY["step"],
}

METRIC_HISTORY = MetricHistory(
    original_size=1,
    subsampled=False,
    key=METRIC_KEY,
    history=[METRIC_DATA_POINT],
)
METRIC_HISTORY_BODY = {
    "originalSize": METRIC_HISTORY.original_size,
    "subsampled": METRIC_HISTORY.subsampled,
    "key": METRIC_HISTORY.key,
    "history": [METRIC_DATA_POINT_BODY],
}

EXPERIMENT_RUN = ExperimentRun(
    id=EXPERIMENT_RUN_ID,
    run_number=EXPERIMENT_RUN_NUMBER,
    name=EXPERIMENT_RUN_NAME,
    parent_run_id=PARENT_RUN_ID,
    experiment_id=EXPERIMENT.id,
    artifact_location="faculty:",
    status=ExperimentRunStatus.RUNNING,
    started_at=RUN_STARTED_AT,
    ended_at=RUN_ENDED_AT,
    deleted_at=DELETED_AT,
    tags=[TAG],
    params=[PARAM],
    metrics=[METRIC],
)
EXPERIMENT_RUN_BODY = {
    "experimentId": EXPERIMENT.id,
    "runId": str(EXPERIMENT_RUN_ID),
    "runNumber": EXPERIMENT_RUN_NUMBER,
    "name": EXPERIMENT_RUN_NAME,
    "parentRunId": str(PARENT_RUN_ID),
    "artifactLocation": "faculty:",
    "status": "running",
    "startedAt": RUN_STARTED_AT_STRING_JAVA,
    "endedAt": RUN_ENDED_AT_STRING,
    "deletedAt": DELETED_AT_STRING,
    "tags": [TAG_BODY],
    "metrics": [METRIC_BODY],
    "params": [PARAM_BODY],
}

EXPERIMENT_RUN_DATA_BODY = {
    "metrics": [METRIC_BODY],
    "params": [PARAM_BODY],
    "tags": [TAG_BODY],
}


PAGE = Page(start=3, limit=10)
PAGE_BODY = {"start": PAGE.start, "limit": PAGE.limit}

PAGINATION = Pagination(
    start=20,
    size=10,
    previous=Page(start=10, limit=10),
    next=Page(start=30, limit=10),
)
PAGINATION_BODY = {
    "start": PAGINATION.start,
    "size": PAGINATION.size,
    "previous": {
        "start": PAGINATION.previous.start,
        "limit": PAGINATION.previous.limit,
    },
    "next": {"start": PAGINATION.next.start, "limit": PAGINATION.next.limit},
}

LIST_EXPERIMENT_RUNS_RESPONSE = ListExperimentRunsResponse(
    runs=[EXPERIMENT_RUN], pagination=PAGINATION
)
LIST_EXPERIMENT_RUNS_RESPONSE_BODY = {
    "runs": [EXPERIMENT_RUN_BODY],
    "pagination": PAGINATION_BODY,
}

DELETE_EXPERIMENT_RUNS_RESPONSE = DeleteExperimentRunsResponse(
    deleted_run_ids=[uuid4(), uuid4()], conflicted_run_ids=[uuid4(), uuid4()]
)
DELETE_EXPERIMENT_RUNS_RESPONSE_BODY = {
    "deletedRunIds": [
        str(run_id)
        for run_id in DELETE_EXPERIMENT_RUNS_RESPONSE.deleted_run_ids
    ],
    "conflictedRunIds": [
        str(run_id)
        for run_id in DELETE_EXPERIMENT_RUNS_RESPONSE.conflicted_run_ids
    ],
}

RESTORE_EXPERIMENT_RUNS_RESPONSE = RestoreExperimentRunsResponse(
    restored_run_ids=[uuid4(), uuid4()], conflicted_run_ids=[uuid4(), uuid4()]
)
RESTORE_EXPERIMENT_RUNS_RESPONSE_BODY = {
    "restoredRunIds": [
        str(run_id)
        for run_id in RESTORE_EXPERIMENT_RUNS_RESPONSE.restored_run_ids
    ],
    "conflictedRunIds": [
        str(run_id)
        for run_id in RESTORE_EXPERIMENT_RUNS_RESPONSE.conflicted_run_ids
    ],
}


def test_experiment_schema():
    data = ExperimentSchema().load(EXPERIMENT_BODY)
    assert data == EXPERIMENT


def test_experiment_schema_nullable_deleted_at():
    body = EXPERIMENT_BODY.copy()
    body["deletedAt"] = None
    data = ExperimentSchema().load(body)
    assert data.deleted_at is None


def test_experiment_schema_invalid():
    with pytest.raises(ValidationError):
        ExperimentSchema().load({})


def test_experiment_run_schema():
    data = ExperimentRunSchema().load(EXPERIMENT_RUN_BODY)
    assert data == EXPERIMENT_RUN


@pytest.mark.parametrize(
    "data_key, field",
    [
        ("parentRunId", "parent_run_id"),
        ("endedAt", "ended_at"),
        ("deletedAt", "deleted_at"),
    ],
)
def test_experiment_run_schema_nullable_field(data_key, field):
    body = EXPERIMENT_RUN_BODY.copy()
    del body[data_key]
    data = ExperimentRunSchema().load(body)
    assert getattr(data, field) is None


@pytest.mark.parametrize("parent_run_id", [None, PARENT_RUN_ID])
@pytest.mark.parametrize(
    "started_at",
    [RUN_STARTED_AT, RUN_STARTED_AT_NO_TIMEZONE],
    ids=["timezone", "no timezone"],
)
@pytest.mark.parametrize("artifact_location", [None, "faculty:project-id"])
@pytest.mark.parametrize("tags", [[], [{"key": "key", "value": "value"}]])
def test_create_run_schema(parent_run_id, started_at, artifact_location, tags):
    data = CreateRunSchema().dump(
        {
            "name": EXPERIMENT_RUN_NAME,
            "parent_run_id": parent_run_id,
            "started_at": started_at,
            "artifact_location": artifact_location,
            "tags": tags,
        }
    )
    assert data == {
        "name": EXPERIMENT_RUN_NAME,
        "parentRunId": None if parent_run_id is None else str(parent_run_id),
        "startedAt": RUN_STARTED_AT_STRING_PYTHON,
        "artifactLocation": artifact_location,
        "tags": tags,
    }


def test_metric_schema():
    data = MetricSchema().load(METRIC_BODY)
    assert data == METRIC


def test_param_schema():
    data = ParamSchema().load(PARAM_BODY)
    assert data == PARAM


def test_tag_schema():
    data = TagSchema().load(TAG_BODY)
    assert data == TAG


def test_tag_schema_dump():
    data = TagSchema().dump(TAG_BODY)
    assert data == TAG_BODY


def test_experiment_run_data_schema():
    data = ExperimentRunDataSchema().dump(
        {"metrics": [METRIC], "params": [PARAM], "tags": [TAG]}
    )
    assert data == EXPERIMENT_RUN_DATA_BODY


def test_experiment_run_data_schema_empty():
    data = ExperimentRunDataSchema().dump({})
    assert data == {}


def test_experiment_run_data_schema_multiple():
    data = ExperimentRunDataSchema().dump({"tags": [TAG, OTHER_TAG]})
    assert data == {"tags": [TAG_BODY, OTHER_TAG_BODY]}


PROJECT_ID_FILTER = ProjectIdFilter(ComparisonOperator.EQUAL_TO, PROJECT_ID)
PROJECT_ID_FILTER_BODY = {
    "by": "projectId",
    "operator": "eq",
    "value": str(PROJECT_ID),
}

TAG_FILTER = TagFilter("tag-key", ComparisonOperator.EQUAL_TO, "tag-value")
TAG_FILTER_BODY = {
    "by": "tag",
    "key": "tag-key",
    "operator": "eq",
    "value": "tag-value",
}


DEFINED_TEST_CASES = [
    (ComparisonOperator.DEFINED, False, "defined", False),
    (ComparisonOperator.DEFINED, True, "defined", True),
    (ComparisonOperator.DEFINED, 0, "defined", False),
    (ComparisonOperator.DEFINED, 1, "defined", True),
]


def discrete_test_cases(value, expected):
    return DEFINED_TEST_CASES + [
        (ComparisonOperator.EQUAL_TO, value, "eq", expected),
        (ComparisonOperator.NOT_EQUAL_TO, value, "ne", expected),
    ]


def continuous_test_cases(value, expected):
    return discrete_test_cases(value, expected) + [
        (ComparisonOperator.GREATER_THAN, value, "gt", expected),
        (ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, value, "ge", expected),
        (ComparisonOperator.LESS_THAN, value, "lt", expected),
        (ComparisonOperator.LESS_THAN_OR_EQUAL_TO, value, "le", expected),
    ]


@pytest.mark.parametrize(
    "operator, value, expected_operator, expected_value",
    discrete_test_cases(PROJECT_ID, str(PROJECT_ID)),
)
def test_filter_schema_project_id(
    operator, value, expected_operator, expected_value
):
    filter = ProjectIdFilter(operator, value)
    data = FilterSchema().dump(filter)
    assert data == {
        "by": "projectId",
        "operator": expected_operator,
        "value": expected_value,
    }


@pytest.mark.parametrize(
    "operator, value, expected_operator, expected_value",
    discrete_test_cases(EXPERIMENT_ID, EXPERIMENT_ID),
)
def test_filter_schema_experiment_id(
    operator, value, expected_operator, expected_value
):
    filter = ExperimentIdFilter(operator, value)
    data = FilterSchema().dump(filter)
    assert data == {
        "by": "experimentId",
        "operator": expected_operator,
        "value": expected_value,
    }


@pytest.mark.parametrize(
    "operator, value, expected_operator, expected_value",
    discrete_test_cases(RUN_ID, str(RUN_ID)),
)
def test_filter_schema_run_id(
    operator, value, expected_operator, expected_value
):
    filter = RunIdFilter(operator, value)
    data = FilterSchema().dump(filter)
    assert data == {
        "by": "runId",
        "operator": expected_operator,
        "value": expected_value,
    }


@pytest.mark.parametrize(
    "operator, value, expected_operator, expected_value",
    continuous_test_cases(DELETED_AT, DELETED_AT_STRING_PYTHON),
)
def test_filter_schema_deleted_at(
    operator, value, expected_operator, expected_value
):
    filter = DeletedAtFilter(operator, value)
    data = FilterSchema().dump(filter)
    assert data == {
        "by": "deletedAt",
        "operator": expected_operator,
        "value": expected_value,
    }


@pytest.mark.parametrize(
    "operator, value, expected_operator, expected_value",
    discrete_test_cases("tag-value", "tag-value"),
)
def test_filter_schema_tag(operator, value, expected_operator, expected_value):
    filter = TagFilter("tag-key", operator, value)
    data = FilterSchema().dump(filter)
    assert data == {
        "by": "tag",
        "key": "tag-key",
        "operator": expected_operator,
        "value": expected_value,
    }


@pytest.mark.parametrize(
    "operator, value, expected_operator, expected_value",
    discrete_test_cases("param-value", "param-value")
    + continuous_test_cases(123.2, 123.2),
)
def test_filter_schema_param(
    operator, value, expected_operator, expected_value
):
    filter = ParamFilter("param-key", operator, value)
    data = FilterSchema().dump(filter)
    assert data == {
        "by": "param",
        "key": "param-key",
        "operator": expected_operator,
        "value": expected_value,
    }


@pytest.mark.parametrize(
    "operator, value, expected_operator, expected_value",
    continuous_test_cases(45.6, 45.6),
)
def test_filter_schema_metric(
    operator, value, expected_operator, expected_value
):
    filter = MetricFilter("metric-key", operator, value)
    data = FilterSchema().dump(filter)
    assert data == {
        "by": "metric",
        "key": "metric-key",
        "operator": expected_operator,
        "value": expected_value,
    }


@pytest.mark.parametrize(
    "filter_type",
    [ProjectIdFilter, ExperimentIdFilter, RunIdFilter, DeletedAtFilter],
)
def test_filter_schema_invalid_value_no_key(filter_type):
    filter = filter_type(ComparisonOperator.EQUAL_TO, "invalid")
    with pytest.raises(ValidationError):
        FilterSchema().dump(filter)


@pytest.mark.parametrize(
    "filter_type, value", [(ParamFilter, None), (MetricFilter, "invalid")]
)
def test_filter_schema_invalid_value_with_key(filter_type, value):
    filter = filter_type("key", ComparisonOperator.EQUAL_TO, value)
    with pytest.raises(ValidationError):
        FilterSchema().dump(filter)


@pytest.mark.parametrize(
    "filter_type, value",
    [
        (ProjectIdFilter, PROJECT_ID),
        (ExperimentIdFilter, EXPERIMENT_ID),
        (RunIdFilter, RUN_ID),
    ],
)
@pytest.mark.parametrize(
    "operator",
    [
        ComparisonOperator.GREATER_THAN,
        ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
        ComparisonOperator.LESS_THAN,
        ComparisonOperator.LESS_THAN_OR_EQUAL_TO,
    ],
)
def test_filter_schema_invalid_operator_no_key(filter_type, value, operator):
    filter = filter_type(operator, value)
    with pytest.raises(ValidationError, match="Not a discrete operator"):
        FilterSchema().dump(filter)


@pytest.mark.parametrize(
    "filter_type, value",
    [(TagFilter, "tag-value"), (ParamFilter, "param-string-value")],
)
@pytest.mark.parametrize(
    "operator",
    [
        ComparisonOperator.GREATER_THAN,
        ComparisonOperator.GREATER_THAN_OR_EQUAL_TO,
        ComparisonOperator.LESS_THAN,
        ComparisonOperator.LESS_THAN_OR_EQUAL_TO,
    ],
)
def test_filter_schema_invalid_operator_with_key(filter_type, value, operator):
    filter = filter_type("key", operator, value)
    with pytest.raises(ValidationError, match="Not a discrete operator"):
        FilterSchema().dump(filter)


@pytest.mark.parametrize(
    "operator, expected_operator",
    [(LogicalOperator.AND, "and"), (LogicalOperator.OR, "or")],
)
def test_filter_schema_compound(operator, expected_operator):
    filter = CompoundFilter(operator, [PROJECT_ID_FILTER, TAG_FILTER])
    data = FilterSchema().dump(filter)
    assert data == {
        "operator": expected_operator,
        "conditions": [PROJECT_ID_FILTER_BODY, TAG_FILTER_BODY],
    }


def test_filter_schema_nested():
    filter = CompoundFilter(
        LogicalOperator.AND,
        [
            CompoundFilter(
                LogicalOperator.AND, [PROJECT_ID_FILTER, TAG_FILTER]
            ),
            CompoundFilter(
                LogicalOperator.OR, [TAG_FILTER, PROJECT_ID_FILTER]
            ),
        ],
    )
    data = FilterSchema().dump(filter)
    assert data == {
        "operator": "and",
        "conditions": [
            {
                "operator": "and",
                "conditions": [PROJECT_ID_FILTER_BODY, TAG_FILTER_BODY],
            },
            {
                "operator": "or",
                "conditions": [TAG_FILTER_BODY, PROJECT_ID_FILTER_BODY],
            },
        ],
    }


@pytest.mark.parametrize(
    "sort_type, by",
    [
        (StartedAtSort, "startedAt"),
        (RunNumberSort, "runNumber"),
        (DurationSort, "duration"),
    ],
)
@pytest.mark.parametrize(
    "order, expected_order", [(SortOrder.ASC, "asc"), (SortOrder.DESC, "desc")]
)
def test_sort_schema_no_key(sort_type, by, order, expected_order):
    sort = sort_type(order)
    data = SortSchema().dump(sort)
    assert data == {"by": by, "order": expected_order}


@pytest.mark.parametrize(
    "sort_type, by",
    [(TagSort, "tag"), (ParamSort, "param"), (MetricSort, "metric")],
)
@pytest.mark.parametrize(
    "order, expected_order", [(SortOrder.ASC, "asc"), (SortOrder.DESC, "desc")]
)
def test_sort_schema_with_key(sort_type, by, order, expected_order):
    sort = sort_type("sort-key", order)
    data = SortSchema().dump(sort)
    assert data == {"by": by, "key": "sort-key", "order": expected_order}


def test_run_query_schema(mocker):
    mocker.patch.object(FilterSchema, "dump")
    mocker.patch.object(SortSchema, "dump")
    mocker.patch.object(PageSchema, "dump")

    filter = mocker.Mock()
    sorts = [mocker.Mock(), mocker.Mock()]
    page = mocker.Mock()

    run_query = RunQuery(filter, sorts, page)
    data = RunQuerySchema().dump(run_query)

    assert data == {
        "filter": FilterSchema.dump.return_value,
        "sort": [SortSchema.dump.return_value, SortSchema.dump.return_value],
        "page": PageSchema.dump.return_value,
    }


def test_run_query_schema_defaults():
    run_query = RunQuery(None, None, None)
    data = RunQuerySchema().dump(run_query)
    assert data == {"filter": None, "sort": None, "page": None}


def test_list_runs_schema(mocker):
    data = ListExperimentRunsResponseSchema().load(
        LIST_EXPERIMENT_RUNS_RESPONSE_BODY
    )
    assert data == LIST_EXPERIMENT_RUNS_RESPONSE


def test_page_schema_load():
    data = PageSchema().load(PAGE_BODY)
    assert data == PAGE


def test_page_schema_dump():
    data = PageSchema().dump(PAGE)
    assert data == PAGE_BODY


def test_pagination_schema():
    data = PaginationSchema().load(PAGINATION_BODY)
    assert data == PAGINATION


@pytest.mark.parametrize("field", ["previous", "next"])
def test_pagination_schema_nullable_field(field):
    body = PAGINATION_BODY.copy()
    del body[field]
    data = PaginationSchema().load(body)
    assert getattr(data, field) is None


def test_delete_experiment_runs_response_schema(mocker):
    data = DeleteExperimentRunsResponseSchema().load(
        DELETE_EXPERIMENT_RUNS_RESPONSE_BODY
    )
    assert data == DELETE_EXPERIMENT_RUNS_RESPONSE


def test_delete_experiment_runs_response_schema_invalid(mocker):
    with pytest.raises(ValidationError):
        DeleteExperimentRunsResponseSchema().load({})


def test_restore_experiment_runs_response_schema(mocker):
    data = RestoreExperimentRunsResponseSchema().load(
        RESTORE_EXPERIMENT_RUNS_RESPONSE_BODY
    )
    assert data == RESTORE_EXPERIMENT_RUNS_RESPONSE


def test_restore_experiment_runs_response_schema_invalid(mocker):
    with pytest.raises(ValidationError):
        RestoreExperimentRunsResponseSchema().load({})


def test_metric_history_schema():
    data = MetricHistorySchema().load(METRIC_HISTORY_BODY)
    assert data == METRIC_HISTORY


def test_metric_history_schema_invalid():
    with pytest.raises(ValidationError):
        MetricHistorySchema().load({})
