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

from faculty.clients.base import Conflict
from faculty.clients.experiment import (
    CreateRunSchema,
    DeleteExperimentRunsResponse,
    DeleteExperimentRunsResponseSchema,
    Experiment,
    ExperimentClient,
    ExperimentNameConflict,
    ExperimentDeleted,
    ExperimentRun,
    ExperimentRunDataSchema,
    ExperimentRunSchema,
    ExperimentRunStatus,
    ExperimentSchema,
    LifecycleStage,
    ListExperimentRunsResponse,
    ListExperimentRunsResponseSchema,
    Metric,
    MetricDataPoint,
    MetricSchema,
    MetricHistory,
    MetricHistorySchema,
    Page,
    PageSchema,
    Pagination,
    PaginationSchema,
    Param,
    ParamConflict,
    ParamSchema,
    RestoreExperimentRunsResponse,
    RestoreExperimentRunsResponseSchema,
    Tag,
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


@pytest.mark.parametrize("description", [None, "experiment description"])
@pytest.mark.parametrize("artifact_location", [None, "s3://mybucket"])
def test_experiment_client_create(mocker, description, artifact_location):
    mocker.patch.object(ExperimentClient, "_post", return_value=EXPERIMENT)
    schema_mock = mocker.patch("faculty.clients.experiment.ExperimentSchema")

    client = ExperimentClient(mocker.Mock())
    returned_experiment = client.create(
        PROJECT_ID, "experiment name", description, artifact_location
    )
    assert returned_experiment == EXPERIMENT

    schema_mock.assert_called_once_with()
    ExperimentClient._post.assert_called_once_with(
        "/project/{}/experiment".format(PROJECT_ID),
        schema_mock.return_value,
        json={
            "name": "experiment name",
            "description": description,
            "artifactLocation": artifact_location,
        },
    )


def test_experiment_client_create_name_conflict(mocker):
    error_code = "experiment_name_conflict"
    exception = Conflict(mocker.Mock(), mocker.Mock(), error_code)
    mocker.patch.object(ExperimentClient, "_post", side_effect=exception)

    client = ExperimentClient(mocker.Mock())
    with pytest.raises(
        ExperimentNameConflict, match="name 'experiment name' already exists"
    ):
        client.create(PROJECT_ID, "experiment name")


def test_experiment_client_get(mocker):
    mocker.patch.object(ExperimentClient, "_get", return_value=EXPERIMENT)
    schema_mock = mocker.patch("faculty.clients.experiment.ExperimentSchema")

    client = ExperimentClient(mocker.Mock())
    returned_experiment = client.get(PROJECT_ID, EXPERIMENT.id)
    assert returned_experiment == EXPERIMENT

    schema_mock.assert_called_once_with()
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/experiment/{}".format(PROJECT_ID, EXPERIMENT.id),
        schema_mock.return_value,
    )


def test_experiment_client_list(mocker):
    mocker.patch.object(ExperimentClient, "_get", return_value=[EXPERIMENT])
    schema_mock = mocker.patch("faculty.clients.experiment.ExperimentSchema")

    client = ExperimentClient(mocker.Mock())
    assert client.list(PROJECT_ID) == [EXPERIMENT]

    schema_mock.assert_called_once_with(many=True)
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/experiment".format(PROJECT_ID),
        schema_mock.return_value,
        params={},
    )


def test_experiment_client_list_lifecycle_filter(mocker):
    mocker.patch.object(ExperimentClient, "_get", return_value=[EXPERIMENT])
    schema_mock = mocker.patch("faculty.clients.experiment.ExperimentSchema")

    client = ExperimentClient(mocker.Mock())
    returned_experiments = client.list(
        PROJECT_ID, lifecycle_stage=LifecycleStage.ACTIVE
    )
    assert returned_experiments == [EXPERIMENT]

    schema_mock.assert_called_once_with(many=True)
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/experiment".format(PROJECT_ID),
        schema_mock.return_value,
        params={"lifecycleStage": "active"},
    )


@pytest.mark.parametrize("name", [None, "new name"])
@pytest.mark.parametrize("description", [None, "new description"])
def test_experiment_client_update(mocker, name, description):
    mocker.patch.object(ExperimentClient, "_patch_raw")

    client = ExperimentClient(mocker.Mock())
    client.update(
        PROJECT_ID, EXPERIMENT_ID, name=name, description=description
    )

    ExperimentClient._patch_raw.assert_called_once_with(
        "/project/{}/experiment/{}".format(PROJECT_ID, EXPERIMENT_ID),
        json={"name": name, "description": description},
    )


def test_experiment_client_update_name_conflict(mocker):
    error_code = "experiment_name_conflict"
    exception = Conflict(mocker.Mock(), mocker.Mock(), error_code)
    mocker.patch.object(ExperimentClient, "_patch_raw", side_effect=exception)

    client = ExperimentClient(mocker.Mock())
    with pytest.raises(
        ExperimentNameConflict, match="name 'new name' already exists"
    ):
        client.update(PROJECT_ID, EXPERIMENT_ID, name="new name")


def test_delete(mocker):
    mocker.patch.object(ExperimentClient, "_delete_raw")

    client = ExperimentClient(mocker.Mock())
    client.delete(PROJECT_ID, EXPERIMENT_ID)

    ExperimentClient._delete_raw.assert_called_once_with(
        "/project/{}/experiment/{}".format(PROJECT_ID, EXPERIMENT_ID)
    )


def test_restore(mocker):
    mocker.patch.object(ExperimentClient, "_put_raw")

    client = ExperimentClient(mocker.Mock())
    client.restore(PROJECT_ID, EXPERIMENT_ID)

    ExperimentClient._put_raw.assert_called_once_with(
        "/project/{}/experiment/{}/restore".format(PROJECT_ID, EXPERIMENT_ID)
    )


def test_experiment_create_run(mocker):
    mocker.patch.object(ExperimentClient, "_post", return_value=EXPERIMENT_RUN)
    request_schema_mock = mocker.patch(
        "faculty.clients.experiment.CreateRunSchema"
    )
    dump_mock = request_schema_mock.return_value.dump
    response_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunSchema"
    )
    started_at = mocker.Mock()
    artifact_location = mocker.Mock()

    client = ExperimentClient(mocker.Mock())
    returned_run = client.create_run(
        PROJECT_ID,
        EXPERIMENT_ID,
        EXPERIMENT_RUN_NAME,
        started_at,
        PARENT_RUN_ID,
        artifact_location=artifact_location,
    )
    assert returned_run == EXPERIMENT_RUN

    request_schema_mock.assert_called_once_with()
    dump_mock.assert_called_once_with(
        {
            "name": EXPERIMENT_RUN_NAME,
            "parent_run_id": PARENT_RUN_ID,
            "started_at": started_at,
            "artifact_location": artifact_location,
            "tags": [],
        }
    )
    response_schema_mock.assert_called_once_with()
    ExperimentClient._post.assert_called_once_with(
        "/project/{}/experiment/{}/run".format(PROJECT_ID, EXPERIMENT_ID),
        response_schema_mock.return_value,
        json=dump_mock.return_value,
    )


def test_experiment_create_run_experiment_deleted_conflict(mocker):
    message = "experiment deleted"
    error_code = "experiment_deleted"
    response_mock = mocker.Mock()
    response_mock.json.return_value = {"experimentId": 42}
    exception = Conflict(response_mock, message, error_code)

    mocker.patch.object(ExperimentClient, "_post", side_effect=exception)
    started_at = mocker.Mock()
    artifact_location = mocker.Mock()

    client = ExperimentClient(mocker.Mock())
    with pytest.raises(ExperimentDeleted, match=message):
        client.create_run(
            PROJECT_ID,
            EXPERIMENT_ID,
            EXPERIMENT_RUN_NAME,
            started_at,
            PARENT_RUN_ID,
            artifact_location=artifact_location,
        )


def test_experiment_client_get_run(mocker):
    mocker.patch.object(ExperimentClient, "_get", return_value=EXPERIMENT_RUN)
    schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunSchema"
    )

    client = ExperimentClient(mocker.Mock())
    returned_run = client.get_run(PROJECT_ID, EXPERIMENT_RUN_ID)
    assert returned_run == EXPERIMENT_RUN

    schema_mock.assert_called_once_with()
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/run/{}".format(PROJECT_ID, EXPERIMENT_RUN_ID),
        schema_mock.return_value,
    )


def test_list_runs_schema(mocker):
    data = ListExperimentRunsResponseSchema().load(
        LIST_EXPERIMENT_RUNS_RESPONSE_BODY
    )
    assert data == LIST_EXPERIMENT_RUNS_RESPONSE


def test_page_schema():
    data = PageSchema().load(PAGE_BODY)
    assert data == PAGE


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


def test_experiment_client_list_runs_all(mocker):
    mocker.patch.object(
        ExperimentClient, "_get", return_value=LIST_EXPERIMENT_RUNS_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.experiment.ListExperimentRunsResponseSchema"
    )

    client = ExperimentClient(mocker.Mock())
    list_result = client.list_runs(PROJECT_ID)
    assert list_result == LIST_EXPERIMENT_RUNS_RESPONSE

    schema_mock.assert_called_once_with()
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/run".format(PROJECT_ID),
        schema_mock.return_value,
        params=[],
    )


def test_experiment_client_list_runs_experiments_filter(mocker):
    mocker.patch.object(
        ExperimentClient, "_get", return_value=LIST_EXPERIMENT_RUNS_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.experiment.ListExperimentRunsResponseSchema"
    )

    client = ExperimentClient(mocker.Mock())
    list_result = client.list_runs(PROJECT_ID, experiment_ids=[123, 456])
    assert list_result == LIST_EXPERIMENT_RUNS_RESPONSE
    schema_mock.assert_called_once_with()
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/run".format(PROJECT_ID),
        schema_mock.return_value,
        params=[("experimentId", 123), ("experimentId", 456)],
    )


def test_experiment_client_list_runs_experiments_filter_empty(mocker):
    client = ExperimentClient(mocker.Mock())
    list_result = client.list_runs(PROJECT_ID, experiment_ids=[])

    assert list_result == ListExperimentRunsResponse(
        runs=[],
        pagination=Pagination(start=0, size=0, previous=None, next=None),
    )


def test_experiment_client_list_runs_page(mocker):
    mocker.patch.object(
        ExperimentClient, "_get", return_value=LIST_EXPERIMENT_RUNS_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.experiment.ListExperimentRunsResponseSchema"
    )

    client = ExperimentClient(mocker.Mock())
    list_result = client.list_runs(PROJECT_ID, start=20, limit=10)
    assert list_result == LIST_EXPERIMENT_RUNS_RESPONSE

    schema_mock.assert_called_once_with()
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/run".format(PROJECT_ID),
        schema_mock.return_value,
        params=[("start", 20), ("limit", 10)],
    )


def test_log_run_data(mocker):
    mocker.patch.object(ExperimentClient, "_patch_raw")
    run_data_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunDataSchema"
    )
    run_data_dump_mock = run_data_schema_mock.return_value.dump

    client = ExperimentClient(mocker.Mock())
    client.log_run_data(
        PROJECT_ID,
        EXPERIMENT_RUN_ID,
        metrics=[METRIC],
        params=[PARAM],
        tags=[TAG],
    )

    run_data_schema_mock.assert_called_once_with()
    run_data_dump_mock.assert_called_once_with(
        {"metrics": [METRIC], "params": [PARAM], "tags": [TAG]}
    )
    ExperimentClient._patch_raw.assert_called_once_with(
        "/project/{}/run/{}/data".format(PROJECT_ID, EXPERIMENT_RUN_ID),
        json=run_data_dump_mock.return_value,
    )


def test_log_run_data_param_conflict(mocker):
    message = "bad params"
    error_code = "conflicting_params"
    response_mock = mocker.Mock()
    response_mock.json.return_value = {"parameterKeys": ["bad-key"]}
    exception = Conflict(response_mock, message, error_code)

    mocker.patch.object(ExperimentClient, "_patch_raw", side_effect=exception)

    client = ExperimentClient(mocker.Mock())

    with pytest.raises(ParamConflict, match=message):
        client.log_run_data(PROJECT_ID, EXPERIMENT_RUN_ID, params=[PARAM])


def test_log_run_data_other_conflict(mocker):
    response_mock = mocker.Mock()
    exception = Conflict(response_mock, "", "")

    mocker.patch.object(ExperimentClient, "_patch_raw", side_effect=exception)
    client = ExperimentClient(mocker.Mock())

    with pytest.raises(Conflict):
        client.log_run_data(PROJECT_ID, EXPERIMENT_RUN_ID, params=[PARAM])


def test_log_run_data_empty(mocker):
    mocker.patch.object(ExperimentClient, "_patch_raw")

    client = ExperimentClient(mocker.Mock())

    client.log_run_data(PROJECT_ID, EXPERIMENT_RUN_ID)
    ExperimentClient._patch_raw.assert_not_called()


def test_update_run_info(mocker):
    mocker.patch.object(
        ExperimentClient, "_patch", return_value=EXPERIMENT_RUN
    )
    run_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunSchema"
    )
    run_info_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunInfoSchema"
    )
    run_info_dump_mock = run_info_schema_mock.return_value.dump

    client = ExperimentClient(mocker.Mock())
    returned_run = client.update_run_info(
        PROJECT_ID,
        EXPERIMENT_RUN_ID,
        EXPERIMENT_RUN.status,
        EXPERIMENT_RUN.ended_at,
    )
    assert returned_run == EXPERIMENT_RUN

    run_schema_mock.assert_called_once_with()
    run_info_schema_mock.assert_called_once_with()
    run_info_dump_mock.assert_called_once_with(
        {"status": EXPERIMENT_RUN.status, "ended_at": EXPERIMENT_RUN.ended_at}
    )
    ExperimentClient._patch.assert_called_once_with(
        "/project/{}/run/{}/info".format(PROJECT_ID, EXPERIMENT_RUN_ID),
        run_schema_mock.return_value,
        json=run_info_dump_mock.return_value,
    )


def test_update_run_info_status_only(mocker):
    mocker.patch.object(
        ExperimentClient, "_patch", return_value=EXPERIMENT_RUN
    )
    run_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunSchema"
    )
    run_info_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunInfoSchema"
    )
    run_info_dump_mock = run_info_schema_mock.return_value.dump

    client = ExperimentClient(mocker.Mock())
    returned_run = client.update_run_info(
        PROJECT_ID, EXPERIMENT_RUN_ID, status=EXPERIMENT_RUN.status
    )
    assert returned_run == EXPERIMENT_RUN

    run_schema_mock.assert_called_once_with()
    run_info_schema_mock.assert_called_once_with()
    run_info_dump_mock.assert_called_once_with(
        {"status": EXPERIMENT_RUN.status, "ended_at": None}
    )
    ExperimentClient._patch.assert_called_once_with(
        "/project/{}/run/{}/info".format(PROJECT_ID, EXPERIMENT_RUN_ID),
        run_schema_mock.return_value,
        json=run_info_dump_mock.return_value,
    )


def test_update_run_info_ended_at_only(mocker):
    mocker.patch.object(
        ExperimentClient, "_patch", return_value=EXPERIMENT_RUN
    )
    run_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunSchema"
    )
    run_info_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunInfoSchema"
    )
    run_info_dump_mock = run_info_schema_mock.return_value.dump

    client = ExperimentClient(mocker.Mock())
    returned_run = client.update_run_info(
        PROJECT_ID, EXPERIMENT_RUN_ID, ended_at=EXPERIMENT_RUN.ended_at
    )
    assert returned_run == EXPERIMENT_RUN

    run_schema_mock.assert_called_once_with()
    run_info_schema_mock.assert_called_once_with()
    run_info_dump_mock.assert_called_once_with(
        {"status": None, "ended_at": EXPERIMENT_RUN.ended_at}
    )
    ExperimentClient._patch.assert_called_once_with(
        "/project/{}/run/{}/info".format(PROJECT_ID, EXPERIMENT_RUN_ID),
        run_schema_mock.return_value,
        json=run_info_dump_mock.return_value,
    )


def test_update_run_info_empty(mocker):
    mocker.patch.object(
        ExperimentClient, "_patch", return_value=EXPERIMENT_RUN
    )
    run_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunSchema"
    )
    run_info_schema_mock = mocker.patch(
        "faculty.clients.experiment.ExperimentRunInfoSchema"
    )
    run_info_dump_mock = run_info_schema_mock.return_value.dump

    client = ExperimentClient(mocker.Mock())
    returned_run = client.update_run_info(PROJECT_ID, EXPERIMENT_RUN_ID)
    assert returned_run == EXPERIMENT_RUN

    run_schema_mock.assert_called_once_with()
    run_info_schema_mock.assert_called_once_with()
    run_info_dump_mock.assert_called_once_with(
        {"status": None, "ended_at": None}
    )
    ExperimentClient._patch.assert_called_once_with(
        "/project/{}/run/{}/info".format(PROJECT_ID, EXPERIMENT_RUN_ID),
        run_schema_mock.return_value,
        json=run_info_dump_mock.return_value,
    )


def test_metric_history_schema():
    data = MetricHistorySchema().load(METRIC_HISTORY_BODY)
    assert data == METRIC_HISTORY


def test_metric_history_schema_invalid():
    with pytest.raises(ValidationError):
        MetricHistorySchema().load({})


def test_get_metric_history(mocker):
    mocker.patch.object(ExperimentClient, "_get", return_value=METRIC_HISTORY)
    metric_history_schema_mock = mocker.patch(
        "faculty.clients.experiment.MetricHistorySchema"
    )

    client = ExperimentClient(mocker.Mock())

    returned_metric_history = client.get_metric_history(
        PROJECT_ID, EXPERIMENT_RUN_ID, METRIC_KEY
    )
    assert returned_metric_history == [METRIC]

    metric_history_schema_mock.assert_called_once_with()

    ExperimentClient._get.assert_called_once_with(
        "/project/{}/run/{}/metric/{}/history".format(
            PROJECT_ID, EXPERIMENT_RUN_ID, METRIC_KEY
        ),
        metric_history_schema_mock.return_value,
    )


def test_delete_runs(mocker):
    mocker.patch.object(
        ExperimentClient, "_post", return_value=DELETE_EXPERIMENT_RUNS_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.experiment.DeleteExperimentRunsResponseSchema"
    )
    run_ids = [uuid4(), uuid4()]

    client = ExperimentClient(mocker.Mock())
    assert (
        client.delete_runs(PROJECT_ID, run_ids)
        == DELETE_EXPERIMENT_RUNS_RESPONSE
    )

    expected_payload = {
        "filter": {
            "operator": "or",
            "conditions": [
                {"by": "runId", "operator": "eq", "value": str(run_ids[0])},
                {"by": "runId", "operator": "eq", "value": str(run_ids[1])},
            ],
        }
    }

    ExperimentClient._post.assert_called_once_with(
        "/project/{}/run/delete/query".format(PROJECT_ID),
        schema_mock.return_value,
        json=expected_payload,
    )


def test_delete_runs_no_run_ids(mocker):
    mocker.patch.object(ExperimentClient, "_post")
    schema_mock = mocker.patch(
        "faculty.clients.experiment.DeleteExperimentRunsResponseSchema"
    )

    client = ExperimentClient(mocker.Mock())
    client.delete_runs(PROJECT_ID)

    ExperimentClient._post.assert_called_once_with(
        "/project/{}/run/delete/query".format(PROJECT_ID),
        schema_mock.return_value,
        json={},
    )


def test_delete_runs_empty_list(mocker):
    client = ExperimentClient(mocker.Mock())

    assert client.delete_runs(
        PROJECT_ID, run_ids=[]
    ) == DeleteExperimentRunsResponse(
        deleted_run_ids=[], conflicted_run_ids=[]
    )


def test_restore_runs(mocker):
    mocker.patch.object(
        ExperimentClient,
        "_post",
        return_value=RESTORE_EXPERIMENT_RUNS_RESPONSE,
    )
    schema_mock = mocker.patch(
        "faculty.clients.experiment.RestoreExperimentRunsResponseSchema"
    )
    run_ids = [uuid4(), uuid4()]

    client = ExperimentClient(mocker.Mock())
    assert (
        client.restore_runs(PROJECT_ID, run_ids)
        == RESTORE_EXPERIMENT_RUNS_RESPONSE
    )

    expected_payload = {
        "filter": {
            "operator": "or",
            "conditions": [
                {"by": "runId", "operator": "eq", "value": str(run_ids[0])},
                {"by": "runId", "operator": "eq", "value": str(run_ids[1])},
            ],
        }
    }

    ExperimentClient._post.assert_called_once_with(
        "/project/{}/run/restore/query".format(PROJECT_ID),
        schema_mock.return_value,
        json=expected_payload,
    )


def test_restore_runs_no_run_ids(mocker):
    mocker.patch.object(ExperimentClient, "_post")
    schema_mock = mocker.patch(
        "faculty.clients.experiment.RestoreExperimentRunsResponseSchema"
    )

    client = ExperimentClient(mocker.Mock())
    client.restore_runs(PROJECT_ID)

    ExperimentClient._post.assert_called_once_with(
        "/project/{}/run/restore/query".format(PROJECT_ID),
        schema_mock.return_value,
        json={},
    )


def test_restore_runs_empty_list(mocker):
    client = ExperimentClient(mocker.Mock())

    assert client.restore_runs(
        PROJECT_ID, run_ids=[]
    ) == RestoreExperimentRunsResponse(
        restored_run_ids=[], conflicted_run_ids=[]
    )
