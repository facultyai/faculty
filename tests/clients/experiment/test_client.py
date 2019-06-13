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


from uuid import uuid4

import pytest

from faculty.clients.base import Conflict
from faculty.clients.experiment._client import (
    ExperimentClient,
    ExperimentDeleted,
    ExperimentNameConflict,
    ParamConflict,
)
from faculty.clients.experiment._models import (
    ComparisonOperator,
    CompoundFilter,
    DeletedAtFilter,
    ExperimentIdFilter,
    LifecycleStage,
    LogicalOperator,
    Metric,
    Page,
    RunIdFilter,
    RunQuery,
)


PROJECT_ID = uuid4()
EXPERIMENT_ID = 234
EXPERIMENT_RUN_ID = uuid4()
PARENT_RUN_ID = uuid4()


@pytest.mark.parametrize("description", [None, "experiment description"])
@pytest.mark.parametrize("artifact_location", [None, "s3://mybucket"])
def test_experiment_client_create(mocker, description, artifact_location):
    experiment = mocker.Mock()
    mocker.patch.object(ExperimentClient, "_post", return_value=experiment)
    schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ExperimentSchema"
    )

    client = ExperimentClient(mocker.Mock())
    returned_experiment = client.create(
        PROJECT_ID, "experiment name", description, artifact_location
    )
    assert returned_experiment == experiment

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
    experiment = mocker.Mock()
    mocker.patch.object(ExperimentClient, "_get", return_value=experiment)
    schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ExperimentSchema"
    )

    client = ExperimentClient(mocker.Mock())
    returned_experiment = client.get(PROJECT_ID, EXPERIMENT_ID)
    assert returned_experiment == experiment

    schema_mock.assert_called_once_with()
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/experiment/{}".format(PROJECT_ID, EXPERIMENT_ID),
        schema_mock.return_value,
    )


def test_experiment_client_list(mocker):
    experiment = mocker.Mock()
    mocker.patch.object(ExperimentClient, "_get", return_value=[experiment])
    schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ExperimentSchema"
    )

    client = ExperimentClient(mocker.Mock())
    assert client.list(PROJECT_ID) == [experiment]

    schema_mock.assert_called_once_with(many=True)
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/experiment".format(PROJECT_ID),
        schema_mock.return_value,
        params={},
    )


def test_experiment_client_list_lifecycle_filter(mocker):
    experiment = mocker.Mock()
    mocker.patch.object(ExperimentClient, "_get", return_value=[experiment])
    schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ExperimentSchema"
    )

    client = ExperimentClient(mocker.Mock())
    returned_experiments = client.list(
        PROJECT_ID, lifecycle_stage=LifecycleStage.ACTIVE
    )
    assert returned_experiments == [experiment]

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
    run = mocker.Mock()
    mocker.patch.object(ExperimentClient, "_post", return_value=run)
    request_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.CreateRunSchema"
    )
    dump_mock = request_schema_mock.return_value.dump
    response_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ExperimentRunSchema"
    )
    run_name = mocker.Mock()
    started_at = mocker.Mock()
    artifact_location = mocker.Mock()

    client = ExperimentClient(mocker.Mock())
    returned_run = client.create_run(
        PROJECT_ID,
        EXPERIMENT_ID,
        run_name,
        started_at,
        PARENT_RUN_ID,
        artifact_location=artifact_location,
    )
    assert returned_run == run

    request_schema_mock.assert_called_once_with()
    dump_mock.assert_called_once_with(
        {
            "name": run_name,
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

    client = ExperimentClient(mocker.Mock())
    with pytest.raises(ExperimentDeleted, match=message):
        client.create_run(
            PROJECT_ID,
            EXPERIMENT_ID,
            name=mocker.Mock(),
            started_at=mocker.Mock(),
            parent_run_id=PARENT_RUN_ID,
            artifact_location=mocker.Mock(),
        )


def test_experiment_client_get_run(mocker):
    run = mocker.Mock()
    mocker.patch.object(ExperimentClient, "_get", return_value=run)
    schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ExperimentRunSchema"
    )

    client = ExperimentClient(mocker.Mock())
    returned_run = client.get_run(PROJECT_ID, EXPERIMENT_RUN_ID)
    assert returned_run == run

    schema_mock.assert_called_once_with()
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/run/{}".format(PROJECT_ID, EXPERIMENT_RUN_ID),
        schema_mock.return_value,
    )


def test_experiment_client_list_runs(mocker):
    mocker.patch.object(ExperimentClient, "query_runs")

    client = ExperimentClient(mocker.Mock())
    response = client.list_runs(
        PROJECT_ID,
        experiment_ids=[123, 456],
        lifecycle_stage=LifecycleStage.DELETED,
        start=20,
        limit=10,
    )

    assert response == ExperimentClient.query_runs.return_value
    expected_filter = CompoundFilter(
        LogicalOperator.AND,
        [
            CompoundFilter(
                LogicalOperator.OR,
                [
                    ExperimentIdFilter(ComparisonOperator.EQUAL_TO, 123),
                    ExperimentIdFilter(ComparisonOperator.EQUAL_TO, 456),
                ],
            ),
            DeletedAtFilter(ComparisonOperator.DEFINED, True),
        ],
    )
    ExperimentClient.query_runs.assert_called_once_with(
        PROJECT_ID, expected_filter, None, 20, 10
    )


def test_experiment_client_list_runs_defaults(mocker):
    mocker.patch.object(ExperimentClient, "query_runs")

    client = ExperimentClient(mocker.Mock())
    response = client.list_runs(PROJECT_ID)

    assert response == ExperimentClient.query_runs.return_value
    ExperimentClient.query_runs.assert_called_once_with(
        PROJECT_ID, None, None, None, None
    )


def test_experiment_client_query_runs(mocker):
    list_response = mocker.Mock()
    mocker.patch.object(ExperimentClient, "_post", return_value=list_response)
    response_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ListExperimentRunsResponseSchema"
    )
    request_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.RunQuerySchema"
    )
    request_dump_mock = request_schema_mock.return_value.dump

    filter = mocker.Mock()
    sort = mocker.Mock()

    client = ExperimentClient(mocker.Mock())
    list_result = client.query_runs(
        PROJECT_ID, filter, sort, start=20, limit=10
    )

    assert list_result == list_response

    request_dump_mock.assert_called_once_with(
        RunQuery(filter, sort, Page(20, 10))
    )
    response_schema_mock.assert_called_once_with()
    ExperimentClient._post.assert_called_once_with(
        "/project/{}/run/query".format(PROJECT_ID),
        response_schema_mock.return_value,
        json=request_dump_mock.return_value,
    )


def test_log_run_data(mocker):
    mocker.patch.object(ExperimentClient, "_patch_raw")
    run_data_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ExperimentRunDataSchema"
    )
    run_data_dump_mock = run_data_schema_mock.return_value.dump

    metric = mocker.Mock()
    param = mocker.Mock()
    tag = mocker.Mock()

    client = ExperimentClient(mocker.Mock())
    client.log_run_data(
        PROJECT_ID,
        EXPERIMENT_RUN_ID,
        metrics=[metric],
        params=[param],
        tags=[tag],
    )

    run_data_schema_mock.assert_called_once_with()
    run_data_dump_mock.assert_called_once_with(
        {"metrics": [metric], "params": [param], "tags": [tag]}
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
        client.log_run_data(
            PROJECT_ID, EXPERIMENT_RUN_ID, params=[mocker.Mock()]
        )


def test_log_run_data_other_conflict(mocker):
    response_mock = mocker.Mock()
    exception = Conflict(response_mock, "", "")

    mocker.patch.object(ExperimentClient, "_patch_raw", side_effect=exception)
    client = ExperimentClient(mocker.Mock())

    with pytest.raises(Conflict):
        client.log_run_data(
            PROJECT_ID, EXPERIMENT_RUN_ID, params=[mocker.Mock()]
        )


def test_log_run_data_empty(mocker):
    mocker.patch.object(ExperimentClient, "_patch_raw")

    client = ExperimentClient(mocker.Mock())

    client.log_run_data(PROJECT_ID, EXPERIMENT_RUN_ID)
    ExperimentClient._patch_raw.assert_not_called()


def test_update_run_info(mocker):
    run = mocker.Mock()
    mocker.patch.object(ExperimentClient, "_patch", return_value=run)
    run_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ExperimentRunSchema"
    )
    run_info_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.ExperimentRunInfoSchema"
    )
    run_info_dump_mock = run_info_schema_mock.return_value.dump

    status = mocker.Mock()
    ended_at = mocker.Mock()

    client = ExperimentClient(mocker.Mock())
    returned_run = client.update_run_info(
        PROJECT_ID, EXPERIMENT_RUN_ID, status, ended_at
    )
    assert returned_run == run

    run_schema_mock.assert_called_once_with()
    run_info_schema_mock.assert_called_once_with()
    run_info_dump_mock.assert_called_once_with(
        {"status": status, "ended_at": ended_at}
    )
    ExperimentClient._patch.assert_called_once_with(
        "/project/{}/run/{}/info".format(PROJECT_ID, EXPERIMENT_RUN_ID),
        run_schema_mock.return_value,
        json=run_info_dump_mock.return_value,
    )


def test_get_metric_history(mocker):
    key = mocker.Mock()
    data_point_0 = mocker.Mock()
    data_point_1 = mocker.Mock()
    metric_history = mocker.Mock(key=key, history=[data_point_0, data_point_1])

    mocker.patch.object(ExperimentClient, "_get", return_value=metric_history)
    metric_history_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.MetricHistorySchema"
    )

    client = ExperimentClient(mocker.Mock())
    metrics = client.get_metric_history(
        PROJECT_ID, EXPERIMENT_RUN_ID, "metric-key"
    )

    expected = [
        Metric(
            key=key,
            step=data_point_0.step,
            timestamp=data_point_0.timestamp,
            value=data_point_0.value,
        ),
        Metric(
            key=key,
            step=data_point_1.step,
            timestamp=data_point_1.timestamp,
            value=data_point_1.value,
        ),
    ]
    assert metrics == expected

    metric_history_schema_mock.assert_called_once_with()
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/run/{}/metric/metric-key/history".format(
            PROJECT_ID, EXPERIMENT_RUN_ID
        ),
        metric_history_schema_mock.return_value,
    )


def test_delete_runs(mocker):
    delete_runs_response = mocker.Mock()
    mocker.patch.object(
        ExperimentClient, "_post", return_value=delete_runs_response
    )
    response_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.DeleteExperimentRunsResponseSchema"
    )
    filter_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.FilterSchema"
    )
    filter_dump_mock = filter_schema_mock.return_value.dump

    run_ids = [uuid4(), uuid4()]

    client = ExperimentClient(mocker.Mock())
    response = client.delete_runs(PROJECT_ID, run_ids)

    assert response == delete_runs_response

    expected_filter = CompoundFilter(
        LogicalOperator.OR,
        [
            RunIdFilter(ComparisonOperator.EQUAL_TO, run_ids[0]),
            RunIdFilter(ComparisonOperator.EQUAL_TO, run_ids[1]),
        ],
    )
    filter_dump_mock.assert_called_once_with(expected_filter)
    ExperimentClient._post.assert_called_once_with(
        "/project/{}/run/delete/query".format(PROJECT_ID),
        response_schema_mock.return_value,
        json={"filter": filter_dump_mock.return_value},
    )


def test_delete_runs_no_run_ids(mocker):
    mocker.patch.object(ExperimentClient, "_post")
    schema_mock = mocker.patch(
        "faculty.clients.experiment._client.DeleteExperimentRunsResponseSchema"
    )

    client = ExperimentClient(mocker.Mock())
    client.delete_runs(PROJECT_ID)

    ExperimentClient._post.assert_called_once_with(
        "/project/{}/run/delete/query".format(PROJECT_ID),
        schema_mock.return_value,
        json={},
    )


def test_delete_runs_empty_list(mocker):
    mocker.patch.object(ExperimentClient, "_post")

    client = ExperimentClient(mocker.Mock())
    response = client.delete_runs(PROJECT_ID, run_ids=[])

    ExperimentClient._post.assert_not_called()
    assert len(response.deleted_run_ids) == 0
    assert len(response.conflicted_run_ids) == 0


def test_restore_runs(mocker):
    restore_runs_response = mocker.Mock()
    mocker.patch.object(
        ExperimentClient, "_post", return_value=restore_runs_response
    )
    response_schema_mock = mocker.patch(
        "faculty.clients.experiment._client."
        "RestoreExperimentRunsResponseSchema"
    )
    filter_schema_mock = mocker.patch(
        "faculty.clients.experiment._client.FilterSchema"
    )
    filter_dump_mock = filter_schema_mock.return_value.dump

    run_ids = [uuid4(), uuid4()]

    client = ExperimentClient(mocker.Mock())
    response = client.restore_runs(PROJECT_ID, run_ids)

    assert response == restore_runs_response

    expected_filter = CompoundFilter(
        LogicalOperator.OR,
        [
            RunIdFilter(ComparisonOperator.EQUAL_TO, run_ids[0]),
            RunIdFilter(ComparisonOperator.EQUAL_TO, run_ids[1]),
        ],
    )
    filter_dump_mock.assert_called_once_with(expected_filter)
    ExperimentClient._post.assert_called_once_with(
        "/project/{}/run/restore/query".format(PROJECT_ID),
        response_schema_mock.return_value,
        json={"filter": filter_dump_mock.return_value},
    )


def test_restore_runs_no_run_ids(mocker):
    mocker.patch.object(ExperimentClient, "_post")
    schema_mock = mocker.patch(
        "faculty.clients.experiment._client."
        "RestoreExperimentRunsResponseSchema"
    )

    client = ExperimentClient(mocker.Mock())
    client.restore_runs(PROJECT_ID)

    ExperimentClient._post.assert_called_once_with(
        "/project/{}/run/restore/query".format(PROJECT_ID),
        schema_mock.return_value,
        json={},
    )


def test_restore_runs_empty_list(mocker):
    mocker.patch.object(ExperimentClient, "_post")

    client = ExperimentClient(mocker.Mock())
    response = client.restore_runs(PROJECT_ID, run_ids=[])

    ExperimentClient._post.assert_not_called()
    assert len(response.restored_run_ids) == 0
    assert len(response.conflicted_run_ids) == 0
