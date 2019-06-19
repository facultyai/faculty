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
import operator

import pytest
from pytz import UTC
import mock

from faculty.clients.experiment import (
    ComparisonOperator,
    CompoundFilter,
    DeletedAtFilter,
    ExperimentIdFilter,
    ExperimentRun as ClientExperimentRun,
    ExperimentRunStatus,
    LogicalOperator,
    MetricFilter,
    ParamFilter,
    RunIdFilter,
    TagFilter,
)

from faculty.experiment import ExperimentRun, ExperimentRunList, FilterBy


DATETIMES = [
    datetime(2018, 3, 10, 11, 39, 12, 110000, tzinfo=UTC),
    datetime(2018, 3, 10, 11, 32, 6, 247000, tzinfo=UTC),
    datetime(2018, 3, 10, 11, 32, 30, 172000, tzinfo=UTC),
    datetime(2018, 3, 10, 11, 37, 42, 482000, tzinfo=UTC),
]


def mock_client_run():
    return ClientExperimentRun(
        **{field: mock.Mock() for field in ClientExperimentRun._fields}
    )


def expected_resource_run_for_client_run(run):
    return ExperimentRun(
        id=run.id,
        run_number=run.run_number,
        experiment_id=run.experiment_id,
        name=run.name,
        parent_run_id=run.parent_run_id,
        artifact_location=run.artifact_location,
        status=run.status,
        started_at=run.started_at,
        ended_at=run.ended_at,
        deleted_at=run.deleted_at,
        tags=run.tags,
        params=run.params,
        metrics=run.metrics,
    )


def test_experiment_run_query(mocker):
    session = mocker.Mock()
    get_session_mock = mocker.patch(
        "faculty.experiment.get_session", return_value=session
    )

    project_id = mocker.Mock()
    resolve_project_id_mock = mocker.patch(
        "faculty.experiment.resolve_project_id", return_value=project_id
    )

    client = mocker.Mock()
    mocker.patch("faculty.experiment.ExperimentClient", return_value=client)
    client_runs = [mock_client_run(), mock_client_run()]
    client_response = mocker.Mock()
    client_response.runs = client_runs
    client_response.pagination.next = None
    client.query_runs.return_value = client_response

    filter = mocker.Mock()
    sort = mocker.Mock()

    runs = ExperimentRun.query("my project", filter, sort, extra_conf="foo")
    assert runs == ExperimentRunList(
        [expected_resource_run_for_client_run(run) for run in client_runs]
    )

    get_session_mock.assert_called_once_with(extra_conf="foo")
    resolve_project_id_mock.assert_called_once_with(session, "my project")
    client.query_runs.assert_called_once_with(project_id, filter, sort)


def test_experiment_run_query_multiple_pages(mocker):
    session = mocker.Mock()
    get_session_mock = mocker.patch(
        "faculty.experiment.get_session", return_value=session
    )

    project_id = mocker.Mock()
    resolve_project_id_mock = mocker.patch(
        "faculty.experiment.resolve_project_id", return_value=project_id
    )

    client = mocker.Mock()
    mocker.patch("faculty.experiment.ExperimentClient", return_value=client)
    client_response_0 = mocker.Mock()
    client_response_0.runs = [mock_client_run(), mock_client_run()]
    client_response_1 = mocker.Mock()
    client_response_1.runs = [mock_client_run(), mock_client_run()]
    client_response_2 = mocker.Mock()
    client_response_2.runs = [mock_client_run(), mock_client_run()]
    client_response_2.pagination.next = None
    client.query_runs.side_effect = [
        client_response_0,
        client_response_1,
        client_response_2,
    ]
    all_client_runs = (
        client_response_0.runs
        + client_response_1.runs
        + client_response_2.runs
    )

    filter = mocker.Mock()
    sort = mocker.Mock()

    runs = ExperimentRun.query("my project", filter, sort, extra_conf="foo")
    assert runs == ExperimentRunList(
        [expected_resource_run_for_client_run(run) for run in all_client_runs]
    )

    get_session_mock.assert_called_once_with(extra_conf="foo")
    resolve_project_id_mock.assert_called_once_with(session, "my project")
    client.query_runs.assert_has_calls(
        [
            mocker.call(project_id, filter, sort),
            mocker.call(
                project_id,
                filter,
                sort,
                start=client_response_0.pagination.next.start,
                limit=client_response_0.pagination.next.limit,
            ),
            mocker.call(
                project_id,
                filter,
                sort,
                start=client_response_1.pagination.next.start,
                limit=client_response_1.pagination.next.limit,
            ),
        ]
    )


def test_experiment_run_list_as_dataframe(mocker):
    run_0 = mocker.Mock(
        experiment_id=1,
        id=uuid4(),
        run_number=3,
        status=ExperimentRunStatus.FINISHED,
        started_at=DATETIMES[0],
        ended_at=DATETIMES[1],
        params=[
            mocker.Mock(key="classic", value="foo"),
            mocker.Mock(key="monty", value="spam"),
        ],
        metrics=[
            mocker.Mock(key="accuracy", value=0.87),
            mocker.Mock(key="f1_score", value=0.76),
        ],
    )
    run_1 = mocker.Mock(
        experiment_id=2,
        id=uuid4(),
        run_number=4,
        status=ExperimentRunStatus.RUNNING,
        started_at=DATETIMES[2],
        ended_at=DATETIMES[3],
        params=[
            mocker.Mock(key="classic", value="bar"),
            mocker.Mock(key="monty", value="eggs"),
        ],
        metrics=[
            mocker.Mock(key="accuracy", value=0.91),
            mocker.Mock(key="f1_score", value=0.72),
        ],
    )

    runs_df = ExperimentRunList([run_0, run_1]).as_dataframe()

    assert list(runs_df.columns) == [
        ("experiment_id", ""),
        ("run_id", ""),
        ("run_number", ""),
        ("status", ""),
        ("started_at", ""),
        ("ended_at", ""),
        ("params", "classic"),
        ("params", "monty"),
        ("metrics", "accuracy"),
        ("metrics", "f1_score"),
    ]
    assert (runs_df.experiment_id == [1, 2]).all()
    assert (runs_df.run_id == [run_0.id, run_1.id]).all()
    assert (runs_df.run_number == [3, 4]).all()
    assert (runs_df.status == ["finished", "running"]).all()
    assert (runs_df.started_at == [DATETIMES[0], DATETIMES[2]]).all()
    assert (runs_df.ended_at == [DATETIMES[1], DATETIMES[3]]).all()
    assert (runs_df.params.classic == ["foo", "bar"]).all()
    assert (runs_df.params.monty == ["spam", "eggs"]).all()
    assert (runs_df.metrics.accuracy == [0.87, 0.91]).all()
    assert (runs_df.metrics.f1_score == [0.76, 0.72]).all()


FILTER_BY_NO_KEY_CASES = [
    (FilterBy.experiment_id, ExperimentIdFilter),
    (FilterBy.run_id, RunIdFilter),
    (FilterBy.deleted_at, DeletedAtFilter),
]
FILTER_BY_WITH_KEY_CASES = [
    (FilterBy.tag, TagFilter),
    (FilterBy.param, ParamFilter),
    (FilterBy.metric, MetricFilter),
]

OPERATOR_CASES = [
    (lambda x, v: x.defined(v), ComparisonOperator.DEFINED),
    (operator.eq, ComparisonOperator.EQUAL_TO),
    (operator.ne, ComparisonOperator.NOT_EQUAL_TO),
    (operator.gt, ComparisonOperator.GREATER_THAN),
    (operator.ge, ComparisonOperator.GREATER_THAN_OR_EQUAL_TO),
    (operator.lt, ComparisonOperator.LESS_THAN),
    (operator.le, ComparisonOperator.LESS_THAN_OR_EQUAL_TO),
]


@pytest.mark.parametrize("method, filter_cls", FILTER_BY_NO_KEY_CASES)
@pytest.mark.parametrize("python_operator, expected_operator", OPERATOR_CASES)
def test_filter_by_no_key(
    mocker, method, filter_cls, python_operator, expected_operator
):
    value = mocker.Mock()
    filter = python_operator(method(), value)
    expected = filter_cls(expected_operator, value)
    assert filter == expected


@pytest.mark.parametrize("method, filter_cls", FILTER_BY_WITH_KEY_CASES)
@pytest.mark.parametrize("python_operator, expected_operator", OPERATOR_CASES)
def test_filter_by_with_key(
    mocker, method, filter_cls, python_operator, expected_operator
):
    key = mocker.Mock()
    value = mocker.Mock()
    filter = python_operator(method(key), value)
    expected = filter_cls(key, expected_operator, value)
    assert filter == expected


@pytest.mark.parametrize("method, filter_cls", FILTER_BY_NO_KEY_CASES)
def test_filter_by_one_of_no_key(mocker, method, filter_cls):
    values = [mocker.Mock(), mocker.Mock()]
    filter = method().one_of(values)
    expected = CompoundFilter(
        LogicalOperator.OR,
        [
            filter_cls(ComparisonOperator.EQUAL_TO, values[0]),
            filter_cls(ComparisonOperator.EQUAL_TO, values[1]),
        ],
    )
    assert filter == expected


@pytest.mark.parametrize("method, filter_cls", FILTER_BY_WITH_KEY_CASES)
def test_filter_by_one_of_with_key(mocker, method, filter_cls):
    key = mocker.Mock()
    values = [mocker.Mock(), mocker.Mock()]
    filter = method(key).one_of(values)
    expected = CompoundFilter(
        LogicalOperator.OR,
        [
            filter_cls(key, ComparisonOperator.EQUAL_TO, values[0]),
            filter_cls(key, ComparisonOperator.EQUAL_TO, values[1]),
        ],
    )
    assert filter == expected
