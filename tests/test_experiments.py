from datetime import datetime
from uuid import uuid4

import pytest
from pytz import UTC

from faculty.clients.experiment import (
    Experiment,
    ExperimentRun,
    ExperimentRunStatus,
    ListExperimentRunsResponse,
    Metric,
    Page,
    Pagination,
    Param,
    SingleFilter,
    SingleFilterBy,
    SingleFilterOperator,
    Sort,
    SortBy,
    SortOrder,
    Tag,
)

from faculty.experiments import (
    ExperimentRun as FacultyExperimentRun,
    ExperimentRunQueryResult,
)


PROJECT_ID = uuid4()
EXPERIMENT_ID = 661
EXPERIMENT_RUN_ID = uuid4()
EXPERIMENT_RUN_NUMBER = 3
EXPERIMENT_RUN_NAME = "run name"
PARENT_RUN_ID = uuid4()
RUN_STARTED_AT = datetime(2018, 3, 10, 11, 39, 12, 110000, tzinfo=UTC)
RUN_ENDED_AT = datetime(2018, 3, 10, 11, 39, 15, 110000, tzinfo=UTC)
CREATED_AT = datetime(2018, 3, 10, 11, 32, 6, 247000, tzinfo=UTC)
LAST_UPDATED_AT = datetime(2018, 3, 10, 11, 32, 30, 172000, tzinfo=UTC)
DELETED_AT = datetime(2018, 3, 10, 11, 37, 42, 482000, tzinfo=UTC)
TAG = Tag(key="tag-key", value="tag-value")
PARAM = Param(key="param-key", value="param-value")
METRIC_KEY = "metric-key"
METRIC_TIMESTAMP = datetime(2018, 3, 12, 16, 20, 22, 122000, tzinfo=UTC)
METRIC = Metric(key=METRIC_KEY, value=123, timestamp=METRIC_TIMESTAMP)

EXPERIMENT = Experiment(
    id=EXPERIMENT_ID,
    name="experiment name",
    description="experiment description",
    artifact_location="https://example.com",
    created_at=CREATED_AT,
    last_updated_at=LAST_UPDATED_AT,
    deleted_at=DELETED_AT,
)

FILTER = SingleFilter(
    SingleFilterBy.EXPERIMENT_ID, None, SingleFilterOperator.EQUAL_TO, "2"
)

SORT = [Sort(SortBy.METRIC, "metric_key", SortOrder.ASC)]

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
PAGINATION = Pagination(0, 1, None, None)
LIST_EXPERIMENT_RUNS_RESPONSE = ListExperimentRunsResponse(
    runs=[EXPERIMENT_RUN], pagination=PAGINATION
)
EXPECTED_RUNS = [
    FacultyExperimentRun(
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
]

PAGINATION_MULTIPLE_1 = Pagination(0, 1, None, Page(1, 1))
LIST_EXPERIMENT_RUNS_RESPONSE_MULTIPLE_1 = ListExperimentRunsResponse(
    runs=[EXPERIMENT_RUN], pagination=PAGINATION_MULTIPLE_1
)
EXPERIMENT_RUN_MULTIPLE_2 = ExperimentRun(
    id=7,
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
PAGINATION_MULTIPLE_2 = Pagination(1, 1, Page(0, 1), None)
LIST_EXPERIMENT_RUNS_RESPONSE_MULTIPLE_2 = ListExperimentRunsResponse(
    runs=[EXPERIMENT_RUN_MULTIPLE_2], pagination=PAGINATION_MULTIPLE_2
)
EXPECTED_RUNS_2 = [
    FacultyExperimentRun(
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
    ),
    FacultyExperimentRun(
        id=7,
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
    ),
]


@pytest.mark.parametrize(
    "query_runs_side_effects,expected_runs",
    [
        [[LIST_EXPERIMENT_RUNS_RESPONSE], EXPECTED_RUNS],
        [
            [
                LIST_EXPERIMENT_RUNS_RESPONSE_MULTIPLE_1,
                LIST_EXPERIMENT_RUNS_RESPONSE_MULTIPLE_2,
            ],
            EXPECTED_RUNS_2,
        ],
    ],
)
def test_experiment_run_query_single_call(
    mocker, query_runs_side_effects, expected_runs
):
    experiment_client_mock = mocker.MagicMock()
    experiment_client_mock.query_runs = mocker.MagicMock(
        side_effect=query_runs_side_effects
    )
    mocker.patch("faculty.client", return_value=experiment_client_mock)

    response = FacultyExperimentRun.query(PROJECT_ID, FILTER, SORT)

    assert isinstance(response, ExperimentRunQueryResult)
    returned_runs = list(response)
    for expected_run, returned_run in zip(expected_runs, returned_runs):
        assert isinstance(returned_run, FacultyExperimentRun)
        assert _are_runs_equal(expected_run, returned_run)


def _are_runs_equal(this, that):
    return all(
        list(
            i == j
            for i, j in (
                list(
                    zip(
                        [getattr(this, attr) for attr in this.__dict__.keys()],
                        [getattr(that, attr) for attr in that.__dict__.keys()],
                    )
                )
            )
        )
    )
