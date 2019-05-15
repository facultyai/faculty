from datetime import datetime
from uuid import uuid4

import pytest
from pytz import UTC
import inspect

from faculty.clients.experiment import (
    Experiment,
    ExperimentRun,
    ExperimentRunStatus,
    ListExperimentRunsResponse,
    Metric,
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

PAGINATION = Pagination(start=20, size=10, previous=None, next=None)

LIST_EXPERIMENT_RUNS_RESPONSE = ListExperimentRunsResponse(
    runs=[EXPERIMENT_RUN], pagination=PAGINATION
)

FILTER = SingleFilter(
    SingleFilterBy.EXPERIMENT_ID, None, SingleFilterOperator.EQUAL_TO, "2"
)

SORT = [Sort(SortBy.METRIC, "metric_key", SortOrder.ASC)]


def test_experiment_run_query(mocker):

    experiment_client_mock = mocker.MagicMock()
    experiment_client_mock.query_runs.return_value = (
        LIST_EXPERIMENT_RUNS_RESPONSE
    )
    mocker.patch("faculty.client", return_value=experiment_client_mock)

    expected_response = FacultyExperimentRun(
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

    response = FacultyExperimentRun.query(PROJECT_ID, FILTER, SORT)

    assert isinstance(response, ExperimentRunQueryResult)
    returned_run = list(response)[0]
    assert isinstance(returned_run, FacultyExperimentRun)
    assert all(
        list(
            i == j
            for i, j in (
                list(
                    zip(
                        [
                            getattr(returned_run, attr)
                            for attr in returned_run.__dict__.keys()
                        ],
                        [
                            getattr(expected_response, attr)
                            for attr in expected_response.__dict__.keys()
                        ],
                    )
                )
            )
        )
    )
