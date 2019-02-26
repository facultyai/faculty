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
from pytz import UTC
from marshmallow import ValidationError

from faculty.clients.experiment import (
    Experiment,
    ExperimentSchema,
    ExperimentClient,
)


PROJECT_ID = uuid4()
CREATED_AT = datetime(2018, 3, 10, 11, 32, 6, 247000, tzinfo=UTC)
CREATED_AT_STRING = "2018-03-10T11:32:06.247Z"
LAST_UPDATED_AT = datetime(2018, 3, 10, 11, 32, 30, 172000, tzinfo=UTC)
LAST_UPDATED_AT_STRING = "2018-03-10T11:32:30.172Z"
DELETED_AT = datetime(2018, 3, 10, 11, 37, 42, 482000, tzinfo=UTC)
DELETED_AT_STRING = "2018-03-10T11:37:42.482Z"


EXPERIMENT = Experiment(
    id=12,
    name="experiment name",
    description="experiment description",
    artifact_location="https://example.com",
    created_at=CREATED_AT,
    last_updated_at=LAST_UPDATED_AT,
    deleted_at=DELETED_AT,
)
EXPERIMENT_BODY = {
    "experimentId": EXPERIMENT.id,
    "name": EXPERIMENT.name,
    "description": EXPERIMENT.description,
    "artifactLocation": EXPERIMENT.artifact_location,
    "createdAt": CREATED_AT_STRING,
    "lastUpdatedAt": LAST_UPDATED_AT_STRING,
    "deletedAt": DELETED_AT_STRING,
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


def test_job_client_list(mocker):
    mocker.patch.object(ExperimentClient, "_get", return_value=[EXPERIMENT])
    schema_mock = mocker.patch("faculty.clients.experiment.ExperimentSchema")

    client = ExperimentClient(mocker.Mock())
    assert client.list(PROJECT_ID) == [EXPERIMENT]

    schema_mock.assert_called_once_with(many=True)
    ExperimentClient._get.assert_called_once_with(
        "/project/{}/experiment".format(PROJECT_ID), schema_mock.return_value
    )
