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

import datetime
import uuid

import pytest
from dateutil.tz import UTC
from marshmallow import ValidationError

from faculty.clients.environment import (
    Environment,
    EnvironmentClient,
    EnvironmentSchema,
)


PROJECT_ID = uuid.uuid4()

ENVIRONMENT = Environment(
    id=uuid.uuid4(),
    project_id=PROJECT_ID,
    name="Test Environment",
    description="Environment description",
    author_id=uuid.uuid4(),
    created_at=datetime.datetime(2018, 10, 3, 4, 20, 0, 0, tzinfo=UTC),
    updated_at=datetime.datetime(2018, 11, 3, 4, 21, 15, 0, tzinfo=UTC),
)

ENVIRONMENT_BODY = {
    "environmentId": str(ENVIRONMENT.id),
    "projectId": str(PROJECT_ID),
    "name": ENVIRONMENT.name,
    "description": ENVIRONMENT.description,
    "authorId": str(ENVIRONMENT.author_id),
    "createdAt": "2018-10-03T04:20:00Z",
    "updatedAt": "2018-11-03T04:21:15Z",
}


def test_environment_schema():
    data = EnvironmentSchema().load(ENVIRONMENT_BODY)
    assert data == ENVIRONMENT


def test_environment_schema_invalid():
    with pytest.raises(ValidationError):
        EnvironmentSchema().load({})


def test_environment_client_list(mocker):
    mocker.patch.object(EnvironmentClient, "_get", return_value=[ENVIRONMENT])
    schema_mock = mocker.patch("faculty.clients.environment.EnvironmentSchema")

    client = EnvironmentClient(mocker.Mock())
    assert client.list(PROJECT_ID) == [ENVIRONMENT]

    schema_mock.assert_called_once_with(many=True)
    EnvironmentClient._get.assert_called_once_with(
        "/project/{}/environment".format(PROJECT_ID), schema_mock.return_value
    )
