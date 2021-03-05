# Copyright 2018-2021 Faculty Science Limited
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


import uuid
from datetime import datetime

import pytest
from marshmallow import ValidationError
from pytz import UTC

from faculty.clients.project import Project, ProjectClient, _ProjectSchema


USER_ID = uuid.uuid4()
ARCHIVED_AT = datetime(2018, 3, 10, 11, 32, 6, 247000, tzinfo=UTC)
ARCHIVED_AT_STRING = "2018-03-10T11:32:06.247Z"

PROJECT = Project(
    id=uuid.uuid4(),
    name="test-project",
    owner_id=uuid.uuid4(),
    archived_at=None,
)
PROJECT_BODY = {
    "projectId": str(PROJECT.id),
    "name": PROJECT.name,
    "ownerId": str(PROJECT.owner_id),
    "archivedAt": None,
}

ARCHIVED_PROJECT = Project(
    id=uuid.uuid4(),
    name="archived-project",
    owner_id=uuid.uuid4(),
    archived_at=ARCHIVED_AT,
)
ARCHIVED_PROJECT_BODY = {
    "projectId": str(ARCHIVED_PROJECT.id),
    "name": ARCHIVED_PROJECT.name,
    "ownerId": str(ARCHIVED_PROJECT.owner_id),
    "archivedAt": ARCHIVED_AT_STRING,
}


@pytest.mark.parametrize(
    "body, expected",
    [(PROJECT_BODY, PROJECT), (ARCHIVED_PROJECT_BODY, ARCHIVED_PROJECT)],
)
def test_project_schema(body, expected):
    data = _ProjectSchema().load(body)
    assert data == expected


def test_project_schema_invalid():
    with pytest.raises(ValidationError):
        _ProjectSchema().load({})


def test_project_client_create(mocker):
    mocker.patch.object(ProjectClient, "_post", return_value=PROJECT)
    schema_mock = mocker.patch("faculty.clients.project._ProjectSchema")

    client = ProjectClient(mocker.Mock(), mocker.Mock())
    assert client.create(PROJECT.owner_id, PROJECT.name) == PROJECT

    schema_mock.assert_called_once_with()
    ProjectClient._post.assert_called_once_with(
        "/project",
        schema_mock.return_value,
        json={"owner_id": str(PROJECT.owner_id), "name": PROJECT.name},
    )


def test_project_client_get(mocker):
    mocker.patch.object(ProjectClient, "_get", return_value=PROJECT)
    schema_mock = mocker.patch("faculty.clients.project._ProjectSchema")

    client = ProjectClient(mocker.Mock(), mocker.Mock())
    assert client.get(PROJECT.id) == PROJECT

    schema_mock.assert_called_once_with()
    ProjectClient._get.assert_called_once_with(
        "/project/{}".format(PROJECT.id), schema_mock.return_value
    )


def test_project_client_get_by_owner_and_name(mocker):
    mocker.patch.object(ProjectClient, "_get", return_value=PROJECT)
    schema_mock = mocker.patch("faculty.clients.project._ProjectSchema")

    client = ProjectClient(mocker.Mock(), mocker.Mock())
    assert client.get_by_owner_and_name(USER_ID, PROJECT.name) == PROJECT

    schema_mock.assert_called_once_with()
    ProjectClient._get.assert_called_once_with(
        "/project/{}/{}".format(USER_ID, PROJECT.name),
        schema_mock.return_value,
    )


def test_project_client_list_accessible_by_user(mocker):
    mocker.patch.object(ProjectClient, "_get", return_value=[PROJECT])
    schema_mock = mocker.patch("faculty.clients.project._ProjectSchema")

    client = ProjectClient(mocker.Mock(), mocker.Mock())
    assert client.list_accessible_by_user(USER_ID) == [PROJECT]

    schema_mock.assert_called_once_with(many=True)
    ProjectClient._get.assert_called_once_with(
        "/user/{}".format(USER_ID), schema_mock.return_value
    )


@pytest.mark.parametrize(
    "include_archived, include_archived_param", [(False, 0), (True, 1)]
)
def test_project_client_list_all(
    mocker, include_archived, include_archived_param
):
    mocker.patch.object(ProjectClient, "_get", return_value=[PROJECT])
    schema_mock = mocker.patch("faculty.clients.project._ProjectSchema")

    client = ProjectClient(mocker.Mock(), mocker.Mock())
    assert client.list_all(include_archived) == [PROJECT]

    schema_mock.assert_called_once_with(many=True)
    ProjectClient._get.assert_called_once_with(
        "/project",
        schema_mock.return_value,
        params={"includeArchived": include_archived_param},
    )
