# Copyright 2018 ASI Data Science
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

import pytest
from marshmallow import ValidationError

from sherlockml.clients.project import ProjectSchema, Project, ProjectClient
from tests.clients.fixtures import PROFILE


USER_ID = uuid.uuid4()

PROJECT = Project(id=uuid.uuid4(), name="test-project", owner_id=uuid.uuid4())

PROJECT_BODY = {
    "projectId": str(PROJECT.id),
    "name": PROJECT.name,
    "ownerId": str(PROJECT.owner_id),
}


def test_project_schema():
    data = ProjectSchema().load(PROJECT_BODY)
    assert data == PROJECT


def test_project_schema_load_invalid():
    with pytest.raises(ValidationError):
        ProjectSchema().load({})


def test_project_client_create(mocker):
    mocker.patch.object(ProjectClient, "_post", return_value=PROJECT)
    schema_mock = mocker.patch("sherlockml.clients.project.ProjectSchema")

    client = ProjectClient(PROFILE)
    assert client.create(PROJECT.owner_id, PROJECT.name) == PROJECT

    schema_mock.assert_called_once_with()
    ProjectClient._post.assert_called_once_with(
        "/project",
        schema_mock.return_value,
        json={"owner_id": str(PROJECT.owner_id), "name": PROJECT.name},
    )


def test_project_client_get(mocker):
    mocker.patch.object(ProjectClient, "_get", return_value=PROJECT)
    schema_mock = mocker.patch("sherlockml.clients.project.ProjectSchema")

    client = ProjectClient(PROFILE)
    assert client.get(PROJECT.id) == PROJECT

    schema_mock.assert_called_once_with()
    ProjectClient._get.assert_called_once_with(
        "/project/{}".format(PROJECT.id), schema_mock.return_value
    )


def test_project_client_get_by_owner_and_name(mocker):
    mocker.patch.object(ProjectClient, "_get", return_value=PROJECT)
    schema_mock = mocker.patch("sherlockml.clients.project.ProjectSchema")

    client = ProjectClient(PROFILE)
    assert client.get_by_owner_and_name(USER_ID, PROJECT.name) == PROJECT

    schema_mock.assert_called_once_with()
    ProjectClient._get.assert_called_once_with(
        "/project/{}/{}".format(USER_ID, PROJECT.name),
        schema_mock.return_value,
    )


def test_project_client_list_accessible_by_user(mocker):
    mocker.patch.object(ProjectClient, "_get", return_value=[PROJECT])
    schema_mock = mocker.patch("sherlockml.clients.project.ProjectSchema")

    client = ProjectClient(PROFILE)
    assert client.list_accessible_by_user(USER_ID) == [PROJECT]

    schema_mock.assert_called_once_with(many=True)
    ProjectClient._get.assert_called_once_with(
        "/user/{}".format(USER_ID), schema_mock.return_value
    )
