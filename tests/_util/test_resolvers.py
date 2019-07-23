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

from faculty._util.resolvers import resolve_project_id


PROJECT_ID = uuid4()


@pytest.fixture(autouse=True)
def clear_cache():
    resolve_project_id.cache_clear()


@pytest.fixture
def mock_session(mocker):
    return mocker.Mock()


@pytest.fixture
def mock_account_client(mocker, mock_session):
    class_mock = mocker.patch("faculty._util.resolvers.AccountClient")
    yield class_mock.return_value
    class_mock.assert_called_once_with(mock_session)


@pytest.fixture
def mock_project_client(mocker, mock_session):
    class_mock = mocker.patch("faculty._util.resolvers.ProjectClient")
    yield class_mock.return_value
    class_mock.assert_called_once_with(mock_session)


def test_resolve_project_id(
    mocker, mock_session, mock_account_client, mock_project_client
):
    project = mocker.Mock()
    project.name = "project name"
    mock_project_client.list_accessible_by_user.return_value = [
        mocker.Mock(),
        project,
        mocker.Mock(),
    ]

    assert resolve_project_id(mock_session, "project name") == project.id

    mock_account_client.authenticated_user_id.assert_called_once_with()
    mock_project_client.list_accessible_by_user.assert_called_once_with(
        mock_account_client.authenticated_user_id.return_value
    )


def test_resolve_project_id_no_matches(
    mocker, mock_session, mock_account_client, mock_project_client
):
    mock_project_client.list_accessible_by_user.return_value = [
        mocker.Mock(),
        mocker.Mock(),
    ]
    with pytest.raises(ValueError, match="No projects .* found"):
        resolve_project_id(mock_session, "project name")


def test_resolve_project_id_multiple_matches(
    mocker, mock_session, mock_account_client, mock_project_client
):
    project = mocker.Mock()
    project.name = "project name"
    mock_project_client.list_accessible_by_user.return_value = [
        project,
        project,
    ]
    with pytest.raises(ValueError, match="Multiple projects .* found"):
        resolve_project_id(mock_session, "project name")


@pytest.mark.parametrize("argument", [PROJECT_ID, str(PROJECT_ID)])
def test_resolve_project_id_is_uuid(mock_session, argument):
    assert resolve_project_id(mock_session, argument) == PROJECT_ID


def test_resolve_project_id_from_context(mocker, mock_session):
    context = mocker.Mock()
    mocker.patch("faculty._util.resolvers.get_context", return_value=context)
    assert resolve_project_id(mock_session) == context.project_id


def test_resolve_project_id_from_context_missing(mocker, mock_session):
    context = mocker.Mock()
    context.project_id = None
    mocker.patch("faculty._util.resolvers.get_context", return_value=context)
    with pytest.raises(ValueError):
        resolve_project_id(mock_session)
