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

import pytest

from datetime import datetime
from dateutil.tz import UTC
from uuid import uuid4

from marshmallow import ValidationError

from sherlockml.clients.workspace import (
    Directory,
    File,
    FileNodeSchema,
    ListResponse,
    ListResponseSchema,
    WorkspaceClient,
)
from tests.clients.fixtures import PROFILE


PROJECT_ID = uuid4()

FILE = File(
    path="/path/to/test-directory/test-file",
    name="test-file",
    last_modified=datetime(2017, 8, 9, 14, 21, 13, tzinfo=UTC),
    size=906,
)

FILE_BODY = {
    "path": FILE.path,
    "name": FILE.name,
    "last_modified": "2017-08-09T14:21:13Z",
    "size": FILE.size,
    "type": "file",
}

DIRECTORY = Directory(
    path="/path/to/test-directory",
    name="test-directory",
    last_modified=datetime(2018, 1, 15, 15, 18, 45, tzinfo=UTC),
    size=6144,
    truncated=False,
    content=[FILE],
)

DIRECTORY_BODY = {
    "path": DIRECTORY.path,
    "name": DIRECTORY.name,
    "last_modified": "2018-01-15T15:18:45Z",
    "size": DIRECTORY.size,
    "type": "directory",
    "truncated": DIRECTORY.truncated,
    "content": [FILE_BODY],
}

LIST_RESPONSE = ListResponse(
    project_id=uuid4(), path="/path/to/test-directory/", content=[DIRECTORY]
)

LIST_RESPONSE_BODY = {
    "project_id": str(LIST_RESPONSE.project_id),
    "path": LIST_RESPONSE.path,
    "content": [DIRECTORY_BODY],
}


def test_file_node_schema_file():
    assert FileNodeSchema().load(FILE_BODY) == FILE


def test_file_node_schema_directory():
    assert FileNodeSchema().load(DIRECTORY_BODY) == DIRECTORY


def test_file_node_schema_invalid():
    with pytest.raises(ValidationError):
        FileNodeSchema().load({})


def test_list_response_schema():
    assert ListResponseSchema().load(LIST_RESPONSE_BODY) == LIST_RESPONSE


def test_list_response_schema_invalid():
    with pytest.raises(ValidationError):
        ListResponseSchema().load({})


def test_workspace_client_get(mocker):
    mocker.patch.object(WorkspaceClient, "_get", return_value=LIST_RESPONSE)
    schema_mock = mocker.patch(
        "sherlockml.clients.workspace.ListResponseSchema"
    )

    client = WorkspaceClient(PROFILE)
    assert client.list(
        PROJECT_ID, prefix="/path/to/test-directory/", depth=1
    ) == [DIRECTORY]

    schema_mock.assert_called_once_with()
    WorkspaceClient._get.assert_called_once_with(
        "/project/{}/file".format(PROJECT_ID),
        schema_mock.return_value,
        params={"prefix": "/path/to/test-directory/", "depth": 1},
    )
