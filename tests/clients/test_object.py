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


import uuid
from datetime import datetime

import pytest
from marshmallow import ValidationError
from pytz import UTC

from faculty.clients.object import (
    ListObjectsResponse,
    ListObjectsResponseSchema,
    Object,
    ObjectClient,
    ObjectSchema,
)


PROJECT_ID = uuid.uuid4()

LAST_MODIFIED_AT = datetime(2018, 3, 10, 11, 32, 30, 172000, tzinfo=UTC)
LAST_MODIFIED_AT_STRING = "2018-03-10T11:32:30.172Z"

OBJECT = Object(
    path="/test/path",
    size=123,
    etag="an etag",
    last_modified_at=LAST_MODIFIED_AT,
)
OBJECT_BODY = {
    "path": OBJECT.path,
    "size": OBJECT.size,
    "etag": OBJECT.etag,
    "lastModifiedAt": LAST_MODIFIED_AT_STRING,
}

LIST_OBJECTS_RESPONSE = ListObjectsResponse(
    objects=[OBJECT], next_page_token="page token"
)
LIST_OBJECTS_RESPONSE_BODY = {
    "objects": [OBJECT_BODY],
    "nextPageToken": LIST_OBJECTS_RESPONSE.next_page_token,
}

LIST_OBJECTS_RESPONSE_WITHOUT_PAGE_TOKEN = ListObjectsResponse(
    objects=[OBJECT], next_page_token=None
)
LIST_OBJECTS_RESPONSE_WITHOUT_PAGE_TOKEN_BODY = {"objects": [OBJECT_BODY]}


def test_object_schema():
    data = ObjectSchema().load(OBJECT_BODY)
    assert data == OBJECT


def test_object_schema_invalid():
    with pytest.raises(ValidationError):
        ObjectSchema().load({})


@pytest.mark.parametrize(
    "body, expected",
    [
        (LIST_OBJECTS_RESPONSE_BODY, LIST_OBJECTS_RESPONSE),
        (
            LIST_OBJECTS_RESPONSE_WITHOUT_PAGE_TOKEN_BODY,
            LIST_OBJECTS_RESPONSE_WITHOUT_PAGE_TOKEN,
        ),
    ],
)
def test_list_objects_response_schema(body, expected):
    data = ListObjectsResponseSchema().load(body)
    assert data == expected


def test_list_objects_response_schema_invalid():
    with pytest.raises(ValidationError):
        ListObjectsResponseSchema().load({})


def test_object_client_list(mocker):
    mocker.patch.object(
        ObjectClient, "_get", return_value=LIST_OBJECTS_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.object.ListObjectsResponseSchema"
    )

    client = ObjectClient(mocker.Mock())
    assert (
        client.list(PROJECT_ID, "/path", page_token="token")
        == LIST_OBJECTS_RESPONSE
    )

    schema_mock.assert_called_once_with()
    ObjectClient._get.assert_called_once_with(
        "/project/{}/object-list/path".format(PROJECT_ID),
        schema_mock.return_value,
        params={"pageToken": "token"},
    )


def test_object_client_list_defaults(mocker):
    mocker.patch.object(
        ObjectClient, "_get", return_value=LIST_OBJECTS_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.object.ListObjectsResponseSchema"
    )

    client = ObjectClient(mocker.Mock())
    assert client.list(PROJECT_ID) == LIST_OBJECTS_RESPONSE

    schema_mock.assert_called_once_with()
    ObjectClient._get.assert_called_once_with(
        "/project/{}/object-list/".format(PROJECT_ID),
        schema_mock.return_value,
        params={},
    )
