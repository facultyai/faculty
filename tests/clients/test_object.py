# Copyright 2018-2020 Faculty Science Limited
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

from faculty.clients.base import BadRequest, Conflict, NotFound
from faculty.clients.object import (
    CloudStorageProvider,
    CompletedUploadPart,
    ListObjectsResponse,
    Object,
    ObjectClient,
    PathAlreadyExists,
    PathNotFound,
    PresignUploadResponse,
    SourceIsADirectory,
    TargetIsADirectory,
    _CompleteMultipartUploadSchema,
    _CompletedUploadPartSchema,
    _ListObjectsResponseSchema,
    _ObjectSchema,
    _PresignUploadResponseSchema,
    _SimplePresignResponse,
    _SimplePresignResponseSchema,
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

SIMPLE_PRESIGN_RESPONSE = _SimplePresignResponse(url="http://example.com")
SIMPLE_PRESIGN_RESPONSE_BODY = {"url": SIMPLE_PRESIGN_RESPONSE.url}

PRESIGN_UPLOAD_RESPONSE_S3 = PresignUploadResponse(
    provider=CloudStorageProvider.S3, upload_id="upload-id", url=None
)
PRESIGN_UPLOAD_RESPONSE_S3_BODY = {
    "provider": "S3",
    "uploadId": PRESIGN_UPLOAD_RESPONSE_S3.upload_id,
}

PRESIGN_UPLOAD_RESPONSE_GCS = PresignUploadResponse(
    provider=CloudStorageProvider.GCS,
    upload_id=None,
    url="https://example.com",
)
PRESIGN_UPLOAD_RESPONSE_GCS_BODY = {
    "provider": "GCS",
    "url": PRESIGN_UPLOAD_RESPONSE_GCS.url,
}

COMPLETED_UPLOAD_PART = CompletedUploadPart(123, "etag-123")
COMPLETED_UPLOAD_PART_BODY = {
    "partNumber": COMPLETED_UPLOAD_PART.part_number,
    "etag": COMPLETED_UPLOAD_PART.etag,
}

COMPLETED_MULTIPART_UPLOAD = {
    "path": "/path",
    "upload_id": "upload-id",
    "parts": [COMPLETED_UPLOAD_PART],
}
COMPLETED_MULTIPART_UPLOAD_BODY = {
    "path": COMPLETED_MULTIPART_UPLOAD["path"],
    "uploadId": COMPLETED_MULTIPART_UPLOAD["upload_id"],
    "parts": [COMPLETED_UPLOAD_PART_BODY],
}


def test_object_schema():
    data = _ObjectSchema().load(OBJECT_BODY)
    assert data == OBJECT


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
    data = _ListObjectsResponseSchema().load(body)
    assert data == expected


def test_simple_presign_response_schema():
    data = _SimplePresignResponseSchema().load(SIMPLE_PRESIGN_RESPONSE_BODY)
    assert data == SIMPLE_PRESIGN_RESPONSE


@pytest.mark.parametrize(
    "body, expected",
    [
        (PRESIGN_UPLOAD_RESPONSE_S3_BODY, PRESIGN_UPLOAD_RESPONSE_S3),
        (PRESIGN_UPLOAD_RESPONSE_GCS_BODY, PRESIGN_UPLOAD_RESPONSE_GCS),
    ],
    ids=["S3", "GCS"],
)
def test_presign_upload_response_schema(body, expected):
    data = _PresignUploadResponseSchema().load(body)
    assert data == expected


@pytest.mark.parametrize(
    "schema",
    [
        _ObjectSchema,
        _ListObjectsResponseSchema,
        _SimplePresignResponseSchema,
        _PresignUploadResponseSchema,
    ],
)
def test_schema_invalid_body(schema):
    with pytest.raises(ValidationError):
        schema().load({})


def test_completed_upload_part_schema_dump():
    data = _CompletedUploadPartSchema().dump(COMPLETED_UPLOAD_PART)
    assert data == COMPLETED_UPLOAD_PART_BODY


def test_complete_multipart_upload_schema_dump():
    data = _CompleteMultipartUploadSchema().dump(COMPLETED_MULTIPART_UPLOAD)
    assert data == COMPLETED_MULTIPART_UPLOAD_BODY


@pytest.mark.parametrize("path", ["test/path", "/test/path", "//test/path"])
def test_object_client_get(mocker, path):
    mocker.patch.object(ObjectClient, "_get", return_value=OBJECT)
    schema_mock = mocker.patch("faculty.clients.object._ObjectSchema")

    client = ObjectClient(mocker.Mock())
    assert client.get(PROJECT_ID, path) == OBJECT

    schema_mock.assert_called_once_with()
    ObjectClient._get.assert_called_once_with(
        "/project/{}/object/test/path".format(PROJECT_ID),
        schema_mock.return_value,
    )


def test_object_client_get_url_encoding(mocker):
    path = "/test/[1].txt"
    mocker.patch.object(ObjectClient, "_get", return_value=OBJECT)
    schema_mock = mocker.patch("faculty.clients.object._ObjectSchema")

    client = ObjectClient(mocker.Mock())
    assert client.get(PROJECT_ID, path) == OBJECT

    schema_mock.assert_called_once_with()
    ObjectClient._get.assert_called_once_with(
        "/project/{}/object/test/%5B1%5D.txt".format(PROJECT_ID),
        schema_mock.return_value,
    )


@pytest.mark.parametrize("path", ["test/path", "/test/path", "//test/path"])
def test_object_client_list(mocker, path):
    mocker.patch.object(
        ObjectClient, "_get", return_value=LIST_OBJECTS_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.object._ListObjectsResponseSchema"
    )

    client = ObjectClient(mocker.Mock())

    response = client.list(PROJECT_ID, path, page_token="token")
    assert response == LIST_OBJECTS_RESPONSE

    schema_mock.assert_called_once_with()
    ObjectClient._get.assert_called_once_with(
        "/project/{}/object-list/test/path".format(PROJECT_ID),
        schema_mock.return_value,
        params={"pageToken": "token"},
    )


def test_object_client_list_defaults(mocker):
    mocker.patch.object(
        ObjectClient, "_get", return_value=LIST_OBJECTS_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.object._ListObjectsResponseSchema"
    )

    client = ObjectClient(mocker.Mock())
    assert client.list(PROJECT_ID) == LIST_OBJECTS_RESPONSE

    schema_mock.assert_called_once_with()
    ObjectClient._get.assert_called_once_with(
        "/project/{}/object-list/".format(PROJECT_ID),
        schema_mock.return_value,
        params={},
    )


def test_object_client_list_url_encoding(mocker):
    path = "/test [1]/"
    mocker.patch.object(
        ObjectClient, "_get", return_value=LIST_OBJECTS_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.object._ListObjectsResponseSchema"
    )

    client = ObjectClient(mocker.Mock())

    response = client.list(PROJECT_ID, path, page_token="token")
    assert response == LIST_OBJECTS_RESPONSE

    schema_mock.assert_called_once_with()
    ObjectClient._get.assert_called_once_with(
        "/project/{}/object-list/test%20%5B1%5D/".format(PROJECT_ID),
        schema_mock.return_value,
        params={"pageToken": "token"},
    )


def test_object_client_create_directory_default(mocker):
    mocker.patch.object(ObjectClient, "_put_raw")

    client = ObjectClient(mocker.Mock())
    client.create_directory(PROJECT_ID, "test-path")

    ObjectClient._put_raw.assert_called_once_with(
        "/project/{}/directory/{}".format(PROJECT_ID, "test-path"),
        params={"parents": 0},
    )


@pytest.mark.parametrize("parents, expected_parent", [(True, 1), (False, 0)])
def test_object_client_create_directory(mocker, parents, expected_parent):
    mocker.patch.object(ObjectClient, "_put_raw")

    client = ObjectClient(mocker.Mock())
    client.create_directory(PROJECT_ID, "test-path", parents=parents)

    ObjectClient._put_raw.assert_called_once_with(
        "/project/{}/directory/{}".format(PROJECT_ID, "test-path"),
        params={"parents": expected_parent},
    )


def test_object_client_create_directory_url_encoding(mocker):
    mocker.patch.object(ObjectClient, "_put_raw")

    client = ObjectClient(mocker.Mock())
    client.create_directory(PROJECT_ID, "[1]")

    ObjectClient._put_raw.assert_called_once_with(
        "/project/{}/directory/%5B1%5D".format(PROJECT_ID),
        params={"parents": 0},
    )


def test_object_client_create_directory_already_exists(mocker):
    error_code = "object_already_exists"
    exception = Conflict(mocker.Mock(), mocker.Mock(), error_code)
    mocker.patch.object(ObjectClient, "_put_raw", side_effect=exception)

    client = ObjectClient(mocker.Mock())
    with pytest.raises(PathAlreadyExists, match="'test-path' already exists"):
        client.create_directory(PROJECT_ID, "test-path")

    ObjectClient._put_raw.assert_called_once_with(
        "/project/{}/directory/{}".format(PROJECT_ID, "test-path"),
        params={"parents": 0},
    )


def test_object_client_copy_default(mocker):
    mocker.patch.object(ObjectClient, "_put_raw")

    client = ObjectClient(mocker.Mock())
    client.copy(PROJECT_ID, "source", "destination")

    ObjectClient._put_raw.assert_called_once_with(
        "/project/{}/object/{}".format(PROJECT_ID, "destination"),
        params={"sourcePath": "source", "recursive": 0},
    )


def test_object_client_copy_url_encoding(mocker):
    mocker.patch.object(ObjectClient, "_put_raw")

    client = ObjectClient(mocker.Mock())
    client.copy(PROJECT_ID, "source", "/[1]/")

    ObjectClient._put_raw.assert_called_once_with(
        "/project/{}/object/%5B1%5D/".format(PROJECT_ID),
        params={"sourcePath": "source", "recursive": 0},
    )


@pytest.mark.parametrize(
    "recursive, expected_recursive", [(True, 1), (False, 0)]
)
def test_object_client_copy(mocker, recursive, expected_recursive):
    mocker.patch.object(ObjectClient, "_put_raw")

    client = ObjectClient(mocker.Mock())
    client.copy(PROJECT_ID, "source", "destination", recursive=recursive)

    ObjectClient._put_raw.assert_called_once_with(
        "/project/{}/object/{}".format(PROJECT_ID, "destination"),
        params={"sourcePath": "source", "recursive": expected_recursive},
    )


def test_object_client_copy_source_not_found(mocker):
    error_code = "source_path_not_found"
    exception = NotFound(mocker.Mock(), mocker.Mock(), error_code)
    mocker.patch.object(ObjectClient, "_put_raw", side_effect=exception)

    client = ObjectClient(mocker.Mock())
    with pytest.raises(PathNotFound, match="'source' cannot be found"):
        client.copy(PROJECT_ID, "source", "destination")


def test_object_client_copy_source_is_a_directory(mocker):
    error_code = "source_is_a_directory"
    exception = BadRequest(mocker.Mock(), mocker.Mock(), error_code)
    mocker.patch.object(ObjectClient, "_put_raw", side_effect=exception)

    client = ObjectClient(mocker.Mock())
    with pytest.raises(SourceIsADirectory, match="'source' is a directory"):
        client.copy(PROJECT_ID, "source", "destination")


def test_object_client_move_url_encoding(mocker):
    mocker.patch.object(ObjectClient, "_put_raw")

    client = ObjectClient(mocker.Mock())
    client.move(PROJECT_ID, "source", "/[1]/")

    ObjectClient._put_raw.assert_called_once_with(
        "/project/{}/object-move/%5B1%5D/".format(PROJECT_ID),
        params={"sourcePath": "source"},
    )


def test_object_client_move_source_not_found(mocker):
    error_code = "source_path_not_found"
    exception = NotFound(mocker.Mock(), mocker.Mock(), error_code)
    mocker.patch.object(ObjectClient, "_put_raw", side_effect=exception)

    client = ObjectClient(mocker.Mock())
    with pytest.raises(PathNotFound, match="'source' cannot be found"):
        client.move(PROJECT_ID, "source", "destination")


def test_object_client_delete_default(mocker):
    path = "test-path"
    mocker.patch.object(ObjectClient, "_delete_raw")

    client = ObjectClient(mocker.Mock())
    client.delete(PROJECT_ID, path)

    ObjectClient._delete_raw.assert_called_once_with(
        "/project/{}/object/{}".format(PROJECT_ID, path),
        params={"recursive": 0},
    )


def test_object_client_delete_url_encoding(mocker):
    path = "[1]"
    mocker.patch.object(ObjectClient, "_delete_raw")

    client = ObjectClient(mocker.Mock())
    client.delete(PROJECT_ID, path)

    ObjectClient._delete_raw.assert_called_once_with(
        "/project/{}/object/%5B1%5D".format(PROJECT_ID),
        params={"recursive": 0},
    )


@pytest.mark.parametrize(
    "recursive, expected_recursive", [(True, 1), (False, 0)]
)
def test_object_client_delete(mocker, recursive, expected_recursive):
    path = "test-path"
    mocker.patch.object(ObjectClient, "_delete_raw")

    client = ObjectClient(mocker.Mock())
    client.delete(PROJECT_ID, path, recursive=recursive)

    ObjectClient._delete_raw.assert_called_once_with(
        "/project/{}/object/{}".format(PROJECT_ID, path),
        params={"recursive": expected_recursive},
    )


def test_object_client_delete_path_not_found(mocker):
    path = "test-path"
    error_code = "object_not_found"
    exception = NotFound(mocker.Mock(), mocker.Mock(), error_code)
    mocker.patch.object(ObjectClient, "_delete_raw", side_effect=exception)

    client = ObjectClient(mocker.Mock())
    with pytest.raises(PathNotFound, match="'test-path' cannot be found"):
        client.delete(PROJECT_ID, path)


def test_object_client_delete_target_is_a_directory(mocker):
    path = "test-path"
    error_code = "target_is_a_directory"
    exception = BadRequest(mocker.Mock(), mocker.Mock(), error_code)
    mocker.patch.object(ObjectClient, "_delete_raw", side_effect=exception)

    client = ObjectClient(mocker.Mock())
    with pytest.raises(TargetIsADirectory, match="'test-path' is a directory"):
        client.delete(PROJECT_ID, path)


def test_object_client_presign_download(mocker):
    mocker.patch.object(
        ObjectClient, "_post", return_value=SIMPLE_PRESIGN_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.object._SimplePresignResponseSchema"
    )

    client = ObjectClient(mocker.Mock())
    returned = client.presign_download(
        PROJECT_ID,
        "/path",
        response_content_disposition="attachement; filename=other",
    )

    assert returned == SIMPLE_PRESIGN_RESPONSE.url

    schema_mock.assert_called_once_with()
    ObjectClient._post.assert_called_once_with(
        "/project/{}/presign/download".format(PROJECT_ID),
        schema_mock.return_value,
        json={
            "path": "/path",
            "responseContentDisposition": "attachement; filename=other",
        },
    )


def test_object_client_presign_download_defaults(mocker):
    mocker.patch.object(
        ObjectClient, "_post", return_value=SIMPLE_PRESIGN_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.object._SimplePresignResponseSchema"
    )

    client = ObjectClient(mocker.Mock())
    returned = client.presign_download(PROJECT_ID, "/path")

    assert returned == SIMPLE_PRESIGN_RESPONSE.url

    schema_mock.assert_called_once_with()
    ObjectClient._post.assert_called_once_with(
        "/project/{}/presign/download".format(PROJECT_ID),
        schema_mock.return_value,
        json={"path": "/path"},
    )


def test_object_client_presign_upload(mocker):
    mocker.patch.object(
        ObjectClient, "_post", return_value=PRESIGN_UPLOAD_RESPONSE_S3
    )
    schema_mock = mocker.patch(
        "faculty.clients.object._PresignUploadResponseSchema"
    )

    client = ObjectClient(mocker.Mock())
    returned = client.presign_upload(PROJECT_ID, "/path")

    assert returned == PRESIGN_UPLOAD_RESPONSE_S3

    schema_mock.assert_called_once_with()
    ObjectClient._post.assert_called_once_with(
        "/project/{}/presign/upload".format(PROJECT_ID),
        schema_mock.return_value,
        json={"path": "/path"},
    )


def test_object_client_presign_upload_part(mocker):
    mocker.patch.object(
        ObjectClient, "_put", return_value=SIMPLE_PRESIGN_RESPONSE
    )
    schema_mock = mocker.patch(
        "faculty.clients.object._SimplePresignResponseSchema"
    )

    client = ObjectClient(mocker.Mock())
    returned = client.presign_upload_part(
        PROJECT_ID, "/path", "upload-id", part_number=2
    )

    assert returned == SIMPLE_PRESIGN_RESPONSE.url

    schema_mock.assert_called_once_with()
    ObjectClient._put.assert_called_once_with(
        "/project/{}/presign/upload/part".format(PROJECT_ID),
        schema_mock.return_value,
        json={"path": "/path", "uploadId": "upload-id", "partNumber": 2},
    )


def test_object_client_complete_multipart_upload(mocker):
    mocker.patch.object(ObjectClient, "_put_raw")
    payload_schema_mock = mocker.patch(
        "faculty.clients.object._CompleteMultipartUploadSchema"
    )

    client = ObjectClient(mocker.Mock())
    client.complete_multipart_upload(
        PROJECT_ID, "/path", "upload-id", [COMPLETED_UPLOAD_PART]
    )

    payload_schema_mock.assert_called_once_with()
    dump_mock = payload_schema_mock.return_value.dump
    dump_mock.assert_called_once_with(
        {
            "path": "/path",
            "upload_id": "upload-id",
            "parts": [COMPLETED_UPLOAD_PART],
        }
    )
    ObjectClient._put_raw.assert_called_once_with(
        "/project/{}/presign/upload/complete".format(PROJECT_ID),
        json=dump_mock.return_value,
    )
