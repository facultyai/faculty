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


from collections import namedtuple

import pytest
from marshmallow import fields, post_load, ValidationError

from faculty.clients.base import (
    BadGateway,
    BadRequest,
    BaseClient,
    BaseSchema,
    Conflict,
    Forbidden,
    GatewayTimeout,
    HttpError,
    InternalServerError,
    MethodNotAllowed,
    NotFound,
    ServiceUnavailable,
    Unauthorized,
    _ErrorSchema,
    ServerSentEventMessage,
)

MOCK_SERVICE_URL = "https://test-service.example.com/"
MOCK_ENDPOINT = "/endpoint"
MOCK_ENDPOINT_URL = "https://test-service.example.com/endpoint"

AUTHORIZATION_HEADER_VALUE = "Bearer mock-token"
AUTHORIZATION_HEADER = {"Authorization": AUTHORIZATION_HEADER_VALUE}

BAD_RESPONSE_STATUSES = [
    (400, BadRequest),
    (401, Unauthorized),
    (403, Forbidden),
    (404, NotFound),
    (405, MethodNotAllowed),
    (409, Conflict),
    (500, InternalServerError),
    (502, BadGateway),
    (503, ServiceUnavailable),
    (504, GatewayTimeout),
    (418, HttpError),
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE", "PATCH"]

STREAM_RESPONSE = [
    "id: 0",
    "event: mock event",
    "data: {}",
    " ",
    "id: 1",
    "event: mock event",
    "data: {}",
    " ",
]
SSE_MOCK_MESSAGES = [
    ServerSentEventMessage(
        id=0,
        event="mock event",
        data="{}",
    ),
    ServerSentEventMessage(
        id=1,
        event="mock event",
        data="{}",
    ),
]


def test_error_schema():
    data = _ErrorSchema().load(
        {"error": "error message", "errorCode": "error code"}
    )
    assert data == {"error": "error message", "error_code": "error code"}


@pytest.fixture
def session(mocker):
    session = mocker.Mock()
    yield session


@pytest.fixture
def patch_auth(mocker, session):
    def _add_auth_headers(request):
        request.headers["Authorization"] = AUTHORIZATION_HEADER_VALUE
        return request

    mock_auth = mocker.patch(
        "faculty.clients.base.FacultyAuth", return_value=_add_auth_headers
    )

    yield

    mock_auth.assert_called_once_with(session)


DummyObject = namedtuple("DummyObject", ["foo"])


class DummySchema(BaseSchema):
    foo = fields.String(required=True)

    @post_load
    def make_test_object(self, data, **kwargs):
        return DummyObject(**data)


def test_base_schema_ignores_unknown_fields():
    """Check that fields in the data but not in the schema do not error.

    marshmallow version 3 changed the default behaviour of schemas to
    raise a ValidationError if there are any fields in the data being
    deserialised which are not configured in the schema. Our BaseSchema
    is configured to disable this behaviour.
    """
    assert BaseSchema().load({"unknown": "field"}) == {}


def test_get(requests_mock, session, patch_auth):
    requests_mock.get(
        MOCK_ENDPOINT_URL,
        request_headers=AUTHORIZATION_HEADER,
        json={"foo": "bar"},
    )

    client = BaseClient(MOCK_SERVICE_URL, session)

    assert client._get(MOCK_ENDPOINT, DummySchema()) == DummyObject(foo="bar")


def test_post(requests_mock, session, patch_auth):
    mock = requests_mock.post(
        MOCK_ENDPOINT_URL,
        request_headers=AUTHORIZATION_HEADER,
        json={"foo": "bar"},
    )

    client = BaseClient(MOCK_SERVICE_URL, session)
    response = client._post(
        MOCK_ENDPOINT, DummySchema(), json={"test": "payload"}
    )

    assert response == DummyObject(foo="bar")
    assert mock.last_request.json() == {"test": "payload"}


def test_put(requests_mock, session, patch_auth):
    mock = requests_mock.put(
        MOCK_ENDPOINT_URL,
        request_headers=AUTHORIZATION_HEADER,
        json={"foo": "bar"},
    )

    client = BaseClient(MOCK_SERVICE_URL, session)
    response = client._put(
        MOCK_ENDPOINT, DummySchema(), json={"test": "payload"}
    )

    assert response == DummyObject(foo="bar")
    assert mock.last_request.json() == {"test": "payload"}


def test_patch(requests_mock, session, patch_auth):

    mock = requests_mock.patch(
        MOCK_ENDPOINT_URL,
        request_headers=AUTHORIZATION_HEADER,
        json={"foo": "bar"},
    )

    client = BaseClient(MOCK_SERVICE_URL, session)
    response = client._patch(
        MOCK_ENDPOINT, DummySchema(), json={"test": "payload"}
    )

    assert response == DummyObject(foo="bar")
    assert mock.last_request.json() == {"test": "payload"}


def test_delete(requests_mock, session, patch_auth):

    requests_mock.delete(
        MOCK_ENDPOINT_URL,
        request_headers=AUTHORIZATION_HEADER,
        json={"foo": "bar"},
    )

    client = BaseClient(MOCK_SERVICE_URL, session)
    response = client._delete(MOCK_ENDPOINT, DummySchema())

    assert response == DummyObject(foo="bar")


def test_stream_server_sent_events(mocker):
    response_content = mocker.MagicMock()
    response_content.iter_lines.return_value = STREAM_RESPONSE
    response_content.__enter__.return_value = STREAM_RESPONSE
    mocker.patch.object(
        BaseClient,
        "_get_raw",
        return_value=response_content,
    )

    client = BaseClient(mocker.Mock(), mocker.Mock())
    messages = [
        message for message in client._stream_server_sent_events("endpoint")
    ]
    assert messages == SSE_MOCK_MESSAGES


@pytest.mark.parametrize(
    "check_status", [True, False], ids=["Check", "NoCheck"]
)
@pytest.mark.parametrize("http_method", HTTP_METHODS)
@pytest.mark.parametrize("status_code, exception", BAD_RESPONSE_STATUSES)
def test_bad_responses(
    requests_mock,
    session,
    patch_auth,
    status_code,
    exception,
    http_method,
    check_status,
):

    mock_method = getattr(requests_mock, http_method.lower())
    mock_method(
        MOCK_ENDPOINT_URL,
        request_headers=AUTHORIZATION_HEADER,
        status_code=status_code,
        json={"foo": "bar"},
    )

    client = BaseClient(MOCK_SERVICE_URL, session)
    method = getattr(client, "_{}".format(http_method.lower()))
    if check_status:
        with pytest.raises(exception):
            method(MOCK_ENDPOINT, DummySchema(), check_status=check_status)
    else:
        method(MOCK_ENDPOINT, DummySchema(), check_status=check_status)


@pytest.mark.parametrize(
    "check_status", [True, False], ids=["Check", "NoCheck"]
)
@pytest.mark.parametrize("http_method", HTTP_METHODS)
@pytest.mark.parametrize("status_code, exception", BAD_RESPONSE_STATUSES)
def test_raw_bad_responses(
    requests_mock,
    session,
    patch_auth,
    status_code,
    exception,
    http_method,
    check_status,
):

    mock_method = getattr(requests_mock, http_method.lower())
    mock_method(
        MOCK_ENDPOINT_URL,
        request_headers=AUTHORIZATION_HEADER,
        status_code=status_code,
    )

    client = BaseClient(MOCK_SERVICE_URL, session)
    method = getattr(client, "_{}_raw".format(http_method.lower()))
    if check_status:
        with pytest.raises(exception):
            method(MOCK_ENDPOINT, check_status=check_status)
    else:
        method(MOCK_ENDPOINT, check_status=check_status)


@pytest.mark.parametrize("http_method", HTTP_METHODS)
def test_invalid_json(requests_mock, session, patch_auth, http_method):

    mock_method = getattr(requests_mock, http_method.lower())
    mock_method(
        MOCK_ENDPOINT_URL,
        request_headers=AUTHORIZATION_HEADER,
        text="invalid-json",
    )

    client = BaseClient(MOCK_SERVICE_URL, session)
    method = getattr(client, "_{}".format(http_method.lower()))
    with pytest.raises(ValueError):
        method(MOCK_ENDPOINT, DummySchema())


@pytest.mark.parametrize("http_method", HTTP_METHODS)
def test_malformatted_json(requests_mock, session, patch_auth, http_method):

    mock_method = getattr(requests_mock, http_method.lower())
    mock_method(
        MOCK_ENDPOINT_URL,
        request_headers=AUTHORIZATION_HEADER,
        json={"bad": "json"},
    )

    client = BaseClient(MOCK_SERVICE_URL, session)
    method = getattr(client, "_{}".format(http_method.lower()))
    with pytest.raises(ValidationError):
        method(MOCK_ENDPOINT, DummySchema())
