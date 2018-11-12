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


from collections import namedtuple

import pytest
from marshmallow import Schema, fields, post_load

from sherlockml.clients.base import (
    BaseClient,
    InvalidResponse,
    HTTPError,
    BadRequest,
    Unauthorized,
    Forbidden,
    NotFound,
    MethodNotAllowed,
    Conflict,
    InternalServerError,
    BadGateway,
    ServiceUnavailable,
    GatewayTimeout,
)
from tests.clients.fixtures import PROFILE

AUTHORIZATION_HEADER_VALUE = "Bearer mock-token"
AUTHORIZATION_HEADER = {"Authorization": AUTHORIZATION_HEADER_VALUE}

HUDSON_URL = "https://hudson.test.domain.com"

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
    (418, HTTPError),
]

HTTP_METHODS = ["GET", "POST", "PUT", "DELETE"]


@pytest.fixture
def patch_sherlockmlauth(mocker):
    def _add_auth_headers(request):
        request.headers["Authorization"] = AUTHORIZATION_HEADER_VALUE
        return request

    mock_auth = mocker.patch(
        "sherlockml.clients.base.SherlockMLAuth",
        return_value=_add_auth_headers,
    )

    yield

    mock_auth.assert_called_once_with(
        HUDSON_URL, PROFILE.client_id, PROFILE.client_secret
    )


DummyObject = namedtuple("DummyObject", ["foo"])


class DummySchema(Schema):
    foo = fields.String(required=True)

    @post_load
    def make_test_object(self, data):
        return DummyObject(**data)


class DummyClient(BaseClient):
    SERVICE_NAME = "test-service"


SERVICE_URL = "https://test-service.{}".format(PROFILE.domain)


def test_get(requests_mock, patch_sherlockmlauth):

    requests_mock.get(
        "{}/test".format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={"foo": "bar"},
    )

    client = DummyClient(PROFILE)
    assert client._get("/test", DummySchema()) == DummyObject(foo="bar")


def test_post(requests_mock, patch_sherlockmlauth):

    mock = requests_mock.post(
        "{}/test".format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={"foo": "bar"},
    )

    client = DummyClient(PROFILE)
    response = client._post("/test", DummySchema(), json={"test": "payload"})

    assert response == DummyObject(foo="bar")
    assert mock.last_request.json() == {"test": "payload"}


def test_put(requests_mock, patch_sherlockmlauth):

    mock = requests_mock.put(
        "{}/test".format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={"foo": "bar"},
    )

    client = DummyClient(PROFILE)
    response = client._put("/test", DummySchema(), json={"test": "payload"})

    assert response == DummyObject(foo="bar")
    assert mock.last_request.json() == {"test": "payload"}


def test_delete(requests_mock, patch_sherlockmlauth):

    requests_mock.delete(
        "{}/test".format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={"foo": "bar"},
    )

    client = DummyClient(PROFILE)
    response = client._delete("/test", DummySchema())

    assert response == DummyObject(foo="bar")


@pytest.mark.parametrize(
    "check_status", [True, False], ids=["Check", "NoCheck"]
)
@pytest.mark.parametrize("http_method", HTTP_METHODS)
@pytest.mark.parametrize("status_code, exception", BAD_RESPONSE_STATUSES)
def test_bad_responses(
    requests_mock,
    patch_sherlockmlauth,
    status_code,
    exception,
    http_method,
    check_status,
):

    mock_method = getattr(requests_mock, http_method.lower())
    mock_method(
        "{}/test".format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        status_code=status_code,
        json={"foo": "bar"},
    )

    client = DummyClient(PROFILE)
    method = getattr(client, "_{}".format(http_method.lower()))
    if check_status:
        with pytest.raises(exception):
            method("/test", DummySchema(), check_status=check_status)
    else:
        method("/test", DummySchema(), check_status=check_status)


@pytest.mark.parametrize(
    "check_status", [True, False], ids=["Check", "NoCheck"]
)
@pytest.mark.parametrize("http_method", HTTP_METHODS)
@pytest.mark.parametrize("status_code, exception", BAD_RESPONSE_STATUSES)
def test_raw_bad_responses(
    requests_mock,
    patch_sherlockmlauth,
    status_code,
    exception,
    http_method,
    check_status,
):

    mock_method = getattr(requests_mock, http_method.lower())
    mock_method(
        "{}/test".format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        status_code=status_code,
    )

    client = DummyClient(PROFILE)
    method = getattr(client, "_{}_raw".format(http_method.lower()))
    if check_status:
        with pytest.raises(exception):
            method("/test", check_status=check_status)
    else:
        method("/test", check_status=check_status)


@pytest.mark.parametrize("http_method", HTTP_METHODS)
def test_invalid_json(requests_mock, patch_sherlockmlauth, http_method):

    mock_method = getattr(requests_mock, http_method.lower())
    mock_method(
        "{}/test".format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        text="invalid-json",
    )

    client = DummyClient(PROFILE)
    method = getattr(client, "_{}".format(http_method.lower()))
    with pytest.raises(InvalidResponse, match="not valid JSON"):
        method("/test", DummySchema())


@pytest.mark.parametrize("http_method", HTTP_METHODS)
def test_malformatted_json(requests_mock, patch_sherlockmlauth, http_method):

    mock_method = getattr(requests_mock, http_method.lower())
    mock_method(
        "{}/test".format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={"bad": "json"},
    )

    client = DummyClient(PROFILE)
    method = getattr(client, "_{}".format(http_method.lower()))
    with pytest.raises(InvalidResponse, match="not match expected format"):
        method("/test", DummySchema())
