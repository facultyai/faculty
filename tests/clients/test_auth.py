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


from datetime import datetime, timedelta

import pytest
import pytz

from sherlockml.clients.auth import (
    AccessToken,
    AccessTokenClient,
    SherlockMLAuth,
)


MOCK_HUDSON_URL = "https://hudson.example.com"
MOCK_CLIENT_ID = "client-id"
MOCK_CLIENT_SECRET = "client-secret"
MOCK_ACCESS_TOKEN_MATERIAL = "access-token"
NOW = datetime.now(tz=pytz.utc)


@pytest.fixture
def mock_datetime_now(mocker):
    datetime_mock = mocker.patch("sherlockml.clients.auth.datetime")
    datetime_mock.now.return_value = NOW
    return datetime_mock


def test_access_token_client(requests_mock, mock_datetime_now):

    requests_mock.post(
        "{}/access_token".format(MOCK_HUDSON_URL),
        json={"access_token": MOCK_ACCESS_TOKEN_MATERIAL, "expires_in": 600},
    )

    client = AccessTokenClient(MOCK_HUDSON_URL)
    access_token = client.get_access_token(MOCK_CLIENT_ID, MOCK_CLIENT_SECRET)

    assert requests_mock.last_request.json() == {
        "client_id": MOCK_CLIENT_ID,
        "client_secret": MOCK_CLIENT_SECRET,
        "grant_type": "client_credentials",
    }
    assert access_token.token == MOCK_ACCESS_TOKEN_MATERIAL
    assert access_token.expires_at == NOW + timedelta(minutes=10)


@pytest.mark.parametrize(
    "cached_token",
    [None, AccessToken("old-token", NOW - timedelta(minutes=10))],
)
def test_sherlockml_auth(mocker, cached_token):

    mock_client = mocker.Mock()
    mock_client.get_access_token.return_value = AccessToken(
        MOCK_ACCESS_TOKEN_MATERIAL, NOW + timedelta(minutes=10)
    )
    client_patch = mocker.patch(
        "sherlockml.clients.auth.AccessTokenClient", return_value=mock_client
    )

    auth = SherlockMLAuth(MOCK_HUDSON_URL, MOCK_CLIENT_ID, MOCK_CLIENT_SECRET)

    unauthenticated_request = mocker.Mock(headers={})
    request = auth(unauthenticated_request)

    assert request.headers["Authorization"] == "Bearer access-token"
    client_patch.assert_called_once_with(MOCK_HUDSON_URL)
    mock_client.get_access_token.assert_called_once_with(
        MOCK_CLIENT_ID, MOCK_CLIENT_SECRET
    )
    assert auth.access_token is not None


def test_sherlockml_auth_cached(mocker):

    mock_client = mocker.Mock()
    mocker.patch(
        "sherlockml.clients.auth.AccessTokenClient", return_value=mock_client
    )

    auth = SherlockMLAuth(MOCK_HUDSON_URL, MOCK_CLIENT_ID, MOCK_CLIENT_SECRET)
    auth.access_token = AccessToken(
        MOCK_ACCESS_TOKEN_MATERIAL, NOW + timedelta(minutes=10)
    )

    unauthenticated_request = mocker.Mock(headers={})
    request = auth(unauthenticated_request)

    assert request.headers["Authorization"] == "Bearer access-token"
    mock_client.get_access_token.assert_not_called()
