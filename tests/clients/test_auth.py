# Copyright 2018-2019 ASI Data Science
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

from faculty.clients.auth import (
    AccessToken,
    _AccessTokenCache,
    _get_access_token,
    _get_access_token_cached,
    FacultyAuth,
)
from tests.clients.fixtures import PROFILE


ACCESS_TOKEN_URL = "{}://hudson.{}/access_token".format(
    PROFILE.protocol, PROFILE.domain
)
ACCESS_TOKEN_MATERIAL = "access-token"
NOW = datetime.now(tz=pytz.utc)
IN_TEN_MINUTES = NOW + timedelta(minutes=10)


@pytest.fixture
def mock_datetime_now(mocker):
    datetime_mock = mocker.patch("faculty.clients.auth.datetime")
    datetime_mock.now.return_value = NOW
    return datetime_mock


def test_access_token_cache(mock_datetime_now):
    cache = _AccessTokenCache()
    access_token = AccessToken(token="token", expires_at=IN_TEN_MINUTES)
    cache.add(PROFILE, access_token)
    assert cache.get(PROFILE) == access_token


def test_access_token_cache_miss(mocker, mock_datetime_now):
    cache = _AccessTokenCache()
    access_token = AccessToken(token="token", expires_at=IN_TEN_MINUTES)
    cache.add(PROFILE, access_token)
    assert cache.get(mocker.Mock()) is None


def test_access_token_cache_expired(mock_datetime_now):
    cache = _AccessTokenCache()
    access_token = AccessToken(
        token="token", expires_at=NOW - timedelta(seconds=1)
    )
    cache.add(PROFILE, access_token)
    assert cache.get(PROFILE) is None


def test_get_access_token(requests_mock, mock_datetime_now):

    requests_mock.post(
        ACCESS_TOKEN_URL,
        json={"access_token": ACCESS_TOKEN_MATERIAL, "expires_in": 600},
    )

    access_token = _get_access_token(PROFILE)

    assert requests_mock.last_request.json() == {
        "client_id": PROFILE.client_id,
        "client_secret": PROFILE.client_secret,
        "grant_type": "client_credentials",
    }
    assert access_token.token == ACCESS_TOKEN_MATERIAL
    assert access_token.expires_at == IN_TEN_MINUTES


def test_get_access_token_cached(mocker):
    """If cache has a token for the provided profile, return it."""
    mock_cache = mocker.patch("faculty.clients.auth._ACCESS_TOKEN_CACHE")
    assert _get_access_token_cached(PROFILE) == mock_cache.get.return_value
    mock_cache.get.assert_called_once_with(PROFILE)


def test_get_access_token_cached_miss(mocker):
    """If cache has no valid token for a profile, get a new one."""

    mock_cache = mocker.patch("faculty.clients.auth._ACCESS_TOKEN_CACHE")
    mock_cache.get.return_value = None

    new_token = mocker.Mock()
    mocker.patch(
        "faculty.clients.auth._get_access_token", return_value=new_token
    )

    assert _get_access_token_cached(PROFILE) == new_token
    mock_cache.add.assert_called_once_with(PROFILE, new_token)


def test_faculty_auth(mocker):

    access_token = AccessToken(token="access-token", expires_at=IN_TEN_MINUTES)
    get_access_token_cached_mock = mocker.patch(
        "faculty.clients.auth._get_access_token_cached",
        return_value=access_token,
    )

    auth = FacultyAuth(PROFILE)

    unauthenticated_request = mocker.Mock(headers={})
    request = auth(unauthenticated_request)

    assert request.headers["Authorization"] == "Bearer access-token"
    get_access_token_cached_mock.assert_called_once_with(PROFILE)
