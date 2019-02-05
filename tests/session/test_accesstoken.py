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


import os
from datetime import datetime, timedelta

import pytest
import pytz

import faculty.config
from faculty.session.accesstoken import (
    AccessToken,
    AccessTokenStore,
    AccessTokenMemoryCache,
    AccessTokenFileSystemCache,
    _default_token_cache_path,
)


PROFILE = faculty.config.Profile(
    domain="test.domain.com",
    protocol="https",
    client_id="test-client-id",
    client_secret="test-client-secret",
)
NOW = datetime.now(tz=pytz.utc)
VALID_ACCESS_TOKEN = AccessToken(
    token="access-token", expires_at=NOW + timedelta(minutes=10)
)
EXPIRED_ACCESS_TOKEN = AccessToken(
    token="access-token", expires_at=NOW - timedelta(seconds=1)
)


@pytest.fixture
def mock_datetime_now(mocker):
    datetime_mock = mocker.patch("faculty.session.accesstoken.datetime")
    datetime_mock.now.return_value = NOW
    return datetime_mock


def test_access_token_store():
    store = AccessTokenStore()
    store[PROFILE] = VALID_ACCESS_TOKEN
    assert store[PROFILE] == VALID_ACCESS_TOKEN


def test_access_token_store_get():
    store = AccessTokenStore()
    store[PROFILE] = VALID_ACCESS_TOKEN
    assert store.get(PROFILE) == VALID_ACCESS_TOKEN


def test_access_token_store_get_default():
    store = AccessTokenStore()
    assert store.get(PROFILE) is None


def test_access_token_memory_cache(mock_datetime_now):
    cache = AccessTokenMemoryCache()
    cache.add(PROFILE, VALID_ACCESS_TOKEN)
    assert cache.get(PROFILE) == VALID_ACCESS_TOKEN


def test_access_token_memory_cache_miss(mocker, mock_datetime_now):
    cache = AccessTokenMemoryCache()
    cache.add(PROFILE, VALID_ACCESS_TOKEN)
    assert cache.get(mocker.Mock()) is None


def test_access_token_memory_cache_expired(mock_datetime_now):
    cache = AccessTokenMemoryCache()
    cache.add(PROFILE, EXPIRED_ACCESS_TOKEN)
    assert cache.get(PROFILE) is None


def test_default_token_cache_path(mocker):
    mocker.patch.dict(os.environ, {"HOME": "/foo/bar"})
    expected_path = "/foo/bar/.cache/faculty/token-cache.json"
    assert _default_token_cache_path() == expected_path


def test_default_token_cache_path_xdg_home(mocker):
    mocker.patch.dict(os.environ, {"XDG_CACHE_HOME": "/xdg/cache/home"})
    expected_path = "/xdg/cache/home/faculty/token-cache.json"
    assert _default_token_cache_path() == expected_path


def test_access_token_file_system_cache(tmpdir, mock_datetime_now):
    cache_path = tmpdir.join("cache.json")
    cache = AccessTokenFileSystemCache(cache_path)
    cache.add(PROFILE, VALID_ACCESS_TOKEN)

    assert cache_path.check(file=True)

    new_cache = AccessTokenFileSystemCache(cache_path)
    assert new_cache.get(PROFILE) == VALID_ACCESS_TOKEN


def test_access_token_file_system_cache_default_location(
    mocker, tmpdir, mock_datetime_now
):
    cache_path = tmpdir.join("cache.json")
    default_path_mock = mocker.patch(
        "faculty.session.accesstoken._default_token_cache_path",
        return_value=str(cache_path),
    )

    cache = AccessTokenFileSystemCache()
    cache.add(PROFILE, VALID_ACCESS_TOKEN)

    default_path_mock.assert_called_once_with()
    assert cache_path.check(file=True)

    new_cache = AccessTokenFileSystemCache(cache_path)
    assert new_cache.get(PROFILE) == VALID_ACCESS_TOKEN


def test_access_token_file_system_cache_miss(
    mocker, tmpdir, mock_datetime_now
):
    cache_path = tmpdir.join("cache.json")
    cache = AccessTokenFileSystemCache(cache_path)
    cache.add(PROFILE, VALID_ACCESS_TOKEN)
    assert cache.get(mocker.Mock()) is None


def test_access_token_file_system_cache_expired(tmpdir, mock_datetime_now):
    cache_path = tmpdir.join("cache.json")
    cache = AccessTokenFileSystemCache(cache_path)
    cache.add(PROFILE, EXPIRED_ACCESS_TOKEN)
    assert cache.get(PROFILE) is None


def test_access_token_file_system_cache_new_directory(
    tmpdir, mock_datetime_now
):
    directory = tmpdir.join("spam").join("eggs")

    cache_path = directory.join("cache.json")
    cache = AccessTokenFileSystemCache(cache_path)
    cache.add(PROFILE, VALID_ACCESS_TOKEN)

    assert directory.check(dir=True)

    new_cache = AccessTokenFileSystemCache(cache_path)
    assert new_cache.get(PROFILE) == VALID_ACCESS_TOKEN


@pytest.mark.parametrize(
    "content", ["invalid json", '{"invalid": "structure"}']
)
def test_access_token_file_system_cache_invalid_file(
    tmpdir, mock_datetime_now, content
):
    cache_path = tmpdir.join("cache.json")
    cache_path.write(content)

    cache = AccessTokenFileSystemCache(cache_path)
    cache.add(PROFILE, VALID_ACCESS_TOKEN)

    new_cache = AccessTokenFileSystemCache(cache_path)
    assert new_cache.get(PROFILE) == VALID_ACCESS_TOKEN
