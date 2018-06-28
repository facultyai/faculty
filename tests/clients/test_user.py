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


import json
import uuid

import pytest
import requests
import marshmallow

from sherlockml.clients.user import UserClient
from tests.clients.fixtures import (
    patch_sherlockmlauth, PROFILE, HUDSON_URL, AUTHORIZATION_HEADER
)

TEST_USER_ID = uuid.uuid4()


def test_authenticated_user_id(requests_mock, patch_sherlockmlauth):

    requests_mock.get(
        '{}/authenticate'.format(HUDSON_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={'account': {'userId': str(TEST_USER_ID)}}
    )

    client = UserClient(PROFILE)
    assert client.authenticated_user_id() == TEST_USER_ID


def test_authenticated_user_id_error_response(
    requests_mock, patch_sherlockmlauth
):

    requests_mock.get(
        '{}/authenticate'.format(HUDSON_URL),
        request_headers=AUTHORIZATION_HEADER,
        status_code=400
    )

    client = UserClient(PROFILE)
    with pytest.raises(requests.HTTPError):
        client.authenticated_user_id()


def test_authenticated_user_id_invalid_json(
    requests_mock, patch_sherlockmlauth
):

    requests_mock.get(
        '{}/authenticate'.format(HUDSON_URL),
        request_headers=AUTHORIZATION_HEADER,
        text='invalid-json'
    )

    client = UserClient(PROFILE)
    with pytest.raises(json.JSONDecodeError):
        client.authenticated_user_id()


def test_authenticated_user_id_malformed_json(
    requests_mock, patch_sherlockmlauth
):

    requests_mock.get(
        '{}/authenticate'.format(HUDSON_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={}
    )

    client = UserClient(PROFILE)
    with pytest.raises(marshmallow.ValidationError):
        client.authenticated_user_id()
