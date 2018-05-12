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

from sherlockml.clients.auth import HudsonClient


MOCK_HUDSON_URL = 'https://hudson.example.com'
MOCK_CLIENT_ID = 'client-id'
MOCK_CLIENT_SECRET = 'client-secret'
MOCK_ACCESS_TOKEN = 'access-token'
MOCK_NOW = datetime(2018, 1, 1, 10, 0, 0, tzinfo=pytz.utc)


@pytest.fixture
def mock_datetime_now(mocker):
    datetime_mock = mocker.patch('sherlockml.clients.auth.datetime')
    datetime_mock.now.return_value = MOCK_NOW
    return datetime_mock


def test_get_access_token(requests_mock, mock_datetime_now):

    requests_mock.post(
        '{}/access_token'.format(MOCK_HUDSON_URL),
        json={'access_token': MOCK_ACCESS_TOKEN, 'expires_in': 600}
    )

    client = HudsonClient(MOCK_HUDSON_URL)
    access_token = client.get_access_token(
        MOCK_CLIENT_ID, MOCK_CLIENT_SECRET
    )

    assert requests_mock.last_request.json() == {
        'client_id': MOCK_CLIENT_ID,
        'client_secret': MOCK_CLIENT_SECRET,
        'grant_type': 'client_credentials'
    }
    assert access_token.token == MOCK_ACCESS_TOKEN
    assert access_token.expires_at == MOCK_NOW + timedelta(minutes=10)
