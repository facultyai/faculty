from datetime import datetime, timedelta

import pytz
import pytest
import requests_mock

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


def test_get_access_token(mock_datetime_now):

    client = HudsonClient(MOCK_HUDSON_URL)

    with requests_mock.Mocker() as mock:
        mock.post(
            '{}/access_token'.format(MOCK_HUDSON_URL),
            json={'access_token': MOCK_ACCESS_TOKEN, 'expires_in': 600}
        )
        access_token = client.get_access_token(
            MOCK_CLIENT_ID, MOCK_CLIENT_SECRET
        )

    assert access_token.token == MOCK_ACCESS_TOKEN
    assert access_token.expires_at == MOCK_NOW + timedelta(minutes=10)
