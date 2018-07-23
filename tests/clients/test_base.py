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
    BaseClient, Unauthorized, NotFound, BadResponseStatus, InvalidResponse
)
from tests.clients.fixtures import PROFILE

AUTHORIZATION_HEADER_VALUE = 'Bearer mock-token'
AUTHORIZATION_HEADER = {'Authorization': AUTHORIZATION_HEADER_VALUE}

HUDSON_URL = 'https://hudson.test.domain.com'

BAD_RESPONSE_STATUSES = [
    (401, Unauthorized),
    (404, NotFound),
    (400, BadResponseStatus),
    (500, BadResponseStatus)
]


@pytest.fixture
def patch_sherlockmlauth(mocker):

    def _add_auth_headers(request):
        request.headers['Authorization'] = AUTHORIZATION_HEADER_VALUE
        return request

    mock_auth = mocker.patch('sherlockml.clients.base.SherlockMLAuth',
                             return_value=_add_auth_headers)

    yield

    mock_auth.assert_called_once_with(
        HUDSON_URL,
        PROFILE.client_id,
        PROFILE.client_secret
    )


DummyObject = namedtuple('DummyObject', ['foo'])


class DummySchema(Schema):
    foo = fields.String(required=True)

    @post_load
    def make_test_object(self, data):
        return DummyObject(**data)


class DummyClient(BaseClient):
    SERVICE_NAME = 'test-service'


SERVICE_URL = 'https://test-service.{}'.format(PROFILE.domain)


def test_get(requests_mock, patch_sherlockmlauth):

    requests_mock.get(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={'foo': 'bar'}
    )

    client = DummyClient(PROFILE)
    assert client._get('/test', DummySchema()) == DummyObject(foo='bar')


@pytest.mark.parametrize('status_code, exception', BAD_RESPONSE_STATUSES)
def test_get_bad_responses(
    requests_mock, patch_sherlockmlauth, status_code, exception
):

    requests_mock.get(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        status_code=status_code
    )

    client = DummyClient(PROFILE)
    with pytest.raises(exception):
        client._get('/test', DummySchema())


def test_get_invalid_json(requests_mock, patch_sherlockmlauth):

    requests_mock.get(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        text='invalid-json'
    )

    client = DummyClient(PROFILE)
    with pytest.raises(InvalidResponse, match='not valid JSON'):
        client._get('/test', DummySchema())


def test_get_malformatted_json(requests_mock, patch_sherlockmlauth):

    requests_mock.get(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={'bad': 'json'}
    )

    client = DummyClient(PROFILE)
    with pytest.raises(InvalidResponse, match='not match expected format'):
        client._get('/test', DummySchema())


def test_post(requests_mock, patch_sherlockmlauth):

    mock = requests_mock.post(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={'foo': 'bar'}
    )

    client = DummyClient(PROFILE)
    response = client._post('/test', DummySchema(), json={'test': 'payload'})

    assert response == DummyObject(foo='bar')
    assert mock.last_request.json() == {'test': 'payload'}


@pytest.mark.parametrize('status_code, exception', BAD_RESPONSE_STATUSES)
def test_post_bad_responses(
    requests_mock, patch_sherlockmlauth, status_code, exception
):

    requests_mock.post(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        status_code=status_code
    )

    client = DummyClient(PROFILE)
    with pytest.raises(exception):
        client._post('/test', DummySchema())


def test_post_invalid_json(requests_mock, patch_sherlockmlauth):

    requests_mock.post(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        text='invalid-json'
    )

    client = DummyClient(PROFILE)
    with pytest.raises(InvalidResponse, match='not valid JSON'):
        client._post('/test', DummySchema())


def test_post_malformatted_json(requests_mock, patch_sherlockmlauth):

    requests_mock.post(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={'bad': 'json'}
    )

    client = DummyClient(PROFILE)
    with pytest.raises(InvalidResponse, match='not match expected format'):
        client._post('/test', DummySchema())


def test_put(requests_mock, patch_sherlockmlauth):

    mock = requests_mock.put(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={'foo': 'bar'}
    )

    client = DummyClient(PROFILE)
    response = client._put('/test', DummySchema(), json={'test': 'payload'})

    assert response == DummyObject(foo='bar')
    assert mock.last_request.json() == {'test': 'payload'}


@pytest.mark.parametrize('status_code, exception', BAD_RESPONSE_STATUSES)
def test_put_bad_responses(
    requests_mock, patch_sherlockmlauth, status_code, exception
):

    requests_mock.put(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        status_code=status_code
    )

    client = DummyClient(PROFILE)
    with pytest.raises(exception):
        client._put('/test', DummySchema())


def test_put_invalid_json(requests_mock, patch_sherlockmlauth):

    requests_mock.put(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        text='invalid-json'
    )

    client = DummyClient(PROFILE)
    with pytest.raises(InvalidResponse, match='not valid JSON'):
        client._put('/test', DummySchema())


def test_put_malformatted_json(requests_mock, patch_sherlockmlauth):

    requests_mock.put(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={'bad': 'json'}
    )

    client = DummyClient(PROFILE)
    with pytest.raises(InvalidResponse, match='not match expected format'):
        client._put('/test', DummySchema())


def test_delete(requests_mock, patch_sherlockmlauth):

    requests_mock.delete(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={'foo': 'bar'}
    )

    client = DummyClient(PROFILE)
    response = client._delete('/test', DummySchema())

    assert response == DummyObject(foo='bar')


@pytest.mark.parametrize('status_code, exception', BAD_RESPONSE_STATUSES)
def test_delete_bad_responses(
    requests_mock, patch_sherlockmlauth, status_code, exception
):

    requests_mock.delete(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        status_code=status_code
    )

    client = DummyClient(PROFILE)
    with pytest.raises(exception):
        client._delete('/test', DummySchema())


def test_delete_invalid_json(requests_mock, patch_sherlockmlauth):

    requests_mock.delete(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        text='invalid-json'
    )

    client = DummyClient(PROFILE)
    with pytest.raises(InvalidResponse, match='not valid JSON'):
        client._delete('/test', DummySchema())


def test_delete_malformatted_json(requests_mock, patch_sherlockmlauth):

    requests_mock.delete(
        '{}/test'.format(SERVICE_URL),
        request_headers=AUTHORIZATION_HEADER,
        json={'bad': 'json'}
    )

    client = DummyClient(PROFILE)
    with pytest.raises(InvalidResponse, match='not match expected format'):
        client._delete('/test', DummySchema())
