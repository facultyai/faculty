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


import pytest

from sherlockml.config import Profile


PROFILE = Profile(
    domain='test.domain.com',
    protocol='https',
    client_id='test-client-id',
    client_secret='test-client-secret'
)

AUTHORIZATION_HEADER_VALUE = 'Bearer mock-token'
AUTHORIZATION_HEADER = {'Authorization': AUTHORIZATION_HEADER_VALUE}

HUDSON_URL = 'https://hudson.test.domain.com'


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
