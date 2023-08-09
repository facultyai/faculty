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


import secrets
import uuid
from copy import deepcopy

import pytest
from marshmallow import ValidationError

from faculty.clients.api import (
    APIClient,
    APIKey,
    _APIKeySchema,
)

ENVIRONMENT_ID = uuid.uuid4()
OWNER_ID = uuid.uuid4()
PROJECT_ID = uuid.uuid4()
SERVER_ID = uuid.uuid4()
USER_ID = uuid.uuid4()

# TODO: replace with full API for tests
TEST_API_ID = uuid.uuid4()

PROD_KEY = APIKey(
    id=uuid.uuid4(),
    material=secrets.token_hex(16),
    label="test_key",
    enabled=True,
)

PROD_KEY_BODY = {
    "keyId": PROD_KEY.id,
    "material": PROD_KEY.material,
    "label": PROD_KEY.label,
    "enabled": PROD_KEY.enabled,
}


def test_apikey_schema():
    data = _APIKeySchema().load(PROD_KEY_BODY)
    assert data == PROD_KEY


def test_apikey_schema_invalid():
    with pytest.raises(ValidationError):
        _APIKeySchema().load({})


@pytest.mark.parametrize(
    "key_starting_enabled",
    [True, False],
)
def test_disable_production_key(mocker, key_starting_enabled):
    disabled_prod_key = deepcopy(PROD_KEY)
    disabled_prod_key.enabled = key_starting_enabled

    mocker.patch.object(APIClient, "_put", return_value=disabled_prod_key)
    schema_mock = mocker.patch("faculty.clients.api._APIKeySchema")

    client = APIClient(mocker.Mock(), mocker.Mock())
    assert (
        client.disable_production_key(PROJECT_ID, TEST_API_ID, PROD_KEY.id)
        == disabled_prod_key
    )

    schema_mock.assert_called_once_with()
    APIClient._put.assert_called_once_with(
        "/project/{}/api/{}/key/{}/enabled".format(
            PROJECT_ID, TEST_API_ID, PROD_KEY.id
        ),
        schema_mock.return_value,
        json={"enabled": False},
    )


@pytest.mark.parametrize(
    "key_starting_enabled",
    [True, False],
)
def test_enable_production_key(mocker, key_starting_enabled):
    disabled_prod_key = deepcopy(PROD_KEY)
    disabled_prod_key.enabled = key_starting_enabled

    mocker.patch.object(APIClient, "_put", return_value=disabled_prod_key)
    schema_mock = mocker.patch("faculty.clients.api._APIKeySchema")

    client = APIClient(mocker.Mock(), mocker.Mock())
    assert (
        client.enable_production_key(PROJECT_ID, TEST_API_ID, PROD_KEY.id)
        == disabled_prod_key
    )

    schema_mock.assert_called_once_with()
    APIClient._put.assert_called_once_with(
        "/project/{}/api/{}/key/{}/enabled".format(
            PROJECT_ID, TEST_API_ID, PROD_KEY.id
        ),
        schema_mock.return_value,
        json={"enabled": True},
    )
