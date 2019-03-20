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

import uuid

import pytest
from marshmallow import ValidationError

from faculty.clients.account import (
    AccountClient,
    Account,
    AccountSchema,
    AuthenticationResponse,
    AuthenticationResponseSchema,
)


USER_ID = uuid.uuid4()
USERNAME = "joe_bloggs"

ACCOUNT = Account(user_id=USER_ID, username=USERNAME)
ACCOUNT_BODY = {"userId": str(USER_ID), "username": USERNAME}


def test_account_schema():
    data = AccountSchema().load(ACCOUNT_BODY)
    assert data == ACCOUNT


@pytest.mark.parametrize(
    "data", [{}, {"userId": "not-a-uuid", "username": USERNAME}]
)
def test_account_schema_invalid(data):
    with pytest.raises(ValidationError):
        AccountSchema().load(data)


def test_authentication_response_schema():
    data = AuthenticationResponseSchema().load({"account": ACCOUNT_BODY})
    assert data == AuthenticationResponse(account=ACCOUNT)


@pytest.mark.parametrize("data", [{}, {"account": "not-an-account"}])
def test_authentication_response_schema_invalid(data):
    with pytest.raises(ValidationError):
        AuthenticationResponseSchema().load(data)


def test_account_client_authenticated_account(mocker):
    mocker.patch.object(
        AccountClient,
        "_get",
        return_value=AuthenticationResponse(account=ACCOUNT),
    )

    schema_mock = mocker.patch(
        "faculty.clients.account.AuthenticationResponseSchema"
    )

    client = AccountClient(mocker.Mock())

    assert client.authenticated_account() == ACCOUNT

    AccountClient._get.assert_called_once_with(
        "/authenticate", schema_mock.return_value
    )


def test_account_client_authenticated_user_id(mocker):
    mocker.patch.object(
        AccountClient, "authenticated_account", return_value=ACCOUNT
    )

    client = AccountClient(mocker.Mock())

    assert client.authenticated_user_id() == USER_ID

    AccountClient.authenticated_account.assert_called_once_with()
