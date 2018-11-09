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

import uuid
from datetime import datetime

import pytest
from dateutil.tz import UTC
from marshmallow import ValidationError

from sherlockml.clients.user import UserClient, User, UserSchema, GlobalRole
from tests.clients.fixtures import PROFILE

USER_ID = uuid.uuid4()
CREATED_AT = datetime(2018, 3, 10, 11, 32, 6, 247000, tzinfo=UTC)
CREATED_AT_STRING = "2018-03-10T11:32:06.247Z"

TEST_USER_JSON = {
    "userId": str(USER_ID),
    "username": "test-user",
    "fullName": "Test User",
    "email": "test@email.com",
    "createdAt": CREATED_AT_STRING,
    "enabled": "true",
    "globalRoles": ["global-basic-user", "global-full-user"],
}

EXPECTED_USER = User(
    id=USER_ID,
    username="test-user",
    full_name="Test User",
    email="test@email.com",
    created_at=CREATED_AT,
    enabled=True,
    global_roles=[GlobalRole.BASIC_USER, GlobalRole.FULL_USER],
)


def test_user_schema():
    data = UserSchema().load(TEST_USER_JSON)
    assert data == EXPECTED_USER


def test_user_schema_invalid():
    with pytest.raises(ValidationError):
        UserSchema().load({})


def test_user_schema_invalid_uuid():
    body = TEST_USER_JSON.copy()
    body["userId"] = "not-a-uuid"
    with pytest.raises(ValidationError):
        UserSchema().load(body)


def test_user_schema_missing_userId():
    body = TEST_USER_JSON.copy()
    body.pop("userId")
    with pytest.raises(ValidationError):
        UserSchema().load(body)


def test_user_schema_invalid_global_role():
    body = TEST_USER_JSON.copy()
    body["globalRoles"] = ["invalid-global-role"]
    with pytest.raises(ValidationError):
        UserSchema().load(body)


def test_get_user(mocker):
    mocker.patch.object(UserClient, "_get", return_value=EXPECTED_USER)
    schema_mock = mocker.patch("sherlockml.clients.user.UserSchema")

    client = UserClient(PROFILE)

    user = client.get_user(str(USER_ID))

    assert user == EXPECTED_USER

    schema_mock.assert_called_once_with()
    UserClient._get.assert_called_once_with(
        "/user/{}".format(str(USER_ID)), schema_mock.return_value
    )


def test_get_all_users(mocker):
    mocker.patch.object(UserClient, "_get", return_value=[EXPECTED_USER])
    schema_mock = mocker.patch("sherlockml.clients.user.UserSchema")

    client = UserClient(PROFILE)

    users = client.get_all_users()

    assert users == [EXPECTED_USER]

    schema_mock.assert_called_once_with(many=True)
    UserClient._get.assert_called_once_with("/users", schema_mock.return_value)


def test_set_global_roles(mocker):
    mocker.patch.object(UserClient, "_put")
    schema_mock = mocker.patch("sherlockml.clients.user.UserSchema")

    client = UserClient(PROFILE)

    roles = ["global-basic-user", "global-full-user"]

    user = client.set_global_roles(str(USER_ID), roles)

    assert user == UserClient._put.return_value

    schema_mock.assert_called_once_with()
    UserClient._put.assert_called_once_with(
        "/user/{}/roles".format(str(USER_ID)),
        schema_mock.return_value,
        json={"roles": roles},
    )
