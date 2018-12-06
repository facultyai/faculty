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

TEST_HUMAN_USER_JSON = {
    "userId": str(USER_ID),
    "username": "test-user",
    "fullName": "Test User",
    "email": "test@email.com",
    "createdAt": CREATED_AT_STRING,
    "enabled": True,
    "isSystem": False,
    "globalRoles": ["global-basic-user", "global-full-user"],
}

EXPECTED_HUMAN_USER = User(
    id=USER_ID,
    username="test-user",
    full_name="Test User",
    email="test@email.com",
    created_at=CREATED_AT,
    enabled=True,
    global_roles=[GlobalRole.BASIC_USER, GlobalRole.FULL_USER],
    is_system=False,
)

TEST_SYSTEM_USER_JSON = {
    "userId": str(USER_ID),
    "username": "test-user",
    "email": "invalid-email",
    "createdAt": CREATED_AT_STRING,
    "enabled": True,
    "isSystem": True,
}

EXPECTED_SYSTEM_USER = User(
    id=USER_ID,
    username="test-user",
    full_name=None,
    email="invalid-email",
    created_at=CREATED_AT,
    enabled=True,
    global_roles=None,
    is_system=True,
)


@pytest.mark.parametrize(
    "body, expected_user",
    [
        (TEST_HUMAN_USER_JSON, EXPECTED_HUMAN_USER),
        (TEST_SYSTEM_USER_JSON, EXPECTED_SYSTEM_USER),
    ],
    ids=["human", "system"],
)
def test_user_schema(body, expected_user):
    data = UserSchema().load(body)
    assert data == expected_user


def test_user_schema_invalid():
    with pytest.raises(ValidationError):
        UserSchema().load({})


def test_user_schema_invalid_uuid():
    body = TEST_HUMAN_USER_JSON.copy()
    body["userId"] = "not-a-uuid"
    with pytest.raises(ValidationError):
        UserSchema().load(body)


def test_user_schema_missing_userId():
    body = TEST_HUMAN_USER_JSON.copy()
    body.pop("userId")
    with pytest.raises(ValidationError):
        UserSchema().load(body)


def test_user_schema_invalid_global_role():
    body = TEST_HUMAN_USER_JSON.copy()
    body["globalRoles"] = ["invalid-global-role"]
    with pytest.raises(ValidationError):
        UserSchema().load(body)


def test_get_user(mocker):
    mocker.patch.object(UserClient, "_get", return_value=EXPECTED_HUMAN_USER)
    schema_mock = mocker.patch("sherlockml.clients.user.UserSchema")

    client = UserClient(PROFILE)

    user = client.get_user(str(USER_ID))

    assert user == EXPECTED_HUMAN_USER

    schema_mock.assert_called_once_with()
    UserClient._get.assert_called_once_with(
        "/user/{}".format(str(USER_ID)), schema_mock.return_value
    )


@pytest.mark.parametrize(
    "is_system, enabled, expected_params",
    [
        (None, None, {}),
        (True, None, {"isSystem": "true"}),
        (False, None, {"isSystem": "false"}),
        (None, True, {"isDisabled": "false"}),
        (None, False, {"isDisabled": "true"}),
        (True, False, {"isSystem": "true", "isDisabled": "true"}),
        (False, False, {"isSystem": "false", "isDisabled": "true"}),
    ],
    ids=[
        "basic",
        "system only",
        "human only",
        "enabled only",
        "disabled only",
        "system and disabled only",
        "human and disabled only",
    ],
)
def test_get_all_users(mocker, is_system, enabled, expected_params):
    mocker.patch.object(UserClient, "_get", return_value=[EXPECTED_HUMAN_USER])
    schema_mock = mocker.patch("sherlockml.clients.user.UserSchema")

    client = UserClient(PROFILE)

    users = client.get_all_users(is_system=is_system, enabled=enabled)

    assert users == [EXPECTED_HUMAN_USER]

    schema_mock.assert_called_once_with(many=True)
    UserClient._get.assert_called_once_with(
        "/users", schema_mock.return_value, params=expected_params
    )


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
