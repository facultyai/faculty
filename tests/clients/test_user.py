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
from marshmallow import ValidationError
from dateutil.tz import UTC

from sherlockml.clients.user import (
    UserClient, User, UserSchema, GlobalRole
)

USER_ID = uuid.uuid4()
CREATED_AT = datetime(2018, 3, 10, 11, 32, 6, 247000, tzinfo=UTC)
CREATED_AT_STRING = "2018-03-10T11:32:06.247Z"

TEST_USER_JSON = dict(
    userId=str(USER_ID),
    username='test-user',
    fullName='Test User',
    email='test@email.com',
    createdAt=CREATED_AT_STRING,
    enabled='true',
    globalRoles=['global-basic-user', 'global-full-user']
)


EXPECTED_USER = User(
    id=USER_ID,
    username='test-user',
    full_name='Test User',
    email='test@email.com',
    created_at=CREATED_AT,
    enabled=True,
    global_roles=[GlobalRole.BASIC_USER, GlobalRole.FULL_USER]
)


def test_user_schema():
    data = UserSchema().load(TEST_USER_JSON)
    assert data == EXPECTED_USER


