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
from enum import Enum

from marshmallow import Schema, fields, post_load
from marshmallow_enum import EnumField

from sherlockml.clients.base import BaseClient


class GlobalRole(Enum):
    BASIC_USER = "global-basic-user"
    FULL_USER = "global-full-user"
    ADMIN = "global-admin"


User = namedtuple(
    "User",
    [
        "id",
        "username",
        "full_name",
        "email",
        "created_at",
        "enabled",
        "global_roles",
        "is_system",
    ],
)


class UserSchema(Schema):

    id = fields.UUID(data_key="userId", required=True)
    username = fields.Str(required=True)
    full_name = fields.Str(data_key="fullName", missing=None)
    email = fields.Str(required=True)
    created_at = fields.DateTime(data_key="createdAt", required=True)
    enabled = fields.Boolean(required=True)
    global_roles = fields.List(
        EnumField(GlobalRole, by_value=True),
        data_key="globalRoles",
        missing=None,
    )
    is_system = fields.Boolean(data_key="isSystem", required=True)

    @post_load
    def make_user(self, data):
        return User(**data)


class UserClient(BaseClient):

    SERVICE_NAME = "flock"

    def get_user(self, user_id):
        endpoint = "/user/{}".format(user_id)
        response = self._get(endpoint, UserSchema())
        return response

    def get_all_users(self, is_system=None, enabled=None):
        params = {}
        if is_system is not None:
            params["isSystem"] = "true" if is_system else "false"
        if enabled is not None:
            params["isDisabled"] = "false" if enabled else "true"
        endpoint = "/users"
        response = self._get(endpoint, UserSchema(many=True), params=params)
        return response

    def set_global_roles(self, user_id, global_roles):
        endpoint = "/user/{}/roles".format(user_id)
        response = self._put(
            endpoint, UserSchema(), json={"roles": global_roles}
        )
        return response
