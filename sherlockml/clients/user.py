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

from marshmallow import Schema, fields, post_load

from sherlockml.clients.base import BaseClient


User = namedtuple("User", ["id"])


class UserSchema(Schema):
    id = fields.UUID(data_key="userId", required=True)

    @post_load
    def make_user(self, data):
        return User(**data)


AuthenticationResponse = namedtuple("AuthenticationResponse", ["user"])


class AuthenticationResponseSchema(Schema):
    user = fields.Nested(UserSchema, data_key="account", required=True)

    @post_load
    def make_authentication_response(self, data):
        return AuthenticationResponse(**data)


class UserClient(BaseClient):

    SERVICE_NAME = "hudson"

    def authenticated_user_id(self):
        data = self._get("/authenticate", AuthenticationResponseSchema())
        return data.user.id
