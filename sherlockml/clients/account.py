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


Account = namedtuple("Account", ["user_id"])


class AccountSchema(Schema):
    user_id = fields.UUID(data_key="userId", required=True)

    @post_load
    def make_account(self, data):
        return Account(**data)


AuthenticationResponse = namedtuple("AuthenticationResponse", ["account"])


class AuthenticationResponseSchema(Schema):
    account = fields.Nested(AccountSchema, required=True)

    @post_load
    def make_authentication_response(self, data):
        return AuthenticationResponse(**data)


class AccountClient(BaseClient):

    SERVICE_NAME = "hudson"

    def authenticated_user_id(self):
        data = self._get("/authenticate", AuthenticationResponseSchema())
        return data.account.user_id
