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


import json
from uuid import UUID

from sherlockml.clients.base import BaseClient, InvalidResponseBody


class UserClient(BaseClient):

    SERVICE_NAME = 'hudson'

    def authenticated_user_id(self):
        response = self._get('/authenticate')
        response.raise_for_status()

        try:
            data = response.json()
        except json.JSONDecodeError:
            raise InvalidResponseBody('received malformed JSON from server')

        try:
            user_id_string = data['account']['userId']
        except (KeyError, TypeError):
            raise InvalidResponseBody('received malformed JSON from server')

        try:
            user_id = UUID(user_id_string)
        except ValueError:
            raise InvalidResponseBody('received invalid user id from server')

        return user_id
