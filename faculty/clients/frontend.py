# Copyright 2020 Faculty Science Limited
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

"""
Interact with the Faculty frontend.
"""

from contextlib import contextmanager
import json

import requests
import sseclient

from faculty.clients.auth import FacultyAuth
from faculty.clients.base import _check_status, BaseClient
import faculty.session

class FrontendClient(BaseClient):

    _SERVICE_NAME = "frontend"

    def user_updates(self, user_id):
        endpoint = "api/updates/user/{}".format(user_id)
        response = self._get_raw(endpoint, stream=True)
        
        client = sseclient.SSEClient(response)
        return client.events()
