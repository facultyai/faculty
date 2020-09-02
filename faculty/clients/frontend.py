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

    # TODO this is a copy-paste modificiation from BaseClient
    def _request(self, method, endpoint, check_status=True, *args, **kwargs):
        """Perform an HTTP request.

        This method should not be called from subclasses directly. Instead,
        call one of the HTTP verb-specific methods. If it does not exist yet
        for the HTTP method you need, contribute it.
        """
        url = "https://gollum.platform.asidata.science/" + endpoint 
        response = self.http_session.request(method, url, *args, **kwargs)
        if check_status:
            _check_status(response)
        return response

    def user_updates(self, user_id):
        endpoint = "api/updates/user/{}".format(user_id)
        response = self._get_raw(endpoint, stream=True)
        
        client = sseclient.SSEClient(response)
        return client.events()
