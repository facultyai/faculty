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

    _SERVICE_NAME = "frontend" # TODO what if used from outside the platform

    def user_updates(self, user_id):
        endpoint = "api/updates/user/{}".format(user_id)
        response = self._get_raw(endpoint, stream=True)
        
        client = sseclient.SSEClient(response)
        return client.events()

    def check_publish_template_result(self, events, project_id):
        for event in events:
            if event.event == '@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED':
                error_body = json.loads(event.data)
                print(error_body)
                if error_body['sourceProjectId'] == str(project_id):
                    msg = _extract_publishing_error_msg(error_body)            
                    raise TemplatePublishingError(msg)
            elif event.event == '@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_COMPLETED':
                return


def _extract_publishing_error_msg(error_body):
    code = error_body["errorCode"]
    if code in {"name_conflict", "unexpected_error"}:
        return error_body["error"]
    elif code == "template_rendering_error":
        errors = error_body["errors"]
        msg = "Failed to render the template with default parameters:"
        for e in errors:
            msg += "\n\t{} in {}".format(e["error"], e["path"])
        return msg
    else:
        return "Unexpected error when publishing the template"

class TemplatePublishingError(Exception):
    pass

