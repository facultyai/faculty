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
Interact with the Faculty knowledge centre templates.
"""

from faculty.clients.base import BaseClient


class TemplateClient(BaseClient):
    """Client for the the Knowledge centre templates.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("template")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    """

    _SERVICE_NAME = "kanto"

    def publish_new(self, template, source_directory, project_id):
        endpoint = "template"
        payload = {
            "sourceProjectId": str(project_id),
            "sourceDirectory": source_directory,
            "name": template,
        }
        return self._post_raw(endpoint, json=payload)
