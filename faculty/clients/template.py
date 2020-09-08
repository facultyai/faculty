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

from faculty.clients.base import BackendServiceClient


class TemplateClient(BackendServiceClient):

    _SERVICE_NAME = "kanto"

    def publish_new(self, template, source_directory, project_id):
        endpoint = "template"
        payload = {
            "sourceProjectId": str(project_id),
            "sourceDirectory": source_directory,
            "name": template,
        }
        self._post_raw(endpoint, json=payload)
