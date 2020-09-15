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


import uuid

from faculty.clients.template import TemplateClient


PROJECT_ID = uuid.uuid4()


def test_publish_new(mocker):
    mocker.patch.object(TemplateClient, "_post_raw")

    client = TemplateClient(mocker.Mock())
    client.publish_new(PROJECT_ID, "template name", "source/dir")

    TemplateClient._post_raw.assert_called_once_with(
        "template",
        json={
            "sourceProjectId": str(PROJECT_ID),
            "sourceDirectory": "source/dir",
            "name": "template name",
        },
    )
