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

from contextlib import contextmanager

from marshmallow import fields

from faculty.clients.base import BaseClient, BaseSchema

class TemplateClient(BaseClient):

    _SERVICE_NAME = "kanto"

    def publish_new(self, template, source_directory):
        endpoint = "template"
        payload = {
            "sourceProjectId": "30ca140a-b454-48f1-8f51-98bbe39b97d3",
            "sourceDirectory": source_directory,
            "name": template
        }
        response = self._post_raw(endpoint, json=payload)
        
    # TODO move from faculty-cli
    @contextmanager 
    def _stream(self, endpoint):
        """Stream from a SSE endpoint.

        Usage
        -----

        >>> with self._stream(endpoint) as stream:
        ...     for sse in stream:
        ...         print(sse.data)

        """
        response = self._get(endpoint, stream=True)

        def sse_stream_iterator():
            buf = []
            for line in response.iter_lines(decode_unicode=True):
                if not line.strip():
                    yield ServerSentEventMessage.from_lines(buf)
                    buf = []
                else:
                    buf.append(line)

        try:
            yield sse_stream_iterator()
        finally:
            response.close()


# class _PublishNewTemplateSchema(BaseSchema):
#     source_project_id = fields.UUID(data_key="sourceProjectId")
#     source_directory = fields.String(data_key="sourceDirectory")
#     name = fields.String()
