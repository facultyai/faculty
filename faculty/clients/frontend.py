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
from faculty.clients.base import BaseClient
import faculty.session

class FrontendClient(BaseClient):

    _SERVICE_NAME = "frontend"

    # def __init__(self, session):
    #     self.session = session

    def foo(self, user_id):
        endpoint = "api/updates/user/{}".format(user_id)
        # endpoint = "https://gollum.platform.asidata.science/api/updates/user/{}".format(user_id)
        # self._stream(endpoint)
        # with self._stream(endpoint) as stream:
        #     print("open")
        #     for sse in stream:
        #         print(sse.data)
        response = requests.get("https://gollum.platform.asidata.science/api/updates/user/07a34d80-d386-4d27-9a46-c2504cba2fc6",headers={"Authorization": "Bearer ???"}, stream=True)
        client = sseclient.SSEClient(response)
        return client.events()

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
        # auth = FacultyAuth(self.session)
        # print(f"streaming endpoint {endpoint}")
        response = requests.get("https://gollum.platform.asidata.science/api/updates/user/07a34d80-d386-4d27-9a46-c2504cba2fc6",headers={"Authorization": "Bearer ???"}, stream=True)
        # response = self._get_raw(endpoint, stream=True)
        print("sse_stream_iter")
        def sse_stream_iterator():
            buf = []
            for line in response.iter_lines(decode_unicode=True):
                print(f"line: {line}.")
                if not line.strip():
                    yield ServerSentEventMessage.from_lines(buf)
                    buf = []
                else:
                    buf.append(line)

        try:
            yield sse_stream_iterator()
        finally:
            response.close()

class ServerSentEventMessage(object):
    """A message from a server sent event stream."""

    def __init__(self, id_, event, data):
        self.id_ = id_
        self.event = event
        self.data = data

    def __str__(self):
        data_string = repr(self.data)
        if len(data_string) > 30:
            data_string = "{}...{}".format(data_string[:12], data_string[-12:])
        return "{}(id={}, event={}, data={})".format(
            self.__class__.__name__, self.id_, self.event, data_string
        )

    @classmethod
    def from_lines(cls, lines):
        print(lines)
        id_ = None
        event = None
        data_lines = []
        for line in lines:
            if line.startswith("id:"):
                id_ = int(line[3:].strip())
            elif line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data_lines.append(line[5:].strip())
            else:
                raise ValueError("unexpected sse line: {}".format(line))
        print(data_lines)
        data = json.loads("\n".join(data_lines))
        return cls(id_, event, data)