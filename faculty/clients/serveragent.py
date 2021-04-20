# Copyright 2018-2021 Faculty Science Limited
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
Interact with server agent.
"""
from contextlib import contextmanager
import json

from attr import attrs, attrib
from marshmallow import fields, post_load
import requests

from faculty.clients.base import BaseSchema, BaseClient

SERVER_RESOURCES_EVENT = "@SSE/SERVER_RESOURCES_UPDATED"


@attrs
class Execution(object):

    id = attrib()
    status = attrib()
    environments = attrib()
    started_at = attrib()
    finished_at = attrib()


@attrs
class EnvironmentExecution(object):

    id = attrib()
    steps = attrib()


@attrs
class EnvironmentExecutionStep(object):

    id = attrib()
    command = attrib()
    status = attrib()
    started_at = attrib()
    finished_at = attrib()
    log_path = attrib()


@attrs
class EnvironmentExecutionLog(object):

    line_number = attrib()
    content = attrib()


@attrs
class ServerSentEventMessage(object):

    id = attrib()
    event = attrib()
    data = attrib()


@attrs
class ServerResources(object):
    """Current CPU and Memory usage on a server."""

    milli_cpus = attrib()
    memory_mb = attrib()


@attrs
class CpuUsage(object):
    """Current CPU usage on a server."""

    total = attrib()
    used = attrib()


@attrs
class MemoryUsage(object):
    """Current Memory usage on a server."""

    total = attrib()
    used = attrib()
    cache = attrib()
    rss = attrib()


class ServerAgentClient(BaseClient):
    def latest_environment_execution(self):
        """Get the latest environment execution on the server."""
        return self._get("/execution/latest", _ExecutionSchema())

    @contextmanager
    def _stream(self, endpoint):
        """Stream from a SSE endpoint.
        Usage
        -----
        >>> with self._stream(endpoint) as stream:
        ...     for sse in stream:
        ...         print(sse.data)
        """
        response = self._get_raw(endpoint, stream=True)

        def sse_stream_iterator():
            buf = []
            for line in response.iter_lines(decode_unicode=True):
                if not line.strip():
                    yield _sse_message_from_lines(buf)
                    buf = []
                else:
                    buf.append(line)

        try:
            yield sse_stream_iterator()
        finally:
            response.close()

    def stream_server_events(self, endpoint):
        """Read from the server events stream."""
        with self._stream(endpoint) as stream:
            for message in stream:
                yield message

    def stream_server_resources(self):
        """Stream the resources used by the server."""

        schema = _ServerResourcesSchema()
        for message in self.stream_server_events("/events"):
            if message.event == SERVER_RESOURCES_EVENT:
                yield schema.load(json.loads("\n".join(message.data)))

    def stream_environment_execution_step_logs(self, execution_id, step_id):
        """Read from the environment step logs."""
        endpoint = "/execution/{}/executor/{}/logs".format(
            execution_id, step_id
        )
        schema = _EnvironmentExecutionLogSchema()
        for message in self.stream_server_events(endpoint):
            if message.event == "log":
                for log in message.data:
                    for line in json.loads(log):
                        yield schema.load(line)


class _EnvironmentExecutionLogSchema(BaseSchema):

    line_number = fields.Integer(data_key="lineNumber", required=True)
    content = fields.String(required=True)

    @post_load
    def make_environment_execution_step_log(self, data, **kwargs):
        return EnvironmentExecutionLog(**data)


class _EnvironmentExecutionStepSchema(BaseSchema):

    id = fields.UUID(required=True)
    command = fields.List(fields.String(required=True), required=True)
    status = fields.String(required=True)
    started_at = fields.DateTime(data_key="startedAt", required=True)
    finished_at = fields.DateTime(data_key="finishedAt", required=True)
    log_path = fields.String(data_key="logUriPath", required=True)

    @post_load
    def make_environment_execution_step(self, data, **kwargs):
        return EnvironmentExecutionStep(**data)


class _EnvironmentExecutionSchema(BaseSchema):

    id = fields.UUID(data_key="environmentId", required=True)
    steps = fields.List(
        fields.Nested(_EnvironmentExecutionStepSchema), required=True
    )

    @post_load
    def make_environment_execution(self, data, **kwargs):
        return EnvironmentExecution(**data)


class _ExecutionSchema(BaseSchema):

    id = fields.UUID(data_key="executionId", required=True)
    status = fields.String(required=True)
    environments = fields.List(
        fields.Nested(_EnvironmentExecutionSchema), required=True
    )
    started_at = fields.DateTime(data_key="startedAt", required=True)
    finished_at = fields.DateTime(data_key="finishedAt", required=True)

    @post_load
    def make_execution(self, data, **kwargs):
        return Execution(**data)


class _CpuUsageSchema(BaseSchema):

    total = fields.Integer(required=True)
    used = fields.Integer(required=True)

    @post_load
    def make_cpu_usage_message(self, data, **kwargs):
        return CpuUsage(**data)


class _MemoryUsageSchema(BaseSchema):

    total = fields.Float(required=True)
    used = fields.Float(required=True)
    cache = fields.Float(required=True)
    rss = fields.Float(required=True)

    @post_load
    def make_memory_usage_message(self, data, **kwargs):
        return MemoryUsage(**data)


class _ServerResourcesSchema(BaseSchema):

    milli_cpus = fields.Nested(
        _CpuUsageSchema, data_key="milliCpus", required=True
    )
    memory_mb = fields.Nested(
        _MemoryUsageSchema, data_key="memoryMB", required=True
    )

    @post_load
    def make_server_resources(self, data, **kwargs):
        return ServerResources(**data)


def _sse_message_from_lines(lines):
    id = None
    event = None
    data = []
    for line in lines:
        if line.startswith("id:"):
            id = int(line[3:].strip())
        elif line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].strip())
        else:
            raise ValueError("unexpected sse line: {}".format(line))

    return ServerSentEventMessage(id, event, data)
