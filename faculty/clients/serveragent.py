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
from enum import Enum

from attr import attrs, attrib
from marshmallow import fields, post_load
from marshmallow_enum import EnumField

from faculty.clients.base import BaseSchema, BaseClient

SERVER_RESOURCES_EVENT = "@SSE/SERVER_RESOURCES_UPDATED"

class ExecutionStatus(Enum):
    """The status of an environment execution."""
    NOT_STARTED = "NOT_STARTED"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

class EnvironmentExecutionStepStatus(Enum):
    """The status of an environment execution step."""
    QUEUED = "QUEUED"
    CANCELLED = "CANCELLED"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"

@attrs
class Execution(object):
    """Server environment execution.
    
    Parameters
    ----------
    id : uuid.UUID
        The ID of the execution.
    status : enum.Enum
        The status of the execution.
    environments : List[EnvironmentExecution]
        list of EnvironmentExecution objects for each environment applied.
    started_at : Optional[datetime]
        startime of the execution.
    finished_at : Optional[datetime]
        endtime of the execution.
    """

    id = attrib()
    status = attrib()
    environments = attrib()
    started_at = attrib()
    finished_at = attrib()


@attrs
class EnvironmentExecution(object):
    """An environment executed on a server.
    
    Parameters
    ----------
    id : uuid.UUID
        The ID of the environment.
    steps : List[EnvironmentExecutionStep]
        List of EnvironmentExecutionStep objects.
    """

    id = attrib()
    steps = attrib()


@attrs
class EnvironmentExecutionStep(object):
    """A single environment execution step on a server.
    
    Parameters
    ----------
    id : uuid.UUID
        The ID of the environment step.
    command : List[str]
        The command executed by the step.
    status : enum.Enum
        The status of the step execution.
    started_at : Optional[datetime]
        The start time of the execution step.
    finished_at : Optional[datetime]
        The finish time of the execution step.
    """

    id = attrib()
    command = attrib()
    status = attrib()
    started_at = attrib()
    finished_at = attrib()


@attrs
class EnvironmentExecutionStepLog(object):
    """The log for an environment execution step.
    
    Parameters
    ----------
    line_number : int
        The line number within the step log.
    content : str
        The content of the step log.
    """

    line_number = attrib()
    content = attrib()



@attrs
class ServerResources(object):
    """Information about current server resource usage.
    
    Parameters
    ----------
    milli_cpus : CpuUsage
        current CPU utilisation.
    memory_mb : MemoryUsage
        current memory utilisation. 
    """

    milli_cpus = attrib()
    memory_mb = attrib()


@attrs
class CpuUsage(object):
    """Current CPU usage on a server.
    
    Parameters
    ----------
    total : int
        total CPU resource.
    used : int
        currently utilised CPU resource.
    """

    total = attrib()
    used = attrib()


@attrs
class MemoryUsage(object):
    """Current Memory usage on a server.
    
    Parameters
    ----------
    total : float
        total memory resource.
    used : float
        current utilised memory resource.
    cache : float
        current cache memory usage.
    rss: float
        resident set size allocation. 
    """

    total = attrib()
    used = attrib()
    cache = attrib()
    rss = attrib()


class ServerAgentClient(BaseClient):
    """Client for the Faculty server events service.

    Build this client with a session directly.

    >>> client = ServerAgenClient(url, session)

    Parameters
    ----------
    url : str
        The URL of the object storage service.
    session : faculty.session.Session
        The session to use to make requests.
    """
    def latest_environment_execution(self):
        """Get the latest environment execution on the server.
        
        Returns
        -------
        Execution
            The latest environment execution on a server. 
        """
        return self._get("/execution/latest", _ExecutionSchema())

    def stream_server_events(self, endpoint):
        """Read the server events stream from an endpoint.
        
        Parameters
        ----------
        endpoint : str
            HTTP request endpoint.

        Yields
        ------
        ServerSentEventMessage
        """
        with self._stream(endpoint) as stream:
            for message in stream:
                yield message

    def stream_server_resources(self):
        """Stream the resources used by the server.
        
        Yields
        ------
        ServerResources
        """

        schema = _ServerResourcesSchema()
        for message in self.stream_server_events("/events"):
            if message.event == SERVER_RESOURCES_EVENT:
                yield schema.load(json.loads(message.data))

    def stream_environment_execution_step_logs(self, execution_id, step_id):
        """Read from the environment step logs.
        
        Parameters
        ----------
        execution_id : Execution.id
            ID of the environment execution.
        step_id : EnvironmentExecutionStep.id
            ID of the environment execution step.

        Yields
        ------
        EnvironmentExecutionStepLog

        """
        endpoint = "/execution/{}/executor/{}/logs".format(
            execution_id, step_id
        )
        schema = _EnvironmentExecutionStepLogSchema()
        for message in self.stream_server_events(endpoint):
            if message.event == "log":
                for line in json.loads(message.data):
                    yield schema.load(line)

    

class _EnvironmentExecutionStepLogSchema(BaseSchema):

    line_number = fields.Integer(data_key="lineNumber", required=True)
    content = fields.String(required=True)

    @post_load
    def make_environment_execution_step_log(self, data, **kwargs):
        return EnvironmentExecutionStepLog(**data)


class _EnvironmentExecutionStepSchema(BaseSchema):

    id = fields.UUID(required=True)
    command = fields.List(fields.String(required=True), required=True)
    status = EnumField(EnvironmentExecutionStepStatus, by_value=True, required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    finished_at = fields.DateTime(data_key="finishedAt", missing=None)

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
    status = EnumField(ExecutionStatus, by_value=True, required=True)
    environments = fields.List(
        fields.Nested(_EnvironmentExecutionSchema), required=True
    )
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    finished_at = fields.DateTime(data_key="finishedAt", missing=None)

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
