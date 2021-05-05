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

import uuid
from datetime import datetime
import json

from pytz import UTC

from faculty.clients.serveragent import (
    EnvironmentExecutionStepLogLine,
    _EnvironmentExecutionStepLogLineSchema,
    EnvironmentExecutionStepStatus,
    EnvironmentExecutionStep,
    _EnvironmentExecutionStepSchema,
    EnvironmentExecution,
    _EnvironmentExecutionSchema,
    ExecutionStatus,
    Execution,
    _ExecutionSchema,
    CpuUsage,
    _CpuUsageSchema,
    MemoryUsage,
    _MemoryUsageSchema,
    ServerResources,
    _ServerResourcesSchema,
    ServerAgentClient,
)
from faculty.clients.base import ServerSentEventMessage

CPU_USAGE = CpuUsage(
    total=200,
    used=50,
)

CPU_USAGE_BODY = {
    "total": CPU_USAGE.total,
    "used": CPU_USAGE.used,
}

MEMORY_USAGE = MemoryUsage(
    total=3814.6953125,
    used=2444.70703125,
    cache=1673.73046875,
    rss=463.75390625,
)

MEMORY_USAGE_BODY = {
    "total": MEMORY_USAGE.total,
    "used": MEMORY_USAGE.used,
    "cache": MEMORY_USAGE.cache,
    "rss": MEMORY_USAGE.rss,
}

SERVER_RESOURCES = ServerResources(
    milli_cpus=CPU_USAGE,
    memory_mb=MEMORY_USAGE,
)

SERVER_RESOURCES_BODY = {
    "milliCpus": CPU_USAGE_BODY,
    "memoryMB": MEMORY_USAGE_BODY,
}

ENVIRONMENT_EXECUTION_STEP_LOG_LINE = EnvironmentExecutionStepLogLine(
    line_number=5,
    content="test content",
)

ENVIRONMENT_EXECUTION_STEP_LOG_LINE_BODY = {
    "lineNumber": ENVIRONMENT_EXECUTION_STEP_LOG_LINE.line_number,
    "content": ENVIRONMENT_EXECUTION_STEP_LOG_LINE.content,
}

STARTED_AT = datetime(2021, 4, 13, 13, 29, 6, 466633, tzinfo=UTC)
STARTED_AT_STRING = "2021-04-13T13:29:06.466633+00:00"
FINISHED_AT = datetime(2021, 4, 13, 13, 29, 14, 271353, tzinfo=UTC)
FINISHED_AT_STRING = "2021-04-13T13:29:14.271353+00:00"

ENVIRONMENT_EXECUTION_STEP = EnvironmentExecutionStep(
    id=uuid.uuid4(),
    command=["test command 1", "test command 2"],
    status=EnvironmentExecutionStepStatus.SUCCESS,
    started_at=STARTED_AT,
    finished_at=FINISHED_AT,
)

ENVIRONMENT_EXECUTION_STEP_BODY = {
    "id": str(ENVIRONMENT_EXECUTION_STEP.id),
    "command": ENVIRONMENT_EXECUTION_STEP.command,
    "status": ENVIRONMENT_EXECUTION_STEP.status,
    "startedAt": STARTED_AT_STRING,
    "finishedAt": FINISHED_AT_STRING,
}

ENVIRONMENT_EXECUTION = EnvironmentExecution(
    id=uuid.uuid4(), steps=[ENVIRONMENT_EXECUTION_STEP]
)

ENVIRONMENT_EXECUTION_BODY = {
    "environmentId": str(ENVIRONMENT_EXECUTION.id),
    "steps": [ENVIRONMENT_EXECUTION_STEP_BODY],
}


EXECUTION = Execution(
    id=uuid.uuid4(),
    status=ExecutionStatus.STARTED,
    environments=[ENVIRONMENT_EXECUTION],
    started_at=STARTED_AT,
    finished_at=FINISHED_AT,
)

EXECUTION_BODY = {
    "executionId": str(EXECUTION.id),
    "status": EXECUTION.status,
    "environments": [ENVIRONMENT_EXECUTION_BODY],
    "startedAt": STARTED_AT_STRING,
    "finishedAt": FINISHED_AT_STRING,
}

SSE_RESOURCE_EVENT = "@SSE/SERVER_RESOURCES_UPDATED"
SSE_RESOURCE_MESSAGES = [
    ServerSentEventMessage(
        _id, SSE_RESOURCE_EVENT, json.dumps(SERVER_RESOURCES_BODY)
    )
    for _id in range(5)
]

SSE_LOG_EVENT = "log"
SSE_LOG_MESSAGES = [
    ServerSentEventMessage(
        _id,
        SSE_LOG_EVENT,
        json.dumps([ENVIRONMENT_EXECUTION_STEP_LOG_LINE_BODY]),
    )
    for _id in range(5)
]


def test_cpu_usage_schema():
    data = _CpuUsageSchema().load(CPU_USAGE_BODY)
    assert data == CPU_USAGE


def test_memory_usage_schema():
    data = _MemoryUsageSchema().load(MEMORY_USAGE_BODY)
    assert data == MEMORY_USAGE


def test_server_resources_schema():
    data = _ServerResourcesSchema().load(SERVER_RESOURCES_BODY)
    assert data == SERVER_RESOURCES


def test_environment_execution_step_log_schema():
    data = _EnvironmentExecutionStepLogLineSchema().load(
        ENVIRONMENT_EXECUTION_STEP_LOG_LINE_BODY
    )
    assert data == ENVIRONMENT_EXECUTION_STEP_LOG_LINE


def test_environment_execution_step_schema():
    data = _EnvironmentExecutionStepSchema().load(
        ENVIRONMENT_EXECUTION_STEP_BODY
    )
    assert data == ENVIRONMENT_EXECUTION_STEP


def test_environment_execution_schema():
    data = _EnvironmentExecutionSchema().load(ENVIRONMENT_EXECUTION_BODY)
    assert data == ENVIRONMENT_EXECUTION


def test_execution_schema():
    data = _ExecutionSchema().load(EXECUTION_BODY)
    assert data == EXECUTION


def test_client_latest_environment_execution(mocker):
    mocker.patch.object(ServerAgentClient, "_get", return_value=EXECUTION)

    client = ServerAgentClient(mocker.Mock(), mocker.Mock())
    assert client.latest_environment_execution() == EXECUTION


def test_client_stream_server_resources(mocker):
    mocker.patch.object(
        ServerAgentClient,
        "_stream_server_sent_events",
        return_value=SSE_RESOURCE_MESSAGES,
    )

    client = ServerAgentClient(mocker.Mock(), mocker.Mock())
    for client_sse, mock_sse in zip(
        client.stream_server_resources(), SSE_RESOURCE_MESSAGES
    ):
        assert client_sse == _ServerResourcesSchema().load(
            json.loads(mock_sse.data)
        )


def test_client_stream_environment_execution_step_logs(mocker):
    mocker.patch.object(
        ServerAgentClient,
        "_stream_server_sent_events",
        return_value=SSE_LOG_MESSAGES,
    )

    client = ServerAgentClient(mocker.Mock(), mocker.Mock())
    client_logs = client.stream_environment_execution_step_logs(
        "execution_id", "step_id"
    )
    for mock_logs in SSE_LOG_MESSAGES:
        for client_line, mock_line in zip(
            client_logs, json.loads(mock_logs.data)
        ):
            assert (
                client_line
                == _EnvironmentExecutionStepLogLineSchema().load(mock_line)
            )
