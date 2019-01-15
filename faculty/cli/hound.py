"""Interact with Hound."""

# Copyright 2016-2018 ASI Data Science
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


import requests

import faculty.cli.client
import faculty.cli.config

SERVER_RESOURCES_EVENT = "@SSE/SERVER_RESOURCES_UPDATED"


class HoundError(faculty.cli.client.FacultyServiceError):
    pass


class CpuUsage(object):
    """Current CPU usage on a server."""

    def __init__(self, used, total):
        self.used = used
        self.total = total

    @classmethod
    def from_json(cls, json_object):
        return cls(json_object["used"], json_object["total"])


class MemoryUsage(object):
    """Current memory usage on a server."""

    def __init__(self, used, rss, cache, total):
        self.used = used
        self.rss = rss
        self.cache = cache
        self.total = total

    @classmethod
    def from_json(cls, json_object):
        return cls(
            json_object["used"],
            json_object["rss"],
            json_object["cache"],
            json_object["total"],
        )


class ServerResources(object):
    """Information about current server resource usage."""

    def __init__(self, milli_cpus, memory_mb):
        self.milli_cpus = milli_cpus
        self.memory_mb = memory_mb

    @classmethod
    def from_json(cls, json_object):
        return cls(
            CpuUsage.from_json(json_object["milliCpus"]),
            MemoryUsage.from_json(json_object["memoryMB"]),
        )


class Execution(object):
    """A server environment execution."""

    def __init__(self, status, environments):
        self.status = status
        self.environments = environments

    @classmethod
    def from_json(cls, json_object):
        return cls(
            json_object["status"].lower(),
            [
                EnvironmentExecution.from_json(environment_json)
                for environment_json in json_object["environments"]
            ],
        )


class EnvironmentExecution(object):
    """A server environment execution."""

    def __init__(self, steps):
        self.steps = steps

    @classmethod
    def from_json(cls, json_object):
        return cls(
            [
                EnvironmentExecutionStep.from_json(step_json)
                for step_json in json_object["steps"]
            ]
        )


class EnvironmentExecutionStep(object):
    """A step of a server environment execution."""

    def __init__(self, command, status, log_path):
        self.command = command
        self.status = status
        self.log_path = log_path

    @classmethod
    def from_json(cls, json_object):
        return cls(
            json_object["command"],
            json_object["status"].lower(),
            json_object["logUriPath"],
        )


class Hound(faculty.cli.client.FacultyService):
    """A Hound client."""

    def __init__(self, hound_url):
        super(Hound, self).__init__(hound_url, cookie_auth=True)

    def stream_server_events(self):
        """Read from the Hound events stream."""
        with self._stream("/events") as stream:
            for message in stream:
                yield message

    def stream_server_resources(self):
        """Stream the resources used by the server."""
        for message in self.stream_server_events():
            if message.event == SERVER_RESOURCES_EVENT:
                yield ServerResources.from_json(message.data)

    def latest_environment_execution(self):
        """Get the latest environment execution on the server."""
        try:
            resp = self._get("/execution/latest")
        except faculty.cli.client.FacultyServiceError as err:
            if err.status_code == 404:
                return None
            else:
                raise
        return Execution.from_json(resp.json())

    def stream_environment_execution_step_logs(self, execution_step):
        """Stream the logs of an environment execution step."""
        last_line = -1
        while True:
            try:
                with self._stream(execution_step.log_path) as stream:
                    for message in stream:
                        if message.event.lower() == "finished":
                            break
                        elif message.event.lower() == "log":
                            for line in message.data:
                                if line["lineNumber"] > last_line:
                                    last_line = line["lineNumber"]
                                    yield line["content"]
            except requests.exceptions.ChunkedEncodingError:
                continue
            else:
                break
