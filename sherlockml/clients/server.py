# Copyright 2018 ASI Data Science
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


from collections import namedtuple
from enum import Enum

from marshmallow import Schema, fields, post_load
from marshmallow_enum import EnumField

from sherlockml.clients.base import BaseClient


class ServerStatus(Enum):
    CREATING = "creating"
    RUNNING = "running"
    ERROR = "error"
    DESTROYED = "destroyed"


Service = namedtuple("Service", ["name", "host", "port", "scheme", "uri"])


class ServiceSchema(Schema):

    name = fields.Str(required=True)
    host = fields.Str(required=True)
    port = fields.Int(required=True)
    scheme = fields.Str(required=True)
    uri = fields.Str(required=True)

    @post_load
    def make_service(self, data):
        return Service(**data)


Server = namedtuple(
    "Server",
    [
        "id",
        "project_id",
        "owner_id",
        "name",
        "type",
        "milli_cpus",
        "memory_mb",
        "created_at",
        "status",
        "services",
    ],
)


class ServerSchema(Schema):

    id = fields.UUID(data_key="instanceId", required=True)
    project_id = fields.UUID(data_key="projectId", required=True)
    owner_id = fields.UUID(data_key="ownerId", required=True)
    name = fields.Str(required=True)
    type = fields.Str(data_key="instanceType", required=True)
    milli_cpus = fields.Int(data_key="milliCpus", required=True)
    memory_mb = fields.Int(data_key="memoryMb", required=True)
    created_at = fields.DateTime(data_key="createdAt", required=True)
    status = EnumField(ServerStatus, by_value=True, required=True)
    services = fields.Nested(ServiceSchema, many=True, required=True)

    @post_load
    def make_server(self, data):
        return Server(**data)


class ServerIdSchema(Schema):

    instanceId = fields.UUID(required=True)

    @post_load
    def make_server_id(self, data):
        return data["instanceId"]


class ServerClient(BaseClient):

    SERVICE_NAME = "galleon"

    def create(
        self,
        project_id,
        server_type,
        milli_cpus,
        memory_mb,
        name=None,
        image_version=None,
        initial_environment_ids=None,
    ):

        payload = {
            "instanceType": server_type,
            "milliCpus": milli_cpus,
            "memoryMb": memory_mb,
        }
        if name:
            payload["name"] = name
        if image_version:
            payload["typeVersion"] = image_version
        if initial_environment_ids:
            payload["environmentIds"] = [
                str(env_id) for env_id in initial_environment_ids
            ]

        return self._post(
            "/instance/{}".format(project_id), ServerIdSchema(), json=payload
        )

    def get(self, project_id, server_id):
        endpoint = "/instance/{}/{}".format(project_id, server_id)
        return self._get(endpoint, ServerSchema())

    def list(self, project_id, name=None):
        endpoint = "/instance/{}".format(project_id)
        params = {"name": name} if name is not None else None
        return self._get(endpoint, ServerSchema(many=True), params=params)
