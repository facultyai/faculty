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

from marshmallow import Schema, fields, post_load, ValidationError
from marshmallow_enum import EnumField

from sherlockml.clients.base import BaseClient


ServerSize = namedtuple("ServerSize", ["milli_cpus", "memory_mb"])


class ServerSizeSchema(Schema):

    milli_cpus = fields.Integer(data_key="milliCpus", required=True)
    memory_mb = fields.Integer(data_key="memoryMb", required=True)

    @post_load
    def make_server_size(self, data):
        return ServerSize(**data)


class ServerStatus(Enum):
    CREATING = "creating"
    RUNNING = "running"
    ERROR = "error"
    DESTROYED = "destroyed"


Service = namedtuple("Service", ["name", "host", "port", "scheme", "uri"])


class ServiceSchema(Schema):

    name = fields.String(required=True)
    host = fields.String(required=True)
    port = fields.Integer(required=True)
    scheme = fields.String(required=True)
    uri = fields.String(required=True)

    @post_load
    def make_service(self, data):
        return Service(**data)


SharedServerResources = namedtuple(
    "SharedServerResources", ["milli_cpus", "memory_mb"]
)
DedicatedServerResources = namedtuple(
    "DedicatedServerResources", ["node_type"]
)
Server = namedtuple(
    "Server",
    [
        "id",
        "project_id",
        "owner_id",
        "name",
        "type",
        "resources",
        "created_at",
        "status",
        "services",
    ],
)


class ServerSchema(Schema):

    id = fields.UUID(data_key="instanceId", required=True)
    project_id = fields.UUID(data_key="projectId", required=True)
    owner_id = fields.UUID(data_key="ownerId", required=True)
    name = fields.String(required=True)
    type = fields.String(data_key="instanceType", required=True)
    server_size_type = fields.String(
        data_key="instanceSizeType", required=True
    )
    server_size = fields.Nested(ServerSizeSchema, data_key="instanceSize")
    created_at = fields.DateTime(data_key="createdAt", required=True)
    status = EnumField(ServerStatus, by_value=True, required=True)
    services = fields.Nested(ServiceSchema, many=True, required=True)

    @post_load
    def make_server(self, data):

        server_size_type = data["server_size_type"]
        server_size = data.get("server_size")

        if server_size_type == "custom":
            if server_size is not None:
                server_resources = SharedServerResources(
                    milli_cpus=server_size.milli_cpus,
                    memory_mb=server_size.memory_mb,
                )
            else:
                raise ValidationError(
                    "server_size must be provided for custom server_size_type"
                )
        else:
            server_resources = DedicatedServerResources(server_size_type)

        return Server(
            id=data["id"],
            project_id=data["project_id"],
            owner_id=data["owner_id"],
            name=data["name"],
            type=data["type"],
            resources=server_resources,
            created_at=data["created_at"],
            status=data["status"],
            services=data["services"],
        )


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
        server_resources,
        name=None,
        image_version=None,
        initial_environment_ids=None,
    ):

        payload = {"instanceType": server_type}

        if isinstance(server_resources, SharedServerResources):
            payload["instanceSizeType"] = "custom"
            payload["instanceSize"] = {
                "milliCpus": server_resources.milli_cpus,
                "memoryMb": server_resources.memory_mb,
            }
        elif isinstance(server_resources, DedicatedServerResources):
            payload["instanceSizeType"] = server_resources.node_type
        else:
            raise ValueError(
                "Invalid server_resources {}".format(server_resources)
            )

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
