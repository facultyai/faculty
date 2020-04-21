# Copyright 2018-2020 Faculty Science Limited
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
Manage Faculty servers.
"""


from collections import namedtuple
from enum import Enum

from marshmallow import fields, post_load, ValidationError
from marshmallow_enum import EnumField

from faculty.clients.base import BaseSchema, BaseClient


class ServerStatus(Enum):
    """Enumeration of server states."""

    CREATING = "creating"
    RUNNING = "running"
    ERROR = "error"
    DESTROYED = "destroyed"


ServerSize = namedtuple("ServerSize", ["milli_cpus", "memory_mb"])
Service = namedtuple("Service", ["name", "host", "port", "scheme", "uri"])
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


SSHDetails = namedtuple("SSHDetails", ["hostname", "port", "username", "key"])


class ServerClient(BaseClient):
    """Client for the Faculty server management service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("server")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    """

    _SERVICE_NAME = "galleon"

    def create(
        self,
        project_id,
        server_type,
        server_resources,
        name=None,
        image_version=None,
        initial_environment_ids=None,
    ):
        """Create a new server.

        Parameters
        ----------
        project_id : uuid.UUID
            The project to create the server in.
        server_type : str
            The type of server to create. Typically one of {"jupyter",
            "jupyterlab", "rstudio"}.
        server_resources : Union[SharedServerResources, \
                                 DedicatedServerResources]
            The computational resources to allocate to the server.
        name : str, optional
            A custom name to give to the server. If not provided, a random one
            will be generated.
        image_version : str, optional
            Deprecated - do not use.
        initial_environment_ids : Sequence[uuid.UUID], optional
            IDs of environments in this project to apply to the server on
            startup.

        Returns
        -------
        uuid.UUID
            The ID of the created server.
        """

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
            "/instance/{}".format(project_id), _ServerIdSchema(), json=payload
        )

    def list_for_user(self, user_id):
        """List servers owned by a user.

        Parameters
        ----------
        user_id : uuid.UUID
            The user to list servers for.

        Returns
        -------
        List[Server]
            The servers owned by the user.
        """
        endpoint = "/user/{}/instances".format(user_id)
        return self._get(endpoint, _ServerSchema(many=True))

    def delete(self, server_id):
        """Terminate a running server.

        Parameters
        ----------
        server_id : uuid.UUID
            The ID of the server to terminate.
        """
        endpoint = "/instance/{}".format(server_id)
        self._delete_raw(endpoint)

    def get(self, project_id, server_id):
        """Get information about a running server.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project that the server is in.
        server_id : uuid.UUID
            The ID of the server.

        Returns
        -------
        Server
            The retrieved server.
        """
        endpoint = "/instance/{}/{}".format(project_id, server_id)
        return self._get(endpoint, _ServerSchema())

    def list(self, project_id, name=None):
        """List servers in a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The project to list servers in.
        name : str, optional
            If provided, only return servers with this name.

        Returns
        -------
        List[Server]
            The matching servers.
        """
        endpoint = "/instance/{}".format(project_id)
        params = {"name": name} if name is not None else None
        return self._get(endpoint, _ServerSchema(many=True), params=params)

    def apply_environment(self, server_id, environment_id):
        """Apply an environment to a running server.

        Parameters
        ----------
        server_id : uuid.UUID
            The ID of the server to apply the environment to.
        environment_id : uuid.UUID
            The ID of the environment to apply.
        """
        endpoint = "/instance/{}/environment/{}".format(
            server_id, environment_id
        )
        self._put_raw(endpoint)

    def get_ssh_details(self, project_id, server_id):
        """Get an address and credentials to SSH to a running server.

        Parameters
        ----------
        project_id : uuid.UUID
            The project containing the server.
        server_id : uuid.UUID
            The server to get SSH details for.

        Returns
        -------
        SSHDetails
            The address and login credentials for the server.
        """
        endpoint = "/instance/{}/{}/ssh".format(project_id, server_id)
        return self._get(endpoint, _SSHDetailsSchema())


class _ServerSizeSchema(BaseSchema):

    milli_cpus = fields.Integer(data_key="milliCpus", required=True)
    memory_mb = fields.Integer(data_key="memoryMb", required=True)

    @post_load
    def make_server_size(self, data):
        return ServerSize(**data)


class _ServiceSchema(BaseSchema):

    name = fields.String(required=True)
    host = fields.String(required=True)
    port = fields.Integer(required=True)
    scheme = fields.String(required=True)
    uri = fields.String(required=True)

    @post_load
    def make_service(self, data):
        return Service(**data)


class _ServerSchema(BaseSchema):

    id = fields.UUID(data_key="instanceId", required=True)
    project_id = fields.UUID(data_key="projectId", required=True)
    owner_id = fields.UUID(data_key="ownerId", required=True)
    name = fields.String(required=True)
    type = fields.String(data_key="instanceType", required=True)
    server_size_type = fields.String(
        data_key="instanceSizeType", required=True
    )
    server_size = fields.Nested(_ServerSizeSchema, data_key="instanceSize")
    created_at = fields.DateTime(data_key="createdAt", required=True)
    status = EnumField(ServerStatus, by_value=True, required=True)
    services = fields.Nested(_ServiceSchema, many=True, required=True)

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


class _ServerIdSchema(BaseSchema):

    instanceId = fields.UUID(required=True)

    @post_load
    def make_server_id(self, data):
        return data["instanceId"]


class _SSHDetailsSchema(BaseSchema):

    hostname = fields.String(required=True)
    port = fields.Integer(required=True)
    username = fields.String(required=True)
    key = fields.String(required=True)

    @post_load
    def make_ssh_details(self, data):
        return SSHDetails(**data)
