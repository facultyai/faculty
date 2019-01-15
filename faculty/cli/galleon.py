"""Interact with Galleon."""

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

import dateutil.parser

import faculty.cli.client
import faculty.cli.config


class GalleonError(faculty.cli.client.FacultyServiceError):
    """Exception for errors interacting with Galleon."""

    pass


class Server(object):
    """A Faculty server."""

    # pylint: disable=too-few-public-methods

    def __init__(
        self,
        id_,
        project_id,
        owner_id,
        name,
        type_,
        machine_type,
        milli_cpus,
        memory_mb,
        created_at,
        status,
        services,
    ):
        self.id_ = id_
        self.project_id = project_id
        self.owner_id = owner_id
        self.name = name
        self.type_ = type_
        self.machine_type = machine_type
        self.milli_cpus = milli_cpus
        self.memory_mb = memory_mb
        self.created_at = created_at
        self.status = status
        self.services = services

        if machine_type == "custom":
            if milli_cpus is None or memory_mb is None:
                raise ValueError(
                    "milli_cpus and memory_mb cannot be None "
                    "if machine_type is custom"
                )
        else:
            if milli_cpus is not None or memory_mb is not None:
                raise ValueError(
                    "milli_cpus and memory_mb must be None "
                    "if machine_type is not custom"
                )

    def __repr__(self):
        template = (
            "Server(id_={}, project_id={}, owner_id={}, name={}, "
            "type_={}, machine_type = {}, milli_cpus={}, memory_mb={}, "
            "created_at={}, status={}, services={})"
        )
        return template.format(
            self.id_,
            self.project_id,
            self.owner_id,
            self.name,
            self.type_,
            self.machine_type,
            self.milli_cpus,
            self.memory_mb,
            self.created_at,
            self.status,
            self.services,
        )

    def _get_service(self, name):
        for service in self.services:
            if service.name == name:
                return service
        raise RuntimeError("cube has no service called {}".format(name))

    @property
    def hound_url(self):
        service = self._get_service("hound")
        return "{}://{}:{}".format(service.scheme, service.host, service.port)

    @classmethod
    def from_json(cls, json_object):
        services = [Service.from_json(o) for o in json_object["services"]]

        try:
            # galleon is on latest version
            machine_type = json_object["instanceSizeType"]
            if machine_type == "custom":
                milli_cpus = json_object["instanceSize"]["milliCpus"]
                memory_mb = json_object["instanceSize"]["memoryMb"]
            else:
                milli_cpus = None
                memory_mb = None
        except KeyError:
            # galleon is on pre-single-tenancy version
            machine_type = "custom"
            milli_cpus = json_object["milliCpus"]
            memory_mb = json_object["memoryMb"]

        return cls(
            json_object["instanceId"],
            json_object["projectId"],
            json_object["ownerId"],
            json_object["name"],
            json_object["instanceType"],
            machine_type,
            milli_cpus,
            memory_mb,
            dateutil.parser.parse(json_object["createdAt"]),
            json_object["status"],
            services,
        )


class Service(object):
    def __init__(self, name, host, port, scheme):
        self.name = name
        self.host = host
        self.port = port
        self.scheme = scheme

    def __repr__(self):
        return "Service(name={}, host={}, port={}, scheme={})".format(
            self.name, self.host, self.port, self.scheme
        )

    @classmethod
    def from_json(cls, json_object):
        return cls(
            json_object["name"],
            json_object["host"],
            json_object["port"],
            json_object["scheme"],
        )


class Galleon(faculty.cli.client.FacultyService):
    """A Galleon client."""

    def __init__(self):
        super(Galleon, self).__init__(faculty.cli.config.galleon_url())

    def get_all_servers(self):
        """List all Faculty servers known to Galleon.

        This method requires administrative privileges not available to normal
        users.
        """
        resp = self._get("/instance")
        servers = [Server.from_json(o) for o in resp.json()]
        return servers

    def get_servers(self, project_id, name=None, status=None):
        """List servers in the given project."""
        params = {"name": name} if name is not None else None
        resp = self._get("/instance/{}".format(project_id), params=params)
        servers = [Server.from_json(o) for o in resp.json()]
        if status is not None:
            servers = [s for s in servers if s.status == status]
        return servers

    def get_server(self, project_id, server_id):
        """Get a server by its id."""
        resp = self._get("/instance/{}/{}".format(project_id, server_id))
        return Server.from_json(resp.json())

    def create_server(
        self,
        project_id,
        type_,
        machine_type="custom",
        milli_cpus=None,
        memory_mb=None,
        name=None,
        type_version=None,
        environment_ids=None,
    ):
        """Create a new Faculty server."""

        if machine_type == "custom":
            if milli_cpus is None or memory_mb is None:
                raise ValueError(
                    "milli_cpus and memory_mb cannot be None "
                    "if machine_type is custom"
                )
            instance_size = {"memoryMb": memory_mb, "milliCpus": milli_cpus}
        else:
            if milli_cpus is not None or memory_mb is not None:
                raise ValueError(
                    "milli_cpus and memory_mb must be None "
                    "if machine_type is not custom"
                )
            instance_size = None

        payload = {
            "instanceType": type_,
            "instanceSizeType": machine_type,
            "instanceSize": instance_size,
        }

        if name:
            payload["name"] = name
        if type_version:
            payload["typeVersion"] = type_version
        if environment_ids:
            payload["environmentIds"] = environment_ids
        resp = self._post("/instance/{}".format(project_id), payload=payload)
        try:
            id_ = resp.json()["instanceId"]
        except KeyError:
            raise GalleonError("Server created but could not retrieve ID")
        return id_

    def terminate_server(self, id_):
        """Terminate the given server."""
        return self._delete("/instance/{}".format(id_))

    def ssh_details(self, project_id, id_):
        """Get SSH login details for the given server in the given project."""
        resp = self._get("/instance/{}/{}/ssh".format(project_id, id_))
        return resp.json()

    def apply_environment(self, id_, environment_id):
        """Apply environment to server"""
        self._put("/instance/{}/environment/{}".format(id_, environment_id))
