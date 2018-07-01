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

from marshmallow import fields, post_load
from marshmallow_enum import EnumField

from sherlockml.clients.base import BaseSchema, BaseClient


class ServerStatus(Enum):
    CREATING = 'creating'
    RUNNING = 'running'
    ERROR = 'error'
    DESTROYED = 'destroyed'


Service = namedtuple('Service', ['name', 'host', 'port', 'scheme', 'uri'])


class ServiceSchema(BaseSchema):

    name = fields.Str(required=True)
    host = fields.Str(required=True)
    port = fields.Int(required=True)
    scheme = fields.Str(required=True)
    uri = fields.Str(required=True)

    @post_load
    def make_service(self, data):
        return Service(**data)


Server = namedtuple('Server', [
    'id', 'project_id', 'owner_id', 'name', 'type', 'milli_cpus', 'memory_mb',
    'created_at', 'status', 'services'
])


class ServerSchema(BaseSchema):

    instanceId = fields.UUID(required=True)
    projectId = fields.UUID(required=True)
    ownerId = fields.UUID(required=True)
    name = fields.Str(required=True)
    instanceType = fields.Str(required=True)
    milliCpus = fields.Int(required=True)
    memoryMb = fields.Int(required=True)
    createdAt = fields.DateTime(required=True)
    status = EnumField(ServerStatus, by_value=True, required=True)
    services = fields.Nested(ServiceSchema, many=True, required=True)

    @post_load
    def make_server(self, data):
        return Server(
            id=data['instanceId'],
            project_id=data['projectId'],
            owner_id=data['ownerId'],
            name=data['name'],
            type=data['instanceType'],
            milli_cpus=data['milliCpus'],
            memory_mb=data['memoryMb'],
            created_at=data['createdAt'],
            status=data['status'],
            services=data['services']
        )


class ServerClient(BaseClient):

    SERVICE_NAME = 'galleon'

    def get(self, project_id, server_id):
        endpoint = '/instance/{}/{}'.format(project_id, server_id)
        return self._get(endpoint, ServerSchema())

    def list(self, project_id, name=None):
        endpoint = '/instance/{}'.format(project_id)
        params = {'name': name} if name is not None else None
        return self._get(endpoint, ServerSchema(many=True), params=params)
