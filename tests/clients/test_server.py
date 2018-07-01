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


import uuid
from datetime import datetime

import pytest
from marshmallow import ValidationError
from dateutil.tz import UTC

from sherlockml.clients.server import (
    Server, ServerStatus, Service, ServerSchema, ServiceSchema
)


SERVICE = Service(
    name='hound',
    host='server-99999-hound.domain.com',
    port=443,
    scheme='https',
    uri='https://server-99999-hound.domain.com:443'
)

SERVICE_BODY = {
    'name': SERVICE.name,
    'host': SERVICE.host,
    'port': SERVICE.port,
    'scheme': SERVICE.scheme,
    'uri': SERVICE.uri
}

CREATED_AT = datetime(2018, 3, 10, 11, 32, 6, 247000, tzinfo=UTC)
CREATED_AT_STRING = '2018-03-10T11:32:06.247Z'

SERVER = Server(
    id=uuid.uuid4(),
    project_id=uuid.uuid4(),
    owner_id=uuid.uuid4(),
    name='test server',
    type='jupyter',
    milli_cpus=1000,
    memory_mb=4096,
    created_at=CREATED_AT,
    status=ServerStatus.RUNNING,
    services=[SERVICE]
)

SERVER_BODY = {
    'instanceId': str(SERVER.id),
    'projectId': str(SERVER.project_id),
    'ownerId': str(SERVER.owner_id),
    'name': SERVER.name,
    'instanceType': SERVER.type,
    'milliCpus': SERVER.milli_cpus,
    'memoryMb': SERVER.memory_mb,
    'createdAt': CREATED_AT_STRING,
    'status': 'running',
    'services': [SERVICE_BODY]
}


def test_service_schema():
    data, _ = ServiceSchema().load(SERVICE_BODY)
    assert data == SERVICE


def test_service_schema_invalid():
    with pytest.raises(ValidationError):
        ServiceSchema().load({})


def test_server_schema():
    data, _ = ServerSchema().load(SERVER_BODY)
    assert data == SERVER


def test_server_schema_invalid():
    with pytest.raises(ValidationError):
        ServerSchema().load({})
