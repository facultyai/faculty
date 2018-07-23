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


from decimal import Decimal

import pytest
from marshmallow import ValidationError

from sherlockml.clients.cluster import NodeType, NodeTypeSchema, ClusterClient
from tests.clients.fixtures import PROFILE


NODE_TYPE = NodeType(
    id='test-id',
    name='test-name',
    instance_group='test-instance-group',
    max_interactive_instances=10,
    max_job_instances=0,
    milli_cpus=1000,
    memory_mb=4096,
    num_gpus=0,
    gpu_name=None,
    cost_usd_per_hour=Decimal('4.2')
)

NODE_TYPE_BODY = {
    'nodeTypeId': 'test-id',
    'name': 'test-name',
    'instanceGroup': 'test-instance-group',
    'milliCpus': 1000,
    'memoryMb': 4096,
    'numGpus': 0,
    'gpuName': None,
    'costUsdPerHour': 4.2,
    'maxInteractiveInstances': 10,
    'maxJobInstances': 0
}


def test_node_type_schema():
    data, _ = NodeTypeSchema().load(NODE_TYPE_BODY)
    assert data == NODE_TYPE


def test_node_type_schema_load_invalid():
    with pytest.raises(ValidationError):
        NodeTypeSchema().load({})


def test_cluster_client_list_single_tenanted_node_types(mocker):
    mocker.patch.object(ClusterClient, '_get', return_value=[NODE_TYPE])
    schema_mock = mocker.patch('sherlockml.clients.cluster.NodeTypeSchema')

    client = ClusterClient(PROFILE)
    assert client.list_single_tenanted_node_types() == [NODE_TYPE]

    schema_mock.assert_called_once_with(many=True)
    ClusterClient._get.assert_called_once_with(
        '/node-type/single-tenanted', schema_mock.return_value
    )
