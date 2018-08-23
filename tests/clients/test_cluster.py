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
    id="test-id",
    name="test-name",
    instance_group="test-instance-group",
    max_interactive_instances=10,
    max_job_instances=0,
    milli_cpus=1000,
    memory_mb=4096,
    num_gpus=0,
    gpu_name=None,
    cost_usd_per_hour=Decimal("4.2"),
)

NODE_TYPE_BODY = {
    "nodeTypeId": "test-id",
    "name": "test-name",
    "instanceGroup": "test-instance-group",
    "milliCpus": 1000,
    "memoryMb": 4096,
    "numGpus": 0,
    "gpuName": None,
    "costUsdPerHour": 4.2,
    "maxInteractiveInstances": 10,
    "maxJobInstances": 0,
}

NODE_TYPE_DEFAULT = NodeType(
    id="other-test-id",
    name=None,
    instance_group=None,
    max_interactive_instances=0,
    max_job_instances=0,
    milli_cpus=1000,
    memory_mb=4096,
    num_gpus=0,
    gpu_name=None,
    cost_usd_per_hour=Decimal("4.2"),
)

NODE_TYPE_BODY_DEFAULT = {
    "nodeTypeId": "other-test-id",
    "name": None,
    "instanceGroup": None,
    "milliCpus": 1000,
    "memoryMb": 4096,
    "numGpus": 0,
    "gpuName": None,
    "costUsdPerHour": 4.2,
    "maxInteractiveInstances": 0,
    "maxJobInstances": 0,
}


def test_node_type_schema():
    data = NodeTypeSchema().load(NODE_TYPE_BODY)
    assert data == NODE_TYPE


def test_node_type_schema_with_defaults():
    data = NodeTypeSchema().load(NODE_TYPE_BODY_DEFAULT)
    assert data == NODE_TYPE_DEFAULT


def test_node_type_schema_load_invalid():
    with pytest.raises(ValidationError):
        NodeTypeSchema().load({})


@pytest.mark.parametrize(
    "kwargs, query_params",
    [
        ({}, {}),
        (
            {"interactive_instances_configured": True},
            {"interactiveInstancesConfigured": "true"},
        ),
        (
            {"interactive_instances_configured": False},
            {"interactiveInstancesConfigured": "false"},
        ),
        (
            {"job_instances_configured": True},
            {"jobInstancesConfigured": "true"},
        ),
        (
            {"job_instances_configured": False},
            {"jobInstancesConfigured": "false"},
        ),
        (
            {
                "interactive_instances_configured": True,
                "job_instances_configured": True,
            },
            {
                "interactiveInstancesConfigured": "true",
                "jobInstancesConfigured": "true",
            },
        ),
        (
            {
                "interactive_instances_configured": True,
                "job_instances_configured": False,
            },
            {
                "interactiveInstancesConfigured": "true",
                "jobInstancesConfigured": "false",
            },
        ),
    ],
)
def test_cluster_client_list_single_tenanted_node_types(
    mocker, kwargs, query_params
):
    mocker.patch.object(ClusterClient, "_get", return_value=[NODE_TYPE])
    schema_mock = mocker.patch("sherlockml.clients.cluster.NodeTypeSchema")

    client = ClusterClient(PROFILE)
    assert client.list_single_tenanted_node_types(**kwargs) == [NODE_TYPE]

    schema_mock.assert_called_once_with(many=True)
    ClusterClient._get.assert_called_once_with(
        "/node-type/single-tenanted",
        schema_mock.return_value,
        params=query_params,
    )


def test_cluster_client_configure_single_tenanted_node_type(mocker):
    put_raw_mock = mocker.patch.object(ClusterClient, "_put_raw")

    client = ClusterClient(PROFILE)
    response = client.configure_single_tenanted_node_type(
        NODE_TYPE.id,
        NODE_TYPE.name,
        NODE_TYPE.instance_group,
        NODE_TYPE.max_interactive_instances,
        NODE_TYPE.max_job_instances,
    )

    assert response == put_raw_mock.return_value

    ClusterClient._put_raw.assert_called_once_with(
        "/node-type/single-tenanted/{}/configuration".format(NODE_TYPE.id),
        json={
            "name": NODE_TYPE.name,
            "instanceGroup": NODE_TYPE.instance_group,
            "maxInteractiveInstances": NODE_TYPE.max_interactive_instances,
            "maxJobInstances": NODE_TYPE.max_job_instances,
        },
    )


def test_cluster_client_disable_single_tenanted_node_type(mocker):
    delete_raw_mock = mocker.patch.object(ClusterClient, "_delete_raw")

    client = ClusterClient(PROFILE)
    response = client.disable_single_tenanted_node_type(NODE_TYPE.id)

    assert response == delete_raw_mock.return_value

    ClusterClient._delete_raw.assert_called_once_with(
        "/node-type/single-tenanted/{}/configuration".format(NODE_TYPE.id)
    )
