from decimal import Decimal

import pytest
from marshmallow import ValidationError

from sherlockml.clients.cluster import NodeType, NodeTypeSchema


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


def test_project_schema_load_invalid():
    with pytest.raises(ValidationError):
        NodeTypeSchema().load({})
