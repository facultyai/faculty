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

from marshmallow import Schema, fields, post_load

from sherlockml.clients.base import BaseClient


NodeType = namedtuple(
    "NodeType",
    [
        "id",
        "name",
        "instance_group",
        "max_interactive_instances",
        "max_job_instances",
        "milli_cpus",
        "memory_mb",
        "num_gpus",
        "gpu_name",
        "cost_usd_per_hour",
    ],
)


class NodeTypeSchema(Schema):

    id = fields.String(data_key="nodeTypeId", required=True)
    name = fields.String(missing=None)
    instance_group = fields.String(data_key="instanceGroup", missing=None)
    max_interactive_instances = fields.Integer(
        data_key="maxInteractiveInstances", required=True
    )
    max_job_instances = fields.Integer(
        data_key="maxJobInstances", required=True
    )
    milli_cpus = fields.Integer(data_key="milliCpus", required=True)
    memory_mb = fields.Integer(data_key="memoryMb", required=True)
    num_gpus = fields.Integer(data_key="numGpus", required=True)
    gpu_name = fields.String(data_key="gpuName", missing=None)
    cost_usd_per_hour = fields.Decimal(
        data_key="costUsdPerHour", required=True
    )

    @post_load
    def make_node_type(self, data):
        return NodeType(**data)


class ClusterClient(BaseClient):

    SERVICE_NAME = "klostermann"

    def list_single_tenanted_node_types(
        self,
        interactive_instances_configured=None,
        job_instances_configured=None,
    ):
        """Get information on single tenanted node types from the cluster.

        Parameters
        ----------
        interactive_instances_configured : bool, optional
            If True, only get node types which are configured to support
            interactive instances, or if False, those which are not configured
            to support interactive instances
        job_instances_configured : bool, optional
            If True, only get node types which are configured to support job
            instances, or if False, those which are not configured to support
            job instances

        Returns
        -------
        list of NodeType
        """

        query_params = {}
        if interactive_instances_configured is not None:
            query_params["interactiveInstancesConfigured"] = (
                "true" if interactive_instances_configured else "false"
            )
        if job_instances_configured is not None:
            query_params["jobInstancesConfigured"] = (
                "true" if job_instances_configured else "false"
            )

        return self._get(
            "/node-type/single-tenanted",
            NodeTypeSchema(many=True),
            params=query_params,
        )

    def configure_single_tenanted_node_type(
        self,
        node_type_id,
        name,
        instance_group,
        max_interactive_instances,
        max_job_instances,
    ):
        payload = {
            "name": name,
            "instanceGroup": instance_group,
            "maxInteractiveInstances": max_interactive_instances,
            "maxJobInstances": max_job_instances,
        }
        return self._put_raw(
            "/node-type/single-tenanted/{}/configuration".format(node_type_id),
            json=payload,
        )

    def disable_single_tenanted_node_type(self, node_type_id):
        return self._delete_raw(
            "/node-type/single-tenanted/{}/configuration".format(node_type_id)
        )
