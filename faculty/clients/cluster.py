# Copyright 2018-2021 Faculty Science Limited
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
Manage the Faculty cluster configuration.
"""


from attr import attrs, attrib
from marshmallow import fields, post_load

from faculty.clients.base import BaseSchema, BaseClient


@attrs
class NodeType:
    """A single tenanted node type in the platform.

    Parameters
    ----------
    id : str
        A unique identifier for the node type, usually the cloud provider's
        machine type.
    name : str
        A short descriptive name for the node type, usually the same as the id.
    instance_group : str
        The Kubernetes instance group that provides this node type.
    max_interactive_instances : int
        The maximum number of instances of this node type that can be used for
        workspace servers, apps and APIs at any given time.
    max_job_instances : int
        The maximum number of instances of this node type that can be used for
        jobs at any given time.
    milli_cpus : int
        The amount of CPU resource available to servers running on nodes of
        this type.
    memory_mb : int
        The amount of memory available to servers running on nodes of this
        type.
    num_gpus : int
        The number of GPUs available to servers running on nodes of this type.
    gpu_name : str, optional
        A descriptive name of the type of GPUs available on this node type,
        when available.
    cost_usd_per_hour : decimal.Decimal
        The on-demand hourly cost for this node type, in USD.
    spot_max_usd_per_hour : decimal.Decimal, optional
        The bid price set for this node type, when using spot instances. The
        actual cost will usually be lower than this, but the bid price sets the
        maximum cost (if the spot price increases beyond this price, AWS will
        shut the instances down). If unset, indicates that the node type is
        configured to run with on demand instances, and the `cost_usd_per_hour`
        will be charged.
    """

    id = attrib()
    name = attrib()
    instance_group = attrib()
    max_interactive_instances = attrib()
    max_job_instances = attrib()
    milli_cpus = attrib()
    memory_mb = attrib()
    num_gpus = attrib()
    gpu_name = attrib()
    cost_usd_per_hour = attrib()
    spot_max_usd_per_hour = attrib()


class ClusterClient(BaseClient):
    """Client for the Faculty cluster configuration service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("cluster")

    Parameters
    ----------
    url : str
        The URL of the cluster configuration service.
    session : faculty.session.Session
        The session to use to make requests.
    """

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
            _NodeTypeSchema(many=True),
            params=query_params,
        )

    def configure_single_tenanted_node_type(
        self,
        node_type_id,
        name,
        instance_group,
        max_interactive_instances,
        max_job_instances,
        spot_max_usd_per_hour=None,
    ):
        """Configure a single tenanted node type on the cluster.

        If this node type is already configured, this will update the
        configuration, otherwise it will be added to those available.

        Parameters
        ----------
        node_type_id : str
            The ID of the node type to configure, e.g. m4.xlarge on AWS or
            n1-standard-4 on GCP.
        name : str
            A name for this node type. Usually the same as the node type ID.
        instance_group : str
            The Kubernetes instance group that provides this node type. The
            instance group must already exist.
        max_interactive_instances : int
            The maximum number of interactive servers (Jupyter, RStudio, apps,
            APIs) using this node type to allow to run simultaneously.
        max_job_instances : int
            The maximum number of jobs runs using this node type to allow to
            run simultaneously.
        spot_max_usd_per_hour : decimal.Decimal, optional
            The bid price in USD, when this instance group is configured to run
            on spot instances.
        """
        payload = {
            "name": name,
            "instanceGroup": instance_group,
            "maxInteractiveInstances": max_interactive_instances,
            "maxJobInstances": max_job_instances,
            "spotMaxUsdPerHour": (
                None
                if spot_max_usd_per_hour is None
                else str(spot_max_usd_per_hour)
            ),
        }
        self._put_raw(
            "/node-type/single-tenanted/{}/configuration".format(node_type_id),
            json=payload,
        )

    def disable_single_tenanted_node_type(self, node_type_id):
        """Disable a single tenanted node type on the cluster.

        Parameters
        ----------
        node_type_id : str
            The ID of the node type to disable, e.g. m4.xlarge on AWS or
            n1-standard-4 on GCP.
        """
        self._delete_raw(
            "/node-type/single-tenanted/{}/configuration".format(node_type_id)
        )


class _NodeTypeSchema(BaseSchema):

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
    spot_max_usd_per_hour = fields.Decimal(
        data_key="spotMaxUsdPerHour", missing=None
    )

    @post_load
    def make_node_type(self, data, **kwargs):
        return NodeType(**data)
