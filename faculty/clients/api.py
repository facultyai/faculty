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
Manage Faculty APIs.
"""
from collections import namedtuple
from enum import Enum

from marshmallow import fields, post_load
from marshmallow_enum import EnumField

from faculty.clients.base import BaseClient, BaseSchema
from faculty.clients.server import _ServerSchema as ServerSchema

Instance = namedtuple("Instance", ["instance", "outdated", "key"])
InstanceSize = namedtuple("InstanceSize", ["milli_cpus", "memory_mb"])
DevInstance = namedtuple("DevInstance", ["instance", "key", "instance_id"])
DevInstanceId = namedtuple("DevInstanceId", ["instance_id"])
ResourceLimit = namedtuple(
    "ResourceLimit",
    [
        "allowed_max_milli_cpus",
        "allowed_max_memory_mb",
        "remaining_milli_cpus",
        "remaining_memory_mb",
    ],
)
APIInstance = namedtuple(
    "APIInstance",
    [
        "project_id",
        "api_id",
        "error_code",
        "error",
        "remaining_resource_limits",
    ],
)

APIKey = namedtuple("APIKey", ["id", "material", "enabled", "label"])
ServerType = namedtuple("ServerType", ["instance_size_type", "instance_size"])
WSGIDefinition = namedtuple(
    "WSGIDefinition",
    [
        "api_type",
        "working_directory",
        "module",
        "wsgi_object",
    ],
)
PlumberDefinition = namedtuple(
    "PlumberDefinition", ["api_type", "working_directory", "script_name"]
)
ScriptDefinition = namedtuple(
    "ScriptDefinition", ["api_type", "working_directory", "script_name"]
)

APIUpdate = namedtuple(
    "APIUpdate", ["definition", "environment_ids", "default_server_size"]
)

APIDefinition = namedtuple(
    "APIDefinition", ["name", "subdomain", "description", "last_updated_at"]
)

API = namedtuple(
    "API",
    [
        "author_id",
        "created_at",
        "default_server_size",
        "definition",
        "deployment_status",
        "description",
        "dev_instances",
        "environment_ids",
        "id",
        "last_deployed_at",
        "last_deployed_by",
        "name",
        "prod_instances",
        "prod_keys",
        "subdomain",
    ],
)


class _APIKeySchema(BaseSchema):
    id = fields.UUID(data_key="keyId", required=True)
    material = fields.String(required=True)
    # Development instance keys do not have these fields
    enabled = fields.Boolean(load_default=None)
    label = fields.String(load_default=None)

    @post_load
    def make_apikey(self, data, **kwargs):
        return APIKey(**data)


class RemainingResourceLimitSchema(BaseSchema):
    allowed_max_milli_cpus = fields.Integer(
        data_key="allowedMaxMilliCpus", required=True
    )
    allowed_max_memory_mb = fields.Integer(
        data_key="allowedMaxMemoryMb", required=True
    )
    remaining_milli_cpus = fields.Integer(
        data_key="remainingMilliCpus", required=True
    )
    remaining_memory_mb = fields.Integer(
        data_key="remaininigMemoryMb", required=True
    )


class DevInstanceIdSchema(BaseSchema):
    instance_id = fields.UUID(data_key="instanceId")


class APIInstanceResponseSchema(BaseSchema):
    project_id = fields.UUID(data_key="projectId")
    api_id = fields.UUID(data_key="apiId")
    error_code = fields.String(data_key="errorCode")
    error = fields.String()
    remaining_resource_limits = fields.Nested(
        RemainingResourceLimitSchema, data_key="remainingResourceLimits"
    )


class APIDevInstanceResponseSchema(APIInstanceResponseSchema):
    instance = fields.Nested(DevInstanceIdSchema)
    key = fields.Nested(_APIKeySchema)
    instance_id = fields.UUID(data_key="instanceId")


class InstanceSizeSchema(BaseSchema):
    milli_cpus = fields.Integer(data_key="milliCpus", required=True)
    memory_mb = fields.Integer(data_key="memoryMb", required=True)


class APIType(str, Enum):
    WSGI = "wsgi"
    SCRIPT = "script"
    PLUMBER = "plumber"


class DeploymentStatus(Enum):
    NOTDEPLOYED = "not-deployed"
    STARTING = "starting"
    DEPLOYED = "deployed"
    ERROR = "error"


class InstanceSchema(BaseSchema):
    instance = fields.Nested(ServerSchema, required=True)
    outdated = fields.Boolean(required=True)
    key = fields.Nested(
        _APIKeySchema, load_default=None
    )  # Production instances do not have this field

    @post_load
    def make_instance(self, data, **kwargs):
        return Instance(**data)


class CommandDefinitionSchema(BaseSchema):
    api_type = EnumField(
        APIType, by_value=True, required=True, data_key="type"
    )
    working_directory = fields.String(
        data_key="workingDirectory",
    )
    module = fields.String()
    wsgi_object = fields.String(data_key="wsgiObject")
    script_name = fields.String(data_key="scriptName")

    @post_load
    def make_command_definition(self, data, **kwargs):
        if data["api_type"] == "wsgi":
            return WSGIDefinition(**data)
        elif data["api_type"] == "script":
            return ScriptDefinition(**data)
        elif data["api_type"] == "plumber":
            return PlumberDefinition(**data)


class ServerTypeSchema(BaseSchema):
    instance_size_type = fields.String(
        data_key="instanceSizeType", required=True
    )
    instance_size = fields.Nested(InstanceSizeSchema, data_key="instanceSize")


class _APISchema(BaseSchema):
    definition = fields.Nested(CommandDefinitionSchema, data_key="definition")
    environment_ids = fields.List(fields.UUID(), data_key="environmentIds")
    default_server_size = fields.Nested(
        ServerTypeSchema, data_key="defaultServerSize"
    )
    name = fields.String(data_key="name")
    subdomain = fields.String(data_key="subdomain")
    description = fields.String(data_key="description")
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt")

    id = fields.UUID(data_key="apiId", required=True)
    author_id = fields.UUID(data_key="authorId", required=True)
    created_at = fields.DateTime(data_key="createdAt", required=True)
    last_deployed_at = fields.DateTime(
        data_key="lastDeployedAt", load_default=None
    )
    last_deployed_by = fields.UUID(
        data_key="lastDeployedBy", load_default=None
    )
    # The following fields do not exist in the response if all APIs are listed
    # TODO: what would be the best way to handle these missing fields for Marshmallow
    deployment_status = EnumField(
        DeploymentStatus,
        by_value=True,
        data_key="deploymentStatus",
        load_default=None,
    )
    prod_instances = fields.Nested(
        InstanceSchema,
        many=True,
        data_key="prodInstances",
        load_default=None,
    )
    prod_keys = fields.Nested(
        _APIKeySchema,
        many=True,
        data_key="prodKeys",
        load_default=None,
    )
    dev_instances = fields.Nested(
        InstanceSchema,
        many=True,
        data_key="devInstances",
        load_default=None,
    )

    @post_load
    def make_api(self, data, **kwargs):
        return API(**data)


class ListAllAPISchema(BaseSchema):
    apis = fields.Nested(_APISchema(many=True))

    @post_load
    def make_list_apis(self, data, **kwargs):
        # Flatten the API response
        return data["apis"]


class APIClient(BaseClient):
    """Client for the Faculty API deployment.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("api")

    Parameters
    ----------
    url : str
        The URL of the server management service.
    session : faculty.session.Session
        The session to use to make requests.
    """

    SERVICE_NAME = "aperture"

    def create(
        self,
        project_id,
        api_definition,
        subdomain,
        name="",
        description="",
        environment_ids=[],
        default_server_size=None,
    ):
        """Create a new API.

        Parameters
        ----------
        project_id : uuid.UUID
            The project to create the API in.
        api_definition : CommandDefinition
            The API's command definition: WSGI(Flask)/Plumber/Script(Custom).
        subdomain : str
            The subdomain where the API should run.
        name : str
            The API's name.
        description : str
            The API's description field.
        environment_ids : List[uuid.UUID]
            The environments to apply to the API's instances.
        default_server_size : ServerType
            The default server size to set
        """
        payload = {
            "definition": CommandDefinitionSchema().dump(api_definition)
        }
        payload["subdomain"] = subdomain
        payload["name"] = name
        payload["description"] = description

        payload["environmentIds"] = environment_ids

        # TODO: turn these into schema handling as well
        if default_server_size is None:
            default_server_size = {
                "instanceSizeType": "custom",
                "instanceSize": {"milliCpus": 1000, "memoryMb": 4096},
            }
        payload["defaultServerSize"] = default_server_size

        return self._post_raw(
            "/project/{}/api".format(project_id), json=payload
        )

    @staticmethod
    def flask_definition(working_directory, module, wsgi_object):
        return WSGIDefinition(
            api_type=APIType.WSGI,
            working_directory=working_directory,
            module=module,
            wsgi_object=wsgi_object,
        )

    @staticmethod
    def plumber_definition(working_directory, script_name):
        return PlumberDefinition(
            api_type=APIType.PLUMBER,
            working_directory=working_directory,
            script_name=script_name,
        )

    @staticmethod
    def script_definition(working_directory, script_name):
        return ScriptDefinition(
            api_type=APIType.SCRIPT,
            working_directory=working_directory,
            script_name=script_name,
        )

    def _get_current_api_definition(self, project_id, api_id):
        current_api = self.get(project_id, api_id)
        current_api = _APISchema().dump(current_api)
        return current_api

    def get(self, project_id, api_id):
        return self._get(
            "/project/{}/api/{}".format(project_id, api_id), _APISchema()
        )

    def update(
        self,
        project_id,
        api_id,
        environment_ids=None,
        api_definition=None,
        server_type=None,
        current_api=None,
    ):
        if not current_api:
            current_api = self._get_current_api_definition(project_id, api_id)
        payload = {}
        payload["defaultServerSize"] = current_api["defaultServerSize"]
        payload["environmentIds"] = current_api["environmentIds"]
        payload["definition"] = current_api["definition"]
        if server_type:
            payload["defaultServerSize"] = server_type
        if environment_ids:
            payload["environmentIds"] = environment_ids

        if api_definition:
            payload["definition"] = api_definition
        endpoint = "/project/{}/api/{}/definition".format(project_id, api_id)
        return self._put(endpoint, _APISchema(), json=payload)

    def delete(self, project_id, api_id):
        return self._delete_raw(
            "/project/{}/api/{}".format(project_id, api_id)
        )

    def list(self, project_id):
        """List APIs in a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The project to list APIs in.
        name : str, optional
            If provided, only return APIs with this name.

        Returns
        -------
        List[API]
            The matching APIs.
        """
        endpoint = "/project/{}/api".format(project_id)
        return self._get(endpoint, _APISchema(many=True))

    def create_production_key(self, project_id, api_id, label=None):
        """Create a production key for a given API.

        Parameters
        ----------
        project_id : uuid.UUID
            The project where the API resides.
        api_id : uuid.UUID
            The API to create a new key in.
        label : str, optional
            If provided, set this value as the new key's label.

        Returns
        -------
        APIKey
            The newly created key.
        """
        endpoint = "/project/{}/api/{}/key".format(project_id, api_id)
        # TODO: maybe more sensible default, the UI doesn't allow empty name but the api accepts this
        payload = {"label": label if label else ""}
        return self._post(endpoint, _APIKeySchema(), json=payload)

    def list_production_keys(self, project_id, api_id):
        """List all production API keys for a given API.

        Parameters
        ----------
        project_id : uuid.UUID
            The project where the API resides in.
        api_id : uuid.UUID
            The API where to list the production keys in.

        Returns
        -------
        List[APIKey]
            The production keys.
        """
        endpoint = "/project/{}/api/{}/key".format(project_id, api_id)
        return self._get(endpoint, _APIKeySchema(many=True))

    def _toggle_production_key(self, project_id, api_id, key_id, enabled):
        endpoint = "/project/{}/api/{}/key/{}/enabled".format(
            project_id, api_id, key_id
        )
        payload = {"enabled": enabled}
        return self._put(endpoint, _APIKeySchema(), json=payload)

    def enable_production_key(self, project_id, api_id, key_id):
        """Enable a specific production API key.

        Parameters
        ----------
        project_id : uuid.UUID
            The project in which the API resides.
        api_id : uuid.UUID
            The API in which to the production key resides.
        key_id : uuid.UUID
            The key to enable.
        """
        return self._toggle_production_key(project_id, api_id, key_id, True)

    def disable_production_key(self, project_id, api_id, key_id):
        """Disable a specific production API key.

        Parameters
        ----------
        project_id : uuid.UUID
            The project in which the API resides.
        api_id : uuid.UUID
            The API in which to the production key resides.
        key_id : uuid.UUID
            The key to disable.
        """
        return self._toggle_production_key(project_id, api_id, key_id, False)

    def update_production_key(self, project_id, api_id, key_id, label):
        """Disable a specific production API key.

        Parameters
        ----------
        project_id : uuid.UUID
            The project in which the API resides.
        api_id : uuid.UUID
            The API in which to the production key resides.
        key_id : uuid.UUID
            The key to update.
        label : str
            The new label to set to the given key.

        Returns
        -------
        APIKey
            The updated key.
        """
        endpoint = "/project/{}/api/{}/key/{}".format(
            project_id, api_id, key_id
        )
        payload = {"label": label}
        return self._put(endpoint, _APIKeySchema(), json=payload)

    def delete_production_key(self, project_id, api_id, key_id):
        endpoint = "/project/{}/api/{}/key/{}".format(
            project_id, api_id, key_id
        )
        self._delete_raw(endpoint)

    def start(
        self,
        project_id,
        api_id,
        key_id,
        server_type=None,
        image_version=None,
        current_api=None,
    ):
        if not current_api:
            current_api = self._get_current_api_definition(project_id, api_id)
        payload = {
            "instanceSizeType": current_api["instanceSizeType"],
            "instanceSize": {
                "milliCpus": current_api["instnaceSize"]["milliCpus"],
                "memoryMb": current_api["instnaceSize"]["memoryMb"],
            },
        }
        if server_type:
            payload["instanceSizeType"] = server_type["instance_size_type"]
            if server_type["instance_size"]:
                instance_size = server_type["instance_size"]
                payload["instanceSize"] = {
                    "milliCpus": instance_size.milli_cpus,
                    "memoryMb": instance_size.memory_mb,
                }
        if image_version:
            payload["imageVersion"] = image_version
        endpoint = "/project/{}/api/{}/prod/start".format(project_id, api_id)
        return self._put(endpoint, APIInstanceResponseSchema(), json=payload)

    def stop(self, project_id, api_id):
        endpoint = "/project/{}/api/{}/prod/stop".format(project_id, api_id)
        self._put(endpoint, APIInstanceResponseSchema())

    def restart(
        self,
        project_id,
        api_id,
        key_id,
        server_type=None,
        image_version=None,
        current_api=None,
    ):
        if not current_api:
            current_api = self._get_current_api_definition(project_id, api_id)
        payload = {
            "instanceSizeType": current_api["instanceSizeType"],
            "instanceSize": {
                "milliCpus": current_api["instnaceSize"]["milliCpus"],
                "memoryMb": current_api["instnaceSize"]["memoryMb"],
            },
        }
        if server_type:
            payload["instanceSizeType"] = server_type["instance_size_type"]
            if server_type["instance_size"]:
                instance_size = server_type["instance_size"]
                payload["instanceSize"] = {
                    "milliCpus": instance_size.milli_cpus,
                    "memoryMb": instance_size.memory_mb,
                }
        if image_version:
            payload["imageVersion"] = image_version
        endpoint = "/project/{}/api/{}/prod/restart".format(project_id, api_id)
        return self._put(endpoint, APIInstanceResponseSchema(), json=payload)

    def reload(self, project_id, api_id):
        endpoint = "/project/{}/api/{}/prod/reload".format(project_id, api_id)
        self._put(endpoint, APIInstanceResponseSchema())

    def start_dev(
        self, project_id, api_id, key_id, server_type=None, image_version=None
    ):

        payload = {}
        if server_type:
            payload["instanceSizeType"] = server_type["instance_size_type"]
            if server_type["instance_size"]:
                instance_size = server_type["instance_size"]
                payload["instanceSize"] = {
                    "milliCpus": instance_size["milli_cpus"],
                    "memoryMb": instance_size["memory_mb"],
                }
        if image_version:
            payload["imageVersion"] = image_version
        endpoint = "/project/{}/api/{}/dev/start".format(project_id, api_id)
        return self._post(
            endpoint, APIDevInstanceResponseSchema(), json=payload
        )

    def stop_dev(self, project_id, api_id, dev_instance_id):
        endpoint = "/project/{}/api/{}/dev/{}".format(
            project_id, api_id, dev_instance_id
        )
        self._delete(endpoint, APIDevInstanceResponseSchema())

    def reload_dev(self, project_id, api_id, dev_instance_id):
        endpoint = "/project/{}/api/{}/dev/{}/reload".format(
            project_id, api_id, dev_instance_id
        )
        self._put(endpoint, APIDevInstanceResponseSchema())

    def list_all(self):
        """List all APIs on the Faculty deployment.

        This method requires administrative privileges not available to most
        users.

        Returns
        -------
        List[API]
            The APIs.
        """
        return self._get("/api", ListAllAPISchema())
