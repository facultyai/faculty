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
import uuid
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union

from marshmallow import fields, post_load
from marshmallow_enum import EnumField

from faculty._oneofschema import OneOfSchema
from faculty.clients.base import BaseClient, BaseSchema
from faculty.clients.job import InstanceSize, _InstanceSizeSchema
from faculty.clients.server import Server, _ServerSchema


class DeploymentStatus(str, Enum):
    """An enumeration of possible API deployment statuses."""

    NOT_DEPLOYED = "not-deployed"
    STARTING = "starting"
    DEPLOYED = "deployed"
    ERROR = "error"


@dataclass
class DevInstance:
    instance_id: uuid.UUID


@dataclass
class APIKey:
    id: uuid.UUID
    material: str
    enabled: bool
    label: str


@dataclass
class ServerType:
    instance_size_type: str
    instance_size: Optional[InstanceSize] = None


@dataclass
class WSGIDefinition:
    working_directory: str
    module: str
    wsgi_object: str


@dataclass
class WSGIDefinitionResponse(WSGIDefinition):
    last_updated_at: datetime


@dataclass
class PlumberDefinition:
    working_directory: str
    script_name: str


@dataclass
class PlumberDefinitionResponse(PlumberDefinition):
    last_updated_at: datetime


@dataclass
class ScriptDefinition:
    working_directory: str
    script_name: str


@dataclass
class ScriptDefinitionResponse(ScriptDefinition):
    last_updated_at: datetime


@dataclass
class Instance:
    instance: Server
    outdated: bool
    key: APIKey


@dataclass
class ProjectTemplateReference:
    template_id: uuid.UUID
    version_id: uuid.UUID


@dataclass
class APIInstance:
    api_id: uuid.UUID
    instance_id: uuid.UUID
    project_id: uuid.UUID


@dataclass
class APIDevInstance:
    api_id: uuid.UUID
    instance: DevInstance
    project_id: uuid.UUID
    key: APIKey


@dataclass
class API:
    name: str
    description: str
    definition: Union[WSGIDefinition, PlumberDefinition, ScriptDefinition]
    environment_ids: List[uuid.UUID]
    default_server_size: ServerType


@dataclass
class APIResponse(API):
    # Override parent classes field
    definition: Union[
        WSGIDefinitionResponse,
        PlumberDefinitionResponse,
        ScriptDefinitionResponse,
    ]
    # New fields
    api_id: uuid.UUID
    author_id: uuid.UUID
    created_at: datetime
    created_from_project_template: Optional[ProjectTemplateReference]
    deployment_status: DeploymentStatus
    dev_instances: List[Instance]
    last_deployed_at: Optional[datetime]
    last_deployed_by: Optional[uuid.UUID]
    prod_instances: List[Instance]
    prod_keys: List[APIKey]
    project_id: uuid.UUID
    subdomain: str
    created_from_project_template: Optional[ProjectTemplateReference]
    last_deployed_at: datetime
    last_deployed_by: uuid.UUID


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

    def list(self, project_id):
        """List the APIs in a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to list APIs in.

        Returns
        -------
        List[APIResponse]
            The APIs in the project.
        """
        endpoint = "/project/{}/api".format(project_id)
        return self._get(endpoint, _APIResponseSchema(many=True))

    def list_all(self):
        """List all APIs on the Faculty deployment.

        This method requires administrative privileges not available to most
        users.

        Returns
        -------
        List[APIResponse]
            The APIs.
        """
        return self._get("/api", _ListAPIsResponseSchema())

    def get(self, project_id, api_id):
        """Get an API.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the API.
        api_id : uuid.UUID
            The ID of the API to get.

        Returns
        -------
        APIResponse
            The retrieved API.
        """
        return self._get(
            "/project/{}/api/{}".format(project_id, api_id),
            _APIResponseSchema(),
        )

    def create(
        self,
        project_id,
        command_definition,
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
        command_definition : CommandDefinition
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

        Returns
        -------
        APIResponse
            The newly created API.
        """
        if default_server_size is None:
            default_server_size = ServerType(
                instance_size_type="custom",
                instance_size=InstanceSize(milli_cpus=1000, memory_mb=4000),
            )

        payload = {
            "definition": _CommandDefinitionSchema().dump(command_definition),
            "subdomain": subdomain,
            "name": name,
            "description": description,
            "environmentIds": environment_ids,
            "defaultServerSize": _ServerTypeSchema().dump(default_server_size),
        }

        return self._post(
            "/project/{}/api".format(project_id),
            _APIResponseSchema(),
            json=payload,
        )

    def update_definition(
        self,
        api,
        command_definition=None,
        default_server_size=None,
        environment_ids=None,
    ):
        """Update an API's definition.

        Parameters
        ----------
        api : Union[API, APIResponse]
            The API to update.
        command_definition : Optional[CommandDefinition]
            The API's command definition: WSGI(Flask)/Plumber/Script(Custom).
            If None then no change.
        environment_ids : Optional[List[uuid.UUID]]
            The environments to apply to the API's instances.
            If None then no change.
        default_server_size : Optional[ServerType]
            The default server size to set. If None then no change.

        Returns
        -------
        API
            A slimmed down version of the API definition
        """
        if command_definition is None:
            command_definition = api.definition
        if environment_ids is None:
            environment_ids = api.environment_ids
        if default_server_size is None:
            default_server_size = api.default_server_size

        payload = {
            "defaultServerSize": _ServerTypeSchema().dump(default_server_size),
            "definition": _CommandDefinitionSchema().dump(command_definition),
            "environmentIds": environment_ids,
        }
        endpoint = "/project/{}/api/{}/definition".format(
            api.project_id, api.api_id
        )
        return self._put(endpoint, _APISchema(), json=payload)

    def create_production_key(self, project_id, api_id, label):
        """Create a production key for a given API.

        Parameters
        ----------
        project_id : uuid.UUID
            The project where the API resides.
        api_id : uuid.UUID
            The API to create a new key in.
        label : str
            Set this value as the new key's label.

        Returns
        -------
        APIKey
            The newly created key.
        """
        endpoint = "/project/{}/api/{}/key".format(project_id, api_id)
        payload = {"label": label}
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
        """Delete a specific production API key.

        Parameters
        ----------
        project_id : uuid.UUID
            The project in which the API resides.
        api_id : uuid.UUID
            The API in which to the production key resides.
        key_id : uuid.UUID
            The key to delete.
        """
        endpoint = "/project/{}/api/{}/key/{}".format(
            project_id, api_id, key_id
        )
        self._delete_raw(endpoint)

    def start(
        self,
        project_id,
        api_id,
        server_size,
        image_version=None,
        restart=False,
    ):
        """Start or restart an API.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the API.
        api_id : uuid.UUID
            The ID of the API to start or restart.
        server_size : ServerType
            The server size to start.
        restart : bool, optional
            If True, then restart an API rather than start. Default: False

        Returns
        -------
        APIInstance
            Information on the started instance.
        """
        payload = _ServerTypeSchema().dump(server_size)
        payload["imageVersion"] = image_version

        action = "restart" if restart else "start"
        endpoint = "/project/{}/api/{}/prod/{}".format(
            project_id, api_id, action
        )
        return self._put(endpoint, _APIInstanceResponseSchema(), json=payload)

    def stop(self, project_id, api_id):
        """Stop an API.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the API.
        api_id : uuid.UUID
            The ID of the API to stop.

        Returns
        -------
        APIInstance
            Information on the stopped instance.
        """
        endpoint = "/project/{}/api/{}/prod/stop".format(project_id, api_id)
        return self._put(endpoint, _APIInstanceResponseSchema())

    def reload(self, project_id, api_id):
        """Reload a deployed API.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the API.
        api_id : uuid.UUID
            The ID of the API to reload.

        Returns
        -------
        APIInstance
            Information on the reloaded instance.
        """
        endpoint = "/project/{}/api/{}/prod/reload".format(project_id, api_id)
        return self._put(endpoint, _APIInstanceResponseSchema())

    def start_dev(
        self,
        project_id,
        api_id,
        server_size,
        image_version=None,
    ):
        """Start a test/development instance for an API.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the API.
        api : API
            The API for which to start the test / development server.
        server_size : ServerType
            The server size to start.

        Returns
        -------
        APIDevInstance
            Information on the started dev API instance.
        """
        payload = _ServerTypeSchema().dump(server_size)
        payload["imageVersion"] = image_version

        endpoint = "/project/{}/api/{}/dev".format(project_id, api_id)
        return self._post(
            endpoint, _APIDevInstanceResponseSchema(), json=payload
        )

    def stop_dev(self, project_id, api_id, instance_id):
        """Stop a test/development instance for an API.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the API.
        api_id : uuid.UUID
            The API for which to stop the test / development server.
        instance_id : uuid.UUID
            The ID of development instance to stop.

        Returns
        -------
        APIInstance
            Information on the deleted dev API instance.
        """
        endpoint = "/project/{}/api/{}/dev/{}".format(
            project_id, api_id, instance_id
        )
        return self._delete(endpoint, _APIInstanceResponseSchema())

    def reload_dev(self, project_id, api_id, instance_id):
        """Reload a test/development instance for an API.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the API.
        api_id : uuid.UUID
            The API for which to reload the test / development server.
        instance_id : uuid.UUID
            The ID of development instance to reload.

        Returns
        -------
        APIInstance
            Information on the reloaded dev API instance.
        """
        endpoint = "/project/{}/api/{}/dev/{}/reload".format(
            project_id, api_id, instance_id
        )
        return self._put(endpoint, _APIInstanceResponseSchema())


class _APIKeySchema(BaseSchema):
    id = fields.UUID(data_key="keyId", required=True)
    material = fields.String(required=True)
    # Development instance keys do not have these fields
    enabled = fields.Boolean(missing=None)
    label = fields.String(missing=None)

    @post_load
    def make_apikey(self, data, **kwargs):
        return APIKey(**data)


class _DevInstanceSchema(BaseSchema):
    instance_id = fields.UUID(data_key="instanceId")

    @post_load
    def make_dev_instance(self, data, **kwargs):
        return DevInstance(**data)


class _APIInstanceResponseSchema(BaseSchema):
    api_id = fields.UUID(data_key="apiId")
    instance_id = fields.UUID(data_key="instanceId", missing=None)
    project_id = fields.UUID(data_key="projectId")

    @post_load
    def make_api_instance(self, data, **kwargs):
        return APIInstance(**data)


class _APIDevInstanceResponseSchema(BaseSchema):
    api_id = fields.UUID(data_key="apiId")
    instance = fields.Nested(_DevInstanceSchema)
    project_id = fields.UUID(data_key="projectId")
    key = fields.Nested(_APIKeySchema)

    @post_load
    def make_api_instance(self, data, **kwargs):
        return APIDevInstance(**data)


class _ProjectTemplateReferenceSchema(BaseSchema):
    template_id = fields.UUID(data_key="templateId", required=True)
    version_id = fields.UUID(data_key="versionId", required=True)

    @post_load
    def make_project_template(self, data, **kwargs):
        return ProjectTemplateReference(**data)


class _InstanceSchema(BaseSchema):
    instance = fields.Nested(_ServerSchema, required=True)
    outdated = fields.Boolean(required=True)
    key = fields.Nested(
        _APIKeySchema, load_default=None
    )  # Production instances do not have this field

    @post_load
    def make_instance(self, data, **kwargs):
        return Instance(**data)


class _WSGIDefinitionSchema(BaseSchema):
    working_directory = fields.String(
        data_key="workingDirectory", required=True
    )
    module = fields.String(required=True)
    wsgi_object = fields.String(data_key="wsgiObject", required=True)

    @post_load
    def make_wsgi_command_definition(self, data, **kwargs):
        return WSGIDefinition(**data)


class _WSGIDefinitionResponseSchema(_WSGIDefinitionSchema):
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt", required=True)

    @post_load
    def make_wsgi_command_definition(self, data, **kwargs):
        return WSGIDefinitionResponse(**data)


class _PlumberDefinitionSchema(BaseSchema):
    working_directory = fields.String(
        data_key="workingDirectory", required=True
    )
    script_name = fields.String(data_key="scriptName", required=True)
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt")

    @post_load
    def make_plumber_command_definition(self, data, **kwargs):
        return PlumberDefinition(**data)


class _PlumberDefinitionResponseSchema(_PlumberDefinitionSchema):
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt", required=True)

    @post_load
    def make_plumber_command_definition(self, data, **kwargs):
        return PlumberDefinitionResponse(**data)


class _ScriptDefinitionSchema(BaseSchema):
    working_directory = fields.String(
        data_key="workingDirectory", required=True
    )
    script_name = fields.String(data_key="scriptName", required=True)
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt")

    @post_load
    def make_plumber_command_definition(self, data, **kwargs):
        return ScriptDefinition(**data)


class _ScriptDefinitionResponseSchema(_ScriptDefinitionSchema):
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt", required=True)

    @post_load
    def make_plumber_command_definition(self, data, **kwargs):
        return ScriptDefinitionResponse(**data)


class _CommandDefinitionSchema(OneOfSchema):
    type_field = "type"
    type_schemas = {
        "wsgi": _WSGIDefinitionSchema,
        "plumber": _PlumberDefinitionSchema,
        "script": _ScriptDefinitionSchema,
    }

    def get_obj_type(self, obj):
        if isinstance(obj, WSGIDefinition):
            return "wsgi"
        elif isinstance(obj, PlumberDefinition):
            return "plumber"
        elif isinstance(obj, ScriptDefinition):
            return "script"
        else:
            raise Exception("Unknown object type: %s" % repr(obj))


class _CommandDefinitionResponseSchema(OneOfSchema):
    type_field = "type"
    type_schemas = {
        "wsgi": _WSGIDefinitionResponseSchema,
        "plumber": _PlumberDefinitionResponseSchema,
        "script": _ScriptDefinitionResponseSchema,
    }

    def get_obj_type(self, obj):
        if isinstance(obj, WSGIDefinitionResponse):
            return "wsgi"
        elif isinstance(obj, PlumberDefinitionResponse):
            return "plumber"
        elif isinstance(obj, ScriptDefinitionResponse):
            return "script"
        else:
            raise Exception("Unknown object type: %s" % repr(obj))


class _ServerTypeSchema(BaseSchema):
    instance_size_type = fields.String(
        data_key="instanceSizeType", required=True
    )
    instance_size = fields.Nested(
        _InstanceSizeSchema, data_key="instanceSize", missing=None
    )

    @post_load
    def make_server_type(self, data, **kwargs):
        return ServerType(**data)


class _APISchema(BaseSchema):
    name = fields.String(missing=None)
    description = fields.String(missing=None)
    definition = fields.Nested(_CommandDefinitionSchema, required=True)
    environment_ids = fields.List(fields.UUID(), data_key="environmentIds")
    default_server_size = fields.Nested(
        _ServerTypeSchema, data_key="defaultServerSize", missing=None
    )

    @post_load
    def make_api(self, data, **kwargs):
        return API(**data)


class _APIResponseSchema(_APISchema):
    definition = fields.Nested(_CommandDefinitionResponseSchema, required=True)
    subdomain = fields.String(missing=None)
    description = fields.String(missing=None)
    api_id = fields.UUID(data_key="apiId", required=True)
    project_id = fields.UUID(data_key="projectId", required=True)
    author_id = fields.UUID(data_key="authorId", missing=None)
    created_at = fields.DateTime(data_key="createdAt", missing=None)
    deployment_status = EnumField(
        DeploymentStatus,
        by_value=True,
        data_key="deploymentStatus",
        missing=None,
    )
    prod_instances = fields.Nested(
        _InstanceSchema,
        many=True,
        data_key="prodInstances",
        missing=None,
    )
    prod_keys = fields.Nested(
        _APIKeySchema,
        many=True,
        data_key="prodKeys",
        missing=None,
    )
    dev_instances = fields.Nested(
        _InstanceSchema,
        many=True,
        data_key="devInstances",
        missing=None,
    )
    created_from_project_template = fields.Nested(
        _ProjectTemplateReferenceSchema,
        data_key="createdFromProjectTemplate",
        missing=None,
    )

    last_deployed_at = fields.DateTime(data_key="lastDeployedAt", missing=None)
    last_deployed_by = fields.UUID(data_key="lastDeployedBy", missing=None)

    @post_load
    def make_api(self, data, **kwargs):
        return APIResponse(**data)


class _ListAPIsResponseSchema(BaseSchema):
    apis = fields.Nested(_APIResponseSchema(many=True))

    @post_load
    def make_list_apis(self, data, **kwargs):
        # Flatten the API response
        return data["apis"]
