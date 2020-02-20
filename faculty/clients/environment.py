# Copyright 2018-2020 Faculty Science Limited
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
Interact with Faculty server environments.
"""


import re
from collections import namedtuple
from enum import Enum

from marshmallow import (
    ValidationError,
    fields,
    post_load,
    validates,
    post_dump,
)
from marshmallow_enum import EnumField

from faculty.clients.base import BaseClient, BaseSchema


class Constraint(Enum):
    """An enumeration of supported version constraints."""

    AT_LEAST = ">="
    EQUAL = "=="


Version = namedtuple("Version", ["constraint", "identifier"])
PythonPackage = namedtuple("PythonPackage", ["name", "version"])
Pip = namedtuple("Pip", ["extra_index_urls", "packages"])
Conda = namedtuple("Conda", ["channels", "packages"])
PythonEnvironment = namedtuple("PythonEnvironment", ["pip", "conda"])
Apt = namedtuple("Apt", ["packages"])
AptPackage = namedtuple("AptPackage", ["name", "version"])
Script = namedtuple("Script", ["script"])
PythonSpecification = namedtuple("PythonSpecification", ["python2", "python3"])
Specification = namedtuple("Specification", ["apt", "bash", "python"])
Environment = namedtuple(
    "Environment",
    [
        "id",
        "project_id",
        "name",
        "description",
        "author_id",
        "created_at",
        "updated_at",
        "specification",
    ],
)
_EnvironmentCreationResponse = namedtuple(
    "_EnvironmentCreationResponse", ["id"]
)
_EnvironmentCreateUpdate = namedtuple(
    "_EnvironmentCreateUpdate", ["name", "description", "specification"]
)

PYTHON_VERSION_REGEX = re.compile(
    r"^(?:\d+\!)?\d+(?:\.\d+)*(?:(?:a|b|rc)\d+)?(?:\.post\d+)?(?:\.dev\d+)?$"
)
APT_VERSION_REGEX = re.compile(r"^[a-zA-Z0-9\\.\\+-:~]+$")


class EnvironmentClient(BaseClient):
    """Client for the Faculty environment service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("environment")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    """

    _SERVICE_NAME = "baskerville"

    def list(self, project_id):
        """List the environments in a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to list environments in.

        Returns
        -------
        list of Environment
        """
        endpoint = "/project/{}/environment".format(project_id)
        return self._get(endpoint, _EnvironmentSchema(many=True))

    def get(self, project_id, environment_id):
        """Get an environment.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the environment.
        environment_id : uuid.UUID
            The ID of the environment.

        Returns
        -------
        list of Environment
        """
        endpoint = "/project/{}/environment/{}".format(
            project_id, environment_id
        )
        return self._get(endpoint, _EnvironmentSchema())

    def update(
        self, project_id, environment_id, name, specification, description=None
    ):
        """Update an existing environment.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the environment.
        environment_id : uuid.UUID
            The ID of the environment.
        name : str
            The name to set on the environment.
        specification : Specification
            The specification of the environment to set.
        description : str, optional
            The description to set on the environment. If None (the default),
            any existing description will be removed.
        """
        content = _EnvironmentCreateUpdate(
            name=name, specification=specification, description=description
        )
        endpoint = "/project/{}/environment/{}".format(
            project_id, environment_id
        )
        self._put_raw(
            endpoint, json=_EnvironmentCreateUpdateSchema().dump(content)
        )

    def create(self, project_id, name, specification, description=None):
        """Create a new environment.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to create the environment.
        name : str
            The name of the environment.
        specification : Specification
            The specification of the environment.
        description : str, optional
            If provided, the description to set on the environment.

        Returns
        -------
        uuid.UUID
            The ID of the created environment.
        """
        endpoint = "/project/{}/environment".format(project_id)
        content = _EnvironmentCreateUpdate(
            name=name, specification=specification, description=description
        )
        response = self._post(
            endpoint,
            _EnvironmentCreationResponseSchema(),
            json=_EnvironmentCreateUpdateSchema().dump(content),
        )
        return response.id

    def delete(self, project_id, environment_id):
        """Delete an environment.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the environment.
        environment_id : uuid.UUID
            The ID of the environment.
        """
        endpoint = "/project/{}/environment/{}".format(
            project_id, environment_id
        )
        self._delete_raw(endpoint)


class _PythonVersionSchema(BaseSchema):
    constraint = EnumField(Constraint, by_value=True, required=True)
    identifier = fields.String(required=True)

    @validates("identifier")
    def validate_version_format(self, data):
        if not PYTHON_VERSION_REGEX.match(data):
            raise ValidationError("Invalid version format")

    @post_load
    def make_version(self, data):
        return Version(**data)

    @post_dump
    def dump_version(self, data):
        self.validate_version_format(data["identifier"])
        return data


class _AptVersionSchema(BaseSchema):
    constraint = EnumField(Constraint, by_value=True, required=True)
    identifier = fields.String(required=True)

    @validates("identifier")
    def validate_version_format(self, data):
        if not APT_VERSION_REGEX.match(data):
            raise ValidationError("Invalid version format")

    @post_load
    def make_version(self, data):
        return Version(**data)

    @post_dump
    def dump_version(self, data):
        self.validate_version_format(data["identifier"])
        return data


class _PythonVersionField(fields.Field):
    """Field that serialises/deserialises a Python package version."""

    def _deserialize(self, value, attr, obj, **kwargs):
        if value == "latest":
            return "latest"
        else:
            return _PythonVersionSchema().load(value)

    def _serialize(self, value, attr, obj, **kwargs):
        if value == "latest":
            return "latest"
        else:
            return _PythonVersionSchema().dump(value)


class _AptVersionField(fields.Field):
    """Field that serialises/deserialises an apt package version."""

    def _deserialize(self, value, attr, obj, **kwargs):
        if value == "latest":
            return "latest"
        else:
            return _AptVersionSchema().load(value)

    def _serialize(self, value, attr, obj, **kwargs):
        if value == "latest":
            return "latest"
        else:
            return _AptVersionSchema().dump(value)


class _PythonPackageSchema(BaseSchema):
    name = fields.String(required=True)
    version = _PythonVersionField(required=True)

    @post_load
    def make_python_package(self, data):
        return PythonPackage(**data)


class _PipSchema(BaseSchema):
    extra_index_urls = fields.List(
        fields.String(), data_key="extraIndexUrls", required=True
    )
    packages = fields.List(
        fields.Nested(_PythonPackageSchema()), required=True
    )

    @post_load
    def make_pip(self, data):
        return Pip(**data)


class _CondaSchema(BaseSchema):
    channels = fields.List(fields.String(), required=True)
    packages = fields.List(
        fields.Nested(_PythonPackageSchema()), required=True
    )

    @post_load
    def make_conda(self, data):
        return Conda(**data)


class _PythonEnvironmentSchema(BaseSchema):
    conda = fields.Nested(_CondaSchema(), required=True)
    pip = fields.Nested(_PipSchema(), required=True)

    @post_load
    def make_python_specification(self, data):
        return PythonEnvironment(**data)


class _PythonSpecificationSchema(BaseSchema):
    python2 = fields.Nested(
        _PythonEnvironmentSchema(), data_key="Python2", missing=None
    )
    python3 = fields.Nested(
        _PythonEnvironmentSchema(), data_key="Python3", missing=None
    )

    @post_load
    def make_python(self, data):
        return PythonSpecification(**data)


class _AptPackageSchema(BaseSchema):
    name = fields.String(required=True)
    version = _AptVersionField(required=True)

    @post_load
    def make_apt_package(self, data):
        return AptPackage(**data)


class _AptSchema(BaseSchema):
    packages = fields.List(fields.Nested(_AptPackageSchema()), required=True)

    @post_load
    def make_apt(self, data):
        return Apt(**data)


class _ScriptSchema(BaseSchema):
    script = fields.String(required=True)

    @post_load
    def make_script(self, data):
        return Script(**data)


class _SpecificationSchema(BaseSchema):
    apt = fields.Nested(_AptSchema(), required=True)
    bash = fields.List(fields.Nested(_ScriptSchema()), required=True)
    python = fields.Nested(_PythonSpecificationSchema(), required=True)

    @post_load
    def make_specification(self, data):
        return Specification(**data)


class _EnvironmentSchema(BaseSchema):
    id = fields.UUID(data_key="environmentId", required=True)
    project_id = fields.UUID(data_key="projectId", required=True)
    name = fields.String(required=True)
    description = fields.String(missing=None)
    author_id = fields.UUID(data_key="authorId", required=True)
    created_at = fields.DateTime(data_key="createdAt", required=True)
    updated_at = fields.DateTime(data_key="updatedAt", required=True)
    specification = fields.Nested(_SpecificationSchema(), required=True)

    @post_load
    def make_environment(self, data):
        return Environment(**data)


class _EnvironmentCreateUpdateSchema(BaseSchema):
    name = fields.String(required=True)
    description = fields.String(missing=None)
    specification = fields.Nested(_SpecificationSchema(), required=True)

    @post_load
    def make_environment_update(self, data):
        return _EnvironmentCreateUpdate(**data)


class _EnvironmentCreationResponseSchema(BaseSchema):
    id = fields.UUID(data_key="environmentId", required=True)

    @post_load
    def make_environment(self, data):
        return _EnvironmentCreationResponse(**data)
