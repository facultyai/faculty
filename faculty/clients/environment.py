# Copyright 2018-2019 Faculty Science Limited
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


import re
from collections import namedtuple
from enum import Enum

from marshmallow import (
    ValidationError,
    fields,
    post_load,
    validates_schema,
    post_dump,
)
from marshmallow_enum import EnumField

from faculty.clients.base import BaseClient, BaseSchema


class Constraint(Enum):
    AT_LEAST = ">="
    EQUAL = "=="


Version = namedtuple("Version", ["constraint", "identifier"])

PythonPackage = namedtuple("PythonPackage", ["name", "version"])

Pip = namedtuple("Pip", ["extra_index_urls", "packages"])

Conda = namedtuple("Conda", ["channels", "packages"])

PythonSpecification = namedtuple("PythonSpecification", ["pip", "conda"])

Apt = namedtuple("Apt", ["packages"])

AptPackage = namedtuple("AptPackage", ["name"])

Script = namedtuple("Script", ["script"])

Python = namedtuple("Python", ["python_2", "python_3"])

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

EnvironmentCreationResponse = namedtuple("EnvironmentCreationResponse", ["id"])

EnvironmentUpdate = namedtuple(
    "EnvironmentUpdate", ["name", "description", "specification"]
)

VERSION_REGEX = re.compile(
    r"^(?:\d+\!)?\d+(?:\.\d+)*(?:(?:a|b|rc)\d+)?(?:\.post\d+)?(?:\.dev\d+)?$"
)


class VersionSchema(BaseSchema):
    constraint = EnumField(Constraint, by_value=True, required=True)
    identifier = fields.String(required=True)

    @validates_schema
    @post_dump
    def validate_version_format(self, data):
        if not VERSION_REGEX.match(data["identifier"]):
            raise ValidationError("Invalid version format", "identifier")

    @post_load
    def make_version(self, data):
        return Version(**data)


class VersionField(fields.Field):
    """
    Field that serialises/deserialises a Python package version.
    """

    def _deserialize(self, value, attr, obj, **kwargs):
        if value == "latest":
            return "latest"
        else:
            return VersionSchema().load(value)

    def _serialize(self, value, attr, obj, **kwargs):
        if value == "latest":
            return "latest"
        else:
            return VersionSchema().dump(value)


class PythonPackageSchema(BaseSchema):
    name = fields.String(required=True)
    version = VersionField(required=True)

    @post_load
    def make_python_package(self, data):
        return PythonPackage(**data)


class PipSchema(BaseSchema):
    extra_index_urls = fields.List(
        fields.String(), data_key="extraIndexUrls", required=True, many=True
    )
    packages = fields.List(fields.Nested(PythonPackageSchema()), required=True)

    @post_load
    def make_pip(self, data):
        return Pip(**data)


class CondaSchema(BaseSchema):
    channels = fields.List(fields.String(), required=True)
    packages = fields.List(fields.Nested(PythonPackageSchema()), required=True)

    @post_load
    def make_conda(self, data):
        return Conda(**data)


class PythonSpecificationSchema(BaseSchema):
    conda = fields.Nested(CondaSchema(), required=True)
    pip = fields.Nested(PipSchema(), required=True)

    @post_load
    def make_python_specification(self, data):
        return PythonSpecification(**data)


class PythonSchema(BaseSchema):
    python_2 = fields.Nested(
        PythonSpecificationSchema(), required=True, data_key="Python2"
    )
    python_3 = fields.Nested(
        PythonSpecificationSchema(), required=True, data_key="Python3"
    )

    @post_load
    def make_python(self, data):
        return Python(**data)


class AptPackageSchema(BaseSchema):
    name = fields.String(required=True)

    @post_load
    def make_apt_package(self, data):
        return AptPackage(**data)


class AptSchema(BaseSchema):
    packages = fields.List(fields.Nested(AptPackageSchema()), required=True)

    @post_load
    def make_apt(self, data):
        return Apt(**data)


class ScriptSchema(BaseSchema):
    script = fields.String(required=True)

    @post_load
    def make_script(self, data):
        return Script(**data)


class SpecificationSchema(BaseSchema):
    apt = fields.Nested(AptSchema(), required=True)
    bash = fields.List(fields.Nested(ScriptSchema()), required=True)
    python = fields.Nested(PythonSchema(), required=True)

    @post_load
    def make_specification(self, data):
        return Specification(**data)


class EnvironmentSchema(BaseSchema):
    id = fields.UUID(data_key="environmentId", required=True)
    project_id = fields.UUID(data_key="projectId", required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    author_id = fields.UUID(data_key="authorId", required=True)
    created_at = fields.DateTime(data_key="createdAt", required=True)
    updated_at = fields.DateTime(data_key="updatedAt", required=True)
    specification = fields.Nested(SpecificationSchema(), required=True)

    @post_load
    def make_environment(self, data):
        return Environment(**data)


class EnvironmentUpdateSchema(BaseSchema):
    name = fields.String(required=True)
    description = fields.String(required=True)
    specification = fields.Nested(SpecificationSchema(), required=True)

    @post_load
    def make_environment_update(self, data):
        return EnvironmentUpdate(**data)


class EnvironmentCreationResponseSchema(BaseSchema):
    id = fields.UUID(data_key="environmentId", required=True)

    @post_load
    def make_environment(self, data):
        return EnvironmentCreationResponse(**data)


class EnvironmentClient(BaseClient):

    SERVICE_NAME = "baskerville"

    def list(self, project_id):
        endpoint = "/project/{}/environment".format(project_id)
        return self._get(endpoint, EnvironmentSchema(many=True))

    def get(self, project_id, environment_id):
        endpoint = "/project/{}/environment/{}".format(
            project_id, environment_id
        )
        return self._get(endpoint, EnvironmentSchema())

    def update(self, project_id, environment_id, update):
        endpoint = "/project/{}/environment/{}".format(
            project_id, environment_id
        )
        self._put_raw(endpoint, json=EnvironmentUpdateSchema().dump(update))

    def create(self, project_id, content):
        endpoint = "/project/{}/environment".format(project_id)
        response = self._post(
            endpoint,
            EnvironmentCreationResponseSchema(),
            json=EnvironmentUpdateSchema().dump(content),
        )
        return response.id

    def delete(self, project_id, environment_id):
        endpoint = "/project/{}/environment/{}".format(
            project_id, environment_id
        )
        self._delete_raw(endpoint)
