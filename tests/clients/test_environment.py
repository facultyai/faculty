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

import datetime
import uuid

import pytest
from dateutil.tz import UTC
from marshmallow import ValidationError

from faculty.clients.environment import (
    Apt,
    AptPackage,
    AptPackageSchema,
    AptSchema,
    Conda,
    CondaSchema,
    Constraint,
    Environment,
    EnvironmentClient,
    EnvironmentCreationResponseSchema,
    EnvironmentCreationResponse,
    EnvironmentSchema,
    EnvironmentUpdate,
    EnvironmentUpdateSchema,
    Pip,
    PipSchema,
    Python,
    PythonPackage,
    PythonPackageSchema,
    PythonSchema,
    PythonSpecification,
    PythonSpecificationSchema,
    Script,
    ScriptSchema,
    VersionSchema,
    Specification,
    SpecificationSchema,
    Version,
)

VERSION = Version(constraint=Constraint.EQUAL, identifier="1.0.0")
VERSION_LATEST = "latest"

INVALID_VERSION = Version(
    constraint=Constraint.EQUAL, identifier="not_semantic"
)
INVALID_VERSION_BODY = {"constraint": "==", "identifier": "not_semantic"}

VERSION_BODY = {"constraint": "==", "identifier": "1.0.0"}
VERSION_BODY_LATEST = "latest"

PYTHON_PACKAGE = PythonPackage(name="tensorflow", version=VERSION)
PYTHON_PACKAGE_BODY = {"name": "tensorflow", "version": VERSION_BODY}

PYTHON_PACKAGE_LATEST = PythonPackage(
    name="tensorflow", version=VERSION_LATEST
)
PYTHON_PACKAGE_BODY_LATEST = {
    "name": "tensorflow",
    "version": VERSION_BODY_LATEST,
}

PIP = Pip(extra_index_urls=["Some URL"], packages=[PYTHON_PACKAGE])

PIP_BODY = {"extraIndexUrls": ["Some URL"], "packages": [PYTHON_PACKAGE_BODY]}

CONDA_BODY = {"channels": ["Some channel"], "packages": [PYTHON_PACKAGE_BODY]}

CONDA = Conda(channels=["Some channel"], packages=[PYTHON_PACKAGE])

PYTHON_SPECIFICATION_BODY = {"pip": PIP_BODY, "conda": CONDA_BODY}

PYTHON_SPECIFICATION = PythonSpecification(conda=CONDA, pip=PIP)

APT_PACKAGE_BODY = {"name": "cuda"}

APT_PACKAGE = AptPackage(name="cuda")

APT_BODY = {"packages": [APT_PACKAGE_BODY]}

APT = Apt(packages=[APT_PACKAGE])

PYTHON_BODY = {
    "Python2": PYTHON_SPECIFICATION_BODY,
    "Python3": PYTHON_SPECIFICATION_BODY,
}

PYTHON = Python(python_2=PYTHON_SPECIFICATION, python_3=PYTHON_SPECIFICATION)

SCRIPT_STR = "# Edit your script\n"

SCRIPT_BODY = {"script": SCRIPT_STR}

SCRIPT = Script(script=SCRIPT_STR)

SPECIFICATION_BODY = {
    "apt": APT_BODY,
    "bash": [SCRIPT_BODY],
    "python": PYTHON_BODY,
}

SPECIFICATION = Specification(apt=APT, bash=[SCRIPT], python=PYTHON)

ENVIRONMENT_UPDATE_BODY = {
    "name": "Test",
    "description": "A test environment",
    "specification": SPECIFICATION_BODY,
}

ENVIRONMENT_UPDATE = EnvironmentUpdate(
    name="Test", description="A test environment", specification=SPECIFICATION
)

PROJECT_ID = uuid.uuid4()
ENVIRONMENT_ID = uuid.uuid4()

ENVIRONMENT = Environment(
    id=ENVIRONMENT_ID,
    project_id=PROJECT_ID,
    name="Test Environment",
    description="Environment description",
    author_id=uuid.uuid4(),
    created_at=datetime.datetime(2018, 10, 3, 4, 20, 0, 0, tzinfo=UTC),
    updated_at=datetime.datetime(2018, 11, 3, 4, 21, 15, 0, tzinfo=UTC),
    specification=SPECIFICATION,
)

ENVIRONMENT_BODY = {
    "environmentId": str(ENVIRONMENT.id),
    "projectId": str(PROJECT_ID),
    "name": ENVIRONMENT.name,
    "description": ENVIRONMENT.description,
    "authorId": str(ENVIRONMENT.author_id),
    "createdAt": "2018-10-03T04:20:00Z",
    "updatedAt": "2018-11-03T04:21:15Z",
    "specification": SPECIFICATION_BODY,
}

ENVIRONMENT_CREATION_RESPONSE = EnvironmentCreationResponse(id=uuid.uuid4())

ENVIRONMENT_CREATION_RESPONSE_BODY = {
    "environmentId": str(ENVIRONMENT_CREATION_RESPONSE.id)
}


def test_version_schema():
    data = VersionSchema().load(VERSION_BODY)
    assert data == VERSION


def test_version_invalid():
    with pytest.raises(ValidationError):
        VersionSchema().load(INVALID_VERSION_BODY)

    with pytest.raises(ValidationError):
        VersionSchema().dump(INVALID_VERSION)


def test_python_package_schema():
    data = PythonPackageSchema().load(PYTHON_PACKAGE_BODY)
    assert data == PYTHON_PACKAGE

    data = PythonPackageSchema().load(PYTHON_PACKAGE_BODY_LATEST)
    assert data == PYTHON_PACKAGE_LATEST


def test_pip_schema():
    data = PipSchema().load(PIP_BODY)
    assert data == PIP


def test_conda_schema():
    data = CondaSchema().load(CONDA_BODY)
    assert data == CONDA


def test_python_specification_schema():
    data = PythonSpecificationSchema().load(PYTHON_SPECIFICATION_BODY)
    assert data == PYTHON_SPECIFICATION


def test_apt_package_schema():
    data = AptPackageSchema().load(APT_PACKAGE_BODY)
    assert data == APT_PACKAGE


def test_apt_schema():
    data = AptSchema().load(APT_BODY)
    assert data == APT


def test_python_schema():
    data = PythonSchema().load(PYTHON_BODY)
    assert data == PYTHON


def test_script_schema():
    data = ScriptSchema().load(SCRIPT_BODY)
    assert data == SCRIPT


def test_specification_schema():
    data = SpecificationSchema().load(SPECIFICATION_BODY)
    assert data == SPECIFICATION


def test_environment_update_schema():
    data = EnvironmentUpdateSchema().load(ENVIRONMENT_UPDATE_BODY)
    assert data == ENVIRONMENT_UPDATE


def test_environment_creation_response_schema():
    data = EnvironmentCreationResponseSchema().load(
        ENVIRONMENT_CREATION_RESPONSE_BODY
    )
    assert data == ENVIRONMENT_CREATION_RESPONSE


def test_environment_schema():
    data = EnvironmentSchema().load(ENVIRONMENT_BODY)
    assert data == ENVIRONMENT


def test_environment_schema_invalid():
    with pytest.raises(ValidationError):
        EnvironmentSchema().load({})


def test_environment_client_list(mocker):
    mocker.patch.object(EnvironmentClient, "_get", return_value=[ENVIRONMENT])
    schema_mock = mocker.patch("faculty.clients.environment.EnvironmentSchema")

    client = EnvironmentClient(mocker.Mock())
    assert client.list(PROJECT_ID) == [ENVIRONMENT]

    schema_mock.assert_called_once_with(many=True)
    EnvironmentClient._get.assert_called_once_with(
        "/project/{}/environment".format(PROJECT_ID), schema_mock.return_value
    )


def test_environment_client_get(mocker):
    mocker.patch.object(EnvironmentClient, "_get", return_value=ENVIRONMENT)
    schema_mock = mocker.patch("faculty.clients.environment.EnvironmentSchema")

    client = EnvironmentClient(mocker.Mock())
    assert client.get(PROJECT_ID, ENVIRONMENT_ID) == ENVIRONMENT

    schema_mock.assert_called_once_with()
    EnvironmentClient._get.assert_called_once_with(
        "/project/{}/environment/{}".format(PROJECT_ID, ENVIRONMENT_ID),
        schema_mock.return_value,
    )


def test_environment_client_update(mocker):
    mocker.patch.object(EnvironmentClient, "_put_raw", return_value=None)
    mocker.patch.object(
        EnvironmentUpdateSchema, "dump", return_value=ENVIRONMENT_UPDATE_BODY
    )

    client = EnvironmentClient(mocker.Mock())
    assert (
        client.update(PROJECT_ID, ENVIRONMENT_ID, ENVIRONMENT_UPDATE) is None
    )

    EnvironmentUpdateSchema.dump.assert_called_once_with(ENVIRONMENT_UPDATE)
    EnvironmentClient._put_raw.assert_called_once_with(
        "/project/{}/environment/{}".format(PROJECT_ID, ENVIRONMENT_ID),
        json=ENVIRONMENT_UPDATE_BODY,
    )


def test_environment_client_create(mocker):
    mocker.patch.object(
        EnvironmentClient, "_post", return_value=ENVIRONMENT_CREATION_RESPONSE
    )
    mocker.patch.object(
        EnvironmentUpdateSchema, "dump", return_value=ENVIRONMENT_UPDATE_BODY
    )
    schema_mock = mocker.patch(
        "faculty.clients.environment.EnvironmentCreationResponseSchema"
    )

    client = EnvironmentClient(mocker.Mock())
    assert (
        client.create(PROJECT_ID, ENVIRONMENT_UPDATE)
        == ENVIRONMENT_CREATION_RESPONSE.id
    )

    EnvironmentUpdateSchema.dump.assert_called_once_with(ENVIRONMENT_UPDATE)
    EnvironmentClient._post.assert_called_once_with(
        "/project/{}/environment".format(PROJECT_ID),
        schema_mock.return_value,
        json=ENVIRONMENT_UPDATE_BODY,
    )


def test_environment_client_delete(mocker):
    mocker.patch.object(EnvironmentClient, "_delete_raw", return_value=None)

    client = EnvironmentClient(mocker.Mock())
    assert client.delete(PROJECT_ID, ENVIRONMENT_ID) is None

    EnvironmentClient._delete_raw.assert_called_once_with(
        "/project/{}/environment/{}".format(PROJECT_ID, ENVIRONMENT_ID)
    )
