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
    AptVersionSchema,
    Conda,
    CondaSchema,
    Constraint,
    Environment,
    EnvironmentClient,
    EnvironmentCreateUpdate,
    EnvironmentCreateUpdateSchema,
    EnvironmentCreationResponse,
    EnvironmentCreationResponseSchema,
    EnvironmentSchema,
    Pip,
    PipSchema,
    PythonSpecification,
    PythonPackage,
    PythonPackageSchema,
    PythonSpecificationSchema,
    PythonEnvironment,
    PythonEnvironmentSchema,
    Script,
    ScriptSchema,
    Specification,
    SpecificationSchema,
    Version,
    PythonVersionSchema,
)

VERSION_BODY = {"constraint": "==", "identifier": "1.0.0"}
VERSION = Version(constraint=Constraint.EQUAL, identifier="1.0.0")

VERSION_BODY_LATEST = "latest"
VERSION_LATEST = "latest"

INVALID_PYTHON_VERSION_BODY = {
    "constraint": "==",
    "identifier": "invalid-identifier",
}
INVALID_PYTHON_VERSION = Version(
    constraint=Constraint.EQUAL, identifier="invalid-identifier"
)
INVALID_APT_VERSION_BODY = {"constraint": "==", "identifier": "    "}
INVALID_APT_VERSION = Version(constraint=Constraint.EQUAL, identifier="    ")

PYTHON_PACKAGE_BODY = {"name": "tensorflow", "version": VERSION_BODY}
PYTHON_PACKAGE = PythonPackage(name="tensorflow", version=VERSION)

PYTHON_PACKAGE_BODY_LATEST = {
    "name": "tensorflow",
    "version": VERSION_BODY_LATEST,
}
PYTHON_PACKAGE_LATEST = PythonPackage(
    name="tensorflow", version=VERSION_LATEST
)


PIP_BODY = {
    "extraIndexUrls": ["http://example.com/"],
    "packages": [PYTHON_PACKAGE_BODY],
}
PIP = Pip(extra_index_urls=["http://example.com/"], packages=[PYTHON_PACKAGE])


CONDA_BODY = {"channels": ["conda-forge"], "packages": [PYTHON_PACKAGE_BODY]}
CONDA = Conda(channels=["conda-forge"], packages=[PYTHON_PACKAGE])

PYTHON_ENVIRONMENT_BODY = {"pip": PIP_BODY, "conda": CONDA_BODY}
PYTHON_ENVIRONMENT = PythonEnvironment(conda=CONDA, pip=PIP)

APT_PACKAGE_BODY = {"name": "cuda", "version": "latest"}
APT_PACKAGE = AptPackage(name="cuda", version="latest")

APT_BODY = {"packages": [APT_PACKAGE_BODY]}
APT = Apt(packages=[APT_PACKAGE])

PYTHON_SPECIFICATION_BODY = {
    "Python2": PYTHON_ENVIRONMENT_BODY,
    "Python3": PYTHON_ENVIRONMENT_BODY,
}
PYTHON_SPECIFICATION = PythonSpecification(
    python2=PYTHON_ENVIRONMENT, python3=PYTHON_ENVIRONMENT
)

PYTHON_SPECIFICATION_BODY_MISSING_PYTHON2 = {
    "Python3": PYTHON_ENVIRONMENT_BODY
}
PYTHON_SPECIFICATION_BODY_PYTHON2_NONE = {
    "Python2": None,
    "Python3": PYTHON_ENVIRONMENT_BODY,
}
PYTHON_SPECIFICATION_PYTHON2_NONE = PythonSpecification(
    python2=None, python3=PYTHON_ENVIRONMENT
)

PYTHON_SPECIFICATION_BODY_MISSING_PYTHON3 = {
    "Python2": PYTHON_ENVIRONMENT_BODY
}
PYTHON_SPECIFICATION_BODY_PYTHON3_NONE = {
    "Python2": PYTHON_ENVIRONMENT_BODY,
    "Python3": None,
}
PYTHON_SPECIFICATION_PYTHON3_NONE = PythonSpecification(
    python2=PYTHON_ENVIRONMENT, python3=None
)

PYTHON_SPECIFICATION_BODY_MISSING_KEYS = {}
PYTHON_SPECIFICATION_BODY_NONE = {"Python2": None, "Python3": None}
PYTHON_SPECIFICATION_NONE = PythonSpecification(python2=None, python3=None)

SCRIPT_STR = "# Edit your script\n"
SCRIPT_BODY = {"script": SCRIPT_STR}
SCRIPT = Script(script=SCRIPT_STR)

SPECIFICATION_BODY = {
    "apt": APT_BODY,
    "bash": [SCRIPT_BODY],
    "python": PYTHON_SPECIFICATION_BODY,
}
SPECIFICATION = Specification(
    apt=APT, bash=[SCRIPT], python=PYTHON_SPECIFICATION
)

PROJECT_ID = uuid.uuid4()
ENVIRONMENT_ID = uuid.uuid4()
AUTHOR_ID = uuid.uuid4()
NAME = "Test environment"
DESCRIPTION = "Environment description"

ENVIRONMENT_BODY = {
    "environmentId": str(ENVIRONMENT_ID),
    "projectId": str(PROJECT_ID),
    "name": NAME,
    "description": DESCRIPTION,
    "authorId": str(AUTHOR_ID),
    "createdAt": "2018-10-03T04:20:00Z",
    "updatedAt": "2018-11-03T04:21:15Z",
    "specification": SPECIFICATION_BODY,
}
ENVIRONMENT = Environment(
    id=ENVIRONMENT_ID,
    project_id=PROJECT_ID,
    name=NAME,
    description=DESCRIPTION,
    author_id=AUTHOR_ID,
    created_at=datetime.datetime(2018, 10, 3, 4, 20, 0, 0, tzinfo=UTC),
    updated_at=datetime.datetime(2018, 11, 3, 4, 21, 15, 0, tzinfo=UTC),
    specification=SPECIFICATION,
)

ENVIRONMENT_CREATION_RESPONSE_BODY = {"environmentId": str(ENVIRONMENT_ID)}
ENVIRONMENT_CREATION_RESPONSE = EnvironmentCreationResponse(id=ENVIRONMENT_ID)

ENVIRONMENT_CREATE_UPDATE_BODY = {
    "name": NAME,
    "description": DESCRIPTION,
    "specification": SPECIFICATION_BODY,
}
ENVIRONMENT_CREATE_UPDATE = EnvironmentCreateUpdate(
    name=NAME, description=DESCRIPTION, specification=SPECIFICATION
)
ENVIRONMENT_CREATE_UPDATE_BODY_NO_DESCRIPTION = {
    "name": NAME,
    "description": None,
    "specification": SPECIFICATION_BODY,
}
ENVIRONMENT_CREATE_UPDATE_NO_DESCRIPTION = EnvironmentCreateUpdate(
    name=NAME, description=None, specification=SPECIFICATION
)


def test_python_version_schema_load():
    data = PythonVersionSchema().load(VERSION_BODY)
    assert data == VERSION


def test_python_version_schema_dump():
    data = PythonVersionSchema().dump(VERSION)
    assert data == VERSION_BODY


def test_python_version_schema_load_invalid():
    with pytest.raises(ValidationError):
        PythonVersionSchema().load(INVALID_PYTHON_VERSION_BODY)


def test_python_version_schema_dump_invalid():
    with pytest.raises(ValidationError):
        PythonVersionSchema().dump(INVALID_PYTHON_VERSION)


def test_apt_version_schema_load():
    data = AptVersionSchema().load(VERSION_BODY)
    assert data == VERSION


def test_apt_version_schema_dump():
    data = AptVersionSchema().dump(VERSION)
    assert data == VERSION_BODY


def test_apt_version_schema_load_invalid():
    with pytest.raises(ValidationError):
        AptVersionSchema().load(INVALID_APT_VERSION_BODY)


def test_apt_version_schema_dump_invalid():
    with pytest.raises(ValidationError):
        AptVersionSchema().dump(INVALID_APT_VERSION)


@pytest.mark.parametrize(
    "body, expected",
    [
        (PYTHON_PACKAGE_BODY, PYTHON_PACKAGE),
        (PYTHON_PACKAGE_BODY_LATEST, PYTHON_PACKAGE_LATEST),
    ],
)
def test_python_package_schema_load(body, expected):
    data = PythonPackageSchema().load(body)
    assert data == expected


@pytest.mark.parametrize(
    "object, expected",
    [
        (PYTHON_PACKAGE, PYTHON_PACKAGE_BODY),
        (PYTHON_PACKAGE_LATEST, PYTHON_PACKAGE_BODY_LATEST),
    ],
)
def test_python_package_schema_dump(object, expected):
    data = PythonPackageSchema().dump(object)
    assert data == expected


def test_pip_schema_load():
    data = PipSchema().load(PIP_BODY)
    assert data == PIP


def test_pip_schema_dump():
    data = PipSchema().dump(PIP)
    assert data == PIP_BODY


def test_conda_schema_load():
    data = CondaSchema().load(CONDA_BODY)
    assert data == CONDA


def test_conda_schema_dump():
    data = CondaSchema().dump(CONDA)
    assert data == CONDA_BODY


def test_python_environment_schema_load():
    data = PythonEnvironmentSchema().load(PYTHON_ENVIRONMENT_BODY)
    assert data == PYTHON_ENVIRONMENT


def test_python_environment_schema_dump():
    data = PythonEnvironmentSchema().dump(PYTHON_ENVIRONMENT)
    assert data == PYTHON_ENVIRONMENT_BODY


def test_apt_package_schema_load():
    data = AptPackageSchema().load(APT_PACKAGE_BODY)
    assert data == APT_PACKAGE


def test_apt_package_schema_dump():
    data = AptPackageSchema().dump(APT_PACKAGE)
    assert data == APT_PACKAGE_BODY


def test_apt_schema_load():
    data = AptSchema().load(APT_BODY)
    assert data == APT


def test_apt_schema_dump():
    data = AptSchema().dump(APT)
    assert data == APT_BODY


@pytest.mark.parametrize(
    "body, expected",
    [
        (PYTHON_SPECIFICATION_BODY, PYTHON_SPECIFICATION),
        (
            PYTHON_SPECIFICATION_BODY_MISSING_PYTHON2,
            PYTHON_SPECIFICATION_PYTHON2_NONE,
        ),
        (
            PYTHON_SPECIFICATION_BODY_MISSING_PYTHON3,
            PYTHON_SPECIFICATION_PYTHON3_NONE,
        ),
        (PYTHON_SPECIFICATION_BODY_MISSING_KEYS, PYTHON_SPECIFICATION_NONE),
    ],
)
def test_python_specification_schema_load(body, expected):
    data = PythonSpecificationSchema().load(body)
    assert data == expected


@pytest.mark.parametrize(
    "object, expected",
    [
        (PYTHON_SPECIFICATION, PYTHON_SPECIFICATION_BODY),
        (
            PYTHON_SPECIFICATION_PYTHON2_NONE,
            PYTHON_SPECIFICATION_BODY_PYTHON2_NONE,
        ),
        (
            PYTHON_SPECIFICATION_PYTHON3_NONE,
            PYTHON_SPECIFICATION_BODY_PYTHON3_NONE,
        ),
        (PYTHON_SPECIFICATION_NONE, PYTHON_SPECIFICATION_BODY_NONE),
    ],
)
def test_python_specification_schema_dump(object, expected):
    data = PythonSpecificationSchema().dump(object)
    assert data == expected


def test_script_schema_load():
    data = ScriptSchema().load(SCRIPT_BODY)
    assert data == SCRIPT


def test_script_schema_dump():
    data = ScriptSchema().dump(SCRIPT)
    assert data == SCRIPT_BODY


def test_specification_schema_load():
    data = SpecificationSchema().load(SPECIFICATION_BODY)
    assert data == SPECIFICATION


def test_specification_schema_dump():
    data = SpecificationSchema().dump(SPECIFICATION)
    assert data == SPECIFICATION_BODY


@pytest.mark.parametrize(
    "body, expected",
    [
        (ENVIRONMENT_CREATE_UPDATE_BODY, ENVIRONMENT_CREATE_UPDATE),
        (
            ENVIRONMENT_CREATE_UPDATE_BODY_NO_DESCRIPTION,
            ENVIRONMENT_CREATE_UPDATE_NO_DESCRIPTION,
        ),
    ],
)
def test_environment_create_update_schema_load(body, expected):
    data = EnvironmentCreateUpdateSchema().load(body)
    assert data == expected


@pytest.mark.parametrize(
    "object, expected",
    [
        (ENVIRONMENT_CREATE_UPDATE, ENVIRONMENT_CREATE_UPDATE_BODY),
        (
            ENVIRONMENT_CREATE_UPDATE_NO_DESCRIPTION,
            ENVIRONMENT_CREATE_UPDATE_BODY_NO_DESCRIPTION,
        ),
    ],
)
def test_environment_create_update_schema_dump(object, expected):
    data = EnvironmentCreateUpdateSchema().dump(object)
    assert data == expected


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
    mocker.patch.object(EnvironmentClient, "_put_raw")
    mocker.patch.object(
        EnvironmentCreateUpdateSchema,
        "dump",
        return_value=ENVIRONMENT_CREATE_UPDATE_BODY,
    )

    client = EnvironmentClient(mocker.Mock())
    client.update(PROJECT_ID, ENVIRONMENT_ID, NAME, SPECIFICATION, DESCRIPTION)

    EnvironmentCreateUpdateSchema.dump.assert_called_once_with(
        ENVIRONMENT_CREATE_UPDATE
    )
    EnvironmentClient._put_raw.assert_called_once_with(
        "/project/{}/environment/{}".format(PROJECT_ID, ENVIRONMENT_ID),
        json=ENVIRONMENT_CREATE_UPDATE_BODY,
    )


def test_environment_client_create(mocker):
    mocker.patch.object(
        EnvironmentClient, "_post", return_value=ENVIRONMENT_CREATION_RESPONSE
    )
    mocker.patch.object(
        EnvironmentCreateUpdateSchema,
        "dump",
        return_value=ENVIRONMENT_CREATE_UPDATE_BODY,
    )
    schema_mock = mocker.patch(
        "faculty.clients.environment.EnvironmentCreationResponseSchema"
    )

    client = EnvironmentClient(mocker.Mock())
    assert (
        client.create(PROJECT_ID, NAME, SPECIFICATION, description=DESCRIPTION)
        == ENVIRONMENT_CREATION_RESPONSE.id
    )
    EnvironmentCreateUpdateSchema.dump.assert_called_once_with(
        ENVIRONMENT_CREATE_UPDATE
    )
    EnvironmentClient._post.assert_called_once_with(
        "/project/{}/environment".format(PROJECT_ID),
        schema_mock.return_value,
        json=ENVIRONMENT_CREATE_UPDATE_BODY,
    )


def test_environment_client_delete(mocker):
    mocker.patch.object(EnvironmentClient, "_delete_raw", return_value=None)

    client = EnvironmentClient(mocker.Mock())
    client.delete(PROJECT_ID, ENVIRONMENT_ID)

    EnvironmentClient._delete_raw.assert_called_once_with(
        "/project/{}/environment/{}".format(PROJECT_ID, ENVIRONMENT_ID)
    )
