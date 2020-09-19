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

import uuid
from datetime import datetime

import pytest
import attr
from dateutil.tz import UTC
from marshmallow import ValidationError

from faculty.clients.model import (
    ExperimentModelSource,
    Model,
    ModelClient,
    ModelVersion,
    _ExperimentModelSourceSchema,
    _ModelSchema,
    _ModelVersionSchema,
)


PROJECT_ID = uuid.uuid4()
MODEL_ID = uuid.uuid4()
VERSION_ID = uuid.uuid4()
USER_ID = uuid.uuid4()

REGISTERED_AT = datetime(2019, 8, 19, 15, 23, 53, 268000, tzinfo=UTC)
REGISTERED_AT_STRING = "2019-08-19T15:23:53.268Z"

EXPERIMENT_MODEL_SOURCE = ExperimentModelSource(
    experiment_id=43, experiment_run_id=uuid.uuid4()
)
EXPERIMENT_MODEL_SOURCE_JSON = {
    "type": "experiment",
    "experimentId": EXPERIMENT_MODEL_SOURCE.experiment_id,
    "experimentRunId": str(EXPERIMENT_MODEL_SOURCE.experiment_run_id),
}

MODEL_VERSION = ModelVersion(
    id=VERSION_ID,
    version_number=23,
    registered_at=REGISTERED_AT,
    registered_by=USER_ID,
    artifact_path="scheme:path/to/artifact",
    source=EXPERIMENT_MODEL_SOURCE,
)
MODEL_VERSION_JSON = {
    "modelVersionId": str(VERSION_ID),
    "modelVersionNumber": MODEL_VERSION.version_number,
    "registeredAt": REGISTERED_AT_STRING,
    "registeredBy": str(USER_ID),
    "artifactPath": MODEL_VERSION.artifact_path,
    "source": EXPERIMENT_MODEL_SOURCE_JSON,
}

MODEL = Model(
    id=MODEL_ID,
    name="model name",
    description="model description",
    user_ids=[USER_ID],
    latest_version=MODEL_VERSION,
)
MODEL_JSON = {
    "modelId": str(MODEL_ID),
    "name": MODEL.name,
    "description": MODEL.description,
    "users": [str(USER_ID)],
    "latestVersion": MODEL_VERSION_JSON,
}


def test_experiment_model_source_schema():
    data = _ExperimentModelSourceSchema().load(EXPERIMENT_MODEL_SOURCE_JSON)
    assert data == EXPERIMENT_MODEL_SOURCE


def test_model_version_schema():
    data = _ModelVersionSchema().load(MODEL_VERSION_JSON)
    assert data == MODEL_VERSION


def test_model_schema():
    data = _ModelSchema().load(MODEL_JSON)
    assert data == MODEL


def test_model_schema_without_latest_version():
    model_json = MODEL_JSON.copy()
    del model_json["latestVersion"]
    data = _ModelSchema().load(model_json)
    assert data == attr.evolve(MODEL, latest_version=None)


@pytest.mark.parametrize(
    "schema", [_ExperimentModelSourceSchema, _ModelVersionSchema, _ModelSchema]
)
def test_schemas_invalid(schema):
    with pytest.raises(ValidationError):
        schema().load({})


def test_model_client_get(mocker):
    mocker.patch.object(ModelClient, "_get", return_value=MODEL)
    schema_mock = mocker.patch("faculty.clients.model._ModelSchema")
    client = ModelClient(mocker.Mock())

    assert client.get(PROJECT_ID, MODEL_ID) == MODEL

    schema_mock.assert_called_once_with()
    ModelClient._get.assert_called_once_with(
        "/project/{}/model/{}".format(PROJECT_ID, MODEL_ID),
        schema_mock.return_value,
    )


def test_model_client_list(mocker):
    mocker.patch.object(ModelClient, "_get", return_value=[MODEL])
    schema_mock = mocker.patch("faculty.clients.model._ModelSchema")
    client = ModelClient(mocker.Mock())

    assert client.list(PROJECT_ID) == [MODEL]

    schema_mock.assert_called_once_with(many=True)
    ModelClient._get.assert_called_once_with(
        "/project/{}/model".format(PROJECT_ID), schema_mock.return_value
    )


def test_model_client_get_version(mocker):
    mocker.patch.object(ModelClient, "_get", return_value=MODEL_VERSION)
    schema_mock = mocker.patch("faculty.clients.model._ModelVersionSchema")
    client = ModelClient(mocker.Mock())

    assert (
        client.get_version(PROJECT_ID, MODEL_ID, VERSION_ID) == MODEL_VERSION
    )

    schema_mock.assert_called_once_with()
    ModelClient._get.assert_called_once_with(
        "/project/{}/model/{}/version/{}".format(
            PROJECT_ID, MODEL_ID, VERSION_ID
        ),
        schema_mock.return_value,
    )


def test_model_client_list_versions(mocker):
    mocker.patch.object(ModelClient, "_get", return_value=[MODEL_VERSION])
    schema_mock = mocker.patch("faculty.clients.model._ModelVersionSchema")
    client = ModelClient(mocker.Mock())

    assert client.list_versions(PROJECT_ID, MODEL_ID) == [MODEL_VERSION]

    schema_mock.assert_called_once_with(many=True)
    ModelClient._get.assert_called_once_with(
        "/project/{}/model/{}/version".format(PROJECT_ID, MODEL_ID),
        schema_mock.return_value,
    )
