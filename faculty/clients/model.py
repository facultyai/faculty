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


from attr import attrs, attrib
from marshmallow import fields, post_load, validate

from faculty.clients.base import BaseSchema, BaseClient


@attrs
class ExperimentModelSource(object):
    experiment_id = attrib()
    experiment_run_id = attrib()


@attrs
class ModelVersion(object):
    id = attrib()
    version_number = attrib()
    registered_at = attrib()
    registered_by = attrib()
    artifact_path = attrib()
    source = attrib()


@attrs
class Model(object):
    id = attrib()
    name = attrib()
    description = attrib()
    user_ids = attrib()
    latest_version = attrib()


class ExperimentModelSourceSchema(BaseSchema):
    # For now, just validate that the type is experiment
    type = fields.String(
        required=True, validate=validate.OneOf(["experiment"])
    )
    experiment_id = fields.Integer(data_key="experimentId", required=True)
    experiment_run_id = fields.UUID(data_key="experimentRunId", required=True)

    @post_load
    def make_experiment_model_source(self, data):
        del data["type"]
        return ExperimentModelSource(**data)


class ModelVersionSchema(BaseSchema):
    id = fields.UUID(data_key="modelVersionId", required=True)
    version_number = fields.Integer(
        data_key="modelVersionNumber", required=True
    )
    registered_at = fields.DateTime(data_key="registeredAt", required=True)
    registered_by = fields.UUID(data_key="registeredBy", required=True)
    artifact_path = fields.String(data_key="artifactPath", required=True)
    source = fields.Nested(ExperimentModelSourceSchema, required=True)

    @post_load
    def make_model_latest_version(self, data):
        return ModelVersion(**data)


class ModelSchema(BaseSchema):
    id = fields.UUID(data_key="modelId", required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    user_ids = fields.List(fields.UUID, data_key="users", required=True)
    latest_version = fields.Nested(
        ModelVersionSchema, data_key="latestVersion", missing=None
    )

    @post_load
    def make_model(self, data):
        return Model(**data)


class ModelClient(BaseClient):

    _SERVICE_NAME = "zoolander"

    def get(self, project_id, model_id):
        endpoint = "/project/{}/model/{}".format(project_id, model_id)
        return self._get(endpoint, ModelSchema())

    def list(self, project_id):
        endpoint = "/project/{}/model".format(project_id)
        return self._get(endpoint, ModelSchema(many=True))

    def get_version(self, project_id, model_id, version_id):
        endpoint = "/project/{}/model/{}/version/{}".format(
            project_id, model_id, version_id
        )
        return self._get(endpoint, ModelVersionSchema())

    def list_versions(self, project_id, model_id):
        endpoint = "/project/{}/model/{}/version".format(project_id, model_id)
        return self._get(endpoint, ModelVersionSchema(many=True))
