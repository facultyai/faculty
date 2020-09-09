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
Interact with the Faculty model registry.
"""


from attr import attrs, attrib
from marshmallow import fields, post_load, validate

from faculty.clients.base import BaseSchema, BaseClient


@attrs
class ExperimentModelSource(object):
    """Description of the experiment used to generate a model in the registry.

    Parameters
    ----------
    experiment_id : int
        The ID of the experiment the model was generated from.
    experiment_run_id : uuid.UUID
        The ID of the experiment run the model was generated from.
    """

    experiment_id = attrib()
    experiment_run_id = attrib()


@attrs
class ModelVersion(object):
    """A version of a model in the registry.

    Parameters
    ----------
    id : uuid.UUID
        The model version ID.
    version_number : int
        The integer version number.
    registered_at : datetime
        The time the model version was created.
    registered_by : uuid.UUID
        The ID of the user who registered the model version.
    artifact_path : str
        The MLflow artifact path where the model is stored.
    source : ExperimentModelSource
        Where this model version was created from.
    """

    id = attrib()
    version_number = attrib()
    registered_at = attrib()
    registered_by = attrib()
    artifact_path = attrib()
    source = attrib()


@attrs
class Model(object):
    """A model in the registry.

    Parameters
    ----------
    id : uuid.UUID
        The model ID.
    name : str
        The name of the model.
    description : str
        The description of the model.
    user_ids : List[uuid.UUID]
        The IDs of users who have registered versions of this model.
    latest_version : ModelVersion
        The latest version of the model.
    """

    id = attrib()
    name = attrib()
    description = attrib()
    user_ids = attrib()
    latest_version = attrib()


class ModelClient(BaseClient):
    """Client for the Faculty model service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("model")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    """

    _SERVICE_NAME = "zoolander"

    def get(self, project_id, model_id):
        """Get a model in the registry.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the model.
        model_id : uuid.UUID
            The ID of the model.

        Returns
        -------
        Model
            The retrieved model.
        """
        endpoint = "/project/{}/model/{}".format(project_id, model_id)
        return self._get(endpoint, _ModelSchema())

    def list(self, project_id):
        """List models in a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to list models in.

        Returns
        -------
        List[Model]
            The retrieved models.
        """
        endpoint = "/project/{}/model".format(project_id)
        return self._get(endpoint, _ModelSchema(many=True))

    def get_version(self, project_id, model_id, version_id):
        """Get a version of a model in the registry.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the model.
        model_id : uuid.UUID
            The ID of the model.
        version_id : uuid.UUID
            The ID of the model version.

        Returns
        -------
        ModelVersion
            The retrieved model version.
        """
        endpoint = "/project/{}/model/{}/version/{}".format(
            project_id, model_id, version_id
        )
        return self._get(endpoint, _ModelVersionSchema())

    def list_versions(self, project_id, model_id):
        """List the versions of a model in the registry.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the model.
        model_id : uuid.UUID
            The ID of the model.

        Returns
        -------
        List[ModelVersion]
            The retrieved model versions.
        """
        endpoint = "/project/{}/model/{}/version".format(project_id, model_id)
        return self._get(endpoint, _ModelVersionSchema(many=True))


class _ExperimentModelSourceSchema(BaseSchema):
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


class _ModelVersionSchema(BaseSchema):
    id = fields.UUID(data_key="modelVersionId", required=True)
    version_number = fields.Integer(
        data_key="modelVersionNumber", required=True
    )
    registered_at = fields.DateTime(data_key="registeredAt", required=True)
    registered_by = fields.UUID(data_key="registeredBy", required=True)
    artifact_path = fields.String(data_key="artifactPath", required=True)
    source = fields.Nested(_ExperimentModelSourceSchema, required=True)

    @post_load
    def make_model_latest_version(self, data):
        return ModelVersion(**data)


class _ModelSchema(BaseSchema):
    id = fields.UUID(data_key="modelId", required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    user_ids = fields.List(fields.UUID, data_key="users", required=True)
    latest_version = fields.Nested(
        _ModelVersionSchema, data_key="latestVersion", missing=None
    )

    @post_load
    def make_model(self, data):
        return Model(**data)
