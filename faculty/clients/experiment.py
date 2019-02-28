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


from collections import namedtuple

from marshmallow import fields, post_load

from faculty.clients.base import BaseSchema, BaseClient


Experiment = namedtuple(
    "Experiment",
    [
        "id",
        "name",
        "description",
        "artifact_location",
        "created_at",
        "last_updated_at",
        "deleted_at",
    ],
)


ExperimentRun = namedtuple("ExperimentRun", ["id", "experiment_id", "artifact_uri"])


class ExperimentSchema(BaseSchema):
    id = fields.Integer(data_key="experimentId", required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    artifact_location = fields.String(
        data_key="artifactLocation", required=True
    )
    created_at = fields.DateTime(data_key="createdAt", required=True)
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt", required=True)
    deleted_at = fields.DateTime(data_key="deletedAt", missing=None)

    @post_load
    def make_experiment(self, data):
        return Experiment(**data)


class ExperimentRunSchema(BaseSchema):
    id = fields.UUID(data_key="runId", required=True)
    experiment_id = fields.Integer(data_key="experimentId", required=True)
    artifact_uri = fields.String(
        data_key="artifactUri", required=True
    )

    @post_load
    def make_experiment_run(self, data):
        return ExperimentRun(**data)


class ExperimentClient(BaseClient):

    SERVICE_NAME = "atlas"

    def create(
        self, project_id, name, description=None, artifact_location=None
    ):
        """Create an experiment.

        Parameters
        ----------
        project_id : uuid.UUID
        name : str
        description : str, optional
        artifact_location : str, optional

        Returns
        -------
        Experiment
        """
        endpoint = "/project/{}/experiment".format(project_id)
        payload = {
            "name": name,
            "description": description,
            "artifactLocation": artifact_location,
        }
        return self._post(endpoint, ExperimentSchema(), json=payload)

    def get(self, project_id, experiment_id):
        """Get a specified experiment.

        Parameters
        ----------
        project_id : uuid.UUID
        experiment_id : uuid.UUID

        Returns
        -------
        Experiment
        """
        endpoint = "/project/{}/experiment/{}".format(
            project_id, experiment_id
        )
        return self._get(endpoint, ExperimentSchema())

    def list(self, project_id):
        """List the experiments in a project.

        Parameters
        ----------
        project_id : uuid.UUID

        Returns
        -------
        List[Experiment]
        """
        endpoint = "/project/{}/experiment".format(project_id)
        return self._get(endpoint, ExperimentSchema(many=True))

    def create_run(
        self, project_id, started_at, experiment_id=None, artifact_uri=None
    ):
        endpoint = "/project/{}/run".format(project_id)
        payload = {
            "experimentId": experiment_id,
            "startedAt": started_at.isoformat(),
            "artifactUri": artifact_uri,
        }
        return self._post(endpoint, ExperimentRunSchema(), json=payload)
