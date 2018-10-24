# Copyright 2018 ASI Data Science
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

from uuid import uuid4

import pytest
from marshmallow import ValidationError

from sherlockml.clients.job import (
    JobMetadata,
    JobIdAndMetadata,
    JobMetadataSchema,
    JobIdAndMetadataSchema,
    JobClient,
    RunIdSchema,
)
from tests.clients.fixtures import PROFILE

PROJECT_ID = uuid4()
JOB_ID = uuid4()
RUN_ID = uuid4()

JOB_METADATA = JobMetadata(name="job name", description="job description")
JOB_METADATA_BODY = {
    "name": JOB_METADATA.name,
    "description": JOB_METADATA.description,
}

JOB_ID_AND_METADATA = JobIdAndMetadata(id=JOB_ID, metadata=JOB_METADATA)
JOB_ID_AND_METADATA_BODY = {"jobId": str(JOB_ID), "meta": JOB_METADATA_BODY}


def test_job_metadata_schema():
    data = JobMetadataSchema().load(JOB_METADATA_BODY)
    assert data == JOB_METADATA


def test_job_metadata_schema_invalid():
    with pytest.raises(ValidationError):
        JobMetadataSchema().load({})


def test_job_id_and_metadata_schema():
    data = JobIdAndMetadataSchema().load(JOB_ID_AND_METADATA_BODY)
    assert data == JOB_ID_AND_METADATA


def test_job_id_and_metadata_schema_invalid():
    with pytest.raises(ValidationError):
        JobIdAndMetadataSchema().load({})


def test_run_id_schema():
    data = RunIdSchema().load({"runId": str(RUN_ID)})
    assert data == RUN_ID


def test_run_id_schema_invalid():
    with pytest.raises(ValidationError):
        RunIdSchema().load({})


def test_job_client_list(mocker):
    mocker.patch.object(JobClient, "_get", return_value=[JOB_ID_AND_METADATA])
    schema_mock = mocker.patch("sherlockml.clients.job.JobIdAndMetadataSchema")

    client = JobClient(PROFILE)
    assert client.list(PROJECT_ID) == [JOB_ID_AND_METADATA]

    schema_mock.assert_called_once_with(many=True)
    JobClient._get.assert_called_once_with(
        "/project/{}/job".format(PROJECT_ID), schema_mock.return_value
    )


def test_job_client_create_run(mocker):
    mocker.patch.object(JobClient, "_post", return_value=RUN_ID)
    schema_mock = mocker.patch("sherlockml.clients.job.RunIdSchema")

    client = JobClient(PROFILE)
    assert (
        client.create_run(PROJECT_ID, JOB_ID, {"param": "one", "other": "two"})
        == RUN_ID
    )

    schema_mock.assert_called_once_with()
    JobClient._post.assert_called_once_with(
        "/project/{}/job/{}/run".format(PROJECT_ID, JOB_ID),
        schema_mock.return_value,
        json={
            "parameterValues": [
                [
                    {"name": "param", "value": "one"},
                    {"name": "other", "value": "two"},
                ]
            ]
        },
    )


def test_job_client_create_run_array(mocker):
    mocker.patch.object(JobClient, "_post", return_value=RUN_ID)
    schema_mock = mocker.patch("sherlockml.clients.job.RunIdSchema")

    client = JobClient(PROFILE)
    assert (
        client.create_run_array(
            PROJECT_ID,
            JOB_ID,
            [{"param": "one", "other": "two"}, {"param": "three"}],
        )
        == RUN_ID
    )

    schema_mock.assert_called_once_with()
    JobClient._post.assert_called_once_with(
        "/project/{}/job/{}/run".format(PROJECT_ID, JOB_ID),
        schema_mock.return_value,
        json={
            "parameterValues": [
                [
                    {"name": "param", "value": "one"},
                    {"name": "other", "value": "two"},
                ],
                [{"name": "param", "value": "three"}],
            ]
        },
    )
