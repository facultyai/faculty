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

from collections import namedtuple
from enum import Enum

from marshmallow import Schema, fields, post_load
from marshmallow_enum import EnumField

from sherlockml.clients.base import BaseClient

JobMetadata = namedtuple("JobMetadata", ["name", "description"])
JobSummary = namedtuple("JobSummary", ["id", "metadata"])
Page = namedtuple("Page", ["start", "limit"])
Pagination = namedtuple("Pagination", ["start", "size", "previous", "next"])
RunSummary = namedtuple(
    "Run",
    ["id", "run_number", "state", "submitted_at", "started_at", "ended_at"],
)
ListRunsResponse = namedtuple("ListRunsResponse", ["runs", "pagination"])
SubrunSummary = namedtuple(
    "Subrun", ["id", "subrun_number", "state", "started_at", "ended_at"]
)
Run = namedtuple(
    "Run",
    [
        "id",
        "run_number",
        "state",
        "submitted_at",
        "started_at",
        "ended_at",
        "subruns",
    ],
)
EnvironmentStepExecution = namedtuple(
    "EnvironmentStepExecution",
    [
        "environment_id",
        "environment_step_id",
        "environment_name",
        "command",
        "state",
        "started_at",
        "ended_at",
    ],
)
Subrun = namedtuple(
    "Subrun",
    [
        "id",
        "subrun_number",
        "state",
        "started_at",
        "ended_at",
        "environment_step_executions",
    ],
)


class RunState(Enum):
    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ERROR = "error"


class SubrunState(Enum):
    QUEUED = "queued"
    SUBMITTED = "submitted"  # TODO: Remove once replaced with 'starting'
    STARTING = "starting"
    ENVIRONMENT_APPLICATION_STARTED = "environment-application-started"
    COMMAND_STARTED = "command-started"
    COMMAND_SUCCEEDED = "command-succeeded"
    COMMAND_FAILED = "command-failed"
    ENVIRONMENT_APPLICATION_FAILED = "environment-application-failed"
    ERROR = "error"
    CANCELLED = "cancelled"


class EnvironmentStepExecutionState(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobMetadataSchema(Schema):
    name = fields.String(required=True)
    description = fields.String(required=True)

    @post_load
    def make_job_metadata(self, data):
        return JobMetadata(**data)


class JobSummarySchema(Schema):
    id = fields.UUID(data_key="jobId", required=True)
    metadata = fields.Nested(JobMetadataSchema, data_key="meta", required=True)

    @post_load
    def make_job_summary(self, data):
        return JobSummary(**data)


class RunIdSchema(Schema):
    runId = fields.UUID(required=True)

    @post_load
    def make_run_id(self, data):
        return data["runId"]


class PageSchema(Schema):
    start = fields.Integer(required=True)
    limit = fields.Integer(required=True)

    @post_load
    def make_page(self, data):
        return Page(**data)


class PaginationSchema(Schema):
    start = fields.Integer(required=True)
    size = fields.Integer(required=True)
    previous = fields.Nested(PageSchema, missing=None)
    next = fields.Nested(PageSchema, missing=None)

    @post_load
    def make_pagination(self, data):
        return Pagination(**data)


class RunSummarySchema(Schema):
    id = fields.UUID(data_key="runId", required=True)
    run_number = fields.Integer(data_key="runNumber", required=True)
    state = EnumField(RunState, by_value=True, required=True)
    submitted_at = fields.DateTime(data_key="submittedAt", required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)

    @post_load
    def make_run_summary(self, data):
        return RunSummary(**data)


class SubrunSummarySchema(Schema):
    id = fields.UUID(data_key="subrunId", required=True)
    subrun_number = fields.Integer(data_key="subrunNumber", required=True)
    state = EnumField(SubrunState, by_value=True, required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)

    @post_load
    def make_subrun_summary(self, data):
        return SubrunSummary(**data)


class RunSchema(Schema):
    id = fields.UUID(data_key="runId", required=True)
    run_number = fields.Integer(data_key="runNumber", required=True)
    state = EnumField(RunState, by_value=True, required=True)
    submitted_at = fields.DateTime(data_key="submittedAt", required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)
    subruns = fields.Nested(SubrunSummarySchema, many=True, required=True)

    @post_load
    def make_run(self, data):
        return Run(**data)


class ListRunsResponseSchema(Schema):
    pagination = fields.Nested(PaginationSchema, required=True)
    runs = fields.Nested(RunSummarySchema, many=True, required=True)

    @post_load
    def make_list_runs_response_schema(self, data):
        return ListRunsResponse(**data)


class EnvironmentStepExecutionSchema(Schema):
    environment_id = fields.UUID(data_key="environmentId", required=True)
    environment_step_id = fields.UUID(
        data_key="environmentStepId", required=True
    )
    environment_name = fields.String(data_key="environmentName", required=True)
    command = fields.String(required=True)
    state = EnumField(RunState, by_value=True, required=True)
    state = EnumField(
        EnvironmentStepExecutionState, by_value=True, required=True
    )
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)

    @post_load
    def make_environment_step_execution_schema(self, data):
        return EnvironmentStepExecution(**data)


class SubrunSchema(Schema):
    id = fields.UUID(data_key="subrunId", required=True)
    subrun_number = fields.Integer(data_key="subrunNumber", required=True)
    state = EnumField(SubrunState, by_value=True, required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)
    environment_step_executions = fields.Nested(
        EnvironmentStepExecutionSchema,
        data_key="environmentExecutionState",
        many=True,
        required=True,
    )

    @post_load
    def make_subrun(self, data):
        return Subrun(**data)


class JobClient(BaseClient):

    SERVICE_NAME = "steve"

    def list(self, project_id):
        endpoint = "/project/{}/job".format(project_id)
        return self._get(endpoint, JobSummarySchema(many=True))

    def create_run(self, project_id, job_id, parameter_value_sets):
        endpoint = "/project/{}/job/{}/run".format(project_id, job_id)
        payload = {
            "parameterValues": [
                [
                    {"name": name, "value": value}
                    for name, value in parameter_values.items()
                ]
                for parameter_values in parameter_value_sets
            ]
        }
        return self._post(endpoint, RunIdSchema(), json=payload)

    def list_runs(self, project_id, job_id, start=None, limit=None):
        endpoint = "/project/{}/job/{}/run".format(project_id, job_id)
        params = {}
        if start is not None:
            params["start"] = start
        if limit is not None:
            params["limit"] = limit
        return self._get(endpoint, ListRunsResponseSchema(), params=params)

    def get_run(self, project_id, job_id, run_identifier):
        endpoint = "/project/{}/job/{}/run/{}".format(
            project_id, job_id, run_identifier
        )
        return self._get(endpoint, RunSchema())

    def get_subrun(
        self, project_id, job_id, run_identifier, subrun_identifier
    ):
        endpoint = "/project/{}/job/{}/run/{}/subrun/{}".format(
            project_id, job_id, run_identifier, subrun_identifier
        )
        return self._get(endpoint, SubrunSchema())
