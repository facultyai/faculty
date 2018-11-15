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


class EnvironmentStepExecutionState(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SubrunState(Enum):
    QUEUED = "queued"
    STARTING = "starting"
    ENVIRONMENT_APPLICATION_STARTED = "environment-application-started"
    COMMAND_STARTED = "command-started"
    COMMAND_SUCCEEDED = "command-succeeded"
    COMMAND_FAILED = "command-failed"
    ENVIRONMENT_APPLICATION_FAILED = "environment-application-failed"
    ERROR = "error"
    CANCELLED = "cancelled"


class RunState(Enum):
    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ERROR = "error"


JobMetadata = namedtuple("JobMetadata", ["name", "description"])
JobSummary = namedtuple("JobSummary", ["id", "metadata"])
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
SubrunSummary = namedtuple(
    "Subrun", ["id", "subrun_number", "state", "started_at", "ended_at"]
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
RunSummary = namedtuple(
    "Run",
    ["id", "run_number", "state", "submitted_at", "started_at", "ended_at"],
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
Page = namedtuple("Page", ["start", "limit"])
Pagination = namedtuple("Pagination", ["start", "size", "previous", "next"])
ListRunsResponse = namedtuple("ListRunsResponse", ["runs", "pagination"])


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


class SubrunSummarySchema(Schema):
    id = fields.UUID(data_key="subrunId", required=True)
    subrun_number = fields.Integer(data_key="subrunNumber", required=True)
    state = EnumField(SubrunState, by_value=True, required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)

    @post_load
    def make_subrun_summary(self, data):
        return SubrunSummary(**data)


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


class ListRunsResponseSchema(Schema):
    pagination = fields.Nested(PaginationSchema, required=True)
    runs = fields.Nested(RunSummarySchema, many=True, required=True)

    @post_load
    def make_list_runs_response_schema(self, data):
        return ListRunsResponse(**data)


class JobClient(BaseClient):

    SERVICE_NAME = "steve"

    def list(self, project_id):
        """List the jobs in a project.

        Parameters
        ----------
        project_id : uuid.UUID

        Returns
        -------
        List[JobSummary]
        """
        endpoint = "/project/{}/job".format(project_id)
        return self._get(endpoint, JobSummarySchema(many=True))

    def create_run(self, project_id, job_id, parameter_value_sets=None):
        """Create a run for a job.

        When creating a run, each item in ``parameter_value_sets`` will be
        translated into an individual subrun. For example, to start a single
        run of job with ``file`` and ``alpha`` arguments:

        >>> client.create_run(
        >>>     project_id, job_id, [{"file": "data.txt", "alpha": "0.1"}]
        >>> )

        Pass additional entries in ``parameter_value_sets`` to start a run
        array with multiple subruns. For example, for a job with a single
        ``file`` argument:

        >>> client.create_run(
        >>>     project_id,
        >>>     job_id,
        >>>     [{"file": "data1.txt"}, {"file": "data2.txt"}]
        >>> )

        Many jobs do not take any arguments. In this case, simply pass a list
        containing empty parameter value dictionaries, with the number of
        entries in the list corresponding to the number of subruns you want:

        >>> client.create_run(project_id, job_id, [{}, {}])

        Parameters
        ----------
        project_id : uuid.UUID
        job_id : uuid.UUID
        parameter_value_sets : List[dict], optional
            A list of parameter value sets. Each set of parameter values will
            result in a subrun with those parameter values passed. Default:
            single subrun with no parameter values.

        Returns
        -------
        uuid.UUID
            The ID of the created run.
        """

        if parameter_value_sets is None:
            parameter_value_sets = [{}]

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
        """List the runs of a job.

        This method returns pages of runs. If less than the full number of runs
        for the job is returned, the ``next`` page of the returned response
        object will not be ``None``:

        >>> response = client.list_runs(project_id, job_id)
        >>> response.pagination.next
        Page(start=10, limit=10)

        Get all the runs for a job by making successive calls to ``list_runs``,
        passing the ``start`` and ``limit`` of the ``next`` page each time
        until ``next`` is returned as ``None``.

        Parameters
        ----------
        project_id : uuid.UUID
        job_id : uuid.UUID
        start : int, optional
            The (zero-indexed) starting point of runs to retrieve.
        limit : int, optional
            The maximum number of runs to retrieve.

        Returns
        -------
        ListRunsResponse
        """
        endpoint = "/project/{}/job/{}/run".format(project_id, job_id)
        params = {}
        if start is not None:
            params["start"] = start
        if limit is not None:
            params["limit"] = limit
        return self._get(endpoint, ListRunsResponseSchema(), params=params)

    def get_run(self, project_id, job_id, run_identifier):
        """Get a run of a job.

        Parameters
        ----------
        project_id : uuid.UUID
        job_id : uuid.UUID
        run_identifier : uuid.UUID or int
            The ID of the run to get or its run number.

        Returns
        -------
        Run
        """
        endpoint = "/project/{}/job/{}/run/{}".format(
            project_id, job_id, run_identifier
        )
        return self._get(endpoint, RunSchema())

    def get_subrun(
        self, project_id, job_id, run_identifier, subrun_identifier
    ):
        """Get a subrun of a job.

        Parameters
        ----------
        project_id : uuid.UUID
        job_id : uuid.UUID
        run_identifier : uuid.UUID or int
            The ID of the run to get or its run number.
        subrun_identifier : uuid.UUID or int
            The ID of the subrun to get or its subrun number.

        Returns
        -------
        Subrun
        """
        endpoint = "/project/{}/job/{}/run/{}/subrun/{}".format(
            project_id, job_id, run_identifier, subrun_identifier
        )
        return self._get(endpoint, SubrunSchema())
