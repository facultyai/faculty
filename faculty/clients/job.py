# Copyright 2018-2021 Faculty Science Limited
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
Configure and run Faculty jobs.
"""


from enum import Enum

from attr import attrs, attrib
from marshmallow import ValidationError, fields, post_load, validates_schema
from marshmallow_enum import EnumField

from faculty.clients.base import BaseClient, BaseSchema


class ParameterType(Enum):
    """An enumeration of allowed parameter types for a job.

    :const:`ParameterType.TEXT` parameters allow any string to be passed when
    running a job, while :const:`ParameterType.NUMBER` parameters must be a
    valid number.
    """

    TEXT = "text"
    NUMBER = "number"


class ImageType(Enum):
    """An enumeration of allows image types for a job."""

    PYTHON = "python"
    R = "r"


class EnvironmentStepExecutionState(Enum):
    """An enumeration of environment step execution states."""

    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


class SubrunState(Enum):
    """An enumeration of states for a job subrun."""

    QUEUED = "queued"
    STARTING = "starting"
    ENVIRONMENT_APPLICATION_STARTED = "environment-application-started"
    COMMAND_STARTED = "command-started"
    COMMAND_SUCCEEDED = "command-succeeded"
    COMMAND_FAILED = "command-failed"
    ENVIRONMENT_APPLICATION_FAILED = "environment-application-failed"
    ERROR = "error"
    CANCELLED = "cancelled"
    TIMED_OUT = "timed-out"


class RunState(Enum):
    """An enumeration of states for a job run."""

    QUEUED = "queued"
    STARTING = "starting"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    ERROR = "error"


@attrs
class JobMetadata:
    """Metadata on a job in Faculty.

    Parameters
    ----------
    name : str
        The name of the job.
    description : str
        A brief description of the job.
    author_id : UUID
        The user ID of the creator of the job.
    created_at : datetime
        The time when the job was created.
    last_updated_at : datetime
        The time when the job was last updated.
    """

    name = attrib()
    description = attrib()
    author_id = attrib()
    created_at = attrib()
    last_updated_at = attrib()


@attrs
class JobSummary:
    """A concise representation of a job in Faculty.

    Parameters
    ----------
    id : UUID
        The ID of the job.
    metadata : JobMetadata
        The metadata of the job.
    """

    id = attrib()
    metadata = attrib()


@attrs
class InstanceSize:
    """The CPU and memory that a Faculty job is configured to use.

    Parameters
    ----------
    milli_cpus : int
        The allocation of CPU for a job, counted in thousandths of a CPU.
    memory_mb : int
        The allocation of memory for a job, counted in megabytes.
    """

    milli_cpus = attrib()
    memory_mb = attrib()


@attrs
class JobParameter:
    """A parameter of a Faculty job.

    Parameters
    ----------
    name : str
        The unique (for this job) name that identifies this parameter.
    type : ParameterType
        The type of this parameter.
    default : str
        The default value for this parameter.
    required : bool
        If True, this parameter must be set when submitting a run of the job.
    """

    name = attrib()
    type = attrib()
    default = attrib()
    required = attrib()


@attrs
class JobCommand:
    """The command to be run by a Faculty job, with associated parameters.

    Parameters will be passed to the job as environment variables. They can be
    used either by accessing them from your script directly or by relying on
    shell variable interpolation.

    For example, with the parameter named `batch_size`, you could read from the
    environment inside your script with `os.environ`:

    >>> import os
    >>> batch_size = int(os.environ["batch_size"])

    or you could use in the command like::

       python myscript.py $batch_size

    Parameters
    ----------
    name : str
        The command to be run by the job, e.g. `python myscript.py`
    parameters : List[JobParameter]
        Any parameters for the job.
    """

    name = attrib()
    parameters = attrib()


@attrs
class JobDefinition:
    """The complete description of how to execute a Faculty job.

    Parameters
    ----------
    working_dir : str
        The working directory where the job is to be executed.
    command : JobCommand
        The command and parameters to be executed.
    image_type : ImageType
        The job image type to be used.
    environment_ids : List[UUID]
        A list of any Faculty environments to be applied before running the job
        command.
    instance_size_type : str
        Either "custom", indicating that a server of `instance_size` should be
        executed, or a supported instance type (e.g. "m5.xlarge").
    instance_size : Optional[InstanceSize]
        The CPU and memory to use when `instance_size_type` is "custom",
        otherwise `None.`
    max_runtime_seconds : int
        The max execution time of the job, in seconds.
    """

    working_dir = attrib()
    command = attrib()
    image_type = attrib()
    environment_ids = attrib()
    instance_size_type = attrib()
    instance_size = attrib()
    max_runtime_seconds = attrib()


@attrs
class Job:
    """A job in Faculty.

    Parameters
    ----------
    id : UUID
        The ID of the job.
    meta : JobMetadata
        The metadata of the job.
    definition : JobDefinition
        The description of how to execute the job.
    """

    id = attrib()
    meta = attrib()
    definition = attrib()


@attrs
class EnvironmentStepExecution:
    """Information about one step in the execution of an environment.

    Parameters
    ----------
    environment_id : UUID
        The ID of the environment being applied.
    environment_step_id : UUID
        The ID of the environment step.
    environment_name : str
        The name of the environment being applied.
    command : str
        The command being executed to perform this environment step.
    state : EnvironmentStepExecutionState
        The current state of this environment step execution.
    started_at : datetime
        The time this step started executing.
    ended_at : Optional[datetime]
        If finished, the time this step ended.
    """

    environment_id = attrib()
    environment_step_id = attrib()
    environment_name = attrib()
    command = attrib()
    state = attrib()
    started_at = attrib()
    ended_at = attrib()


@attrs
class SubrunSummary:
    """A concise representation of a subrun of a job.

    Parameters
    ----------
    id : UUID
        The ID of the subrun.
    subrun_number : int
        The position of this subrun within its parent run (numbering starts
        from 1).
    state : SubrunState
        The current state of the subrun.
    started_at : datetime
        The time this subrun started executing.
    ended_at : Optional[datetime]
        If finished, the time this subrun ended.
    """

    id = attrib()
    subrun_number = attrib()
    state = attrib()
    started_at = attrib()
    ended_at = attrib()


@attrs
class Subrun:
    """A subrun of a job.

    Parameters
    ----------
    id : UUID
        The ID of the subrun.
    subrun_number : int
        The position of this subrun within its parent run (numbering starts
        from 1).
    state : SubrunState
        The current state of the subrun.
    started_at : datetime
        The time this subrun started executing.
    ended_at : Optional[datetime]
        If finished, the time this subrun ended.
    environment_step_executions : List[EnvironmentStepExecution]
        The environment steps run by this subrun.
    """

    id = attrib()
    subrun_number = attrib()
    state = attrib()
    started_at = attrib()
    ended_at = attrib()
    environment_step_executions = attrib()


@attrs
class RunSummary:
    """A concise representation of a run of a job.

    Parameters
    ----------
    id : UUID
        The ID of the run.
    run_number : int
        The number of this run. For each job, runs are numbered as increasing
        integers starting from 1.
    state : RunState
        The current state of the run.
    started_at : datetime
        The time this run started executing.
    ended_at : Optional[datetime]
        If finished, the time this run ended.
    """

    id = attrib()
    run_number = attrib()
    state = attrib()
    submitted_at = attrib()
    started_at = attrib()
    ended_at = attrib()


@attrs
class Run:
    """A run of a job.

    Parameters
    ----------
    id : UUID
        The ID of the run.
    run_number : int
        The number of this run. For each job, runs are numbered as increasing
        integers starting from 1.
    state : RunState
        The current state of the run.
    started_at : datetime
        The time this run started executing.
    ended_at : Optional[datetime]
        If finished, the time this run ended.
    subruns : List[SubrunSummary]
        The subruns of this run.
    """

    id = attrib()
    run_number = attrib()
    state = attrib()
    submitted_at = attrib()
    started_at = attrib()
    ended_at = attrib()
    subruns = attrib()


@attrs
class Page:
    """A reference to a page of entities.

    Parameters
    ----------
    start : int
        The index of the first entity in the page.
    limit : int
        The maximum number of entities in the page.
    """

    start = attrib()
    limit = attrib()


@attrs
class Pagination:
    """A description of the pagination context of a returned set of entities.

    Parameters
    ----------
    start : int
        The index of the first entity in this page.
    size : int
        The number of entities in this page.
    previous : Optional[Page]
        Description of the previous page of entities, if any.
    next : Optional[Page]
        Description of the next page of entities, if any.
    """

    start = attrib()
    size = attrib()
    previous = attrib()
    next = attrib()


@attrs
class ListRunsResponse:
    """A paginated response of job runs.

    Parameters
    ----------
    runs : List[Run]
        The runs in this page.
    pagination : Pagination
        Information about this and neighbouring pages of runs.
    """

    runs = attrib()
    pagination = attrib()


class JobClient(BaseClient):
    """Client for the Faculty job service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("job")

    Parameters
    ----------
    url : str
        The URL of the job service.
    session : faculty.session.Session
        The session to use to make requests.
    """

    SERVICE_NAME = "steve"

    def list(self, project_id):
        """List the jobs in a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to list jobs in.

        Returns
        -------
        List[JobSummary]
            The jobs in the project.
        """
        endpoint = "/project/{}/job".format(project_id)
        return self._get(endpoint, _JobSummarySchema(many=True))

    def create(self, project_id, name, description, job_definition):
        """Create a job.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to create a job in.
        name : str
            The name of the new job.
        description : str
            The description of the new job.
        job_definition : JobDefinition
            The configuration of the new job.

        Returns
        -------
        uuid.UUID
            The ID of the created job.
        """

        job_metadata_body = {"name": name, "description": description}
        job_definition_body = _JobDefinitionSchema().dump(job_definition)
        endpoint = "/project/{}/job".format(project_id)
        payload = {
            "meta": job_metadata_body,
            "definition": job_definition_body,
        }

        return self._post(endpoint, _JobIdSchema(), json=payload)

    def get(self, project_id, job_id):
        """Get a job.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the job.
        job_id : uuid.UUID
            The ID of the job to get.

        Returns
        -------
        Job
            The retrieved job.
        """
        endpoint = "/project/{}/job/{}".format(project_id, job_id)
        return self._get(endpoint, _JobSchema())

    def update_metadata(self, project_id, job_id, name, description):
        """Update the metadata of a job.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the job.
        job_id : uuid.UUID
            The ID of the job to update.
        name : str
            The new name of the job.
        description : str
            The new description of the job
        """
        endpoint = "/project/{}/job/{}/meta".format(project_id, job_id)
        payload = {"name": name, "description": description}
        self._put_raw(endpoint, json=payload)

    def update_definition(self, project_id, job_id, job_definition):
        """Update the definition of a job.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the job.
        job_id : uuid.UUID
            The ID of the job to update.
        job_definition : JobDefinition
            The new definition of the job.
        """
        endpoint = "/project/{}/job/{}/definition".format(project_id, job_id)
        payload = _JobDefinitionSchema().dump(job_definition)

        self._put_raw(endpoint, json=payload)

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
            The ID of the project containing the job.
        job_id : uuid.UUID
            The ID of the job to run.
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
        return self._post(endpoint, _RunIdSchema(), json=payload)

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
            The ID of the project containing the job.
        job_id : uuid.UUID
            The ID of the job to list runs from.
        start : int, optional
            The (zero-indexed) starting point of runs to retrieve.
        limit : int, optional
            The maximum number of runs to retrieve.

        Returns
        -------
        ListRunsResponse
            The retrieved job runs.
        """
        endpoint = "/project/{}/job/{}/run".format(project_id, job_id)
        params = {}
        if start is not None:
            params["start"] = start
        if limit is not None:
            params["limit"] = limit
        return self._get(endpoint, _ListRunsResponseSchema(), params=params)

    def get_run(self, project_id, job_id, run_identifier):
        """Get a run of a job.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the job.
        job_id : uuid.UUID
            The ID of the job containing the run.
        run_identifier : uuid.UUID or int
            The ID of the run to get or its run number.

        Returns
        -------
        Run
            The retrieved run.
        """
        endpoint = "/project/{}/job/{}/run/{}".format(
            project_id, job_id, run_identifier
        )
        return self._get(endpoint, _RunSchema())

    def get_subrun(
        self, project_id, job_id, run_identifier, subrun_identifier
    ):
        """Get a subrun of a job.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the job.
        job_id : uuid.UUID
            The ID of the job containing the run.
        run_identifier : uuid.UUID or int
            The ID of the run to get or its run number.
        subrun_identifier : uuid.UUID or int
            The ID of the subrun to get or its subrun number.

        Returns
        -------
        Subrun
            The retrieved subrun.
        """
        endpoint = "/project/{}/job/{}/run/{}/subrun/{}".format(
            project_id, job_id, run_identifier, subrun_identifier
        )
        return self._get(endpoint, _SubrunSchema())

    def cancel_run(self, project_id, job_id, run_identifier):
        """Cancel a running job.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the job.
        job_id : uuid.UUID
            The ID of the job containing the run.
        run_identifier : uuid.UUID or int
            The ID or number of the run to cancel.
        """

        endpoint = "/project/{}/job/{}/run/{}".format(
            project_id, job_id, run_identifier
        )
        self._delete_raw(endpoint)


class _JobMetadataSchema(BaseSchema):
    name = fields.String(required=True)
    description = fields.String(required=True)
    author_id = fields.UUID(data_key="authorId", required=True)
    created_at = fields.DateTime(data_key="createdAt", required=True)
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt", required=True)

    @post_load
    def make_job_metadata(self, data, **kwargs):
        return JobMetadata(**data)


class _JobSummarySchema(BaseSchema):
    id = fields.UUID(data_key="jobId", required=True)
    metadata = fields.Nested(
        _JobMetadataSchema, data_key="meta", required=True
    )

    @post_load
    def make_job_summary(self, data, **kwargs):
        return JobSummary(**data)


class _InstanceSizeSchema(BaseSchema):
    milli_cpus = fields.Integer(data_key="milliCpus", required=True)
    memory_mb = fields.Integer(data_key="memoryMb", required=True)

    @post_load
    def make_instance_size(self, data, **kwargs):
        return InstanceSize(**data)


class _JobParameterSchema(BaseSchema):
    name = fields.String(required=True)
    type = EnumField(ParameterType, by_value=True, required=True)
    default = fields.String(required=True)
    required = fields.Boolean(required=True)

    @post_load
    def make_job_parameter(self, data, **kwargs):
        return JobParameter(**data)


class _JobCommandSchema(BaseSchema):
    name = fields.String(required=True)
    parameters = fields.List(fields.Nested(_JobParameterSchema), required=True)

    @post_load
    def make_job_command(self, data, **kwargs):
        return JobCommand(**data)


class _JobDefinitionSchema(BaseSchema):
    working_dir = fields.String(data_key="workingDir", required=True)
    command = fields.Nested(_JobCommandSchema, required=True)
    image_type = EnumField(
        ImageType, by_value=True, data_key="imageType", required=True
    )
    environment_ids = fields.List(
        fields.String(), data_key="environmentIds", required=True
    )
    instance_size_type = fields.String(
        data_key="instanceSizeType", required=True
    )
    instance_size = fields.Nested(
        _InstanceSizeSchema, data_key="instanceSize", missing=None
    )
    max_runtime_seconds = fields.Integer(
        data_key="maxRuntimeSeconds", required=True
    )

    @validates_schema
    def validate_instance_size(self, data, **kwargs):
        custom_type = data["instance_size_type"] == "custom"
        instance_size_set = data["instance_size"] is not None
        if custom_type and not instance_size_set:
            raise ValidationError(
                "need to specify instance size for custom instances"
            )
        elif not custom_type and instance_size_set:
            raise ValidationError(
                "instance_size must be None for non-custom instances "
            )

    @post_load
    def make_job_definition(self, data, **kwargs):
        return JobDefinition(**data)


class _JobSchema(BaseSchema):
    id = fields.UUID(data_key="jobId", required=True)
    meta = fields.Nested(_JobMetadataSchema, required=True)
    definition = fields.Nested(_JobDefinitionSchema, required=True)

    @post_load
    def make_job(self, data, **kwargs):
        return Job(**data)


class _JobIdSchema(BaseSchema):
    jobId = fields.UUID(required=True)

    @post_load
    def make_job_id(self, data, **kwargs):
        return data["jobId"]


class _EnvironmentStepExecutionSchema(BaseSchema):
    environment_id = fields.UUID(data_key="environmentId", required=True)
    environment_step_id = fields.UUID(
        data_key="environmentStepId", required=True
    )
    environment_name = fields.String(data_key="environmentName", required=True)
    command = fields.String(required=True)
    state = EnumField(
        EnvironmentStepExecutionState, by_value=True, required=True
    )
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)

    @post_load
    def make_environment_step_execution(self, data, **kwargs):
        return EnvironmentStepExecution(**data)


class _SubrunSummarySchema(BaseSchema):
    id = fields.UUID(data_key="subrunId", required=True)
    subrun_number = fields.Integer(data_key="subrunNumber", required=True)
    state = EnumField(SubrunState, by_value=True, required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)

    @post_load
    def make_subrun_summary(self, data, **kwargs):
        return SubrunSummary(**data)


class _SubrunSchema(BaseSchema):
    id = fields.UUID(data_key="subrunId", required=True)
    subrun_number = fields.Integer(data_key="subrunNumber", required=True)
    state = EnumField(SubrunState, by_value=True, required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)
    environment_step_executions = fields.Nested(
        _EnvironmentStepExecutionSchema,
        data_key="environmentExecutionState",
        many=True,
        required=True,
    )

    @post_load
    def make_subrun(self, data, **kwargs):
        return Subrun(**data)


class _RunSummarySchema(BaseSchema):
    id = fields.UUID(data_key="runId", required=True)
    run_number = fields.Integer(data_key="runNumber", required=True)
    state = EnumField(RunState, by_value=True, required=True)
    submitted_at = fields.DateTime(data_key="submittedAt", required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)

    @post_load
    def make_run_summary(self, data, **kwargs):
        return RunSummary(**data)


class _RunSchema(BaseSchema):
    id = fields.UUID(data_key="runId", required=True)
    run_number = fields.Integer(data_key="runNumber", required=True)
    state = EnumField(RunState, by_value=True, required=True)
    submitted_at = fields.DateTime(data_key="submittedAt", required=True)
    started_at = fields.DateTime(data_key="startedAt", missing=None)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)
    subruns = fields.Nested(_SubrunSummarySchema, many=True, required=True)

    @post_load
    def make_run(self, data, **kwargs):
        return Run(**data)


class _RunIdSchema(BaseSchema):
    runId = fields.UUID(required=True)

    @post_load
    def make_run_id(self, data, **kwargs):
        return data["runId"]


class _PageSchema(BaseSchema):
    start = fields.Integer(required=True)
    limit = fields.Integer(required=True)

    @post_load
    def make_page(self, data, **kwargs):
        return Page(**data)


class _PaginationSchema(BaseSchema):
    start = fields.Integer(required=True)
    size = fields.Integer(required=True)
    previous = fields.Nested(_PageSchema, missing=None)
    next = fields.Nested(_PageSchema, missing=None)

    @post_load
    def make_pagination(self, data, **kwargs):
        return Pagination(**data)


class _ListRunsResponseSchema(BaseSchema):
    pagination = fields.Nested(_PaginationSchema, required=True)
    runs = fields.Nested(_RunSummarySchema, many=True, required=True)

    @post_load
    def make_list_runs_response_schema(self, data, **kwargs):
        return ListRunsResponse(**data)
