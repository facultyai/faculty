# Copyright 2020 Faculty Science Limited
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
Interact with the Faculty knowledge centre templates.
"""
from collections import namedtuple

from marshmallow import fields, post_load

from faculty.clients.base import BaseClient, BaseSchema, HttpError


class TemplateClient(BaseClient):
    """Client for the the Knowledge centre templates.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("template")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    """

    _SERVICE_NAME = "kanto"

    def publish_new(self, project_id, template, source_directory):
        endpoint = "template"
        payload = {
            "sourceProjectId": str(project_id),
            "sourceDirectory": source_directory,
            "name": template,
        }
        try:
            self._post_raw(endpoint, json=payload)
        except HttpError as err:
            _handle_publishing_error(err)

    def add_to_project_from_directory(
        self,
        source_project_id,
        source_directory,
        target_project_id,
        target_directory,
        parameters,
    ):
        endpoint = "project/{}/apply".format(target_project_id)
        payload = {
            "sourceProjectId": str(source_project_id),
            "sourceDirectory": source_directory,
            "targetDirectory": target_directory,
            "parameterValues": parameters,
        }
        try:
            self._post_raw(endpoint, json=payload)
        except HttpError as err:
            _handle_publishing_error(err)


def _handle_publishing_error(exception):
    code = exception.error_code
    body = exception.response.json()
    if code == "resources_validation_failure":
        raise ResourceValidationErrorResponseSchema().load(body)
    elif code == "parameter_validation_failure":
        raise ParameterValidationErrorSchema().load(body)
    elif code == "template_retrieval_failure":
        raise TemplateRetrievalErrorResponseSchema().load(body)
    elif code == "default_parameters_parsing_error":
        raise DefaultParametersParsingErrorSchema().load(body)
    elif code == "generic_parsing_failure":
        raise GenericParsingErrorSchema().load(body)
    elif code == "workspace_files_validation_error":
        raise WorkspaceFilesValidationErrorSchema().load(body)
    else:
        raise


class TemplateException(Exception):
    pass


FileTooLarge = namedtuple(
    "FileTooLarge", ["path", "actual_size_bytes", "max_bytes"]
)


class FileTooLargeSchema(BaseSchema):
    path = fields.String(required=True)
    actual_size_bytes = fields.Int(data_key="actualSizeBytes", required=True)
    max_bytes = fields.Int(data_key="maxBytes", required=True)

    @post_load
    def make_error(self, data):
        return FileTooLarge(**data)


TooManyFiles = namedtuple("TooManyFiles", ["actual_files", "max_files"])


class TooManyFilesSchema(BaseSchema):
    actual_files = fields.Int(data_key="actualFiles", required=True)
    max_files = fields.Int(data_key="maxFiles", required=True)

    @post_load
    def make_error(self, data):
        return TooManyFiles(**data)


class WorkpaceFilesValidationError(TemplateException):
    def __init__(self, files_too_large, too_many_files):
        self.files_too_large = files_too_large
        self.too_many_files = too_many_files


class WorkspaceFilesValidationErrorSchema(BaseSchema):
    files_too_large = fields.List(
        fields.Nested(FileTooLargeSchema()),
        data_key="filesTooLarge",
        required=True,
    )
    too_many_files = fields.Nested(
        TooManyFilesSchema(), data_key="tooManyFiles"
    )

    @post_load
    def make_error(self, data):
        return WorkpaceFilesValidationError(**data)


class GenericParsingError(TemplateException):
    def __init__(self, error):
        self.error = error


class GenericParsingErrorSchema(BaseSchema):
    error = fields.String(required=True)

    @post_load
    def make_error(self, data, **kwargs):
        return GenericParsingError(**data)


class DefaultParametersParsingError(TemplateException):
    def __init__(self, error):
        self.error = error


class DefaultParametersParsingErrorSchema(BaseSchema):
    error = fields.String(required=True)

    @post_load
    def make_error(self, data, **kwargs):
        return DefaultParametersParsingError(**data)


class AppValidationFailureSchema(BaseSchema):
    subdomain_conflicts = fields.List(
        fields.String(), data_key="subdomainConflicts", required=True
    )
    name_conflicts = fields.List(
        fields.String(), data_key="nameConflicts", required=True
    )
    invalid_working_dirs = fields.List(
        fields.String(), data_key="invalidWorkingDirs", required=True
    )

    @post_load
    def make_failure(self, data, **kwargs):
        return AppValidationFailure(**data)


AppValidationFailure = namedtuple(
    "AppValidationFailure",
    ["subdomain_conflicts", "name_conflicts", "invalid_working_dirs"],
)


class ApiValidationFailureSchema(BaseSchema):
    subdomain_conflicts = fields.List(
        fields.String(), data_key="subdomainConflicts", required=True
    )
    name_conflicts = fields.List(
        fields.String(), data_key="nameConflicts", required=True
    )
    invalid_working_dirs = fields.List(
        fields.String(), data_key="invalidWorkingDirs", required=True
    )

    @post_load
    def make_failure(self, data, **kwargs):
        return ApiValidationFailure(**data)


ApiValidationFailure = namedtuple(
    "ApiValidationFailure",
    ["subdomain_conflicts", "name_conflicts", "invalid_working_dirs"],
)


class EnvironmentValidationFailureSchema(BaseSchema):
    name_conflicts = fields.List(
        fields.String(), data_key="nameConflicts", required=True
    )
    invalid_names = fields.List(
        fields.String(), data_key="invalidNames", required=True
    )

    @post_load
    def make_failure(self, data, **kwargs):
        return EnvironmentValidationFailure(**data)


EnvironmentValidationFailure = namedtuple(
    "EnvironmentValidationFailure", ["name_conflicts", "invalid_names"]
)


class JobValidationFailureSchema(BaseSchema):
    name_conflicts = fields.List(
        fields.String(), data_key="nameConflicts", required=True
    )
    invalid_working_dirs = fields.List(
        fields.String(), data_key="invalidWorkingDirs", required=True
    )
    invalid_names = fields.List(
        fields.String(), data_key="invalidNames", required=True
    )

    @post_load
    def make_failure(self, data, **kwargs):
        return JobValidationFailure(**data)


JobValidationFailure = namedtuple(
    "JobValidationFailure",
    ["name_conflicts", "invalid_working_dirs", "invalid_names"],
)


class WorkspaceValidationFailureSchema(BaseSchema):
    name_conflicts = fields.List(
        fields.String(), data_key="nameConflicts", required=True
    )

    @post_load
    def make_failure(self, data, **kwargs):
        return WorkspaceValidationFailure(**data)


WorkspaceValidationFailure = namedtuple(
    "WorkspaceValidationFailure", ["name_conflicts"]
)


class ResourceValidationErrorsSchema(BaseSchema):
    apps = fields.Nested(AppValidationFailureSchema(), required=True)
    apis = fields.Nested(ApiValidationFailureSchema(), required=True)
    environments = fields.Nested(
        EnvironmentValidationFailureSchema(), required=True
    )
    jobs = fields.Nested(JobValidationFailureSchema(), required=True)
    workspace = fields.Nested(
        WorkspaceValidationFailureSchema(), required=True
    )


class ResourceValidationErrorResponseSchema(BaseSchema):
    errors = fields.Nested(ResourceValidationErrorsSchema(), required=True)

    @post_load
    def make_error(self, data, **kwargs):
        errors = data["errors"]
        return ResourceValidationError(**errors)


class ResourceValidationError(TemplateException):
    def __init__(self, apps, apis, environments, jobs, workspace):
        self.apps = apps
        self.apis = apis
        self.environments = environments
        self.jobs = jobs
        self.workspace = workspace


class ParameterValidationError(TemplateException):
    def __init__(self, errors):
        self.errors = errors


class ParameterValidationErrorSchema(BaseSchema):
    errors = fields.List(fields.String(), required=True)

    @post_load()
    def make_error(self, data, **kwargs):
        return ParameterValidationError(**data)


class TemplateRetrievalError(TemplateException):
    def __init__(self, apps, apis, environments, jobs):
        self.apps = apps
        self.apis = apis
        self.environments = environments
        self.jobs = jobs


class TemplateRetrievalErrorSchema(BaseSchema):
    apps = fields.List(fields.String(), required=True)
    apis = fields.List(fields.String(), required=True)
    environments = fields.List(fields.String(), required=True)
    jobs = fields.List(fields.String(), required=True)


class TemplateRetrievalErrorResponseSchema(BaseSchema):
    errors = fields.Nested(TemplateRetrievalErrorSchema(), required=True)

    @post_load()
    def make_error(self, data, **kwargs):
        errors = data["errors"]
        return TemplateRetrievalError(**errors)
