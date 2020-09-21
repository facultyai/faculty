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

from faculty.clients.base import BaseClient, BaseSchema


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
        response = self._post_raw(endpoint, json=payload, check_status=False)
        return _handle_publishing_response(response)

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
        response = self._post_raw(endpoint, json=payload, check_status=False)
        return _handle_publishing_response(response)


def _handle_publishing_response(response):
    if 200 <= response.status_code < 300:
        return response
    elif 400 <= response.status_code < 500:
        response_body = response.json()
        error_code = response_body.get("errorCode")
        if error_code == "resources_validation_failure":
            raise ResourceValidationFailureResponseSchema().load(response_body)
        elif error_code == "parameter_validation_failure":
            raise ParameterValidationFailureSchema().load(response_body)
        elif error_code == "template_retrieval_failure":
            raise TemplateRetrievalFailureResponseSchema().load(response_body)
        elif error_code == "default_parameters_parsing_error":
            raise DefaultParametersParsingErrorSchema().load(response_body)
        elif error_code == "generic_parsing_failure":
            raise GenericParsingErrorSchema().load(response_body)
        elif error_code == "workspace_files_validation_error":
            raise WorkspaceFilesValidationErrorSchema().load(response_body)
    else:
        raise Exception(
            "Unexpected response from the server:\n", response.text
        )


class TemplateException(Exception):
    pass


FileTooLargeError = namedtuple(
    "FileTooLargeError", ["path", "actual_size_bytes", "max_bytes"]
)


class FileTooLargeSchema(BaseSchema):
    path = fields.String(required=True)
    actual_size_bytes = fields.Int(data_key="actualSizeBytes", required=True)
    max_bytes = fields.Int(data_key="maxBytes", required=True)

    @post_load
    def make_error(self, data):
        return FileTooLargeError(**data)


TooManyFilesError = namedtuple(
    "TooManyFilesError", ["actual_files", "max_files"]
)


class TooManyFilesSchema(BaseSchema):
    actual_files = fields.Int(data_key="actualFiles", required=True)
    max_files = fields.Int(data_key="maxFiles", required=True)

    @post_load
    def make_error(self, data):
        return TooManyFilesError(**data)


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


class ResourceValidationFailuresSchema(BaseSchema):
    apps = fields.Nested(AppValidationFailureSchema(), required=True)
    apis = fields.Nested(ApiValidationFailureSchema(), required=True)
    environments = fields.Nested(
        EnvironmentValidationFailureSchema(), required=True
    )
    jobs = fields.Nested(JobValidationFailureSchema(), required=True)
    workspace = fields.Nested(
        WorkspaceValidationFailureSchema(), required=True
    )


class ResourceValidationFailureResponseSchema(BaseSchema):
    errors = fields.Nested(ResourceValidationFailuresSchema(), required=True)

    @post_load
    def make_failure(self, data, **kwargs):
        return ResourceValidationFailure.from_errors(**data)


class ResourceValidationFailure(TemplateException):
    def __init__(self, apps, apis, environments, jobs, workspace):
        self.apps = apps
        self.apis = apis
        self.environments = environments
        self.jobs = jobs
        self.workspace = workspace

    @staticmethod
    def from_errors(errors):
        return ResourceValidationFailure(
            apps=errors["apps"],
            apis=errors["apis"],
            environments=errors["environments"],
            jobs=errors["jobs"],
            workspace=errors["workspace"],
        )


class ParameterValidationFailure(TemplateException):
    def __init__(self, errors):
        self.errors = errors


class ParameterValidationFailureSchema(BaseSchema):
    errors = fields.List(fields.String(), required=True)

    @post_load()
    def make_failure(self, data, **kwargs):
        return ParameterValidationFailure(**data)


class TemplateRetrievalFailure(TemplateException):
    def __init__(self, apps, apis, environments, jobs):
        self.apps = apps
        self.apis = apis
        self.environments = environments
        self.jobs = jobs

    @staticmethod
    def from_errors(errors):
        return TemplateRetrievalFailure(
            apps=errors["apps"],
            apis=errors["apis"],
            environments=errors["environments"],
            jobs=errors["jobs"],
        )


class TemplateRetrievalFailureSchema(BaseSchema):
    apps = fields.List(fields.String(), required=True)
    apis = fields.List(fields.String(), required=True)
    environments = fields.List(fields.String(), required=True)
    jobs = fields.List(fields.String(), required=True)


class TemplateRetrievalFailureResponseSchema(BaseSchema):
    errors = fields.Nested(TemplateRetrievalFailureSchema(), required=True)

    @post_load()
    def make_failure(self, data, **kwargs):
        return TemplateRetrievalFailure.from_errors(**data)
