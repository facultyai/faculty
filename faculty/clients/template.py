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
        return self._post_raw(endpoint, json=payload)

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
        print(response.status_code)
        print(response.text)
        if 200 <= response.status_code < 300:
            return
        elif 400 <= response.status_code < 500:
            response_body = response.json()
            error_code = response_body.get("errorCode")
            if error_code == "resources_validation_failure":
                raise ResourceValidationFailureResponseSchema().load(
                    response_body
                )
                # _resource_validation_error(response_body)
            elif error_code == "parameter_validation_failure":
                raise ParameterValidationFailureSchema().load(response_body)
                # msg = "Parameter validation failed:"
                # for e in response_body["errors"]["parameters"]:
                #     msg += "\n" + e
                # raise ParameterValidationFailure(msg)
            elif error_code == "template_retrieval_failure":
                raise TemplateRetrievalFailureResponseSchema().load(
                    response_body
                )
                # raise _retrieval_error(response_body)
            elif error_code == "default_parameters_parsing_error":
                raise DefaultParametersParsingErrorSchema().load(response_body)
            elif error_code == "generic_parsing_failure":
                raise TemplateRetrievalFailure(response_body["error"])
            # TODO more cases
            else:
                _unexpected_response(response)
        else:
            _unexpected_response(response)


# def _map_prefix(prefix, errors):
#     return [prefix + e for e in errors]
#
#
# def _one_per_line(error_message_lists):
#     messages = [msg for sublist in error_message_lists for msg in sublist]
#     return "\n".join(messages)
#
#
# def _retrieval_error(response_body):
#     errors = response_body["errors"]
#     apps = errors["apps"]
#     apis = errors["apis"]
#     envs = errors["environments"]
#     jobs = errors["jobs"]
#     message_lists = [
#         _map_prefix(prefix, errors)
#         for prefix, errors in [
#             ("Error reading app resource definition: ", apps),
#             ("Error reading API resource definition: ", apis),
#             ("Error reading environment resource definition: ", envs),
#             ("Error reading app job definition: ", jobs),
#         ]
#     ]
#     raise TemplateRetrievalFailure(_one_per_line(message_lists))
#
#
# def _resource_validation_error(response_body):
#     errors = response_body["errors"]
#     apps = errors["apps"]
#     apis = errors["apis"]
#     envs = errors["environments"]
#     jobs = errors["jobs"]
#     workspace = errors["workspace"]
#     message_lists = [
#         _map_prefix(prefix, errors)
#         for prefix, errors in [
#             ("App subdomain already exists: ", apps["subdomainConflicts"]),
#             ("App name already exists: ", apps["nameConflicts"]),
#             ("Invalid app working directory: ", apps["invalidWorkingDirs"]),
#             ("API subdomain already exists: ", apis["subdomainConflicts"]),
#             ("API name already exists: ", apis["nameConflicts"]),
#             ("Invalid API working directory: ", apis["invalidWorkingDirs"]),
#             ("Environment name already exists: ", envs["nameConflicts"]),
#             ("Invalid environment name: ", envs["invalidNames"]),
#             ("Job name already exists: ", jobs["nameConflicts"]),
#             ("Invalid job name: ", jobs["invalidNames"]),
#             ("Invalid job working directory: ", jobs["invalidWorkingDirs"]),
#             ("Workspace file conflicts: ", workspace["nameConflicts"]),
#         ]
#     ]
#     raise ResourceValidationFailure(_one_per_line(message_lists))


def _unexpected_response(response):
    raise Exception("Unexpected response from the server:\n", response.text)


class TemplateException(Exception):
    pass


class DefaultParametersParsingError(TemplateException):
    def __init__(self, error):
        self.errors = error


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
