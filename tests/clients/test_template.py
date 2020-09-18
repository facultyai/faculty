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


import uuid

import pytest

from faculty.clients.template import (
    TemplateClient,
    ResourceValidationFailure,
    AppValidationFailure,
    ApiValidationFailure,
    EnvironmentValidationFailure,
    JobValidationFailure,
    WorkspaceValidationFailure,
    ParameterValidationFailure,
    TemplateRetrievalFailure,
    DefaultParametersParsingError,
)

SOURCE_PROJECT_ID = uuid.uuid4()
TARGET_PROJECT_ID = uuid.uuid4()


def test_publish_new(mocker):
    mocker.patch.object(TemplateClient, "_post_raw")

    client = TemplateClient(mocker.Mock())
    client.publish_new(SOURCE_PROJECT_ID, "template name", "source/dir")

    TemplateClient._post_raw.assert_called_once_with(
        "template",
        json={
            "sourceProjectId": str(SOURCE_PROJECT_ID),
            "sourceDirectory": "source/dir",
            "name": "template name",
        },
    )


def test_add_to_project_from_directory(mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 202
    mocker.patch.object(
        TemplateClient, "_post_raw", return_value=mock_response
    )

    client = TemplateClient(mocker.Mock())
    client.add_to_project_from_directory(
        SOURCE_PROJECT_ID,
        "source/dir",
        TARGET_PROJECT_ID,
        "target/dir",
        {"paramA": "a", "paramB": "B"},
    )

    TemplateClient._post_raw.assert_called_once_with(
        "project/{}/apply".format(TARGET_PROJECT_ID),
        json={
            "sourceProjectId": str(SOURCE_PROJECT_ID),
            "sourceDirectory": "source/dir",
            "targetDirectory": "target/dir",
            "parameterValues": {"paramA": "a", "paramB": "B"},
        },
        check_status=False,
    )


@pytest.mark.parametrize(
    "mock_response_payload, expected_exception",
    [
        (
            {
                "errorCode": "resources_validation_failure",
                "errors": {
                    "apps": {
                        "subdomainConflicts": ["app-subdomain-1"],
                        "nameConflicts": [
                            "test-app-name-1",
                            "test-app-name-2",
                        ],
                        "invalidWorkingDirs": ["invalid/app/dir"],
                    },
                    "apis": {
                        "subdomainConflicts": ["api-subdomain-1"],
                        "nameConflicts": ["test-api-name"],
                        "invalidWorkingDirs": ["invalid/API/dir"],
                    },
                    "environments": {
                        "nameConflicts": ["test-env-name"],
                        "invalidNames": ["invalid#env"],
                    },
                    "jobs": {
                        "nameConflicts": ["test-job-name"],
                        "invalidWorkingDirs": [],
                        "invalidNames": ["invalid#job"],
                    },
                    "workspace": {"nameConflicts": ["test-file-path"]},
                },
            },
            ResourceValidationFailure(
                apps=AppValidationFailure(
                    subdomain_conflicts=["app-subdomain-1"],
                    name_conflicts=["test-app-name-1", "test-app-name-2"],
                    invalid_working_dirs=["invalid/app/dir"],
                ),
                apis=ApiValidationFailure(
                    subdomain_conflicts=["api-subdomain-1"],
                    name_conflicts=["test-api-name"],
                    invalid_working_dirs=["invalid/API/dir"],
                ),
                environments=EnvironmentValidationFailure(
                    name_conflicts=["test-env-name"],
                    invalid_names=["invalid#env"],
                ),
                jobs=JobValidationFailure(
                    name_conflicts=["test-job-name"],
                    invalid_working_dirs=[],
                    invalid_names=["invalid#job"],
                ),
                workspace=WorkspaceValidationFailure(
                    name_conflicts=["test-file-path"]
                ),
            ),
        ),
        (
            {
                "errorCode": "parameter_validation_failure",
                "errors": ["test param 1 error", "test param 2 error"],
            },
            ParameterValidationFailure(
                errors=["test param 1 error", "test param 2 error"]
            ),
        ),
        (
            {
                "errorCode": "template_retrieval_failure",
                "errors": {
                    "apps": [],
                    "apis": [],
                    "environments": [],
                    "jobs": [],
                },
            },
            TemplateRetrievalFailure(
                apps=[], apis=[], environments=[], jobs=[]
            ),
        ),
        (
            {
                "errorCode": "default_parameters_parsing_error",
                "error": "parameter error",
            },
            DefaultParametersParsingError(error=["parameter error"]),
        ),
    ],
)
def test_add_to_project_from_directory_errors(
    mocker, mock_response_payload, expected_exception
):
    mock_response = mocker.Mock()
    mock_response.status_code = 400
    mocker.patch.object(
        mock_response, "json", return_value=mock_response_payload
    )
    mocker.patch.object(
        TemplateClient, "_post_raw", return_value=mock_response
    )

    client = TemplateClient(mocker.Mock())
    with pytest.raises(type(expected_exception)) as e:
        client.add_to_project_from_directory(
            SOURCE_PROJECT_ID,
            "source/dir",
            TARGET_PROJECT_ID,
            "target/dir",
            {"paramA": "a", "paramB": "B"},
        )

    assert e.value.args == expected_exception.args

    TemplateClient._post_raw.assert_called_once_with(
        "project/{}/apply".format(TARGET_PROJECT_ID),
        json={
            "sourceProjectId": str(SOURCE_PROJECT_ID),
            "sourceDirectory": "source/dir",
            "targetDirectory": "target/dir",
            "parameterValues": {"paramA": "a", "paramB": "B"},
        },
        check_status=False,
    )


#             """App subdomain already exists: app-subdomain-1
# App name already exists: test-app-name-1
# App name already exists: test-app-name-2
# Invalid app working directory: invalid/app/dir
# API subdomain already exists: api-subdomain-1
# API name already exists: test-api-name
# Invalid API working directory: invalid/API/dir
# Environment name already exists: test-env-name
# Invalid environment name: invalid#env
# """,
