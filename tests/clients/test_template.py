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
    GenericParsingError,
    WorkpaceFilesValidationError,
    FileTooLargeError,
    TooManyFilesError,
)

SOURCE_PROJECT_ID = uuid.uuid4()
TARGET_PROJECT_ID = uuid.uuid4()


PUBLISHING_ERRORS = [
    (
        {
            "errorCode": "resources_validation_failure",
            "errors": {
                "apps": {
                    "subdomainConflicts": ["app-subdomain-1"],
                    "nameConflicts": ["test-app-name-1", "test-app-name-2"],
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
                name_conflicts=["test-env-name"], invalid_names=["invalid#env"]
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
                "apps": ["app error"],
                "apis": ["API error"],
                "environments": ["env error"],
                "jobs": ["job error"],
            },
        },
        TemplateRetrievalFailure(
            apps=["app error"],
            apis=["API error"],
            environments=["env error"],
            jobs=["job error"],
        ),
    ),
    (
        {
            "errorCode": "default_parameters_parsing_error",
            "error": "parameter error",
        },
        DefaultParametersParsingError(error=["parameter error"]),
    ),
    (
        {
            "errorCode": "generic_parsing_failure",
            "error": "generic parsing error",
        },
        GenericParsingError(error=["generic parsing error"]),
    ),
    (
        {
            "errorCode": "workspace_files_validation_error",
            "filesTooLarge": [
                {"path": "/too/large", "actualSizeBytes": 2, "maxBytes": 1}
            ],
            "tooManyFiles": {"actualFiles": 2, "maxFiles": 1},
        },
        WorkpaceFilesValidationError(
            files_too_large=[
                FileTooLargeError(
                    "/too/large", actual_size_bytes=2, max_bytes=1
                )
            ],
            too_many_files=TooManyFilesError(actual_files=2, max_files=1),
        ),
    ),
]


def test_publish_new(mocker):
    mock_response = mocker.Mock()
    mock_response.status_code = 202
    mocker.patch.object(
        TemplateClient, "_post_raw", return_value=mock_response
    )

    client = TemplateClient(mocker.Mock())
    client.publish_new(SOURCE_PROJECT_ID, "template name", "source/dir")

    TemplateClient._post_raw.assert_called_once_with(
        "template",
        json={
            "sourceProjectId": str(SOURCE_PROJECT_ID),
            "sourceDirectory": "source/dir",
            "name": "template name",
        },
        check_status=False,
    )


@pytest.mark.parametrize(
    "mock_response_payload, expected_exception", PUBLISHING_ERRORS
)
def test_publish_new_errors(mocker, mock_response_payload, expected_exception):
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
        client.publish_new(SOURCE_PROJECT_ID, "template name", "source/dir")
    assert e.value.args == expected_exception.args

    TemplateClient._post_raw.assert_called_once_with(
        "template",
        json={
            "sourceProjectId": str(SOURCE_PROJECT_ID),
            "sourceDirectory": "source/dir",
            "name": "template name",
        },
        check_status=False,
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
    "mock_response_payload, expected_exception", PUBLISHING_ERRORS
)
def test_add_to_project_from_directory_errors(
    mocker, mock_response_payload, expected_exception
):
    mock_response = mocker.Mock()
    mock_response.status_code = 409
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
