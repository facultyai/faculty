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

import uuid
import datetime
from dateutil.tz import UTC

import pytest
from marshmallow import ValidationError

from sherlockml.clients.report import (
    Report,
    ReportSchema,
    ReportWithVersions,
    ReportWithVersionsSchema,
    ReportVersion,
    ReportClient,
)

from tests.clients.fixtures import PROFILE


PROJECT_ID = uuid.uuid4()
USER_ID = uuid.uuid4()
REPORT_ID = uuid.uuid4()
VERSION_ID = uuid.uuid4()
PROJECT_ID = uuid.uuid4()

ACTIVE_VERSION = ReportVersion(
    created_at=datetime.datetime(2018, 10, 3, 9, 23, 5, 0, tzinfo=UTC),
    author_id=USER_ID,
    report_path="/.sml/tavern/{}/{}/index.html".format(REPORT_ID, VERSION_ID),
    report_key="{}/.sml/tavern/{}/{}/index.html".format(
        PROJECT_ID, REPORT_ID, VERSION_ID
    ),
    report_bucket="sml-projects-test-bucket",
    notebook_path="/test-notebook-path.ipynb",
    id=VERSION_ID,
)

REPORT = Report(
    id=REPORT_ID,
    name="Test Report Name",
    description="Looking forward to the test reports on this Test Report",
    created_at=datetime.datetime(2018, 10, 3, 9, 23, 0, 0, tzinfo=UTC),
    active_version=ACTIVE_VERSION,
)

VERSIONED_REPORT = ReportWithVersions(
    id=REPORT_ID,
    name="Test Report Name",
    description="Looking forward to the test reports on this Test Report",
    created_at=datetime.datetime(2018, 10, 3, 9, 23, 0, 0, tzinfo=UTC),
    active_version_id=VERSION_ID,
    versions=[ACTIVE_VERSION],
)

VERSION_BODY = {
    "created_at": "2018-10-03T09:23:05Z",
    "author_id": str(USER_ID),
    "report_path": ACTIVE_VERSION.report_path,
    "report_key": ACTIVE_VERSION.report_key,
    "report_bucket": ACTIVE_VERSION.report_bucket,
    "notebook_path": ACTIVE_VERSION.notebook_path,
    "version_id": str(ACTIVE_VERSION.id),
}

REPORT_BODY = {
    "created_at": "2018-10-03T09:23:00Z",
    "report_name": REPORT.name,
    "report_id": str(REPORT.id),
    "description": REPORT.description,
    "active_version": VERSION_BODY,
}

VERSIONED_REPORT_BODY = {
    "created_at": "2018-10-03T09:23:00Z",
    "report_name": REPORT.name,
    "report_id": str(REPORT.id),
    "description": REPORT.description,
    "active_version_id": str(ACTIVE_VERSION.id),
    "versions": [VERSION_BODY],
}


def test_report_schema():
    data = ReportSchema().load(REPORT_BODY)
    assert data == REPORT
    with pytest.raises(ValidationError):
        ReportSchema().load({})


def test_versioned_report_schema():
    data = ReportWithVersionsSchema().load(VERSIONED_REPORT_BODY)
    assert data == VERSIONED_REPORT
    with pytest.raises(ValidationError):
        ReportWithVersionsSchema().load({})


def test_report_client_list(mocker):
    mocker.patch.object(ReportClient, "_get", return_value=[REPORT])
    schema_mock = mocker.patch("sherlockml.clients.report.ReportSchema")

    client = ReportClient(PROFILE)

    assert client.list(PROJECT_ID) == [REPORT]

    schema_mock.assert_called_once_with(many=True)

    ReportClient._get.assert_called_once_with(
        "/project/{}".format(PROJECT_ID), schema_mock.return_value
    )


def test_report_client_get(mocker):
    mocker.patch.object(ReportClient, "_get", return_value=REPORT)
    schema_mock = mocker.patch("sherlockml.clients.report.ReportSchema")

    client = ReportClient(PROFILE)

    assert client.get(REPORT.id) == REPORT

    schema_mock.assert_called_once_with()

    ReportClient._get.assert_called_once_with(
        "/report/{}/active".format(REPORT.id), schema_mock.return_value
    )


def test_report_client_get_with_versions(mocker):
    mocker.patch.object(ReportClient, "_get", return_value=VERSIONED_REPORT)
    schema_mock = mocker.patch(
        "sherlockml.clients.report.ReportWithVersionsSchema"
    )

    client = ReportClient(PROFILE)

    assert client.get_with_versions(REPORT.id) == VERSIONED_REPORT

    schema_mock.assert_called_once_with()

    ReportClient._get.assert_called_once_with(
        "/report/{}/versions".format(REPORT.id), schema_mock.return_value
    )


def test_report_client_create(mocker):
    mocker.patch.object(ReportClient, "_post", return_value=REPORT)
    schema_mock = mocker.patch("sherlockml.clients.report.ReportSchema")

    client = ReportClient(PROFILE)

    assert (
        client.create(
            PROJECT_ID,
            REPORT.name,
            "/test-notebook-path.ipynb",
            USER_ID,
            description=REPORT.description,
        )
        == REPORT
    )

    schema_mock.assert_called_once_with()

    ReportClient._post.assert_called_once_with(
        "/project/{}".format(PROJECT_ID),
        schema_mock.return_value,
        json={
            "report_name": REPORT.name,
            "author_id": str(USER_ID),
            "notebook_path": "/test-notebook-path.ipynb",
            "description": REPORT.description,
            "show_input_cells": False,
        },
    )


def test_report_client_create_version(mocker):
    mocker.patch.object(ReportClient, "_post", return_value=ACTIVE_VERSION)
    schema_mock = mocker.patch("sherlockml.clients.report.ReportVersionSchema")

    client = ReportClient(PROFILE)

    assert (
        client.create_version(REPORT.id, "/test-notebook-path.ipynb", USER_ID)
        == ACTIVE_VERSION
    )

    schema_mock.assert_called_once_with()

    ReportClient._post.assert_called_once_with(
        "/report/{}/version".format(REPORT.id),
        schema_mock.return_value,
        json={
            "author_id": str(USER_ID),
            "notebook_path": "/test-notebook-path.ipynb",
            "show_input_cells": False,
            "draft": False,
        },
    )
