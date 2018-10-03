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

import pytest
from marshmallow import ValidationError

from sherlockml.clients.report import (
    Report,
    ReportSchema,
    VersionedReport,
    VersionedReportSchema,
    ReportVersion,
    ReportVersionSchema,
)

from tests.clients.fixtures import PROFILE


PROJECT_ID = uuid.uuid4()
USER_ID = uuid.uuid4()
REPORT_ID = uuid.uuid4()
VERSION_ID = uuid.uuid4()
PROJECT_ID = uuid.uuid4()

ACTIVE_VERSION = ReportVersion(
    created_at=datetime.datetime(2018, 10, 3, 9, 23, 5, 0),
    author_id=USER_ID,
    report_path=f"/.sml/tavern/{REPORT_ID}/{VERSION_ID}/index.html",
    report_key=f"{PROJECT_ID}/.sml/tavern/{REPORT_ID}/{VERSION_ID}/index.html",
    report_bucket="sml-projects-test-bucket",
    notebook_path="/test-notebook-path.ipynb",
    report_id=REPORT_ID,
    id=VERSION_ID,
)

ACTIVE_REPORT = Report(
    id=REPORT_ID,
    name="Test Report Name",
    description="Looking forward to the test reports on this Test Report",
    created_at=datetime.datetime(2018, 10, 3, 9, 23, 0, 0),
    active_version=ACTIVE_VERSION,
)

VERSIONED_REPORT = VersionedReport(
    id=REPORT_ID,
    name="Test Report Name",
    description="Looking forward to the test reports on this Test Report",
    created_at=datetime.datetime(2018, 10, 3, 9, 23, 0, 0),
    active_version_id=VERSION_ID,
    versions=[ACTIVE_VERSION],
)


ACTIVE_REPORT_BODY = {
    "created_at": ACTIVE_REPORT.created_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[
        :-3
    ],
    "report_name": ACTIVE_REPORT.name,
    "report_id": str(ACTIVE_REPORT.id),
    "description": ACTIVE_REPORT.description,
    "active_version": {
        "created_at": ACTIVE_VERSION.created_at.strftime(
            "%Y-%m-%dT%H:%M:%S.%f"
        )[:-3],
        "author_id": str(USER_ID),
        "report_path": ACTIVE_VERSION.report_path,
        "report_key": ACTIVE_VERSION.report_key,
        "report_bucket": ACTIVE_VERSION.report_bucket,
        "notebook_path": ACTIVE_VERSION.notebook_path,
        "version_id": str(ACTIVE_VERSION.id),
    },
}

VERSIONED_REPORT_BODY = {
    "created_at": ACTIVE_REPORT.created_at.strftime("%Y-%m-%dT%H:%M:%S.%f")[
        :-3
    ],
    "report_name": ACTIVE_REPORT.name,
    "report_id": str(ACTIVE_REPORT.id),
    "description": ACTIVE_REPORT.description,
    "active_version_id": ACTIVE_VERSION.id,
    "versions": [
        {
            "created_at": ACTIVE_VERSION.created_at.strftime(
                "%Y-%m-%dT%H:%M:%S.%f"
            )[:-3],
            "author_id": str(USER_ID),
            "report_path": ACTIVE_VERSION.report_path,
            "report_key": ACTIVE_VERSION.report_key,
            "report_bucket": ACTIVE_VERSION.report_bucket,
            "notebook_path": ACTIVE_VERSION.notebook_path,
            "version_id": str(ACTIVE_VERSION.id),
        }
    ],
}


def test_active_report_schema():
    data = ReportSchema().load(ACTIVE_REPORT_BODY)
    assert data == ACTIVE_REPORT


def test_versioned_report_schema():
    data = VersionedReportSchema().load(VERSIONED_REPORT_BODY)
    assert data == VERSIONED_REPORT
