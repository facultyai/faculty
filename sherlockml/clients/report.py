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

from marshmallow import Schema, fields, post_load

from sherlockml.clients.base import BaseClient

ActiveReport = namedtuple(
    "ActiveReport",
    ["created_at", "name", "id", "description", "active_version"],
)

VersionedReport = namedtuple(
    "VersionedReport",
    [
        "created_at",
        "name",
        "id",
        "description",
        "active_version_id",
        "versions",
    ],
)

ReportVersion = namedtuple(
    "ReportVersion",
    [
        "created_at",
        "author_id",
        "report_path",
        "report_key",
        "report_bucket",
        "notebook_path",
        "id",
    ],
)


class ReportVersionSchema(Schema):
    id = fields.UUID(data_key="version_id", required=True)
    created_at = fields.DateTime(required=True)
    author_id = fields.UUID(required=True)
    report_path = fields.String(required=True)
    report_key = fields.String(required=True)
    report_bucket = fields.String(required=True)
    notebook_path = fields.String(required=True)

    @post_load
    def make_report_version(self, data):
        return ReportVersion(**data)


class ActiveReportSchema(Schema):
    created_at = fields.DateTime(required=True)
    name = fields.String(required=True, data_key="report_name")
    id = fields.UUID(required=True, data_key="report_id")
    description = fields.String(required=True)
    active_version = fields.Nested(ReportVersionSchema, required=True)

    @post_load
    def make_active_report(self, data):
        return ActiveReport(**data)


class VersionedReportSchema(Schema):
    created_at = fields.DateTime(required=True)
    name = fields.String(required=True, data_key="report_name")
    id = fields.UUID(required=True, data_key="report_id")
    description = fields.String(required=True)
    active_version_id = fields.UUID(required=True)
    versions = fields.Nested(ReportVersionSchema, required=True, many=True)

    @post_load
    def make_versioned_report(self, data):
        return VersionedReport(**data)


class ReportClient(BaseClient):

    SERVICE_NAME = "tavern"

    def list_reports_in_project(self, project_id):
        endpoint = "/project/{}".format(project_id)
        return self._get(endpoint, VersionedReportSchema(many=True))
