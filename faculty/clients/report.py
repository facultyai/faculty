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
Interact with Faculty reports.
"""


from collections import namedtuple

from marshmallow import fields, post_load

from faculty.clients.base import BaseSchema, BaseClient

Report = namedtuple(
    "Report", ["created_at", "name", "id", "description", "active_version"]
)

ReportWithVersions = namedtuple(
    "ReportWithVersions",
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
    ["created_at", "author_id", "report_path", "notebook_path", "id"],
)


class ReportClient(BaseClient):
    """Client for the Faculty report service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("report")

    Parameters
    ----------
    url : str
        The URL of the report service.
    session : faculty.session.Session
        The session to use to make requests.
    """

    SERVICE_NAME = "tavern"

    def list(self, project_id):
        """List the reports in a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to list reports in.

        Returns
        -------
        List[Report]
            The reports in the project.
        """
        endpoint = "/project/{}".format(project_id)
        return self._get(endpoint, _ReportSchema(many=True))

    def get_with_versions(self, report_id):
        """Get a report with all of its versions.

        Parameters
        ----------
        report_id : uuid.UUID
            The ID of the report.

        Returns
        -------
        ReportWithVersions
            The report with all versions.
        """
        endpoint = "/report/{}/versions".format(report_id)
        return self._get(endpoint, _ReportWithVersionsSchema())

    def get(self, report_id):
        """Get a report.

        Parameters
        ----------
        report_id : uuid.UUID
            The ID of the report.

        Returns
        -------
        Report
            The report.
        """
        endpoint = "/report/{}/active".format(report_id)
        return self._get(endpoint, _ReportSchema())

    def create(
        self,
        project_id,
        name,
        notebook_path,
        author_id,
        description=None,
        show_code=False,
    ):
        """Create a new report.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to create the report in.
        name : str
            The name of the new report.
        notebook_path : str
            The path of the notebook to create the report from. This path is
            relative to the project root (/project).
        author_id : uuid.UUID
            The ID of the user creating the report.
        description : str, optional
            A description of the report.
        show_code : bool, optional
            If set to True, the report will include the notebook's code.
            Default: False.

        Returns
        -------
        Report
            The created report.
        """
        payload = {
            "report_name": name,
            "author_id": str(author_id),
            "notebook_path": str(notebook_path),
            "description": description,
            "show_input_cells": show_code,
        }

        endpoint = "/project/{project_id}".format(project_id=project_id)
        return self._post(endpoint, _ReportSchema(), json=payload)

    def create_version(
        self, report_id, notebook_path, author_id, show_code=False
    ):
        """Create a new version of a report.

        Parameters
        ----------
        report_id : uuid.UUID
            The ID of the report to create a new version of.
        notebook_path : str
            The path of the notebook to create the report version from. This
            path is relative to the project root (/project).
        author_id : uuid.UUID
            The ID of the user creating the report version.
        show_code : bool, optional
            If set to True, the report will include the notebook's code.
            Default: False.

        Returns
        -------
        ReportVersion
            The created report version.
        """
        payload = {
            "notebook_path": str(notebook_path),
            "author_id": str(author_id),
            "show_input_cells": show_code,
            "draft": False,
        }

        endpoint = "/report/{report_id}/version".format(report_id=report_id)
        return self._post(endpoint, _ReportVersionSchema(), json=payload)


class _ReportVersionSchema(BaseSchema):
    id = fields.UUID(data_key="version_id", required=True)
    created_at = fields.DateTime(required=True)
    author_id = fields.UUID(required=True)
    report_path = fields.String(required=True)
    notebook_path = fields.String(required=True)

    @post_load
    def make_report_version(self, data, **kwargs):
        return ReportVersion(**data)


class _ReportSchema(BaseSchema):
    created_at = fields.DateTime(required=True)
    name = fields.String(required=True, data_key="report_name")
    id = fields.UUID(required=True, data_key="report_id")
    description = fields.String(required=True)
    active_version = fields.Nested(_ReportVersionSchema, required=True)

    @post_load
    def make_report(self, data, **kwargs):
        return Report(**data)


class _ReportWithVersionsSchema(BaseSchema):
    created_at = fields.DateTime(required=True)
    name = fields.String(required=True, data_key="report_name")
    id = fields.UUID(required=True, data_key="report_id")
    description = fields.String(required=True)
    active_version_id = fields.UUID(required=True)
    versions = fields.Nested(_ReportVersionSchema, required=True, many=True)

    @post_load
    def make_report_with_versions(self, data, **kwargs):
        return ReportWithVersions(**data)
