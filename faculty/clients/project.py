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
Manage Faculty projects.
"""

from attr import attrs, attrib

from marshmallow import fields, post_load

from faculty.clients.base import BaseSchema, BaseClient


@attrs
class Project:
    """A project in Faculty.

    Parameters
    ----------
    id : uuid.UUID
        The ID of the project.
    name : str
        The name of the project.
    owner_id : uuid.UUID
        The user ID of the owner of the project.
    archived_at : datetime, optional
        If project is not archived, this will be None, otherwise it will be the
        time at which the project was archived.
    """

    id = attrib()
    name = attrib()
    owner_id = attrib()
    archived_at = attrib()


class ProjectClient(BaseClient):
    """Client for the Faculty project service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("project")

    Parameters
    ----------
    url : str
        The URL of the project service.
    session : faculty.session.Session
        The session to use to make requests.
    """

    SERVICE_NAME = "casebook"

    def create(self, owner_id, project_name):
        """Create a new project.

        Parameters
        ----------
        owner_id : uuid.UUID
            The ID of the user who will be the owner of the project.
        project_name : str
            The name of the new project. The owner must not have other projects
            with this name.

        Returns
        -------
        Project
            The created project.
        """
        payload = {"owner_id": str(owner_id), "name": project_name}
        return self._post("/project", _ProjectSchema(), json=payload)

    def get(self, project_id):
        """Get information about a project.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project.

        Returns
        -------
        Project
            The retrieved project.
        """
        endpoint = "/project/{}".format(project_id)
        return self._get(endpoint, _ProjectSchema())

    def get_by_owner_and_name(self, owner_id, project_name):
        """Get information about a project using its owner and name.

        Parameters
        ----------
        owner_id : uuid.UUID
            The ID of the project owner.
        project_name : str
            The name of the project.

        Returns
        -------
        Project
            The retrieved project.
        """
        endpoint = "/project/{}/{}".format(owner_id, project_name)
        return self._get(endpoint, _ProjectSchema())

    def list_accessible_by_user(self, user_id):
        """List the projects that a user can access.

        This included both projects they own and projects others have granted
        them access to.

        Parameters
        ----------
        user_id : uuid.UUID
            The ID of the user.

        Returns
        -------
        List[Project]
            The projects the user has access to.
        """
        endpoint = "/user/{}".format(user_id)
        return self._get(endpoint, _ProjectSchema(many=True))

    def list_all(self, include_archived=False):
        """List all projects on the Faculty deployment.

        This method requires administrative privileges not available to most
        users.

        Parameters
        ----------
        include_archived : bool, optional
            If True, return archived projects. Default: False.

        Returns
        -------
        List[Project]
            The projects in Faculty.
        """
        params = {"includeArchived": int(include_archived)}
        return self._get("/project", _ProjectSchema(many=True), params=params)


class _ProjectSchema(BaseSchema):

    id = fields.UUID(data_key="projectId", required=True)
    name = fields.Str(required=True)
    owner_id = fields.UUID(data_key="ownerId", required=True)
    archived_at = fields.DateTime(data_key="archivedAt", missing=None)

    @post_load
    def make_project(self, data, **kwargs):
        return Project(**data)
