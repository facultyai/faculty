# Copyright 2018-2020 Faculty Science Limited
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
Interact with files in the Faculty workspace.
"""


from enum import Enum
from collections import namedtuple

from marshmallow import fields, post_load, validates_schema, ValidationError
from marshmallow_enum import EnumField

from faculty.clients.base import BaseSchema, BaseClient


File = namedtuple("File", ["path", "name", "last_modified", "size"])
Directory = namedtuple(
    "Directory",
    ["path", "name", "last_modified", "size", "truncated", "content"],
)


class WorkspaceClient(BaseClient):
    """Client for the Faculty workspace service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("workspace")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    """

    _SERVICE_NAME = "workspace"

    def list(self, project_id, prefix, depth):
        """List files in a project workspace.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project to list files in.
        prefix : str
            A file prefix to list files under.
        depth : int
            How deep down the file tree to list.

        Returns
        -------
        Union[File, Directory]
            The listed file tree.
        """
        endpoint = "/project/{}/file".format(project_id)
        params = {"depth": depth, "prefix": prefix}
        response = self._get(endpoint, _ListResponseSchema(), params=params)
        return response.content


class _FileNodeType(Enum):
    FILE = "file"
    DIRECTORY = "directory"


_ListResponse = namedtuple("_ListResponse", ["project_id", "path", "content"])


class _FileNodeSchema(BaseSchema):

    path = fields.String(required=True)
    name = fields.String(required=True)
    type = EnumField(_FileNodeType, by_value=True, required=True)
    last_modified = fields.DateTime(required=True)
    size = fields.Integer(required=True)
    truncated = fields.Boolean()
    content = fields.Nested("self", many=True)

    @validates_schema
    def validate_type(self, data):
        if data["type"] == _FileNodeType.DIRECTORY:
            required_fields = Directory._fields
        elif data["type"] == _FileNodeType.FILE:
            required_fields = File._fields
        if set(data.keys()) != set(required_fields).union({"type"}):
            raise ValidationError("Wrong fields for {}.".format(data["type"]))

    @post_load
    def make_file_node(self, data):
        if data["type"] == _FileNodeType.DIRECTORY:
            return Directory(**{key: data[key] for key in Directory._fields})
        elif data["type"] == _FileNodeType.FILE:
            return File(**{key: data[key] for key in File._fields})
        else:
            raise ValueError("Invalid file node type.")


class _ListResponseSchema(BaseSchema):

    project_id = fields.UUID(data_key="project_id", required=True)
    path = fields.String(required=True)
    content = fields.List(fields.Nested(_FileNodeSchema), required=True)

    @post_load
    def make_list_response(self, data):
        return _ListResponse(**data)
