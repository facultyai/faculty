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

from enum import Enum
from collections import namedtuple

from marshmallow import Schema, fields, post_load
from marshmallow_enum import EnumField

from sherlockml.clients.base import BaseClient


class FileNodeType(Enum):
    FILE = "file"
    DIRECTORY = "directory"


ListResponse = namedtuple("ListResponse", ["project_id", "path", "content"])


FileNode = namedtuple(
    "FileNode",
    ["path", "name", "type", "last_modified", "size", "truncated", "content"],
)
FileNode.__new__.__defaults__ = (None,) * len(FileNode._fields)


class FileNodeSchema(Schema):

    path = fields.String(required=True)
    name = fields.String(required=True)
    type = EnumField(FileNodeType, by_value=True, required=True)
    last_modified = fields.DateTime(required=True)
    size = fields.Integer(required=True)
    truncated = fields.Boolean()
    content = fields.Nested("self", many=True)

    @post_load
    def make_path(self, data):
        return FileNode(**data)


class ListResponseSchema(Schema):

    project_id = fields.UUID(data_key="project_id", required=True)
    path = fields.Str(required=True)
    content = fields.List(fields.Nested(FileNodeSchema))

    @post_load
    def make_base_path(self, data):
        return ListResponse(**data)


class WorkspaceClient(BaseClient):

    SERVICE_NAME = "workspace"

    def list(self, project_id, prefix, depth):
        endpoint = "/project/{}/file".format(project_id)
        params = {"depth": depth, "prefix": prefix}
        return self._get(endpoint, ListResponseSchema(), params=params)
