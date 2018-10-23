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

from marshmallow import (
    Schema,
    fields,
    post_load,
    validates_schema,
    ValidationError,
)
from marshmallow_enum import EnumField

from sherlockml.clients.base import BaseClient


class FileNodeType(Enum):
    FILE = "file"
    DIRECTORY = "directory"


File = namedtuple("File", ["path", "name", "last_modified", "size"])
Directory = namedtuple(
    "Directory",
    ["path", "name", "last_modified", "size", "truncated", "content"],
)
ListResponse = namedtuple("ListResponse", ["project_id", "path", "content"])


class FileNodeSchema(Schema):

    path = fields.String(required=True)
    name = fields.String(required=True)
    type = EnumField(FileNodeType, by_value=True, required=True)
    last_modified = fields.DateTime(required=True)
    size = fields.Integer(required=True)
    truncated = fields.Boolean()
    content = fields.Nested("self", many=True)

    @validates_schema
    def validate_type(self, data):
        if data["type"] == FileNodeType.DIRECTORY:
            required_fields = Directory._fields
        elif data["type"] == FileNodeType.FILE:
            required_fields = File._fields
        if set(data.keys()) != set(required_fields).union({"type"}):
            raise ValidationError("Wrong fields for {}.".format(data["type"]))

    @post_load
    def make_file_node(self, data):
        if data["type"] == FileNodeType.DIRECTORY:
            return Directory(**{key: data[key] for key in Directory._fields})
        elif data["type"] == FileNodeType.FILE:
            return File(**{key: data[key] for key in File._fields})
        else:
            raise ValueError("Invalid file node type.")


class ListResponseSchema(Schema):

    project_id = fields.UUID(data_key="project_id", required=True)
    path = fields.String(required=True)
    content = fields.List(fields.Nested(FileNodeSchema), required=True)

    @post_load
    def make_list_response(self, data):
        return ListResponse(**data)


class WorkspaceClient(BaseClient):

    SERVICE_NAME = "workspace"

    def list(self, project_id, prefix, depth):
        endpoint = "/project/{}/file".format(project_id)
        params = {"depth": depth, "prefix": prefix}
        response = self._get(endpoint, ListResponseSchema(), params=params)
        return response.content
