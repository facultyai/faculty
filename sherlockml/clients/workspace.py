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


Path = namedtuple("Path", ["project_id", "path", "content"])


SubPath = namedtuple(
    "SubPath",
    ["path", "name", "type", "last_modified", "size", "truncated", "content"],
)
SubPath.__new__.__defaults__ = (None,) * len(SubPath._fields)


class SubPathSchema(Schema):

    path = fields.Str(required=True)
    name = fields.Str(required=True)
    type = fields.Str(required=True)
    last_modified = fields.DateTime(required=True)
    size = fields.Integer(required=True)
    truncated = fields.Boolean(required=False)
    content = fields.Nested("self", many=True, required=False)

    @post_load
    def make_path(self, data):
        return SubPath(**data)


class PathSchema(Schema):

    project_id = fields.UUID(data_key="project_id", required=True)
    path = fields.Str(required=True)
    content = fields.List(fields.Nested(SubPathSchema))

    @post_load
    def make_base_path(self, data):
        return Path(**data)


class WorkspaceClient(BaseClient):

    SERVICE_NAME = "workspace"

    def list_files(self, project_id, prefix, depth):
        endpoint = "/project/{}/file".format(project_id)
        params = {"depth": depth, "prefix": prefix}
        return self._get(endpoint, PathSchema(), params=params)
