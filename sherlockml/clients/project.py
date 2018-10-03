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


Project = namedtuple("Project", ["id", "name", "owner_id"])


class ProjectSchema(Schema):

    id = fields.UUID(data_key="projectId", required=True)
    name = fields.Str(required=True)
    owner_id = fields.UUID(data_key="ownerId", required=True)

    @post_load
    def make_project(self, data):
        return Project(**data)


class ProjectClient(BaseClient):

    SERVICE_NAME = "casebook"

    def list_accessible_by_user(self, user_id):
        endpoint = "/user/{}".format(user_id)
        return self._get(endpoint, ProjectSchema(many=True))

    def get(self, project_id):
        endpoint = "/project/{}".format(project_id)
        return self._get(endpoint, ProjectSchema())
