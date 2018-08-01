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


Environment = namedtuple('Environment',
                         ['id', 'project_id', 'name', 'author_id'])


class EnvironmentSchema(Schema):
    id = fields.UUID(data_key='environmentId', required=True)
    project_id = fields.UUID(data_key='projectId', required=True)
    name = fields.String(required=True)
    author_id = fields.UUID(data_key='authorId', required=True)

    @post_load
    def make_environment(self, data):
        return Environment(**data)


class EnvironmentClient(BaseClient):

    SERVICE_NAME = 'baskerville'

    def list(self, project_id):
        endpoint = '/project/{}/environment'.format(project_id)
        return self._get(endpoint, EnvironmentSchema(many=True))
