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

DatasetsSecrets = namedtuple(
    "DatasetsSecrets", ["bucket", "access_key", "secret_key", "verified"]
)


class DatasetsSecretsSchema(Schema):
    bucket = fields.String(required=True)
    access_key = fields.String(required=True)
    secret_key = fields.String(required=True)
    verified = fields.Boolean(required=True)

    @post_load
    def make_project_datasets_secrets(self, data):
        return DatasetsSecrets(**data)


class SecretClient(BaseClient):

    SERVICE_NAME = "secret-service"

    def datasets_secrets(self, project_id):
        endpoint = "sfs/{}".format(project_id)
        return self._get(endpoint, DatasetsSecretsSchema())
