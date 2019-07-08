# Copyright 2018-2019 Faculty Science Limited
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

from marshmallow import fields, post_load

from faculty.clients.base import BaseSchema, BaseClient


Object = namedtuple("Object", ["path", "size", "etag", "last_modified_at"])
ListObjectsResponse = namedtuple(
    "ListObjectsResponse", ["objects", "next_page_token"]
)


class ObjectSchema(BaseSchema):
    path = fields.String(required=True)
    size = fields.Integer(required=True)
    etag = fields.String(required=True)
    last_modified_at = fields.DateTime(
        data_key="lastModifiedAt", required=True
    )

    @post_load
    def make_object(self, data):
        return Object(**data)


class ListObjectsResponseSchema(BaseSchema):
    objects = fields.List(fields.Nested(ObjectSchema), required=True)
    next_page_token = fields.String(data_key="nextPageToken", missing=None)

    @post_load
    def make_list_objects_response(self, data):
        return ListObjectsResponse(**data)


class ObjectClient(BaseClient):

    SERVICE_NAME = "hoard"

    def list(self, project_id, prefix="/", page_token=None):
        endpoint = "/project/{}/object-list/{}".format(
            project_id, prefix.lstrip("/")
        )
        params = {}
        if page_token is not None:
            params["pageToken"] = page_token
        return self._get(endpoint, ListObjectsResponseSchema(), params=params)
