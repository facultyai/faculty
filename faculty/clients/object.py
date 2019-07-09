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
from enum import Enum

from marshmallow import fields, post_load
from marshmallow_enum import EnumField

from faculty.clients.base import BaseSchema, BaseClient


class CloudStorageProvider(Enum):
    S3 = "S3"
    GCS = "GCS"


Object = namedtuple("Object", ["path", "size", "etag", "last_modified_at"])
ListObjectsResponse = namedtuple(
    "ListObjectsResponse", ["objects", "next_page_token"]
)
PresignDownloadResponse = namedtuple("PresignDownloadResponse", ["url"])
PresignUploadResponse = namedtuple(
    "PresignUploadResponse", ["provider", "upload_id", "url"]
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


class PresignDownloadResponseSchema(BaseSchema):
    url = fields.String(required=True)

    @post_load
    def make_presign_download_response(self, data):
        return PresignDownloadResponse(**data)


class PresignUploadResponseSchema(BaseSchema):
    provider = EnumField(CloudStorageProvider, by_value=True, required=True)
    upload_id = fields.String(data_key="uploadId", missing=None)
    url = fields.String(missing=None)

    @post_load
    def make_presign_upload_response(self, data):
        return PresignUploadResponse(**data)


class ObjectClient(BaseClient):

    SERVICE_NAME = "hoard"

    def get(self, project_id, path):
        endpoint = "/project/{}/object/{}".format(project_id, path.lstrip("/"))
        return self._get(endpoint, ObjectSchema())

    def list(self, project_id, prefix="/", page_token=None):
        endpoint = "/project/{}/object-list/{}".format(
            project_id, prefix.lstrip("/")
        )
        params = {}
        if page_token is not None:
            params["pageToken"] = page_token
        return self._get(endpoint, ListObjectsResponseSchema(), params=params)

    def presign_download(
        self, project_id, path, response_content_disposition=None
    ):
        endpoint = "/project/{}/presign/download".format(project_id)
        body = {"path": path}
        if response_content_disposition is not None:
            body["responseContentDisposition"] = response_content_disposition
        response = self._post(
            endpoint, PresignDownloadResponseSchema(), json=body
        )
        return response.url

    def presign_upload(self, project_id, path):
        endpoint = "/project/{}/presign/upload".format(project_id)
        body = {"path": path}
        return self._post(endpoint, PresignUploadResponseSchema(), json=body)
