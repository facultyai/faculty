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
Interact with Faculty datasets.
"""


from collections import namedtuple
from enum import Enum
from six.moves import urllib

from marshmallow import fields, post_load
from marshmallow_enum import EnumField

from faculty.clients.base import (
    BadRequest,
    BaseSchema,
    BaseClient,
    Conflict,
    NotFound,
)


class PathNotFound(Exception):
    def __init__(self, source_path):
        tpl = "Path provided '{}' cannot be found"
        message = tpl.format(source_path)
        super(PathNotFound, self).__init__(message)


class SourceIsADirectory(Exception):
    def __init__(self, source_path):
        tpl = (
            "Source provided '{}' is a directory and must be "
            "copied recursively"
        )
        message = tpl.format(source_path)
        super(SourceIsADirectory, self).__init__(message)


class TargetIsADirectory(Exception):
    def __init__(self, source_path):
        tpl = (
            "Target provided '{}' is a directory and must be "
            "deleted recursively"
        )
        message = tpl.format(source_path)
        super(TargetIsADirectory, self).__init__(message)


class PathAlreadyExists(Exception):
    def __init__(self, source_path):
        tpl = "Path provided '{}' already exists"
        message = tpl.format(source_path)
        super(PathAlreadyExists, self).__init__(message)


class CloudStorageProvider(Enum):
    S3 = "S3"
    GCS = "GCS"


Object = namedtuple("Object", ["path", "size", "etag", "last_modified_at"])
ListObjectsResponse = namedtuple(
    "ListObjectsResponse", ["objects", "next_page_token"]
)
PresignUploadResponse = namedtuple(
    "PresignUploadResponse", ["provider", "upload_id", "url"]
)
CompletedUploadPart = namedtuple(
    "CompletedUploadPart", ["part_number", "etag"]
)


class ObjectClient(BaseClient):
    """Client for the Faculty object storage service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("object")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    """

    _SERVICE_NAME = "hoard"

    def get(self, project_id, path):
        """Get metadata about a single object.

        Parameters
        ----------
        project_id : uuid.UUID
            The project containing the object.
        path : str
            The path of the object in the project's datasets.

        Returns
        -------
        Object
            Information about the requested object.
        """

        url_encoded_path = urllib.parse.quote(path.lstrip("/"))

        endpoint = "/project/{}/object/{}".format(project_id, url_encoded_path)
        return self._get(endpoint, _ObjectSchema())

    def list(self, project_id, prefix="/", page_token=None):
        """List objects in the store.

        If more than the maximum number of objects per page matches the list
        query, the returned ListObjectsResponse will contain a 'next page
        token' that can be passed to subsequent calls to retrieve the full
        result, e.g.:

        >>> response = client.list(my_project_id)
        >>> while response.next_page_token is not None:
        ...     response = client.list(
        ...         my_project_id, page_token=response.next_page_token
        ...     )

        Parameters
        ----------
        project_id : uuid.UUID
            The project containing the objects.
        prefix : str, optional
            If specified, only list files in the store matching this prefix.
        page_token : str, optional
            A page token returned from a previous query.

        Returns
        -------
        ListObjectsResponse
            Containing a list of matching objects and a token to get the next
            page of objects, when relevant.
        """
        url_encoded_prefix = urllib.parse.quote(prefix.lstrip("/"))

        endpoint = "/project/{}/object-list/{}".format(
            project_id, url_encoded_prefix
        )
        params = {}
        if page_token is not None:
            params["pageToken"] = page_token
        return self._get(endpoint, _ListObjectsResponseSchema(), params=params)

    def create_directory(self, project_id, path, parents=False):
        """Create empty object as placeholder to a directory in the store.

        Parameters
        ----------
        project_id : uuid.UUID
            The project to create a directory in.
        path : str
            Create empty object at this source path as a placeholder
            for a directory.
        parents : bool, optional
            Create also missing parent directories, with behaviour similar to
            ``mkdir -p``.

        Raises
        ------
        PathAlreadyExists
            When the path that we want to create as a directory already exists.
        """
        url_encoded_path = urllib.parse.quote(path.lstrip("/"))

        endpoint = "/project/{}/directory/{}".format(
            project_id, url_encoded_path
        )
        params = {"parents": 1 if parents else 0}
        try:
            self._put_raw(endpoint, params=params)
        except Conflict as err:
            if err.error_code == "object_already_exists":
                raise PathAlreadyExists(path)
            else:
                raise

    def copy(self, project_id, source, destination, recursive=False):
        """Copy objects in the store.

        Parameters
        ----------
        project_id : uuid.UUID
            The project to copy objects in.
        source : str
            Copy object(s) from this source path.
        destination : str
            Copy to this destination path.
        recursive : bool, optional
            If present allows to copy whole paths with all its content,
            like a recursive copy in a filesystem. By default the action
            is not recursive.

        Raises
        ------
        PathNotFound
            When the source path does not exist or is not found.
        SourceIsADirectory
            When the source path to copy is a directory but recursive is
            ``False``.
        """

        url_encoded_destination = urllib.parse.quote(destination.lstrip("/"))

        endpoint = "/project/{}/object/{}".format(
            project_id, url_encoded_destination
        )
        params = {"sourcePath": source}
        if recursive is not None:
            params["recursive"] = 1 if recursive else 0
        try:
            self._put_raw(endpoint, params=params)
        except NotFound as err:
            if err.error_code == "source_path_not_found":
                raise PathNotFound(source)
            else:
                raise
        except BadRequest as err:
            if err.error_code == "source_is_a_directory":
                raise SourceIsADirectory(source)
            else:
                raise

    def move(self, project_id, source, destination):
        """Move objects in the store

        Parameters
        ----------
        project_id : uuid.UUID
            The project to move objects in.
        source : str
            Move object from this source path.
        destination : str
            Move object to this destination path.


        Raises
        ------
        PathNotFound
            When the source path does not exist or is not found.
        """
        url_encoded_destination = urllib.parse.quote(destination.lstrip("/"))

        endpoint = "/project/{}/object-move/{}".format(
            project_id, url_encoded_destination
        )
        params = {"sourcePath": source}

        try:
            self._put_raw(endpoint, params=params)
        except NotFound as err:
            if err.error_code == "source_path_not_found":
                raise PathNotFound(source)

    def delete(self, project_id, path, recursive=False):
        """Delete objects in the store.

        Parameters
        ----------
        project_id : uuid.UUID
            The project to delete objects from.
        path : str
            Delete object(s) from this path.
        recursive : bool, optional
            If present allows to delete whole paths with all its content,
            like a recursive delete in a filesystem. By default the action
            is not recursive.

        Raises
        ------
        PathNotFound
            When the path does not exist or is not found.
        TargetIsADirectory
            When the target to delete is a directory but recursive is
            ``False``.
        """

        url_encoded_path = urllib.parse.quote(path.lstrip("/"))
        endpoint = "/project/{}/object/{}".format(project_id, url_encoded_path)
        params = {}
        if recursive is not None:
            params["recursive"] = 1 if recursive else 0

        try:
            self._delete_raw(endpoint, params=params)
        except NotFound as err:
            if err.error_code == "object_not_found":
                raise PathNotFound(path)
            else:
                raise
        except BadRequest as err:
            if err.error_code == "target_is_a_directory":
                raise TargetIsADirectory(path)
            else:
                raise

    def presign_download(
        self, project_id, path, response_content_disposition=None
    ):
        """Generate a presigned URL for download of an object over HTTP.

        Parameters
        ----------
        project_id : uuid.UUID
            The project containing the object.
        path : str
            The path of the object.
        response_content_disposition : str, optional
            Sets the 'Content-Disposition' header in the response when the
            generated presigned URL is used.

        Returns
        -------
        str
            The presigned URL.
        """
        endpoint = "/project/{}/presign/download".format(project_id)
        body = {"path": path}
        if response_content_disposition is not None:
            body["responseContentDisposition"] = response_content_disposition
        response = self._post(
            endpoint, _SimplePresignResponseSchema(), json=body
        )
        return response.url

    def presign_upload(self, project_id, path):
        """Generate a presigned URL for upload of an object over HTTP.

        Due to differences in how S3 and GCS handle uploads of large files, the
        content of the :class:`PresignUploadResponse` returned by this method
        will vary depending on the storage backend.

        Parameters
        ----------
        project_id : uuid.UUID
            The project to upload to.
        path : str
            The path to upload to.

        Returns
        -------
        PresignUploadResponse
            Containing the storage provider plus an upload ID or presigned URL,
            as appropriate for the storage provider.

        Notes
        -----

        When the object store is backed by AWS S3 (Simple Storage Service), the
        the returned ``provider`` will be :const:`CloudStorageProvider.S3`. In
        this case, the uploaded file must be broken up into chunks of at least
        5MB size (the last chunk may be any size), then each chunk must be
        uploaded by:

        1. Assign each chunk a part number, starting from 1
        2. Presign the chunk for upload with :meth:`presign_upload_part`
        3. Upload the chunk by PUTting to the returned URL
        4. Get the 'ETag' header from the response

        Once all chunks have been uploaded, make a final call to
        :meth:`complete_multipart_upload` to finalise the upload. The last
        argument to :meth:`complete_multipart_upload` must be a collection of
        :class:`CompletedUploadPart` objects containing the part numbers and
        etags generated above.

        Alternatively, when the object store is backed by GCS (Google Cloud
        Storage), the returned ``provider`` will be
        :const:`CloudStorageProvider.GCS`. In this case, you can directly PUT
        the full contents of the object to the returned URL in a single
        request, and then resume the download with subsequent requests if the
        connection drops at some point.

        For full details, see the GCP documentation, starting from "Step 3 -
        Upload the file":
        https://cloud.google.com/storage/docs/xml-api/resumable-upload
        """
        endpoint = "/project/{}/presign/upload".format(project_id)
        body = {"path": path}
        return self._post(endpoint, _PresignUploadResponseSchema(), json=body)

    def presign_upload_part(self, project_id, path, upload_id, part_number):
        """Generate a presigned URL for a part of an S3 multipart upload.

        Parameters
        ----------
        project_id : uuid.UUID
            The project being uploaded to.
        path : str
            The path being uploaded to.
        upload_id : str
            The S3 upload ID returned by :meth:`presign_upload`.
        part_number : int
            The number of the part determining its ordering. Part numbers start
            from 1.

        Returns
        -------
        str
            The presigned URL.
        """
        endpoint = "/project/{}/presign/upload/part".format(project_id)
        body = {"path": path, "uploadId": upload_id, "partNumber": part_number}
        response = self._put(
            endpoint, _SimplePresignResponseSchema(), json=body
        )
        return response.url

    def complete_multipart_upload(
        self, project_id, path, upload_id, completed_parts
    ):
        """Complete an S3 multipart upload.

        Parameters
        ----------
        project_id : uuid.UUID
            The project being uploaded to.
        path : str
            The path being uploaded to.
        upload_id : str
            The S3 upload ID returned by :meth:`presign_upload`.
        completed_parts : List[CompletedUploadPart]
            Information about the uploaded parts. Each
            :class:`CompletedUploadPart` contains the part number and the
            ETag for each previously uploaded part. The ETags are obtained from
            the ``ETag`` header in the HTTP response when uploading each part.
        """
        endpoint = "/project/{}/presign/upload/complete".format(project_id)
        schema = _CompleteMultipartUploadSchema()
        body = schema.dump(
            {"path": path, "upload_id": upload_id, "parts": completed_parts}
        )
        self._put_raw(endpoint, json=body)


_SimplePresignResponse = namedtuple("_SimplePresignResponse", ["url"])


class _ObjectSchema(BaseSchema):
    path = fields.String(required=True)
    size = fields.Integer(required=True)
    etag = fields.String(required=True)
    last_modified_at = fields.DateTime(
        data_key="lastModifiedAt", required=True
    )

    @post_load
    def make_object(self, data):
        return Object(**data)


class _ListObjectsResponseSchema(BaseSchema):
    objects = fields.List(fields.Nested(_ObjectSchema), required=True)
    next_page_token = fields.String(data_key="nextPageToken", missing=None)

    @post_load
    def make_list_objects_response(self, data):
        return ListObjectsResponse(**data)


class _SimplePresignResponseSchema(BaseSchema):
    url = fields.String(required=True)

    @post_load
    def make_simple_presign_response(self, data):
        return _SimplePresignResponse(**data)


class _PresignUploadResponseSchema(BaseSchema):
    provider = EnumField(CloudStorageProvider, by_value=True, required=True)
    upload_id = fields.String(data_key="uploadId", missing=None)
    url = fields.String(missing=None)

    @post_load
    def make_presign_upload_response(self, data):
        return PresignUploadResponse(**data)


class _CompletedUploadPartSchema(BaseSchema):
    part_number = fields.Integer(data_key="partNumber")
    etag = fields.String()


class _CompleteMultipartUploadSchema(BaseSchema):
    path = fields.String()
    upload_id = fields.String(data_key="uploadId")
    parts = fields.List(fields.Nested(_CompletedUploadPartSchema))
