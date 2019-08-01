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


import requests

from faculty.clients.object import CloudStorageProvider, CompletedUploadPart
from faculty.datasets.util import DatasetsError


KILOBYTE = 1024
MEGABYTE = 1024 * KILOBYTE


def download(object_client, project_id, datasets_path):
    """Download the contents of file from the object store.

    Parameters
    ----------
    object_client : faculty.clients.object.ObjectClient
    project_id : uuid.UUID
    datasets_path : str
        The target path to download to in the object store

    Returns
    -------
    bytes
        The content of the file
    """

    content = b""
    for chunk in download_stream(object_client, project_id, datasets_path):
        content += chunk

    return content


def download_stream(object_client, project_id, datasets_path):
    """Stream the contents of file from the object store.

    Parameters
    ----------
    object_client : faculty.clients.object.ObjectClient
    project_id : uuid.UUID
    datasets_path : str
        The target path to download to in the object store

    Returns
    -------
    Iterable[bytes]
        The content of the file, chunked
    """

    url = object_client.presign_download(project_id, datasets_path)

    with requests.get(url, stream=True) as response:

        if response.status_code == 404:
            raise DatasetsError(
                "No such object {} in project {}".format(
                    datasets_path, project_id
                )
            )

        response.raise_for_status()

        for chunk in response.iter_content(chunk_size=KILOBYTE):
            if chunk:  # Filter out keep-alive chunks
                yield chunk


def download_file(object_client, project_id, datasets_path, local_path):
    """Download a file from the object store.

    Parameters
    ----------
    object_client : faculty.clients.object.ObjectClient
    project_id : uuid.UUID
    datasets_path : str
        The target path to download to in the object store
    local_path : str
        The local path of the object to download
    """

    # Initiate the download to allow any failures to happen before opening the
    # file
    stream = download_stream(object_client, project_id, datasets_path)

    with open(local_path, "wb") as fp:
        for chunk in stream:
            fp.write(chunk)


def upload(object_client, project_id, datasets_path, content):
    """Upload data to the object store.

    Parameters
    ----------
    object_client : faculty.clients.object.ObjectClient
    project_id : uuid.UUID
    datasets_path : str
        The target path to upload to in the object store
    content : bytes
        The data to upload
    """
    # upload_stream will rechunk the data anyway so just pass as a single chunk
    upload_stream(object_client, project_id, datasets_path, [content])


def upload_stream(object_client, project_id, datasets_path, content):
    """Upload data to the object store from an iterable.

    Parameters
    ----------
    object_client : faculty.clients.object.ObjectClient
    project_id : uuid.UUID
    datasets_path : str
        The target path to upload to in the object store
    content : Iterable[bytes]
        The data to upload, chunked
    """

    presign_response = object_client.presign_upload(project_id, datasets_path)

    if presign_response.provider == CloudStorageProvider.S3:
        _s3_upload(
            object_client,
            project_id,
            datasets_path,
            content,
            presign_response.upload_id,
        )
    elif presign_response.provider == CloudStorageProvider.GCS:
        _gcs_upload(presign_response.url, content)
    else:
        raise ValueError(
            "Unsupported cloud storage provider: {}".format(
                presign_response.provider
            )
        )


def upload_file(object_client, project_id, datasets_path, local_path):
    """Upload a file to the object store.

    Parameters
    ----------
    object_client : faculty.clients.object.ObjectClient
    project_id : uuid.UUID
    datasets_path : str
        The target path to upload to in the object store
    local_path : str
        The local path of the object to upload
    """
    upload_stream(
        object_client,
        project_id,
        datasets_path,
        _file_chunk_iterator(local_path),
    )


def _s3_upload(object_client, project_id, datasets_path, content, upload_id):

    completed_parts = []

    for i, chunk in enumerate(_rechunk_data(content)):

        part_number = i + 1

        chunk_url = object_client.presign_upload_part(
            project_id, datasets_path, upload_id, part_number
        )

        upload_response = requests.put(chunk_url, data=chunk)
        upload_response.raise_for_status()

        completed_parts.append(
            CompletedUploadPart(
                part_number=part_number, etag=upload_response.headers["ETag"]
            )
        )

    object_client.complete_multipart_upload(
        project_id, datasets_path, upload_id, completed_parts
    )


def _gcs_upload(upload_url, content):
    raise NotImplementedError("GCS upload is not implemented")


def _file_chunk_iterator(local_path, chunk_size=5 * MEGABYTE):
    with open(local_path, "rb") as fp:
        chunk = fp.read(chunk_size)
        while chunk:
            yield chunk
            chunk = fp.read(chunk_size)


def _rechunk_data(content, chunk_size=5 * MEGABYTE):

    chunk = b""

    for original_chunk in content:

        while len(original_chunk) > 0:

            remaining = chunk_size - len(chunk)
            chunk += original_chunk[:remaining]
            original_chunk = original_chunk[remaining:]

            if len(chunk) >= chunk_size:
                yield chunk
                chunk = b""

    if len(chunk) > 0:
        yield chunk
