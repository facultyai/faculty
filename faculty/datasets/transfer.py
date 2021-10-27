# Copyright 2018-2021 Faculty Science Limited
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

"""Transfer data to and from Faculty datasets."""


import os
import math

import requests

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from faculty.clients.object import CloudStorageProvider, CompletedUploadPart
from faculty.datasets.util import DatasetsError

KILOBYTE = 1024
MEGABYTE = 1024 * KILOBYTE
GIGABYTE = 1024 * MEGABYTE

S3_MAX_CHUNKS = 10000
DEFAULT_CHUNK_SIZE = 5 * MEGABYTE

FILE_CHUNK_SIZE = 5 * MEGABYTE


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
    chunk_generator = download_stream(object_client, project_id, datasets_path)
    return b"".join(chunk_generator)


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

    with open(str(local_path), "wb") as fp:
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
    _upload_stream(
        object_client,
        project_id,
        datasets_path,
        [content],
        known_file_size=len(content),
    )


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
    _upload_stream(object_client, project_id, datasets_path, content)


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
    file_size = os.path.getsize(local_path)
    _upload_stream(
        object_client,
        project_id,
        datasets_path,
        _file_chunk_iterator(local_path),
        known_file_size=file_size,
    )


def _upload_stream(
    object_client, project_id, datasets_path, content, known_file_size=None
):

    presign_response = object_client.presign_upload(project_id, datasets_path)
    chunk_size = _chunk_size(presign_response.provider, known_file_size)

    if presign_response.provider == CloudStorageProvider.S3:
        _s3_upload(
            object_client,
            project_id,
            datasets_path,
            content,
            presign_response.upload_id,
            chunk_size,
        )
    elif presign_response.provider == CloudStorageProvider.GCS:
        _gcs_upload(presign_response.url, content, chunk_size)
    else:
        raise ValueError(
            "Unsupported cloud storage provider: {}".format(
                presign_response.provider
            )
        )


def _s3_upload(
    object_client, project_id, datasets_path, content, upload_id, chunk_size
):

    #  See
    #  https://aws.amazon.com/premiumsupport/knowledge-center/http-5xx-errors-s3
    retries = Retry(
        backoff_factor=0.1,
        status=10,
        status_forcelist=[500, 502, 503, 504],
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retries))

    completed_parts = []
    for i, chunk in enumerate(_rechunk_data(content, chunk_size)):

        part_number = i + 1

        chunk_url = object_client.presign_upload_part(
            project_id, datasets_path, upload_id, part_number
        )

        upload_response = session.put(chunk_url, data=chunk)
        upload_response.raise_for_status()
        completed_parts.append(
            CompletedUploadPart(
                part_number=part_number, etag=upload_response.headers["ETag"]
            )
        )

    object_client.complete_multipart_upload(
        project_id, datasets_path, upload_id, completed_parts
    )


def _gcs_upload(upload_url, content, chunk_size):

    start_index = 0

    for i, (chunk, is_last) in enumerate(
        _rechunk_and_label_as_last(content, chunk_size)
    ):
        if is_last:
            total_file_size = start_index + len(chunk)
        else:
            total_file_size = "*"

        _gcs_upload_chunk(upload_url, chunk, start_index, total_file_size)
        start_index += len(chunk)


def _gcs_upload_chunk(upload_url, content, start_index, total_file_size):
    headers = {"Content-Length": "{0}".format(len(content))}
    # Only add Content-Range if not empty, otherwise this will result in
    # a bad request
    if start_index or content:
        end_index = start_index + len(content) - 1
        headers["Content-Range"] = "bytes {0}-{1}/{2}".format(
            start_index, end_index, total_file_size
        )
    result = requests.put(upload_url, data=content, headers=headers)

    result.raise_for_status()


def _file_chunk_iterator(local_path):
    with open(local_path, "rb") as fp:
        chunk = fp.read(FILE_CHUNK_SIZE)
        while chunk:
            yield chunk
            chunk = fp.read(FILE_CHUNK_SIZE)


def _rechunk_data(content, chunk_size):
    chunk = b""
    has_yielded = False
    for original_chunk in content:

        while len(original_chunk) > 0:
            remaining = chunk_size - len(chunk)
            chunk += original_chunk[:remaining]
            original_chunk = original_chunk[remaining:]
            if len(chunk) >= chunk_size:
                has_yielded = True
                yield chunk
                chunk = b""

    if not has_yielded or len(chunk) > 0:
        yield chunk


def _rechunk_and_label_as_last(content, chunk_size):
    chunks = _rechunk_data(content=content, chunk_size=chunk_size)
    current_chunk = next(chunks, b"")
    while True:
        try:
            next_chunk = next(chunks)
            yield (current_chunk, False)
            current_chunk = next_chunk
        except StopIteration:
            yield (current_chunk, True)
            break


def _chunk_size(provider, known_file_size):
    if known_file_size is None:
        return DEFAULT_CHUNK_SIZE
    elif provider == CloudStorageProvider.S3:
        new_chunk_size = math.ceil(known_file_size / float(S3_MAX_CHUNKS))
        return int(max([new_chunk_size, DEFAULT_CHUNK_SIZE]))
    else:
        return DEFAULT_CHUNK_SIZE
