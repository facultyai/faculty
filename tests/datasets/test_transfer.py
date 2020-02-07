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


import random
import string
import math
from uuid import uuid4

import pytest

from faculty.clients.object import CloudStorageProvider, CompletedUploadPart
from faculty.datasets import transfer


PROJECT_ID = uuid4()
TEST_PATH = "/path/to/file"
TEST_URL = "https://example.com/presigned/url"
OTHER_URL = "https://example.com/other-presigned/url"
TEST_S3_UPLOAD_ID = 123

TEST_ETAG = "5d24e152bcdfa5a0357f46471be3be6c"
TEST_COMPLETED_PART = CompletedUploadPart(1, TEST_ETAG)

OTHER_ETAG = "d084dd881a190aa5ffdf0ce21cff9509"
OTHER_COMPLETED_PART = CompletedUploadPart(2, OTHER_ETAG)

TEST_CONTENT = "".join(
    random.choice(string.printable) for _ in range(2000)
).encode("utf8")


@pytest.fixture
def mock_client_download(mocker, requests_mock):
    object_client = mocker.Mock()
    object_client.presign_download.return_value = TEST_URL

    requests_mock.get(TEST_URL, content=TEST_CONTENT)

    yield object_client

    object_client.presign_download.assert_called_once_with(
        PROJECT_ID, TEST_PATH
    )


@pytest.fixture
def mock_presigned_response_s3(mocker):
    presigned_response = mocker.Mock()
    presigned_response.provider = CloudStorageProvider.S3
    yield presigned_response


@pytest.fixture
def mock_client_upload_s3(mocker, requests_mock):
    presigned_response_mock = mocker.Mock()
    presigned_response_mock.provider = CloudStorageProvider.S3
    presigned_response_mock.upload_id = TEST_S3_UPLOAD_ID

    object_client = mocker.Mock()
    object_client.presign_upload.return_value = presigned_response_mock

    yield object_client
    object_client.presign_upload.assert_called_once_with(PROJECT_ID, TEST_PATH)


@pytest.fixture
def mock_client_upload_gcs(mocker, requests_mock):
    presigned_response_mock = mocker.Mock()
    presigned_response_mock.provider = CloudStorageProvider.GCS
    presigned_response_mock.url = TEST_URL

    object_client = mocker.Mock()
    object_client.presign_upload.return_value = presigned_response_mock

    yield object_client
    object_client.presign_upload.assert_called_once_with(PROJECT_ID, TEST_PATH)


def _assert_contains(dict_1, dict_2):
    for key, value in dict_2.items():
        assert dict_1[key] == value


def test_download(mock_client_download):
    assert (
        transfer.download(mock_client_download, PROJECT_ID, TEST_PATH)
        == TEST_CONTENT
    )


def test_download_stream(mock_client_download):
    stream = transfer.download_stream(
        mock_client_download, PROJECT_ID, TEST_PATH
    )
    assert b"".join(stream) == TEST_CONTENT


def test_download_file(mock_client_download, tmpdir):
    destination = tmpdir.join("destination.txt")

    transfer.download_file(
        mock_client_download, PROJECT_ID, TEST_PATH, destination
    )
    assert destination.read(mode="rb") == TEST_CONTENT


@pytest.mark.parametrize("content", [[], [b""]])
def test_chunking_of_empty_files(mocker, content):
    chunk_size = 4
    chunks = transfer._rechunk_data(content, chunk_size)
    assert list(chunks) == [b""]


def test_chunking_and_labelling_data_of_exact_sizes(mocker):
    chunk_size = 4
    content = [b"1111", b"2222", b"last"]
    chunks = transfer._rechunk_and_label_as_last(content, chunk_size)
    assert list(chunks) == [
        (b"1111", False),
        (b"2222", False),
        (b"last", True),
    ]


def test_chunking_and_labelling_of_smaller_sizes(mocker):
    chunk_size = 12
    content = [b"1111", b"2222", b"last"]
    chunks = transfer._rechunk_and_label_as_last(content, chunk_size)
    assert list(chunks) == [(b"11112222last", True)]


def test_chunking_and_labelling_of_greater_sizes(mocker):
    chunk_size = 4
    content = [b"11112222last"]
    chunks = transfer._rechunk_and_label_as_last(content, chunk_size)
    assert list(chunks) == [
        (b"1111", False),
        (b"2222", False),
        (b"last", True),
    ]


@pytest.mark.parametrize(
    "file_size, expected_chunk_size", [(100, 20), (50, 10), (None, 10)]
)
def test_s3_chunk_sizing(
    mocker, mock_presigned_response_s3, file_size, expected_chunk_size
):
    mocker.patch("faculty.datasets.transfer.S3_MAX_CHUNKS", 5)
    mocker.patch("faculty.datasets.transfer.DEFAULT_CHUNK_SIZE", 10)

    chunk_size = transfer._chunk_size(
        mock_presigned_response_s3.provider, file_size
    )
    assert chunk_size == expected_chunk_size


def test_s3_upload(mock_client_upload_s3, requests_mock):
    def chunk_request_matcher(request):
        return TEST_CONTENT == request.text.encode("utf-8")

    requests_mock.put(
        TEST_URL,
        additional_matcher=chunk_request_matcher,
        headers={"ETag": TEST_ETAG},
        status_code=200,
    )

    mock_client_upload_s3.presign_upload_part.return_value = TEST_URL

    transfer.upload(mock_client_upload_s3, PROJECT_ID, TEST_PATH, TEST_CONTENT)
    mock_client_upload_s3.complete_multipart_upload.assert_called_once_with(
        PROJECT_ID, TEST_PATH, TEST_S3_UPLOAD_ID, [TEST_COMPLETED_PART]
    )


def test_s3_upload_chunks(mocker, mock_client_upload_s3, requests_mock):
    mocker.patch("faculty.datasets.transfer.DEFAULT_CHUNK_SIZE", 1000)

    def first_chunk_request_matcher(request):
        return TEST_CONTENT[0:1000] == request.text.encode("utf-8")

    def second_chunk_request_matcher(request):
        return TEST_CONTENT[1000::] == request.text.encode("utf-8")

    mock_client_upload_s3.presign_upload_part.side_effect = [
        TEST_URL,
        OTHER_URL,
    ]

    requests_mock.put(
        TEST_URL,
        additional_matcher=first_chunk_request_matcher,
        headers={"ETag": TEST_ETAG},
        status_code=200,
    )

    requests_mock.put(
        OTHER_URL,
        additional_matcher=second_chunk_request_matcher,
        headers={"ETag": OTHER_ETAG},
        status_code=200,
    )

    transfer.upload(mock_client_upload_s3, PROJECT_ID, TEST_PATH, TEST_CONTENT)

    history = requests_mock.request_history
    assert len(history) == 2

    mock_client_upload_s3.complete_multipart_upload.assert_called_once_with(
        PROJECT_ID,
        TEST_PATH,
        TEST_S3_UPLOAD_ID,
        [TEST_COMPLETED_PART, OTHER_COMPLETED_PART],
    )


@pytest.mark.parametrize("max_chunks, expected_chunks", [(10, 10), (100, 20)])
def test_s3_dynamic_chunk_upload(
    mocker, mock_client_upload_s3, requests_mock, max_chunks, expected_chunks
):
    mocker.patch("faculty.datasets.transfer.DEFAULT_CHUNK_SIZE", 100)
    mocker.patch("faculty.datasets.transfer.S3_MAX_CHUNKS", max_chunks)
    chunk_size = int(math.ceil(len(TEST_CONTENT) / expected_chunks))
    chunks = iter(
        [
            TEST_CONTENT[i * chunk_size : (i + 1) * chunk_size]
            for i in range(expected_chunks)
        ]
    )
    chunk_matchers = [
        lambda x: next(chunks) == x.text.encode("utf-8")
        for i in range(max_chunks)
    ]
    urls = [
        "https://example.com/presigned-url-{i}/url".format(i=i)
        for i in range(max_chunks)
    ]
    etags = ["tag-{tagid}".format(tagid=i) for i in range(max_chunks)]

    for url, matcher, etag in zip(urls, chunk_matchers, etags):
        requests_mock.put(
            url,
            status_code=200,
            additional_matcher=matcher,
            headers={"ETag": etag},
        )

    mock_client_upload_s3.presign_upload_part.side_effect = urls

    transfer.upload(mock_client_upload_s3, PROJECT_ID, TEST_PATH, TEST_CONTENT)

    history = requests_mock.request_history
    assert len(history) == expected_chunks


def test_gcs_upload(mock_client_upload_gcs, requests_mock):
    requests_mock.put(
        TEST_URL,
        request_headers={
            "Content-Length": "2000",
            "Content-Range": "bytes 0-1999/2000",
        },
        status_code=200,
    )

    transfer.upload(
        mock_client_upload_gcs, PROJECT_ID, TEST_PATH, TEST_CONTENT
    )


def test_gcs_upload_chunking(mocker, mock_client_upload_gcs, requests_mock):
    mocker.patch("faculty.datasets.transfer.DEFAULT_CHUNK_SIZE", 1000)
    chunk_headers = [
        {"Content-Length": "1000", "Content-Range": "bytes 0-999/*"},
        {"Content-Length": "1000", "Content-Range": "bytes 1000-1999/2000"},
    ]

    for headers in chunk_headers:
        requests_mock.put(TEST_URL, status_code=200, request_headers=headers)

    transfer.upload(
        mock_client_upload_gcs, PROJECT_ID, TEST_PATH, TEST_CONTENT
    )

    history = requests_mock.request_history
    assert len(history) == 2

    _assert_contains(history[0].headers, chunk_headers[0])
    _assert_contains(history[1].headers, chunk_headers[1])
    assert history[0].text.encode("utf8") == TEST_CONTENT[:1000]
    assert history[1].text.encode("utf8") == TEST_CONTENT[1000:]


def test_gcs_upload_empty_object(mock_client_upload_gcs, requests_mock):
    test_content = "".encode("utf8")

    requests_mock.put(
        TEST_URL, request_headers={"Content-Length": "0"}, status_code=200
    )

    transfer.upload(
        mock_client_upload_gcs, PROJECT_ID, TEST_PATH, test_content
    )
