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


import hashlib
import random
import requests
import string
from uuid import uuid4
from unittest.mock import patch

import pytest
import six

from faculty.datasets import transfer


PROJECT_ID = uuid4()
TEST_PATH = "/path/to/file"
TEST_URL = "https://example.com/presigned/url"


TEST_CONTENT = "".join(
    random.choice(string.printable) for _ in range(2000)
).encode("utf8")

if six.PY2:
    defaults_attribute_name = "func_defaults"
else:
    defaults_attribute_name = "__defaults__"


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
def mock_client_upload(mocker, requests_mock):
    object_client = mocker.Mock()
    response_mock = mocker.Mock()
    response_mock.provider.return_value = "GCS"
    object_client.presign_upload.return_value = response_mock

    yield object_client


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


@patch.object(transfer._rechunk_data, defaults_attribute_name, (4,))
def test_chunking_and_labelling_data_of_exact_sizes(mocker):
    content = [b"1111", b"2222", b"last"]
    a = transfer._rechunk_and_label_as_last(content)
    assert list(a) == [(b"1111", False), (b"2222", False), (b"last", True)]


@patch.object(transfer._rechunk_data, defaults_attribute_name, (12,))
def test_chunking_and_labelling_of_smaller_sizes(mocker):
    content = [b"1111", b"2222", b"last"]
    a = transfer._rechunk_and_label_as_last(content)
    assert list(a) == [(b"11112222last", True)]


@patch.object(transfer._rechunk_data, defaults_attribute_name, (4,))
def test_chunking_and_labelling_of_greater_sizes(mocker):
    content = [b"11112222last"]
    a = transfer._rechunk_and_label_as_last(content)
    assert list(a) == [(b"1111", False), (b"2222", False), (b"last", True)]

# def test_gcs_upload(mock_client_upload, requests_mock):
#     requests_mock.put(TEST_URL, exc=requests.exceptions.ConnectTimeout)
#     requests_mock.put(
#         TEST_URL,
#         [
#             {
#                 "headers": {
#                     "Range": f"bytes=0-{100}",
#                     "X-Range-MD5": hashlib.md5(
#                         TEST_CONTENT[0:101]
#                     ).hexdigest(),
#                 },
#                 "status_code": 308,
#             },
#             {"headers": {}, "status_code": 200},
#         ],
#     )

#     transfer.upload_file(mock_client_download, PROJECT_ID, TEST_PATH)
