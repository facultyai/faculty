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


from uuid import uuid4
import random
import string

import pytest

from faculty.datasets import transfer


PROJECT_ID = uuid4()
TEST_PATH = "/path/to/file"
TEST_URL = "https://example.com/presigned/url"

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
