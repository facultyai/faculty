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


import os
import tempfile
import uuid

import pytest

from faculty.clients.secret import DatasetsSecrets

from tests.datasets.fixtures import (
    s3_client,
    make_file,
    temporary_directory,
    TEST_BUCKET_NAME,
    TEST_FILE_CONTENT,
    TEST_FILE_CONTENT_CHANGED,
    TEST_FILE_NAME,
    TEST_TREE,
)


@pytest.fixture
def project_env(monkeypatch):
    monkeypatch.setenv("FACULTY_PROJECT_ID", str(uuid.uuid4()))


def _test_secrets():
    return DatasetsSecrets(
        bucket=TEST_BUCKET_NAME,
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=os.environ["AWS_REGION"],
        verified=True,
    )


@pytest.fixture
def mock_secret_client(mocker):
    mock_client = mocker.Mock()
    mock_client.datasets_secrets.return_value = _test_secrets()
    mocker.patch("faculty.client", return_value=mock_client)


@pytest.fixture
def project_directory(request, mock_secret_client, project_env):

    project_id = os.environ["FACULTY_PROJECT_ID"]

    # Make the empty directory
    make_file(request, "")
    yield

    # Tear down
    client = s3_client()
    response = client.list_objects_v2(
        Bucket=TEST_BUCKET_NAME, Prefix=project_id
    )
    objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
    client.delete_objects(Bucket=TEST_BUCKET_NAME, Delete={"Objects": objects})


@pytest.fixture
def remote_file(request, project_directory):
    make_file(request, TEST_FILE_NAME, TEST_FILE_CONTENT)
    return TEST_FILE_NAME


@pytest.fixture
def remote_tree(request, project_directory):
    for path in TEST_TREE:
        if path.endswith("/"):
            # Just touch, is a directory
            make_file(request, path)
        else:
            # Add test file content
            make_file(request, path, TEST_FILE_CONTENT)
    return TEST_TREE


@pytest.fixture
def local_file():
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        f.write(TEST_FILE_CONTENT)
        f.flush()
        yield f.name


@pytest.fixture
def local_file_changed():
    with tempfile.NamedTemporaryFile(mode="wb") as f:
        f.write(TEST_FILE_CONTENT_CHANGED)
        f.flush()
        yield f.name


@pytest.fixture
def local_tree():

    with temporary_directory() as temp_dir:

        for tree_path in TEST_TREE:

            path = os.path.join(temp_dir, tree_path)

            if tree_path.endswith("/"):
                if not os.path.exists(path):
                    os.makedirs(path)
            else:
                dirname = os.path.dirname(path)
                if not os.path.exists(dirname):
                    os.makedirs(dirname)
                with open(path, "wb") as fp:
                    fp.write(TEST_FILE_CONTENT)

        yield temp_dir
