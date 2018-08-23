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


import os
import posixpath
from contextlib import contextmanager
import tempfile
import shutil
import uuid

import boto3
import pytest

from sherlockml.clients.secret import DatasetsSecrets

TEST_BUCKET_NAME = "sml-sfs-test"

TEST_FILE_NAME = "test_file"
TEST_FILE_CONTENT = b"this is a test file"
TEST_FILE_CONTENT_CHANGED = b"this is a different test file"
TEST_TREE = [
    "input/",
    "input/dir1/",
    "input/dir1/file1",
    "input/dir1/.file2",
    "input/.dir2/",
    "input/.dir2/file3",
    "output/",
    "output/file4",
    "empty/",
]
TEST_TREE_NO_HIDDEN_FILES = [
    "input/",
    "input/dir1/",
    "input/dir1/file1",
    "output/",
    "output/file4",
    "empty/",
]
VALID_ROOT_PATHS = ["", "./", "/"]
VALID_DIRECTORIES = [
    "input",
    "input/",
    "./input",
    "./input/",
    "/input",
    "/input/",
    "/input/dir1/",
    "/input/.dir2",
]
VALID_FILES = [
    "input/dir1/file1",
    "./input/dir1/file1",
    "/input/dir1/file1",
    "input/.dir2/file3",
]
EMPTY_DIRECTORY = "empty/"
INVALID_PATHS = [
    "inp",
    "inp/",
    "./inp",
    "./inp/",
    "/inp",
    "/inp/",
    "input/dir1/fil",
    "./input/dir1/fil",
    "/input/dir1/fil",
    "input/dir1/file1/",
    "./input/dir1/file1/",
    "/input/dir1/file1/",
]
TEST_DIRECTORY = "test_directory"
TEST_NON_EXISTENT = "test_non_existent"


@pytest.fixture
def project_env(monkeypatch):
    monkeypatch.setenv("SHERLOCKML_PROJECT_ID", str(uuid.uuid4()))


def _test_secrets():
    return DatasetsSecrets(
        bucket=TEST_BUCKET_NAME,
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        verified=True,
    )


@pytest.fixture
def mock_secret_client(mocker):
    mock_client = mocker.Mock()
    mock_client.datasets_secrets.return_value = _test_secrets()
    mocker.patch("sherlockml.client", return_value=mock_client)


@pytest.fixture
def project_directory(request, mock_secret_client, project_env):

    project_id = os.environ["SHERLOCKML_PROJECT_ID"]

    # Make the empty directory
    _make_file(request, "")
    yield

    # Tear down
    client = _s3_client()
    response = client.list_objects_v2(
        Bucket=TEST_BUCKET_NAME, Prefix=project_id
    )
    objects = [{"Key": obj["Key"]} for obj in response["Contents"]]
    client.delete_objects(Bucket=TEST_BUCKET_NAME, Delete={"Objects": objects})


@pytest.fixture
def remote_file(request, project_directory):
    _make_file(request, TEST_FILE_NAME, TEST_FILE_CONTENT)
    return TEST_FILE_NAME


@pytest.fixture
def remote_tree(request, project_directory):
    for path in TEST_TREE:
        if path.endswith("/"):
            # Just touch, is a directory
            _make_file(request, path)
        else:
            # Add test file content
            _make_file(request, path, TEST_FILE_CONTENT)
    return TEST_TREE


_S3_CLIENT_CACHE = None


def _s3_client():
    global _S3_CLIENT_CACHE
    if _S3_CLIENT_CACHE is None:
        secrets = _test_secrets()
        boto_session = boto3.session.Session(
            aws_access_key_id=secrets.access_key,
            aws_secret_access_key=secrets.secret_key,
            region_name="eu-west-1",
        )
        _S3_CLIENT_CACHE = boto_session.client("s3")
    return _S3_CLIENT_CACHE


def _path_in_bucket(path):
    project_id = os.environ["SHERLOCKML_PROJECT_ID"]
    combined = posixpath.join(project_id, path.lstrip("/"))
    if path.endswith("/"):
        return posixpath.normpath(combined) + "/"
    else:
        return posixpath.normpath(combined)


def write_remote_object(path, content):
    client = _s3_client()
    client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=_path_in_bucket(path),
        Body=content,
        ServerSideEncryption="AES256",
    )


def read_remote_object(path):
    client = _s3_client()
    obj = client.get_object(Bucket=TEST_BUCKET_NAME, Key=_path_in_bucket(path))
    return obj["Body"].read()


def _make_file(request, path, content=""):
    write_remote_object(path, content)

    def tear_down():
        _s3_client().delete_object(
            Bucket=TEST_BUCKET_NAME, Key=_path_in_bucket(path)
        )

    request.addfinalizer(tear_down)


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


@contextmanager
def temporary_directory():
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)


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
