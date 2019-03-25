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
import posixpath
from contextlib import contextmanager
import tempfile
import shutil

import boto3

from faculty.clients.secret import DatasetsSecrets

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


def _test_secrets():
    return DatasetsSecrets(
        bucket=TEST_BUCKET_NAME,
        access_key=os.environ["AWS_ACCESS_KEY_ID"],
        secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region=os.environ["AWS_REGION"],
        verified=True,
    )


_S3_CLIENT_CACHE = None


def s3_client():
    global _S3_CLIENT_CACHE
    if _S3_CLIENT_CACHE is None:
        secrets = _test_secrets()
        boto_session = boto3.session.Session(
            aws_access_key_id=secrets.access_key,
            aws_secret_access_key=secrets.secret_key,
            region_name=secrets.region,
        )
        _S3_CLIENT_CACHE = boto_session.client("s3")
    return _S3_CLIENT_CACHE


def _path_in_bucket(path):
    project_id = os.environ["FACULTY_PROJECT_ID"]
    combined = posixpath.join(project_id, path.lstrip("/"))
    if path.endswith("/"):
        return posixpath.normpath(combined) + "/"
    else:
        return posixpath.normpath(combined)


def write_remote_object(path, content):
    client = s3_client()
    client.put_object(
        Bucket=TEST_BUCKET_NAME,
        Key=_path_in_bucket(path),
        Body=content,
        ServerSideEncryption="AES256",
    )


def read_remote_object(path):
    client = s3_client()
    obj = client.get_object(Bucket=TEST_BUCKET_NAME, Key=_path_in_bucket(path))
    return obj["Body"].read()


def make_file(request, path, content=""):
    write_remote_object(path, content)

    def tear_down():
        s3_client().delete_object(
            Bucket=TEST_BUCKET_NAME, Key=_path_in_bucket(path)
        )

    request.addfinalizer(tear_down)


@contextmanager
def temporary_directory():
    temp_dir = tempfile.mkdtemp()
    try:
        yield temp_dir
    finally:
        shutil.rmtree(temp_dir)
