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

import pytest

from sherlockml.datasets import path


@pytest.mark.parametrize(
    "input_path, rationalised_path",
    [
        ("", "/"),
        ("./", "/"),
        ("/", "/"),
        ("path", "/path"),
        ("./path", "/path"),
        ("/path", "/path"),
        ("path/", "/path/"),
        ("./path/", "/path/"),
        ("/path/", "/path/"),
    ],
)
def test_rationalise_projectpath(input_path, rationalised_path):
    assert path.rationalise_projectpath(input_path) == rationalised_path


@pytest.mark.parametrize(
    "project_path, project_id, bucket_path",
    [
        ("", "id", "id/"),
        ("./", "id", "id/"),
        ("/", "id", "id/"),
        ("path", "id", "id/path"),
        ("./path", "id", "id/path"),
        ("/path", "id", "id/path"),
        ("path/", "id", "id/path/"),
        ("./path/", "id", "id/path/"),
        ("/path/", "id", "id/path/"),
    ],
)
def test_projectpath_to_bucketpath(project_path, project_id, bucket_path):
    result = path.projectpath_to_bucketpath(project_path, project_id)
    assert result == bucket_path


@pytest.mark.parametrize(
    "bucket_path, project_path",
    [
        ("id/", "/"),
        ("id/path", "/path"),
        ("id/path/", "/path/"),
        ("id/nested/path", "/nested/path"),
    ],
)
def test_bucketpath_to_projectpath(bucket_path, project_path):
    result = path.bucketpath_to_projectpath(bucket_path)
    assert result == project_path


@pytest.mark.parametrize(
    "project_path",
    [
        "input/path/to/somefile.csv",
        "./input/path/to/somefile.csv",
        "/input/path/to/somefile.csv",
        "input/path/to/somedir/",
        "./input/path/to/somedir/",
        "/input/path/to/somedir/",
    ],
)
def test_project_parent_directories(project_path):
    correct = ["/", "/input/", "/input/path/", "/input/path/to/"]
    result = path.project_parent_directories(project_path)
    assert set(result) == set(correct)
