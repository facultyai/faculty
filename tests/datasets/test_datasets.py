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


import fnmatch
import os
import posixpath
import pytest

from sherlockml import datasets

from tests.datasets.fixtures import (  # noqa: F401
    mock_secret_client,
    project_env,
    project_directory,
    write_remote_object,
    read_remote_object,
    remote_file,
    remote_tree,
    local_file,
    local_file_changed,
    local_tree,
    temporary_directory,
    TEST_DIRECTORY,
    TEST_FILE_NAME,
    TEST_TREE,
    TEST_TREE_NO_HIDDEN_FILES,
    VALID_ROOT_PATHS,
    VALID_DIRECTORIES,
    VALID_FILES,
    INVALID_PATHS,
    TEST_FILE_CONTENT,
    TEST_NON_EXISTENT,
    EMPTY_DIRECTORY,
)


pytestmark = pytest.mark.usefixtures("project_directory")


@pytest.mark.parametrize(  # noqa: F811
    "path, show_hidden, expected",
    [(path, True, TEST_TREE) for path in VALID_ROOT_PATHS]
    + [(path, False, TEST_TREE_NO_HIDDEN_FILES) for path in VALID_ROOT_PATHS],
)
def test_ls_root(remote_tree, path, show_hidden, expected):
    assert set(datasets.ls(path, show_hidden=show_hidden)) == {
        "/" + path for path in expected
    }


@pytest.mark.parametrize(  # noqa: F811
    "prefix, show_hidden, expected",
    [(directory, True, TEST_TREE) for directory in VALID_DIRECTORIES]
    + [
        (directory, False, TEST_TREE_NO_HIDDEN_FILES)
        for directory in VALID_DIRECTORIES
    ],
)
def test_ls_subdirectory(remote_tree, prefix, show_hidden, expected):
    if prefix.startswith("./"):
        prefix = prefix[2:]
    prefix = prefix.lstrip("/")
    matches = ["/" + path for path in expected if path.startswith(prefix)]
    assert set(datasets.ls(prefix, show_hidden=show_hidden)) == set(matches)


@pytest.mark.parametrize(  # noqa: F811
    "pattern, prefix, show_hidden, expected",
    [("*dir2*", directory, True, TEST_TREE) for directory in VALID_DIRECTORIES]
    + [
        ("*file1", directory, False, TEST_TREE_NO_HIDDEN_FILES)
        for directory in VALID_DIRECTORIES
    ],
)
def test_glob(pattern, remote_tree, prefix, show_hidden, expected):
    if prefix.startswith("./"):
        prefix = prefix[2:]
    prefix = prefix.lstrip("/")
    matches = [
        "/" + path
        for path in expected
        if path.startswith(prefix) and fnmatch.fnmatch(path, pattern)
    ]
    assert set(datasets.glob(pattern, prefix, show_hidden=show_hidden)) == set(
        matches
    )


@pytest.mark.parametrize(  # noqa: F811
    "path, result",
    [(path, True) for path in VALID_DIRECTORIES]
    + [(path, False) for path in VALID_FILES + INVALID_PATHS],
)
def test_isdir(remote_tree, path, result):
    assert datasets._isdir(path) is result


@pytest.mark.parametrize(  # noqa: F811
    "path, result",
    [(path, True) for path in VALID_FILES]
    + [(path, False) for path in VALID_DIRECTORIES + INVALID_PATHS],
)
def test_isfile(remote_tree, path, result):
    assert datasets._isfile(path) is result


@pytest.mark.parametrize(  # noqa: F811
    "destination",
    [TEST_FILE_NAME, "/" + TEST_FILE_NAME, "./" + TEST_FILE_NAME],
)
def test_put_file(local_file, destination):
    datasets.put(local_file, destination)
    content = read_remote_object(TEST_FILE_NAME)
    assert content == TEST_FILE_CONTENT


def test_put_file_nonexistent_directory(local_file):  # noqa: F811
    datasets.put(local_file, "/path/to/newdir/test_file")
    for path in ["/path/", "/path/to/", "/path/to/newdir/"]:
        content = read_remote_object(path)
        assert content == b""
    content = read_remote_object("/path/to/newdir/test_file")
    assert content == TEST_FILE_CONTENT


@pytest.mark.parametrize(  # noqa: F811
    "destination",
    [
        "",
        "./",
        "/",
        TEST_DIRECTORY + "/",
        "./" + TEST_DIRECTORY + "/",
        "/" + TEST_DIRECTORY + "/",
    ],
)
def test_put_file_in_directory(local_file, destination):
    with pytest.raises(datasets.SherlockMLDatasetsError):
        datasets.put(local_file, destination)


@pytest.mark.parametrize(  # noqa: F811
    "destination, resolved_destination",
    [
        ("", ""),
        ("./", ""),
        ("/", ""),
        (TEST_DIRECTORY, TEST_DIRECTORY),
        ("./" + TEST_DIRECTORY, TEST_DIRECTORY),
        ("/" + TEST_DIRECTORY, TEST_DIRECTORY),
        (TEST_DIRECTORY + "/", TEST_DIRECTORY),
        ("./" + TEST_DIRECTORY + "/", TEST_DIRECTORY),
        ("/" + TEST_DIRECTORY + "/", TEST_DIRECTORY),
    ],
)
def test_put_tree(local_tree, destination, resolved_destination):
    datasets.put(local_tree, destination)
    for filename in TEST_TREE:
        path = posixpath.join(resolved_destination, filename)
        content = read_remote_object(path)
        if filename.endswith("/"):
            assert content == b""
        else:
            assert content == TEST_FILE_CONTENT


def test_get_file(remote_file):  # noqa: F811
    with temporary_directory() as dirname:
        path = os.path.join(dirname, TEST_FILE_NAME)
        datasets.get(remote_file, path)
        with open(path, "rb") as f:
            content = f.read()
    assert content == TEST_FILE_CONTENT


def test_get_file_in_directory(remote_file):  # noqa: F811
    with temporary_directory() as dirname:
        # Ensure dirname ends with exactly one '/'
        dirname = dirname.rstrip("/") + "/"
        with pytest.raises(datasets.SherlockMLDatasetsError):
            datasets.get(remote_file, dirname)


def test_get_file_bad_local_path(remote_file):  # noqa: F811
    with temporary_directory() as dirname:
        path = os.path.join(dirname, "directory/does/not/exist")
        with pytest.raises(IOError):
            datasets.get(remote_file, path)


def _validate_local_tree(root, tree):
    for path in tree:
        full_path = os.path.join(root, path)
        if full_path.endswith("/"):
            assert os.path.isdir(full_path)
        else:
            with open(full_path, "rb") as f:
                content = f.read()
            assert content == TEST_FILE_CONTENT


@pytest.mark.parametrize("path", VALID_ROOT_PATHS)  # noqa: F811
def test_get_tree(remote_tree, path):
    with temporary_directory() as dirname:
        datasets.get(path, dirname)
        _validate_local_tree(dirname, TEST_TREE)


@pytest.mark.parametrize("path", VALID_ROOT_PATHS)  # noqa: F811
def test_get_tree_in_directory(remote_tree, path):
    with temporary_directory() as dirname:
        # Ensure dirname ends with exactly one '/'
        dirname = dirname.rstrip("/") + "/"
        datasets.get(path, dirname)
        _validate_local_tree(dirname, TEST_TREE)


@pytest.mark.parametrize("path", VALID_DIRECTORIES)  # noqa: F811
def test_get_subtree(remote_tree, path):
    prefix = path.strip("/") + "/"
    subtree = [
        tree_path[len(prefix) :]
        for tree_path in TEST_TREE
        if tree_path.startswith(prefix)
    ]
    with temporary_directory() as dirname:
        datasets.get(path, dirname)
        _validate_local_tree(dirname, subtree)


@pytest.mark.parametrize("path", VALID_DIRECTORIES)  # noqa: F811
def test_get_subtree_in_directory(remote_tree, path):
    prefix = path.strip("/") + "/"
    subtree = [
        tree_path[len(prefix) :]
        for tree_path in TEST_TREE
        if tree_path.startswith(prefix)
    ]
    with temporary_directory() as dirname:
        # Ensure dirname ends with exactly one '/'
        dirname = dirname.rstrip("/") + "/"
        datasets.get(path, dirname)
        _validate_local_tree(dirname, subtree)


@pytest.mark.parametrize("path", VALID_DIRECTORIES)  # noqa: F811
def test_get_tree_bad_local_path(remote_tree, path):
    with temporary_directory() as dirname:
        local_path = os.path.join(dirname, "directory/does/not/exist/")
        with pytest.raises(IOError):
            datasets.get(path, local_path)


def test_get_non_existent():
    with temporary_directory() as dirname:
        with pytest.raises(datasets.SherlockMLDatasetsError):
            datasets.get(
                TEST_NON_EXISTENT, os.path.join(dirname, TEST_NON_EXISTENT)
            )


@pytest.mark.parametrize("destination", ["new_file"])  # noqa: F811
def test_mv(remote_file, destination):
    datasets.mv(remote_file, destination)
    content = read_remote_object(destination)
    assert "/" + destination in datasets.ls()
    assert "/" + remote_file not in datasets.ls()
    assert content == TEST_FILE_CONTENT


@pytest.mark.parametrize("remote_dir, destination", [("/input/", "/output/")])
def test_mv_directory_source(remote_dir, destination):
    with pytest.raises(datasets.SherlockMLDatasetsError):
        datasets.mv(remote_dir, destination)


@pytest.mark.parametrize("destination", ["/output/"])  # noqa: F811
def test_mv_directory_destination(remote_file, destination):
    with pytest.raises(datasets.SherlockMLDatasetsError):
        datasets.mv(remote_file, destination)


@pytest.mark.parametrize("destination", ["new_file"])  # noqa: F811
def test_cp(remote_file, destination):
    datasets.cp(remote_file, destination)
    destination_content = read_remote_object(destination)
    source_content = read_remote_object(remote_file)
    assert "/" + destination in datasets.ls()
    assert destination_content == source_content


@pytest.mark.parametrize(
    "remote_dir, destination",
    [("/input/no-such-file", "/output/new-file"), ("/input/", "/output/")],
)
def test_cp_directory_source(remote_dir, destination):
    with pytest.raises(datasets.SherlockMLDatasetsError):
        datasets.cp(remote_dir, destination)


@pytest.mark.parametrize("destination", ["/output/"])  # noqa: F811
def test_cp_directory_destination(remote_file, destination):
    with pytest.raises(datasets.SherlockMLDatasetsError):
        datasets.cp(remote_file, destination)


def test_rm(remote_file):  # noqa: F811
    assert "/" + remote_file in datasets.ls()
    datasets.rm(remote_file)
    assert "/" + remote_file not in datasets.ls()


@pytest.mark.parametrize("remote_dir", ["/input/", "/input/no-such-file"])
def test_rm_directory_source(remote_dir):
    with pytest.raises(datasets.SherlockMLDatasetsError):
        datasets.rm(remote_dir)


def test_rmdir(remote_tree):  # noqa: F811
    assert "/" + EMPTY_DIRECTORY in datasets.ls()
    datasets.rmdir(EMPTY_DIRECTORY)
    assert "/" + EMPTY_DIRECTORY not in datasets.ls()


def test_rmdir_missing():
    with pytest.raises(datasets.SherlockMLDatasetsError):
        datasets.rmdir("missing/directory")


def test_rmdir_filetype(remote_tree):  # noqa: F811
    with pytest.raises(datasets.SherlockMLDatasetsError):
        datasets.rmdir(VALID_FILES[0])


def test_rmdir_nonempty(remote_tree):  # noqa: F811
    with pytest.raises(datasets.SherlockMLDatasetsError):
        datasets.rmdir("input/")


def test_etag(remote_file):  # noqa: F811
    etag = datasets.etag(remote_file)
    assert isinstance(etag, str)
    assert len(etag) > 0


def test_etag_change(remote_file, local_file_changed):  # noqa: F811
    initial_etag = datasets.etag(remote_file)
    datasets.put(local_file_changed, remote_file)
    final_etag = datasets.etag(remote_file)
    assert final_etag != initial_etag


def test_open_read(remote_file):  # noqa: F811
    with datasets.open(remote_file, "rb") as fp:
        assert fp.read() == TEST_FILE_CONTENT


def test_open_defaultmode(remote_file):  # noqa: F811
    with datasets.open(remote_file) as fp:
        assert fp.read() == TEST_FILE_CONTENT.decode("utf-8")


def test_open_missing():
    with pytest.raises(datasets.SherlockMLDatasetsError):
        with datasets.open("missing/file", "r"):
            pass


def test_open_directory(remote_tree):  # noqa: F811
    with pytest.raises(datasets.SherlockMLDatasetsError):
        with datasets.open("/input", "r"):
            pass
