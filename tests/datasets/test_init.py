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


import pytest
import uuid

from faculty import datasets
from faculty.datasets.util import DatasetsError


PROJECT_ID = uuid.uuid4()


@pytest.fixture
def mock_client(mocker):
    session = mocker.Mock()
    get_session_mock = mocker.patch(
        "faculty.datasets.get_session", return_value=session
    )

    object_client = mocker.Mock()
    mocker.patch("faculty.datasets.ObjectClient", return_value=object_client)

    yield object_client

    get_session_mock.assert_called_once_with()


def test_ls_all_files(mocker, mock_client):
    list_response = mocker.Mock()
    list_response.objects = [
        mocker.Mock(path=".test-hidden"),
        mocker.Mock(path="not-hidden"),
    ]
    list_response.next_page_token = None
    mock_client.list.return_value = list_response

    objects = datasets.ls("test-prefix", PROJECT_ID, show_hidden=True)
    assert objects == [".test-hidden", "not-hidden"]
    mock_client.list.assert_called_once_with(PROJECT_ID, "test-prefix")


def test_ls_files_hide_hidden_files(mocker, mock_client):
    list_response = mocker.Mock()
    list_response.objects = [
        mocker.Mock(path=".test-hidden"),
        mocker.Mock(path="not-hidden"),
    ]
    list_response.next_page_token = None
    mock_client.list.return_value = list_response

    objects = datasets.ls("test-prefix", PROJECT_ID)
    assert objects == ["not-hidden"]
    mock_client.list.assert_called_once_with(PROJECT_ID, "test-prefix")


def test_ls_with_continuation(mocker, mock_client):
    list_response1 = mocker.Mock()
    mock_object1 = mocker.Mock()
    mock_object1.path = ".test-hidden-path"
    list_response1.objects = [mock_object1]

    list_response2 = mocker.Mock()
    mock_object2 = mocker.Mock()
    mock_object2.path = "test-path"
    list_response2.objects = [mock_object2]
    list_response2.next_page_token = None

    mock_client.list.side_effect = [list_response1, list_response2]

    objects = datasets.ls("test-prefix", PROJECT_ID)
    assert objects == ["test-path"]
    mock_client.list.assert_has_calls(
        [
            mocker.call(PROJECT_ID, "test-prefix"),
            mocker.call(
                PROJECT_ID, "test-prefix", list_response1.next_page_token
            ),
        ]
    )


def test_glob(mocker):
    content = [
        "/project-path/",
        "/project-path/this-path",
        "/project-path/other-path",
    ]
    ls_mock = mocker.patch("faculty.datasets.ls", return_value=content)

    result = datasets.glob(
        "*this*",
        prefix="project-path",
        project_id=PROJECT_ID,
        show_hidden=True,
        object_client=None,
    )

    assert result == ["/project-path/this-path"]

    ls_mock.assert_called_once_with(
        prefix="project-path",
        project_id=PROJECT_ID,
        show_hidden=True,
        object_client=None,
    )


def test_get_file(mocker, mock_client):
    ls_mock = mocker.patch("faculty.datasets.ls", return_value=[])

    download_mock = mocker.patch("faculty.datasets.transfer.download_file")

    datasets.get("project-path", "local-path", PROJECT_ID)
    ls_mock.assert_called_once_with(
        "project-path/",
        project_id=PROJECT_ID,
        show_hidden=True,
        object_client=mock_client,
    )
    download_mock.assert_called_once_with(
        mock_client, PROJECT_ID, "project-path", "local-path"
    )


def test_get_empty_directory(mocker, mock_client):
    dirname = "local-path/"
    os_path_dirname_mock = mocker.patch(
        "os.path.dirname", return_value=dirname
    )
    os_path_isdir_mock = mocker.patch("os.path.isdir", return_value=True)
    local_dest = "local-path/test-path"
    os_path_join_mock = mocker.patch("os.path.join", return_value=local_dest)
    os_path_exists_mock = mocker.patch("os.path.exists", return_value=False)
    os_makedirs_mock = mocker.patch("os.makedirs", return_value=None)

    relative_path = mocker.Mock()
    get_relative_path_mock = mocker.patch(
        "faculty.datasets._get_relative_path", return_value=relative_path
    )

    ls_mock = mocker.patch(
        "faculty.datasets.ls", return_value=["/project-path/"]
    )

    datasets.get("project-path", "local-path", PROJECT_ID)

    os_path_dirname_mock.assert_called_once_with("local-path")
    os_path_isdir_mock.assert_called_once_with("local-path/")
    get_relative_path_mock.assert_called_once_with(
        "project-path", "/project-path/"
    )
    os_path_join_mock.assert_called_once_with("local-path", relative_path)
    os_path_exists_mock.assert_called_once_with(local_dest)
    os_makedirs_mock.assert_called_once_with(local_dest)
    ls_mock.assert_has_calls(
        [
            mocker.call(
                "project-path/",
                project_id=PROJECT_ID,
                show_hidden=True,
                object_client=mock_client,
            ),
            mocker.call(
                "project-path",
                project_id=PROJECT_ID,
                show_hidden=True,
                object_client=mock_client,
            ),
        ]
    )


def test_get_directory(mocker, mock_client):
    dirname = "local-path/"
    os_path_dirname_mock = mocker.patch(
        "os.path.dirname", return_value=dirname
    )
    os_path_isdir_mock = mocker.patch("os.path.isdir", return_value=True)
    local_dests = ["local-path/project-path", "local-path/project-path/file"]
    os_path_join_mock = mocker.patch("os.path.join", side_effect=local_dests)
    os_path_exists_mock = mocker.patch("os.path.exists", return_value=False)
    os_makedirs_mock = mocker.patch("os.makedirs", return_value=None)

    relative_path1 = mocker.Mock()
    relative_path2 = mocker.Mock()
    relative_paths = [relative_path1, relative_path2]
    get_relative_path_mock = mocker.patch(
        "faculty.datasets._get_relative_path", side_effect=relative_paths
    )

    ls_mock = mocker.patch(
        "faculty.datasets.ls",
        return_value=["/project-path/", "/project-path/test-file"],
    )
    _get_file_mock = mocker.patch(
        "faculty.datasets._get_file", return_value=None
    )

    datasets.get("project-path", "local-path", PROJECT_ID)

    os_path_dirname_mock.assert_has_calls(
        [mocker.call("local-path"), mocker.call(local_dests[1])]
    )
    os_path_isdir_mock.assert_called_once_with("local-path/")
    get_relative_path_mock.assert_has_calls(
        [
            mocker.call("project-path", "/project-path/"),
            mocker.call("project-path", "/project-path/test-file"),
        ]
    )
    os_path_join_mock.assert_has_calls(
        [
            mocker.call("local-path", relative_path1),
            mocker.call("local-path", relative_path2),
        ]
    )
    os_path_exists_mock.assert_has_calls(
        [mocker.call(local_dests[0]), mocker.call(dirname)]
    )
    os_makedirs_mock.assert_has_calls(
        [mocker.call(local_dests[0]), mocker.call(dirname)]
    )
    ls_mock.assert_has_calls(
        [
            mocker.call(
                "project-path/",
                project_id=PROJECT_ID,
                show_hidden=True,
                object_client=mock_client,
            ),
            mocker.call(
                "project-path",
                project_id=PROJECT_ID,
                show_hidden=True,
                object_client=mock_client,
            ),
        ]
    )
    _get_file_mock.assert_called_once_with(
        "/project-path/test-file", local_dests[1], PROJECT_ID, mock_client
    )


def test_put_file(mocker, mock_client):
    posixpath_dirname_mock = mocker.patch(
        "posixpath.dirname", return_value="/"
    )

    os_path_isdir_mock = mocker.patch("os.path.isdir", return_value=False)

    upload_mock = mocker.patch("faculty.datasets.transfer.upload_file")

    datasets.put("local-path", "project-path", PROJECT_ID)

    posixpath_dirname_mock.assert_called_once_with("project-path")
    mock_client.create_directory.assert_called_once_with(
        PROJECT_ID, "/", parents=True
    )
    os_path_isdir_mock.assert_called_once_with("local-path")
    upload_mock.assert_called_once_with(
        mock_client, PROJECT_ID, "project-path", "local-path"
    )


def test_put_directory(mocker, mock_client):
    posixpath_dirname_mock = mocker.patch(
        "posixpath.dirname", return_value="/"
    )

    os_path_isdir_mock = mocker.patch(
        "os.path.isdir", side_effect=[True, False]
    )

    entry_mock = "test-file"
    os_listdir_mock = mocker.patch("os.listdir", return_value=[entry_mock])

    _put_file_mock = mocker.patch("faculty.datasets._put_file")

    datasets.put("local-path", "project-path", PROJECT_ID)

    posixpath_dirname_mock.assert_called_once_with("project-path")
    mock_client.create_directory.assert_has_calls(
        [
            mocker.call(PROJECT_ID, "/", parents=True),
            mocker.call(PROJECT_ID, "project-path"),
        ]
    )
    os_path_isdir_mock.assert_has_calls(
        [mocker.call("local-path"), mocker.call("local-path/test-file")]
    )
    os_listdir_mock.assert_called_once_with("local-path")
    _put_file_mock.assert_called_once_with(
        "local-path/test-file",
        "project-path/test-file",
        PROJECT_ID,
        mock_client,
    )


def test_cp(mocker, mock_client):
    posixpath_dirname_mock = mocker.patch(
        "posixpath.dirname", return_value="/"
    )

    datasets.cp(
        "source-path",
        "destination-path",
        project_id=PROJECT_ID,
        recursive=True,
    )

    posixpath_dirname_mock.assert_called_once_with("destination-path")
    mock_client.create_directory.assert_called_once_with(
        PROJECT_ID, "/", parents=True
    )
    mock_client.copy.assert_called_once_with(
        PROJECT_ID, "source-path", "destination-path", recursive=True
    )


def test_rm(mocker, mock_client):
    datasets.rm("project-path", project_id=PROJECT_ID, recursive=True)

    mock_client.delete.assert_called_once_with(
        PROJECT_ID, "project-path", recursive=True
    )


@pytest.mark.parametrize("prefix,suffix", [("", ""), ("/", ""), ("/", "/")])
def test_rmdir(mocker, prefix, suffix):
    project_path = prefix + "project-path" + suffix
    ls_mock = mocker.patch(
        "faculty.datasets.ls", return_value=["/project-path/"]
    )
    rm_mock = mocker.patch("faculty.datasets.rm")

    datasets.rmdir(project_path, project_id=PROJECT_ID)

    ls_mock.assert_called_once_with(
        prefix=project_path,
        project_id=PROJECT_ID,
        show_hidden=True,
        object_client=None,
    )
    rm_mock.assert_called_once_with(
        "/project-path/",
        project_id=PROJECT_ID,
        object_client=None,
        recursive=True,
    )


@pytest.mark.parametrize("prefix", ["", "/"])
def test_rmdir_not_a_directory(mocker, prefix):
    ls_mock = mocker.patch(
        "faculty.datasets.ls", return_value=["/project-path"]
    )

    with pytest.raises(DatasetsError, match="Not a directory"):
        datasets.rmdir(prefix + "project-path", project_id=PROJECT_ID)

    ls_mock.assert_called_once_with(
        prefix=prefix + "project-path",
        project_id=PROJECT_ID,
        show_hidden=True,
        object_client=None,
    )


@pytest.mark.parametrize("prefix", ["", "/"])
@pytest.mark.parametrize(
    "ls_result",
    [[], ["/project-path/some-file"]],
    ids=["missing", "implicit directory"],
)
def test_rmdir_no_such_file_or_directory(mocker, prefix, ls_result):
    ls_mock = mocker.patch("faculty.datasets.ls", return_value=ls_result)

    with pytest.raises(DatasetsError, match="No such file or directory"):
        datasets.rmdir(prefix + "project-path", project_id=PROJECT_ID)

    ls_mock.assert_called_once_with(
        prefix=prefix + "project-path",
        project_id=PROJECT_ID,
        show_hidden=True,
        object_client=None,
    )


@pytest.mark.parametrize("prefix", ["", "/"])
def test_rmdir_nonempty_directory(mocker, prefix):
    ls_mock = mocker.patch(
        "faculty.datasets.ls",
        return_value=["/project-path/", "/project-path/some-file"],
    )

    with pytest.raises(DatasetsError, match="Directory is not empty"):
        datasets.rmdir(prefix + "project-path", project_id=PROJECT_ID)

    ls_mock.assert_called_once_with(
        prefix=prefix + "project-path",
        project_id=PROJECT_ID,
        show_hidden=True,
        object_client=None,
    )


def test_mv(mocker, mock_client):
    datasets.mv("source-path", "destination-path", project_id=PROJECT_ID)
    mock_client.move.assert_called_once_with(
        PROJECT_ID, "source-path", "destination-path"
    )


def test_etag(mocker, mock_client):
    object_mock = mocker.Mock()
    object_mock.etag = "test-etag"
    mock_client.get.return_value = object_mock

    etag = datasets.etag("project-path", project_id=PROJECT_ID)

    assert etag == object_mock.etag
    mock_client.get.assert_called_once_with(PROJECT_ID, "project-path")


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
def test_rationalise_path(input_path, rationalised_path):
    assert datasets._rationalise_path(input_path) == rationalised_path
