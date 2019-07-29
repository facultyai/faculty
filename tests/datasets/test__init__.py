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


import pytest

from faculty import datasets


@pytest.fixture
def mock_client(mocker):
    session = mocker.Mock()
    get_session_mock = mocker.patch(
        "faculty.datasets.get_session", return_value=session
    )

    project_id = mocker.Mock()

    object_client = mocker.Mock()
    mocker.patch("faculty.datasets.ObjectClient", return_value=object_client)

    yield object_client, project_id

    get_session_mock.assert_called_once_with()


def test_datasets_ls_all_files(mocker, mock_client):
    object_client, project_id = mock_client

    list_response = mocker.Mock()
    mock_object = mocker.Mock()
    mock_object.path = "test-path"
    list_response.objects = [mock_object]
    list_response.next_page_token = None
    object_client.list.return_value = list_response

    objects = datasets.ls("test-prefix", project_id, show_hidden=True)
    assert objects == ["test-path"]
    object_client.list.assert_called_once_with(project_id, "test-prefix")


def test_datasets_ls_non_hidden_files(mocker, mock_client):
    object_client, project_id = mock_client

    list_response = mocker.Mock()
    mock_object = mocker.Mock()
    mock_object.path = ".test-hidden-path"
    list_response.objects = [mock_object]
    list_response.next_page_token = None
    object_client.list.return_value = list_response

    objects = datasets.ls("test-prefix", project_id)
    assert objects == []
    object_client.list.assert_called_once_with(project_id, "test-prefix")


def test_datasets_ls_with_continuation(mocker, mock_client):
    object_client, project_id = mock_client

    list_response1 = mocker.Mock()
    mock_object1 = mocker.Mock()
    mock_object1.path = ".test-hidden-path"
    list_response1.objects = [mock_object1]
    list_response1.next_page_token = mocker.Mock()

    list_response2 = mocker.Mock()
    mock_object2 = mocker.Mock()
    mock_object2.path = "test-path"
    list_response2.objects = [mock_object2]
    list_response2.next_page_token = None

    object_client.list.side_effect = [list_response1, list_response2]

    objects = datasets.ls("test-prefix", project_id)
    assert objects == ["test-path"]
    object_client.list.assert_has_calls(
        [
            mocker.call(project_id, "test-prefix"),
            mocker.call(
                project_id, "test-prefix", list_response1.next_page_token
            ),
        ]
    )


def test_datasets_glob_path(mocker):
    project_id = mocker.Mock()

    content_mock = mocker.Mock()
    ls_mock = mocker.patch("faculty.datasets.ls", return_value=content_mock)
    fnmatch_filter_mock = mocker.patch(
        "fnmatch.filter", return_value=mocker.Mock()
    )

    pattern_mock = mocker.Mock()
    datasets.glob(
        pattern_mock,
        prefix="project-path",
        project_id=project_id,
        show_hidden=True,
        object_client=None,
    )

    ls_mock.assert_called_once_with(
        prefix="project-path",
        project_id=project_id,
        show_hidden=True,
        object_client=None,
    )
    fnmatch_filter_mock.assert_called_once_with(content_mock, pattern_mock)


def test_datasets_get_file(mocker, mock_client):
    object_client, project_id = mock_client

    ls_mock = mocker.patch("faculty.datasets.ls", return_value=[])

    download_result_mock = mocker.Mock()
    download_mock = mocker.patch(
        "faculty.datasets.transfer.download_file",
        return_value=download_result_mock,
    )

    datasets.get("project-path", "local-path", project_id)
    ls_mock.assert_called_once_with(
        "project-path/",
        project_id=project_id,
        show_hidden=True,
        object_client=object_client,
    )
    download_mock.assert_called_once_with(
        object_client, project_id, "project-path", "local-path"
    )


def test_datasets_get_empty_directory(mocker, mock_client):
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
    project_relative_path_mock = mocker.patch(
        "faculty.datasets.path.project_relative_path",
        return_value=relative_path,
    )

    object_client, project_id = mock_client

    ls_mock = mocker.patch(
        "faculty.datasets.ls", return_value=["project-path/"]
    )

    datasets.get("project-path", "local-path", project_id)

    os_path_dirname_mock.assert_called_once_with("local-path")
    os_path_isdir_mock.assert_called_once_with("local-path/")
    project_relative_path_mock.assert_called_once_with(
        "project-path", "project-path/"
    )
    os_path_join_mock.assert_called_once_with("local-path", relative_path)
    os_path_exists_mock.assert_called_once_with(local_dest)
    os_makedirs_mock.assert_called_once_with(local_dest)
    ls_mock.assert_has_calls(
        [
            mocker.call(
                "project-path/",
                project_id=project_id,
                show_hidden=True,
                object_client=object_client,
            ),
            mocker.call(
                "project-path",
                project_id=project_id,
                show_hidden=True,
                object_client=object_client,
            ),
        ]
    )


def test_datasets_get_directory(mocker, mock_client):
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
    project_relative_path_mock = mocker.patch(
        "faculty.datasets.path.project_relative_path",
        side_effect=relative_paths,
    )

    object_client, project_id = mock_client

    ls_mock = mocker.patch(
        "faculty.datasets.ls",
        return_value=["project-path/", "project-path/test-file"],
    )
    _get_file_mock = mocker.patch(
        "faculty.datasets._get_file", return_value=None
    )

    datasets.get("project-path", "local-path", project_id)

    os_path_dirname_mock.assert_has_calls(
        [mocker.call("local-path"), mocker.call(local_dests[1])]
    )
    os_path_isdir_mock.assert_called_once_with("local-path/")
    project_relative_path_mock.assert_has_calls(
        [
            mocker.call("project-path", "project-path/"),
            mocker.call("project-path", "project-path/test-file"),
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
                project_id=project_id,
                show_hidden=True,
                object_client=object_client,
            ),
            mocker.call(
                "project-path",
                project_id=project_id,
                show_hidden=True,
                object_client=object_client,
            ),
        ]
    )
    _get_file_mock.assert_called_once_with(
        "project-path/test-file", local_dests[1], project_id, object_client
    )


def test_datasets_put_file_and_create_parent_directories(mocker, mock_client):
    object_client, project_id = mock_client

    path_project_parent_directory = mocker.patch(
        "faculty.datasets.path.project_parent_directory", return_value="/"
    )
    object_client.create_directory.return_value = mocker.Mock()

    os_path_isdir_mock = mocker.patch("os.path.isdir", return_value=False)

    upload_result_mock = mocker.Mock()
    upload_mock = mocker.patch(
        "faculty.datasets.transfer.upload_file",
        return_value=upload_result_mock,
    )

    datasets.put("local-path", "project-path", project_id)

    path_project_parent_directory.assert_called_once_with("project-path")
    object_client.create_directory.assert_called_once_with(
        project_id, "/", parents=True
    )
    os_path_isdir_mock.assert_called_once_with("local-path")
    upload_mock.assert_called_once_with(
        object_client, project_id, "project-path", "local-path"
    )


def test_datasets_put_directory(mocker, mock_client):
    object_client, project_id = mock_client

    path_project_parent_directory = mocker.patch(
        "faculty.datasets.path.project_parent_directory", return_value="/"
    )
    object_client.create_directory.return_value = mocker.Mock()

    os_path_isdir_mock = mocker.patch(
        "os.path.isdir", side_effect=[True, False]
    )

    entry_mock = "test-file"
    os_listdir_mock = mocker.patch("os.listdir", return_value=[entry_mock])

    _put_file_mock = mocker.patch(
        "faculty.datasets._put_file", return_value=mocker.Mock()
    )

    datasets.put("local-path", "project-path", project_id)

    path_project_parent_directory.assert_called_once_with("project-path")
    object_client.create_directory.assert_has_calls(
        [
            mocker.call(project_id, "/", parents=True),
            mocker.call(project_id, "project-path"),
        ]
    )
    os_path_isdir_mock.assert_has_calls(
        [mocker.call("local-path"), mocker.call("local-path/test-file")]
    )
    os_listdir_mock.assert_called_once_with("local-path")
    _put_file_mock.assert_called_once_with(
        "local-path/test-file",
        "project-path/test-file",
        project_id,
        object_client,
    )


def test_datasets_cp_path(mocker, mock_client):
    object_client, project_id = mock_client
    object_client.copy.return_value = mocker.Mock()

    datasets.cp(
        "source-path",
        "destination-path",
        project_id=project_id,
        recursive=True,
    )

    object_client.copy.assert_called_once_with(
        project_id, "source-path", "destination-path", recursive=True
    )


def test_datasets_rm_path(mocker, mock_client):
    object_client, project_id = mock_client
    object_client.delete.return_value = mocker.Mock()

    datasets.rm("project-path", project_id=project_id, recursive=True)

    object_client.delete.assert_called_once_with(
        project_id, "project-path", recursive=True
    )


def test_datasets_rmdir_path(mocker):
    project_id = mocker.Mock()

    rm_mock = mocker.patch("faculty.datasets.rm", return_value=mocker.Mock())

    datasets.rmdir("project-path", project_id=project_id, object_client=None)

    rm_mock.assert_called_once_with(
        "project-path",
        project_id=project_id,
        recursive=True,
        object_client=None,
    )


def test_datasets_mv_path(mocker, mock_client):
    object_client, project_id = mock_client

    cp_mock = mocker.patch("faculty.datasets.cp", return_value=mocker.Mock())
    rm_mock = mocker.patch("faculty.datasets.rm", return_value=mocker.Mock())

    datasets.mv("source-path", "destination-path", project_id=project_id)

    cp_mock.assert_called_once_with(
        "source-path",
        "destination-path",
        project_id=project_id,
        recursive=True,
        object_client=object_client,
    )
    rm_mock.assert_called_once_with(
        "source-path",
        project_id=project_id,
        recursive=True,
        object_client=object_client,
    )


def test_datasets_etag(mocker, mock_client):
    object_client, project_id = mock_client

    object_mock = mocker.Mock()
    object_mock.etag = "test-etag"
    object_client.get.return_value = object_mock

    etag = datasets.etag("project-path", project_id=project_id)

    assert etag == object_mock.etag
    object_client.get.assert_called_once_with(project_id, "project-path")
