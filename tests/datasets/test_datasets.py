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


# import fnmatch
# import os
# import posixpath
# import pytest
# import mock

from faculty import datasets

# from tests.datasets.fixtures import (
#     read_remote_object,
#     temporary_directory,
#     TEST_DIRECTORY,
#     TEST_FILE_NAME,
#     TEST_TREE,
#     TEST_TREE_NO_HIDDEN_FILES,
#     VALID_ROOT_PATHS,
#     VALID_DIRECTORIES,
#     VALID_FILES,
#     INVALID_PATHS,
#     TEST_FILE_CONTENT,
#     TEST_NON_EXISTENT,
#     EMPTY_DIRECTORY,
# )


# pytestmark = pytest.mark.usefixtures("project_directory")


def test_datasets_ls_all_files(mocker):
    session = mocker.Mock()
    get_session_mock = mocker.patch(
        "faculty.datasets.get_session", return_value=session
    )

    project_id = mocker.Mock()

    client = mocker.Mock()
    mocker.patch("faculty.datasets.ObjectClient", return_value=client)

    list_response = mocker.Mock()
    mock_object = mocker.Mock()
    mock_object.path = "test-path"
    list_response.objects = [mock_object]
    list_response.next_page_token = None
    client.list.return_value = list_response

    objects = datasets.ls("test-prefix", project_id, show_hidden=True)
    assert objects == ["test-path"]
    get_session_mock.assert_called_once()
    client.list.assert_called_once_with(project_id, "test-prefix")


def test_datasets_ls_non_hidden_files(mocker):
    session = mocker.Mock()
    get_session_mock = mocker.patch(
        "faculty.datasets.get_session", return_value=session
    )

    project_id = mocker.Mock()

    client = mocker.Mock()
    mocker.patch("faculty.datasets.ObjectClient", return_value=client)

    list_response = mocker.Mock()
    mock_object = mocker.Mock()
    mock_object.path = ".test-hidden-path"
    list_response.objects = [mock_object]
    list_response.next_page_token = None
    client.list.return_value = list_response

    objects = datasets.ls("test-prefix", project_id)
    assert objects == []
    get_session_mock.assert_called_once()
    client.list.assert_called_once_with(project_id, "test-prefix")


def test_datasets_ls_with_continuation(mocker):
    session = mocker.Mock()
    get_session_mock = mocker.patch(
        "faculty.datasets.get_session", return_value=session
    )

    project_id = mocker.Mock()

    client = mocker.Mock()
    mocker.patch("faculty.datasets.ObjectClient", return_value=client)

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

    client.list.side_effect = [list_response1, list_response2]

    objects = datasets.ls("test-prefix", project_id)
    assert objects == ["test-path"]
    get_session_mock.assert_called_once()
    client.list.assert_has_calls(
        [
            mocker.call(project_id, "test-prefix"),
            mocker.call(
                project_id, "test-prefix", list_response1.next_page_token
            ),
        ]
    )


def test_datasets_get_file(mocker):
    session = mocker.Mock()
    get_session_mock = mocker.patch(
        "faculty.datasets.get_session", return_value=session
    )

    project_id = mocker.Mock()

    client = mocker.Mock()
    mocker.patch("faculty.datasets.ObjectClient", return_value=client)

    list_response = mocker.Mock()
    list_response.objects = []
    list_response.next_page_token = None
    client.list.return_value = list_response

    download_result = mocker.Mock()
    download_mock = mocker.patch(
        "faculty.datasets.transfer.download", return_value=download_result
    )

    datasets.get("project-path", "local-path", project_id)
    get_session_mock.assert_called_once()
    client.list.assert_called_once_with(project_id, "project-path/")
    download_mock.assert_called_once_with(
        client, project_id, "project-path", "local-path"
    )


def test_datasets_get_empty_directory(mocker):
    pass
    # dirname = "local-path/"
    # os_path_dirname_mock = mocker.patch(
    #     "faculty.datasets.os.path.dirname", return_value=dirname
    # )
    # isdir = True
    # os_path_isdir_mock = mocker.patch(
    #     "faculty.datasets.os.path.isdir", return_value=isdir
    # )
    # local_dest = "local-path/test-path"
    # os_path_join_mock = mocker.patch(
    #     "faculty.datasets.os.path.join", return_value=isdir
    # )

    # relative_path = mocker.Mock()
    # get_session_mock = mocker.patch(
    #     "faculty.datasets.path.project_relative_path",
    #     return_value=relative_path,
    # )

    # session = mocker.Mock()
    # get_session_mock = mocker.patch(
    #     "faculty.datasets.get_session", return_value=session
    # )

    # project_id = mocker.Mock()

    # client = mocker.Mock()
    # mocker.patch("faculty.datasets.ObjectClient", return_value=client)

    # list_response1 = mocker.Mock()
    # mock_object1 = mocker.Mock()
    # mock_object1.path = "test-dir/"
    # list_response1.objects = [mock_object1]
    # list_response1.next_page_token = mocker.Mock()

    # list_response2 = mocker.Mock()
    # mock_object2 = mocker.Mock()
    # mock_object2.path = "test-path"
    # list_response2.objects = [mock_object2]
    # list_response2.next_page_token = None

    # client.list.side_effect = [list_response1, list_response2]

    # download_result = mocker.Mock()
    # download_mock = mocker.patch(
    #     "faculty.datasets.transfer.download", return_value=download_result
    # )

    # datasets.get("project-path", "local-path", project_id)
    # # os_path_dirname_mock.assert_called_once_with("local-path/")
    # # os_path_isdir_mock.assert_called_once_with("local-path/")
    # get_session_mock.assert_called_once()
    # client.list.assert_called_once_with(project_id, "project-path/")
    # download_mock.assert_called_once_with(
    #     client, project_id, "project-path", "local-path/")


def test_datasets_get_directory(mocker):
    pass


def test_datasets_get_local_path_nonexistent(mocker):
    pass


def test_datasets_get_mismatch_with_destination(mocker):
    pass
