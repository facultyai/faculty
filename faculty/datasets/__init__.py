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

"""Query, read and write Faculty datasets."""


import fnmatch
import os
import posixpath
import contextlib
import tempfile
import io

from faculty.session import get_session
from faculty.context import get_context
from faculty.clients.object import ObjectClient
from faculty.datasets import transfer
from faculty.datasets.util import DatasetsError


# For backwards compatibility
SherlockMLDatasetsError = DatasetsError


def ls(prefix="/", project_id=None, show_hidden=False, object_client=None):
    """List contents of project datasets.

    Parameters
    ----------
    prefix : str, optional
        List only files in the datasets matching this prefix. Default behaviour
        is to list all files.
    project_id : str, optional
        The project to list files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    show_hidden : bool, optional
        Include hidden files in the output. Defaults to False.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.

    Returns
    -------
    list
        The list of files from the project datasets.
    """

    project_id = project_id or get_context().project_id
    object_client = object_client or ObjectClient(get_session())

    list_response = object_client.list(project_id, prefix)
    paths = [obj.path for obj in list_response.objects]

    while list_response.next_page_token is not None:
        list_response = object_client.list(
            project_id, prefix, list_response.next_page_token
        )
        paths += [obj.path for obj in list_response.objects]

    if show_hidden:
        return paths
    else:
        non_hidden_paths = [
            path
            for path in paths
            if not any(element.startswith(".") for element in path.split("/"))
        ]
        return non_hidden_paths


def glob(
    pattern, prefix="/", project_id=None, show_hidden=False, object_client=None
):
    """List contents of project datasets that match a glob pattern.

    Parameters
    ----------
    pattern : str
        The pattern that contents need to match.
    prefix : str, optional
        List only files in the project datasets that have this prefix. Default
        behaviour is to list all files.
    project_id : str, optional
        The project to list files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCK_PROJECT_ID in
        your environment.
    show_hidden : bool, optional
        Include hidden files in the output. Defaults to False.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.

    Returns
    -------
    list
        The list of files from the project that match the glob pattern.
    """
    contents = ls(
        prefix=prefix,
        project_id=project_id,
        show_hidden=show_hidden,
        object_client=object_client,
    )
    return fnmatch.filter(contents, pattern)


def _isdir(project_path, project_id=None, object_client=None):
    """Determine if a path in a project's datasets is a directory.

    Parameters
    ----------
    project_path : str
        The path in the project datasets to test.
    project_id : str, optional
        The project to list files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.

    Returns
    -------
    bool
    """
    # 'Directories' always end in a '/'
    if not project_path.endswith("/"):
        project_path += "/"
    matches = ls(
        project_path,
        project_id=project_id,
        show_hidden=True,
        object_client=object_client,
    )
    return len(matches) >= 1


def _isfile(project_path, project_id=None, object_client=None):
    """Determine if a path in a project's datasets is a file.

    Parameters
    ----------
    project_path : str
        The path in the project directory to test.
    project_id : str, optional
        The project to list files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.

    Returns
    -------
    bool
    """
    if _isdir(project_path, project_id):
        return False
    matches = ls(
        project_path,
        project_id=project_id,
        show_hidden=True,
        object_client=object_client,
    )
    rationalised_path = _rationalise_path(project_path)
    return any(match == rationalised_path for match in matches)


def _create_parent_directories(project_path, project_id, object_client):
    parent_path = posixpath.dirname(project_path)
    object_client.create_directory(project_id, parent_path, parents=True)


def _put_file(local_path, project_path, project_id, object_client):
    transfer.upload_file(object_client, project_id, project_path, local_path)


def _put_directory(local_path, project_path, project_id, object_client):
    object_client.create_directory(project_id, project_path)

    # Recursively put the contents of the directory
    for entry in os.listdir(local_path):
        _put_recursive(
            os.path.join(local_path, entry),
            posixpath.join(project_path, entry),
            project_id,
            object_client,
        )


def _put_recursive(local_path, project_path, project_id, object_client):
    """Puts a file/directory without checking that parent directory exists."""
    if os.path.isdir(local_path):
        _put_directory(local_path, project_path, project_id, object_client)
    else:
        _put_file(local_path, project_path, project_id, object_client)


def put(local_path, project_path, project_id=None, object_client=None):
    """Copy from the local filesystem to a project's datasets.

    Parameters
    ----------
    local_path : str or os.PathLike
        The source path in the local filesystem to copy.
    project_path : str
        The destination path in the project directory.
    project_id : str, optional
        The project to put files in. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.
    """

    project_id = project_id or get_context().project_id
    object_client = object_client or ObjectClient(get_session())

    if hasattr(os, "fspath"):
        local_path = os.fspath(local_path)

    _create_parent_directories(project_path, project_id, object_client)
    _put_recursive(local_path, project_path, project_id, object_client)


def _get_file(project_path, local_path, project_id, object_client):

    if local_path.endswith("/"):
        msg = (
            "the source path {} is a normal file but the destination "
            "path {} indicates a directory - please provide a "
            "full destination path"
        ).format(repr(project_path), repr(local_path))
        raise DatasetsError(msg)

    transfer.download_file(object_client, project_id, project_path, local_path)


def _get_directory(project_path, local_path, project_id, object_client):

    # Firstly, make sure that the location to write to locally exists
    containing_dir = os.path.dirname(local_path)
    if not containing_dir:
        containing_dir = "."
    if not os.path.isdir(containing_dir):
        msg = "No such directory: {}".format(repr(containing_dir))
        raise IOError(msg)

    paths_to_get = ls(
        project_path,
        project_id=project_id,
        show_hidden=True,
        object_client=object_client,
    )
    for object_path in paths_to_get:

        local_dest = os.path.join(
            local_path, _get_relative_path(project_path, object_path)
        )

        if object_path.endswith("/"):
            # Objects with a trailing '/' indicate directories
            if not os.path.exists(local_dest):
                os.makedirs(local_dest)
        else:
            # Make sure directory exists to put files into
            dirname = os.path.dirname(local_dest)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            _get_file(object_path, local_dest, project_id, object_client)


def get(project_path, local_path, project_id=None, object_client=None):
    """Copy from a project's datasets to the local filesystem.

    Parameters
    ----------
    project_path : str
        The source path in the project datasets to copy.
    local_path : str or os.PathLike
        The destination path in the local filesystem.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.
    """

    project_id = project_id or get_context().project_id
    object_client = object_client or ObjectClient(get_session())

    if hasattr(os, "fspath"):
        local_path = os.fspath(local_path)

    if _isdir(project_path, project_id, object_client):
        _get_directory(project_path, local_path, project_id, object_client)
    else:
        _get_file(project_path, local_path, project_id, object_client)


def mv(source_path, destination_path, project_id=None, object_client=None):
    """Move a file or directory within a project's datasets.

    Parameters
    ----------
    source_path : str
        The source path in the project datasets to move.
    destination_path : str
        The destination path in the project datasets.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.
    """

    project_id = project_id or get_context().project_id
    object_client = object_client or ObjectClient(get_session())

    object_client.move(project_id, source_path, destination_path)


def cp(
    source_path,
    destination_path,
    project_id=None,
    recursive=False,
    object_client=None,
):
    """Copy a file or directory within a project's datasets.

    Parameters
    ----------
    source_path : str
        The source path in the project datasets to copy.
    destination_path : str
        The destination path in the project datasets.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    recursive : bool, optional
        If True, allows copying directories
        like a recursive copy in a filesystem. By default the action
        is not recursive.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.
    """

    project_id = project_id or get_context().project_id
    object_client = object_client or ObjectClient(get_session())

    _create_parent_directories(destination_path, project_id, object_client)
    object_client.copy(
        project_id, source_path, destination_path, recursive=recursive
    )


def rm(project_path, project_id=None, recursive=False, object_client=None):
    """Remove a file or directory from the project directory.

    Parameters
    ----------
    project_path : str
        The path in the project datasets to remove.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    recursive : bool, optional
        If True, allows deleting directories
        like a recursive delete in a filesystem. By default the action
        is not recursive.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.
    """

    project_id = project_id or get_context().project_id
    object_client = object_client or ObjectClient(get_session())

    object_client.delete(project_id, project_path, recursive=recursive)


def rmdir(project_path, project_id=None, object_client=None):
    """Remove an empty directory from the project datasets.

    Parameters
    ----------
    remote_path : str
        The path of the directory to remove.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.
    """

    contents = ls(
        prefix=project_path,
        project_id=project_id,
        show_hidden=True,
        object_client=object_client,
    )

    rationalised_path = _rationalise_path(project_path)
    project_path_as_file = rationalised_path.rstrip("/")
    project_path_as_dir = project_path_as_file + "/"

    if contents == [project_path_as_dir]:
        rm(
            project_path_as_dir,
            project_id=project_id,
            object_client=object_client,
            recursive=True,
        )
    elif contents == [project_path_as_file]:
        raise DatasetsError("'{}' Not a directory".format(project_path))
    elif project_path_as_dir not in contents:
        raise DatasetsError(
            "'{}' No such file or directory".format(project_path)
        )
    else:
        raise DatasetsError("'{}' Directory is not empty".format(project_path))


def etag(project_path, project_id=None, object_client=None):
    """Get a unique identifier for the current version of a file.

    Parameters
    ----------
    project_path : str
        The path in the project datasets.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    object_client : faculty.clients.object.ObjectClient, optional
        Advanced - can be used to benefit from caching in chain interactions
        with datasets.

    Returns
    -------
    str
    """

    project_id = project_id or get_context().project_id
    object_client = object_client or ObjectClient(get_session())

    object = object_client.get(project_id, project_path)

    return object.etag


@contextlib.contextmanager
def open(project_path, mode="r", temp_dir=None, project_id=None, **kwargs):
    """Open a file from a project's datasets for reading.

    This downloads the file into a temporary directory before opening it, so if
    your files are very large, this function can take a long time.

    Parameters
    ----------
    project_path : str
        The path of the file in the project's datasets to open.
    mode : str
        The opening mode, either 'r' or 'rb'. This is passed down to the
        standard python open function. Writing is currently not supported.
    temp_dir : str
        A directory on the local filesystem where you would like the file to be
        saved into temporarily. Note that on SherlockML servers, the default
        temporary directory can break with large files, so if your file is
        upwards of 2GB, it is recommended to specify temp_dir='/project'.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by FACULTY_PROJECT_ID in
        your environment.
    """

    if _isdir(project_path, project_id=project_id):
        raise DatasetsError("Can't open directories.")

    if any(char in mode for char in ("w", "a", "x")):
        raise NotImplementedError("Currently, only reading is implemented.")

    tmpdir = tempfile.mkdtemp(prefix=".", dir=temp_dir)
    local_path = os.path.join(tmpdir, os.path.basename(project_path))

    try:
        get(project_path, local_path, project_id=project_id)
        with io.open(local_path, mode, **kwargs) as file_object:
            yield file_object
    finally:
        if os.path.isfile(local_path):
            os.remove(local_path)
        if os.path.isdir(tmpdir):
            os.rmdir(tmpdir)


def _rationalise_path(path):

    # All paths should be relative to root
    path = posixpath.join("/", path)

    normed = posixpath.normpath(path)

    if path.endswith("/") and not normed.endswith("/"):
        normed += "/"

    return normed


def _get_relative_path(parent_directory, directory):

    parent_directory = _rationalise_path(parent_directory)
    directory = _rationalise_path(directory)

    if not directory.startswith(parent_directory):
        tpl = "{} is not a sub path of {}"
        raise ValueError(tpl.format(directory, parent_directory))

    # Remove the root
    relative_path = posixpath.relpath(directory, parent_directory)

    return relative_path
