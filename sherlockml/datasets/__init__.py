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
import contextlib
import tempfile
import io

from botocore.client import ClientError

from sherlockml.datasets import path, session
from sherlockml.datasets.session import SherlockMLDatasetsError


# For backwards compatability
SherlockMLFileSystemError = SherlockMLDatasetsError


def _s3_client(project_id=None):
    """Generate a boto S3 client to access this project's datasets."""

    # At present, calls to the interface of this module run this function once
    # to generate an S3 client, then pass it around to avoid making many calls
    # to the secret service. This could result in an invalid client in the
    # middle of an operation, if we get unlucky and the AWS credentials expire
    # at that moment.

    # In the future, we may wish to make a wrapper class that manages the S3
    # client and proxies calls to its methods. This wrapper would catch
    # failures resulting from invalid AWS credentials and build a new client
    # and retry the operation accordingly.

    return session.get().s3_client(project_id)


def _bucket(project_id=None):
    return session.get().bucket(project_id)


def ls(prefix="/", project_id=None, show_hidden=False, s3_client=None):
    """List contents of project datasets.

    Parameters
    ----------
    prefix : str, optional
        List only files in the datasets matching this prefix. Default behaviour
        is to list all files.
    project_id : str, optional
        The project to list files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.
    show_hidden : bool, optional
        Include hidden files in the output. Defaults to False.
    s3_client : botocore.client.S3, optional
        Advanced - a specific boto client for AWS S3 to use.

    Returns
    -------
    list
        The list of files from the project datasets.
    """

    if s3_client is None:
        s3_client = _s3_client(project_id)

    bucket = _bucket(project_id)

    # Use a paginator to enable listing projects with more than 1000 files
    paginator = s3_client.get_paginator("list_objects_v2")
    response_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=path.projectpath_to_bucketpath(prefix, project_id),
    )

    paths = []
    for part in response_iterator:

        try:
            objects = part["Contents"]
        except KeyError:
            continue

        for obj in objects:
            project_path = path.bucketpath_to_projectpath(obj["Key"])

            # Ignore the root of the project directory
            if project_path != "/":
                paths.append(project_path)

    if show_hidden:
        return paths
    else:
        non_hidden_paths = [
            path_
            for path_ in paths
            if not any(element.startswith(".") for element in path_.split("/"))
        ]
        return non_hidden_paths


def glob(
    pattern, prefix="/", project_id=None, show_hidden=False, s3_client=None
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
    s3_client : botocore.client.S3, optional
        Advanced - a specific boto client for AWS S3 to use.

    Returns
    -------
    list
        The list of files from the project that match the glob pattern.
    """

    contents = ls(
        prefix=prefix,
        project_id=project_id,
        show_hidden=show_hidden,
        s3_client=s3_client,
    )

    return fnmatch.filter(contents, pattern)


def _isdir(project_path, project_id=None, s3_client=None):
    """Determine if a path in a project's datasets is a directory.

    Parameters
    ----------
    project_path : str
        The path in the project datasets to test.
    project_id : str, optional
        The project to list files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.
    s3_client : botocore.client.S3, optional
        Advanced - a specific boto client for AWS S3 to use.

    Returns
    -------
    bool
    """
    # 'Directories' in the S3 bucket always end in a '/'
    if not project_path.endswith("/"):
        project_path += "/"
    matches = ls(
        project_path,
        project_id=project_id,
        show_hidden=True,
        s3_client=s3_client,
    )
    return len(matches) >= 1


def _isfile(project_path, project_id=None, s3_client=None):
    """Determine if a path in a project's datasets is a file.

    Parameters
    ----------
    project_path : str
        The path in the project directory to test.
    project_id : str, optional
        The project to list files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.
    s3_client : botocore.client.S3, optional
        Advanced - a specific boto client for AWS S3 to use.

    Returns
    -------
    bool
    """
    if _isdir(project_path, project_id, s3_client):
        return False
    matches = ls(
        project_path,
        project_id=project_id,
        show_hidden=True,
        s3_client=s3_client,
    )
    rationalised_path = path.rationalise_projectpath(project_path)
    return any(match == rationalised_path for match in matches)


def _create_parent_directories(project_path, project_id, s3_client):

    bucket = _bucket(project_id)

    # Make sure empty objects exist for directories
    # List once for speed
    all_objects = set(
        ls("/", project_id=project_id, show_hidden=True, s3_client=s3_client)
    )

    for dirname in path.project_parent_directories(project_path):

        if dirname == "/":
            # Root is not returned by ls
            continue

        # We're doing this manually instead of using _isdir as _isdir will
        # return true if '/somedir/myfile' exists, even if '/somedir/' does not
        if dirname not in all_objects:
            bucket_path = path.projectpath_to_bucketpath(dirname, project_id)
            # Directories on S3 are empty objects with trailing '/' on the key
            s3_client.put_object(
                Bucket=bucket, Key=bucket_path, ServerSideEncryption="AES256"
            )


def _put_file(local_path, project_path, project_id, s3_client):

    bucket = _bucket(project_id)
    bucket_path = path.projectpath_to_bucketpath(project_path, project_id)

    if bucket_path.endswith("/"):
        msg = (
            "the destination path {} indicates a directory but the "
            "source path {} is a normal file - please provide a full "
            "destination path"
        ).format(repr(project_path), repr(local_path))
        raise SherlockMLDatasetsError(msg)

    s3_client.upload_file(
        local_path,
        bucket,
        bucket_path,
        ExtraArgs={"ServerSideEncryption": "AES256"},
    )


def _put_directory(local_path, project_path, project_id, s3_client):

    bucket = _bucket(project_id)
    bucket_path = path.projectpath_to_bucketpath(project_path, project_id)

    # Directories on S3 are empty objects with trailing '/' on the key
    if not bucket_path.endswith("/"):
        bucket_path += "/"
    s3_client.put_object(
        Bucket=bucket, Key=bucket_path, ServerSideEncryption="AES256"
    )

    # Recursively put the contents of the directory
    for entry in os.listdir(local_path):
        _put_recursive(
            os.path.join(local_path, entry),
            posixpath.join(project_path, entry),
            project_id,
            s3_client,
        )


def _put_recursive(local_path, project_path, project_id, s3_client):
    """Puts a file/directory without checking that parent directory exists."""
    if os.path.isdir(local_path):
        _put_directory(local_path, project_path, project_id, s3_client)
    else:
        _put_file(local_path, project_path, project_id, s3_client)


def put(local_path, project_path, project_id=None):
    """Copy from the local filesystem to a project's datasets.

    Parameters
    ----------
    local_path : str or os.PathLike
        The source path in the local filesystem to copy.
    project_path : str
        The destination path in the project directory.
    project_id : str, optional
        The project to put files in. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.
    """

    if hasattr(os, "fspath"):
        local_path = os.fspath(local_path)

    s3_client = _s3_client(project_id)

    _create_parent_directories(project_path, project_id, s3_client)
    _put_recursive(local_path, project_path, project_id, s3_client)


def _get_file(project_path, local_path, project_id, s3_client):

    if local_path.endswith("/"):
        msg = (
            "the destination path {} indicates a directory but the "
            "source path {} is a normal file - please provide a full "
            "destination path"
        ).format(repr(project_path), repr(local_path))
        raise SherlockMLDatasetsError(msg)

    bucket = _bucket(project_id)
    bucket_path = path.projectpath_to_bucketpath(project_path, project_id)

    try:
        s3_client.download_file(bucket, bucket_path, local_path)
    except ClientError as err:
        if "404" in err.args[0]:
            msg = "no file {} in project".format(repr(project_path))
            raise SherlockMLDatasetsError(msg)
        else:
            raise


def _get_directory(project_path, local_path, project_id, s3_client):

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
        s3_client=s3_client,
    )
    for object_path in paths_to_get:

        local_dest = os.path.join(
            local_path, path.project_relative_path(project_path, object_path)
        )

        if object_path.endswith("/"):
            # Objects with a trailing '/' on S3 indicate directories
            if not os.path.exists(local_dest):
                os.makedirs(local_dest)
        else:
            # Make sure directory exists to put files into
            dirname = os.path.dirname(local_dest)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            _get_file(object_path, local_dest, project_id, s3_client)


def get(project_path, local_path, project_id=None):
    """Copy from a project's datasets to the local filesystem.

    Parameters
    ----------
    project_path : str
        The source path in the project datasets to copy.
    local_path : str or os.PathLike
        The destination path in the local filesystem.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.
    """

    if hasattr(os, "fspath"):
        local_path = os.fspath(local_path)

    client = _s3_client(project_id)

    if _isdir(project_path, project_id, client):
        _get_directory(project_path, local_path, project_id, client)
    else:
        _get_file(project_path, local_path, project_id, client)


def mv(source_path, destination_path, project_id=None):
    """Move a file within a project's datasets.

    Parameters
    ----------
    source_path : str
        The source path in the project datasets to move.
    destination_path : str
        The destination path in the project datasets.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.
    """

    s3_client = _s3_client(project_id)

    cp(source_path, destination_path, project_id, s3_client)
    rm(source_path, project_id, s3_client)


def cp(source_path, destination_path, project_id=None, s3_client=None):
    """Copy a file within a project's datasets.

    Parameters
    ----------
    source_path : str
        The source path in the project datasets to copy.
    destination_path : str
        The destination path in the project datasets.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.
    s3_client : botocore.client.S3, optional
        Advanced - a specific boto client for AWS S3 to use.
    """

    if s3_client is None:
        s3_client = _s3_client(project_id)

    if not _isfile(source_path, project_id, s3_client):
        raise SherlockMLDatasetsError("source_path must be a file")

    if destination_path.endswith("/"):
        raise SherlockMLDatasetsError("destination_path must be a file path")

    bucket = _bucket(project_id)
    source_bucket_path = path.projectpath_to_bucketpath(
        source_path, project_id
    )
    destination_bucket_path = path.projectpath_to_bucketpath(
        destination_path, project_id
    )

    copy_source = {"Bucket": bucket, "Key": source_bucket_path}
    s3_client.copy(
        copy_source,
        bucket,
        destination_bucket_path,
        ExtraArgs={"ServerSideEncryption": "AES256"},
    )


def rm(project_path, project_id=None, s3_client=None):
    """Remove a file from the project directory.

    Parameters
    ----------
    project_path : str
        The path in the project datasets to remove.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.
    s3_client : botocore.client.S3, optional
        Advanced - a specific boto client for AWS S3 to use.
    """

    if s3_client is None:
        s3_client = _s3_client(project_id)

    if not _isfile(project_path, project_id, s3_client):
        raise SherlockMLDatasetsError("not a file")

    bucket = _bucket(project_id)
    bucket_path = path.projectpath_to_bucketpath(project_path, project_id)

    s3_client.delete_object(Bucket=bucket, Key=bucket_path)


def rmdir(project_path, project_id=None):
    """Remove a directory from the project datasets.

    Parameters
    ----------
    remote_path : str
        The path of the directory to remove.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.
    """

    s3_client = _s3_client(project_id)

    if not _isdir(project_path, project_id, s3_client):
        raise SherlockMLDatasetsError("not a directory")

    contents = ls(
        project_path, project_id, show_hidden=True, s3_client=s3_client
    )
    if not len(contents) == 1:
        raise SherlockMLDatasetsError("directory is not empty")

    # Directory paths must end with '/'
    if not project_path.endswith("/"):
        project_path += "/"

    bucket = _bucket(project_id)
    bucket_path = path.projectpath_to_bucketpath(project_path, project_id)

    s3_client.delete_object(Bucket=bucket, Key=bucket_path)


def etag(project_path, project_id=None):
    """Get a unique identifier for the current version of a file.

    Parameters
    ----------
    project_path : str
        The path in the project datasets.
    project_id : str, optional
        The project to get files from. You need to have access to this project
        for it to work. Defaults to the project set by SHERLOCKML_PROJECT_ID in
        your environment.

    Returns
    -------
    str
    """

    client = _s3_client(project_id)

    bucket = _bucket(project_id)
    bucket_path = path.projectpath_to_bucketpath(project_path, project_id)

    s3_object = client.get_object(Bucket=bucket, Key=bucket_path)

    return s3_object["ETag"].strip('"')


@contextlib.contextmanager
def open(project_path, mode="r", temp_dir=None, **kwargs):
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
    """

    if _isdir(project_path):
        raise SherlockMLDatasetsError("Can't open directories.")

    if any(char in mode for char in ("w", "a", "x")):
        raise NotImplementedError("Currently, only reading is implemented.")

    tmpdir = tempfile.mkdtemp(prefix=".", dir=temp_dir)
    local_path = os.path.join(tmpdir, os.path.basename(project_path))

    try:
        local_path = os.path.join(tmpdir, os.path.basename(project_path))
        get(project_path, local_path)
        with io.open(local_path, mode, **kwargs) as file_object:
            yield file_object
    finally:
        if os.path.isfile(local_path):
            os.remove(local_path)
        if os.path.isdir(tmpdir):
            os.rmdir(tmpdir)
