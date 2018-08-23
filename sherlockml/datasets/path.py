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

import posixpath

from sherlockml.datasets import session


def rationalise_projectpath(path):

    # All paths should be relative to root
    path = posixpath.join("/", path)

    normed = posixpath.normpath(path)

    if path.endswith("/") and not normed.endswith("/"):
        normed += "/"

    return normed


def projectpath_to_bucketpath(project_path, project_id=None):

    if project_id is None:
        project_id = session.project_id_from_environment()

    # Project path will already start with '/' so just prepend the project ID
    bucket_path = project_id + rationalise_projectpath(project_path)

    return bucket_path


def bucketpath_to_projectpath(path):
    """Drop the project ID from the front of the path."""
    parts = path.split("/")
    return posixpath.join("/", *parts[1:])


def project_relative_path(project_root, project_path):

    project_root = rationalise_projectpath(project_root)
    project_path = rationalise_projectpath(project_path)

    if not project_path.startswith(project_root):
        tpl = "{} is not a sub path of {}"
        raise ValueError(tpl.format(project_path, project_root))

    # Remove the root
    relative_path = project_path[len(project_root) :]

    # Get rid of any leading '/'es
    relative_path = relative_path.lstrip("/")

    return relative_path


def project_parent_directories(project_path):
    """List all the directories in the tree containing this file.

    Parameters
    ----------
    project_path : str
        The file to list the parent directories of

    Returns
    -------
    list of str
        The paths of the parent directories
    """

    # Ensure in assumed format - can now assume to be absolute
    project_path = rationalise_projectpath(project_path)

    # Stripping trailing slashes as if it's a directory we still just want to
    # get its parent
    dirname = posixpath.dirname(project_path.rstrip("/"))

    directories = []

    parts = dirname.split("/")
    for i_last in range(1, len(parts) + 1):
        directories.append("/".join(parts[:i_last]) + "/")

    return directories
