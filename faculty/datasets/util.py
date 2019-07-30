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

import posixpath


class DatasetsError(Exception):
    pass


def rationalise_path(path):

    # All paths should be relative to root
    path = posixpath.join("/", path)

    normed = posixpath.normpath(path)

    if path.endswith("/") and not normed.endswith("/"):
        normed += "/"

    return normed


def get_relative_path(parent_directory, directory):

    parent_directory = rationalise_path(parent_directory)
    directory = rationalise_path(directory)

    if not directory.startswith(parent_directory):
        tpl = "{} is not a sub path of {}"
        raise ValueError(tpl.format(directory, parent_directory))

    # Remove the root
    relative_path = posixpath.relpath(directory, parent_directory)

    return relative_path
