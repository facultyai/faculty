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

import pandas

from faculty.session import get_session
from faculty.context import get_context
from faculty.clients.object import ObjectClient
from faculty.datasets import transfer


def read_csv(project_path, project_id=None, *args, **kwargs):
    """Read a CSV file from a project's datasets as a pandas DataFrame.

    Requires pandas to be installed.

    Parameters
    ----------
    project_path : str
        The path of the CSV file in the project's datasets to read.
    project_id : str, optional
        The project to get the CSV file from. You need to have access to this
        project for it to work. Defaults to the project set by
        FACULTY_PROJECT_ID in your environment.
    *args
        Additional positional arguments to pass to ``pandas.read_csv``.
    **kwargs
        Additional keyword arguments to pass to ``pandas.read_csv``.
    """

    project_id = project_id or get_context().project_id
    object_client = ObjectClient(get_session())
    presigned_url = object_client.presign_download(project_id, project_path)

    return pandas.read_csv(presigned_url, *args, **kwargs)


def to_csv(dataframe, project_path, project_id=None, *args, **kwargs):
    """Write a pandas DataFrame to a CSV file in a project's datasets.

    Requires pandas to be installed.

    Parameters
    ----------
    dataframe : pandas.DataFrame
        The DataFrame to write.
    project_path : str
        The path in the project's datasets to write to.
    project_id : str, optional
        The project to write the CSV file to. You need to have access to this
        project for it to work. Defaults to the project set by
        FACULTY_PROJECT_ID in your environment.
    *args
        Additional positional arguments to pass to ``pandas.DataFrame.to_csv``.
    **kwargs
        Additional keyword arguments to pass to ``pandas.DataFrame.to_csv``.
    """

    project_id = project_id or get_context().project_id
    object_client = ObjectClient(get_session())

    content = dataframe.to_csv(path_or_buf=None, *args, **kwargs)

    transfer.upload(
        object_client, project_id, project_path, content.encode("utf-8")
    )
