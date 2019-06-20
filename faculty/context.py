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


import os
import warnings
from uuid import UUID

from attr import attrs, attrib


@attrs
class PlatformContext(object):
    """Information about the runtime context in Faculty platform."""

    project_id = attrib()

    server_id = attrib()
    server_name = attrib()
    server_cpus = attrib()
    server_gpus = attrib()
    server_memory_mb = attrib()

    app_id = attrib()
    api_id = attrib()

    job_id = attrib()
    job_name = attrib()
    job_run_id = attrib()
    job_run_number = attrib()
    job_subrun_id = attrib()
    job_subrun_number = attrib()


def _get_environ_as_type(key, cls):
    try:
        return cls(os.environ[key])
    except KeyError:
        # Not in environment
        return None
    except ValueError:
        # Badly formatted
        template = (
            "Error interpreting badly formatted environment variable {}={}"
        )
        warnings.warn(template.format(key, os.environ[key]))
        return None


def get_context():
    """Get information about the Faculty platform runtime context.

    Returns
    -------
    PlatformContext
    """
    return PlatformContext(
        project_id=_get_environ_as_type("FACULTY_PROJECT_ID", UUID),
        server_id=_get_environ_as_type("FACULTY_SERVER_ID", UUID),
        server_name=_get_environ_as_type("FACULTY_SERVER_NAME", str),
        server_cpus=_get_environ_as_type("NUM_CPUS", int),
        server_gpus=_get_environ_as_type("NUM_GPUS", int),
        server_memory_mb=_get_environ_as_type("AVAILABLE_MEMORY_MB", int),
        app_id=_get_environ_as_type("FACULTY_APP_ID", UUID),
        api_id=_get_environ_as_type("FACULTY_API_ID", UUID),
        job_id=_get_environ_as_type("FACULTY_JOB_ID", UUID),
        job_name=_get_environ_as_type("FACULTY_JOB_NAME", str),
        job_run_id=_get_environ_as_type("FACULTY_RUN_ID", UUID),
        job_run_number=_get_environ_as_type("FACULTY_RUN_NUMBER", int),
        job_subrun_id=_get_environ_as_type("FACULTY_SUBRUN_ID", UUID),
        job_subrun_number=_get_environ_as_type("FACULTY_SUBRUN_NUMBER", int),
    )
