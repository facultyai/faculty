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
from uuid import uuid4

import pytest
import attr

from faculty.context import PlatformContext, get_context


CONTEXT = PlatformContext(
    project_id=uuid4(),
    server_id=uuid4(),
    server_name="mad_boltzmann",
    server_cpus=2,
    server_gpus=0,
    server_memory_mb=8000,
    app_id=uuid4(),
    api_id=uuid4(),
    job_id=uuid4(),
    job_name="retrain model",
    job_run_id=uuid4(),
    job_run_number=6,
    job_subrun_id=uuid4(),
    job_subrun_number=2,
)
ALL_NONE_CONTEXT = PlatformContext(
    **{attribute.name: None for attribute in attr.fields(PlatformContext)}
)

MOCK_ENVIRON = {
    "FACULTY_PROJECT_ID": str(CONTEXT.project_id),
    "FACULTY_SERVER_ID": str(CONTEXT.server_id),
    "FACULTY_SERVER_NAME": CONTEXT.server_name,
    "NUM_CPUS": str(CONTEXT.server_cpus),
    "NUM_GPUS": str(CONTEXT.server_gpus),
    "AVAILABLE_MEMORY_MB": str(CONTEXT.server_memory_mb),
    "FACULTY_APP_ID": str(CONTEXT.app_id),
    "FACULTY_API_ID": str(CONTEXT.api_id),
    "FACULTY_JOB_ID": str(CONTEXT.job_id),
    "FACULTY_JOB_NAME": CONTEXT.job_name,
    "FACULTY_RUN_ID": str(CONTEXT.job_run_id),
    "FACULTY_RUN_NUMBER": str(CONTEXT.job_run_number),
    "FACULTY_SUBRUN_ID": str(CONTEXT.job_subrun_id),
    "FACULTY_SUBRUN_NUMBER": str(CONTEXT.job_subrun_number),
}


def test_get_context(mocker):
    mocker.patch.dict(os.environ, MOCK_ENVIRON)
    assert get_context() == CONTEXT


def test_get_context_defaults(mocker):
    """Check that context fields default to None when not available."""
    mocker.patch.dict(os.environ, {})
    assert get_context() == ALL_NONE_CONTEXT


def test_get_context_malformatted(mocker):
    mocker.patch.dict(os.environ, {"FACULTY_PROJECT_ID": "invalid-uuid"})
    with pytest.warns(UserWarning, match="badly formatted"):
        assert get_context() == ALL_NONE_CONTEXT
