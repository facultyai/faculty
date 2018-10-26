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

from datetime import datetime

import pytest
from dateutil.tz import UTC
from marshmallow import ValidationError

from sherlockml.clients.log import (
    LogPart,
    LogPartSchema,
    LogPartsResponse,
    LogPartsResponseSchema,
)

TIMESTAMP = datetime(2018, 3, 10, 11, 32, 6, 247000, tzinfo=UTC)
TIMESTAMP_STRING = "2018-03-10T11:32:06.247Z"

LOG_PART = LogPart(
    log_part_number=3,
    line_number=1,
    content="example content",
    timestamp=TIMESTAMP,
)
LOG_PART_BODY = {
    "logPartNumber": LOG_PART.log_part_number,
    "lineNumber": LOG_PART.line_number,
    "content": LOG_PART.content,
    "timestamp": TIMESTAMP_STRING,
}

LOG_PARTS_RESPONSE = LogPartsResponse(log_parts=[LOG_PART])
LOG_PARTS_RESPONSE_BODY = {"logParts": [LOG_PART_BODY]}


def test_log_part_schema():
    data = LogPartSchema().load(LOG_PART_BODY)
    assert data == LOG_PART


def test_log_part_schema_invalid():
    with pytest.raises(ValidationError):
        LogPartSchema().load({})


def test_log_parts_response_schema():
    data = LogPartsResponseSchema().load(LOG_PARTS_RESPONSE_BODY)
    assert data == LOG_PARTS_RESPONSE


def test_log_parts_response_schema_invalid():
    with pytest.raises(ValidationError):
        LogPartsResponseSchema().load({})
