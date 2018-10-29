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

from collections import namedtuple

from marshmallow import Schema, fields, post_load

from sherlockml.clients.base import BaseClient


LogPart = namedtuple(
    "LogPart", ["log_part_number", "line_number", "content", "timestamp"]
)
LogPartsResponse = namedtuple("LogPartsResponse", ["log_parts"])


class LogPartSchema(Schema):
    log_part_number = fields.Integer(data_key="logPartNumber", required=True)
    line_number = fields.Integer(data_key="lineNumber", required=True)
    content = fields.String(required=True)
    timestamp = fields.DateTime(required=True)

    @post_load
    def make_log_part(self, data):
        return LogPart(**data)


class LogPartsResponseSchema(Schema):
    log_parts = fields.Nested(
        LogPartSchema, data_key="logParts", many=True, required=True
    )

    @post_load
    def make_log_parts_response(self, data):
        return LogPartsResponse(**data)


class LogClient(BaseClient):

    SERVICE_NAME = "wozniak"

    def get_subrun_command_logs(self, project_id, job_id, run_id, subrun_id):
        template = "/project/{}/job/{}/run/{}/subrun/{}/command/log-part"
        endpoint = template.format(project_id, job_id, run_id, subrun_id)
        return self._get_logs(endpoint)

    def get_subrun_environment_step_logs(
        self, project_id, job_id, run_id, subrun_id, environment_step_id
    ):
        template = (
            "/project/{}/job/{}/run/{}/subrun/{}/environment-step/{}/log-part"
        )
        endpoint = template.format(
            project_id, job_id, run_id, subrun_id, environment_step_id
        )
        return self._get_logs(endpoint)

    def _get_logs(self, endpoint):
        response = self._get(endpoint, LogPartsResponseSchema())
        return response.log_parts
