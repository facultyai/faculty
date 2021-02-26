# Copyright 2018-2021 Faculty Science Limited
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

"""
Interact with Hound.
"""

from attr import attrs, attrib

from marshmallow import fields, post_load

from faculty.clients.base import BaseSchema, BaseClient

@attrs
class Hound(object):
    
    status = attrib()
    environments = attrib()

@attrs
class EnvironmentExecution(object):
    
    steps = attrib()

@attrs
class EnvironmentExecutionStep(object):
    
    command = attrib()
    status = attrib()
    log_path = attrib()

class HoundClient(BaseClient):

    _SERVICE_NAME = ""

    def latest_environment_execution(self):
        """Get the latest environment execution on the server."""
        print("Placeholder")

class _EnvironmentExecutionStepSchema(BaseSchema):

    command = fields.List(fields.String)
    status = fields.String()
    log_path = fields.String()

    @post_load
    def make_environment_execution_step(self, data, **kwargs):
        return EnvironmentExecutionStep(**data)

class _EnvironmentExecutionSchema(BaseSchema):

    steps = fields.List(fields.Nested(_EnvironmentExecutionStepSchema))

    @post_load
    def make_environment_execution(self, data, **kwargs):
        return EnvironmentExecution(**data)

class _HoundSchema(BaseSchema):

    status = fields.String(required=True)
    environments = fields.List(fields.Nested(_EnvironmentExecutionSchema))

    @post_load
    def make_hound(self, data, **kwargs):
        return Hound(**data)