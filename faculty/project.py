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
import uuid

from attr import attrs, attrib

import faculty
from faculty.user import User


@attrs
class Project(object):

    id = attrib()
    name = attrib()
    owner = attrib()

    @classmethod
    def get(cls, project_name_or_id=None):
        project_client = faculty.client("project")
        if project_name_or_id is None:
            project_id = uuid.UUID(os.environ["SHERLOCKML_PROJECT_ID"])
            project = project_client.get(project_id)
        else:
            try:
                project_id = uuid.UUID(project_name_or_id)
                project = project_client.get(project_id)
            except ValueError:
                # TODO: Support getting projects by both owner and name
                owner = User.me()
                project = project_client.get_by_owner_and_name(
                    owner.id, project_name_or_id
                )
        return cls(
            id=project.id, name=project.name, owner=User(project.owner_id)
        )
