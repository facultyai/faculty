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


from uuid import UUID

from faculty.context import get_context
from faculty.clients import AccountClient, ProjectClient


def _make_uuid(value):
    """Make a UUID from the passed value.

    Pass through UUID objects as the UUID constructor fails when passed UUID
    objects.
    """
    if isinstance(value, UUID):
        return value
    else:
        return UUID(value)


def _project_from_name(session, name):

    user_id = AccountClient(session).authenticated_user_id()
    projects = ProjectClient(session).list_accessible_by_user(user_id)

    matches = [project for project in projects if project.name == name]
    if len(matches) == 1:
        return matches[0]
    elif len(matches) == 0:
        raise ValueError("No projects of name {} found".format(name))
    else:
        raise ValueError("Multiple projects of name {} found".format(name))


def resolve_project_id(session, project=None):
    if project is None:
        context = get_context()
        if context.project_id is None:
            raise ValueError(
                "Must pass a project when none can be determined from the "
                "runtime context"
            )
        else:
            return context.project_id
    else:
        try:
            return _make_uuid(project)
        except ValueError:
            return _project_from_name(project).id
