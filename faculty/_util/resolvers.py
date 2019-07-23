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

from cachetools.func import lru_cache

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
    """Provided a project name, find a matching project ID.

    This method searches all the projects accessible to the active user for a
    matching project. If not exactly one project matches, a ValueError is
    raised.
    """

    user_id = AccountClient(session).authenticated_user_id()
    projects = ProjectClient(session).list_accessible_by_user(user_id)

    matches = [project for project in projects if project.name == name]
    if len(matches) == 1:
        return matches[0]
    elif len(matches) == 0:
        raise ValueError("No projects of name {} found".format(name))
    else:
        raise ValueError("Multiple projects of name {} found".format(name))


@lru_cache()
def resolve_project_id(session, project=None):
    """Resolve the ID of a project based on ID, name or the current context.

    This helper encapsulates logic for determining a project in three
    situations:

    * If ``None`` is passed as the project, or if no project is passed, the
      project will be inferred from the runtime context (i.e. environment
      variables), and so will correspond to the 'current project' when run
      inside Faculty platform.
    * If a ``uuid.UUID`` or a string containing a valid UUID is passed, this
      will be assumed to be the ID of the project and will be returned.
    * If any other string is passed, the Faculty platform will be queried for
      projects matching that name. If exactly one of that name is accessible to
      the user, its ID will be returned, otherwise a ``ValueError`` will be
      raised.

    Parameters
    ----------
    session : faculty.session.Session
    project : str, uuid.UUID or None
        Information to use to determine the active project.

    Returns
    -------
    uuid.UUID
        The ID of the project
    """

    if project is None:
        context = get_context()
        if context.project_id is None:
            raise ValueError(
                "Must pass a project name or ID when none can be determined "
                "from the runtime context"
            )
        else:
            return context.project_id
    else:
        try:
            return _make_uuid(project)
        except ValueError:
            return _project_from_name(session, project).id
