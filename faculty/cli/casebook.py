"""Interact with Casebook."""

# Copyright 2016-2018 ASI Data Science
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

import faculty.cli.client
import faculty.cli.config


class Project(object):
    """A Faculty project."""

    # pylint: disable=too-few-public-methods

    def __init__(self, id_, name, owner_id):
        self.id_ = id_
        self.name = name
        self.owner_id = owner_id

    def __repr__(self):
        template = "Project(id_={}, name={}, owner_id={})"
        return template.format(self.id_, self.name, self.owner_id)

    @classmethod
    def from_json(cls, json_object):
        return cls(
            json_object["project_id"],
            json_object["name"],
            json_object["owner_id"],
        )


class Casebook(faculty.cli.client.FacultyService):
    """A Casebook client."""

    def __init__(self):
        super(Casebook, self).__init__(faculty.cli.config.casebook_url())

    def get_projects(self, user_id):
        """List projects accessible by the given user."""
        resp = self._get("/user/{}".format(user_id))
        return [Project.from_json(o) for o in resp.json()]

    def get_project_by_name(self, user_id, project_name):
        """List projects with a given name accessible by the given user."""
        try:
            resp = self._get("/project/{}/{}".format(user_id, project_name))
        except faculty.cli.client.FacultyServiceError:
            projects = self.get_projects(user_id)
            matching_projects = [p for p in projects if p.name == project_name]
            if len(matching_projects) == 1:
                return matching_projects[0]
            else:
                raise
        return Project.from_json(resp.json())
