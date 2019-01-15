"""Interact with Baskerville."""

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


class BaskervilleError(faculty.cli.client.FacultyServiceError):
    """Exception for errors interacting with Baskerville."""

    pass


class ServerEnvironment(object):
    """A Faculty server environment."""

    # pylint: disable=too-few-public-methods

    def __init__(self, id_, project_id, name, author_id):
        self.id_ = id_
        self.project_id = project_id
        self.name = name
        self.author_id = author_id

    def __repr__(self):
        template = (
            "ServerEnvironment(id_={}, project_id={}, name={}, author_id={})"
        )
        return template.format(
            self.id_, self.project_id, self.name, self.author_id
        )

    @classmethod
    def from_json(cls, json_object):
        return cls(
            json_object["environmentId"],
            json_object["projectId"],
            json_object["name"],
            json_object["authorId"],
        )


class Baskerville(faculty.cli.client.FacultyService):
    """A Baskerville client."""

    def __init__(self):
        super(Baskerville, self).__init__(faculty.cli.config.baskerville_url())

    def get_environments(self, project_id, name=None):
        """List environment in the given project."""
        resp = self._get("/project/{}/environment".format(project_id))
        environments = [ServerEnvironment.from_json(o) for o in resp.json()]
        if name is not None:
            environments = [e for e in environments if e.name == name]
        return environments
