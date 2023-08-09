# Copyright 2018-2023 Faculty Science Limited
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
Manipulate Faculty APIs.
"""

from attr import attrs, attrib
from marshmallow import fields

from faculty.clients.base import BaseSchema, BaseClient


@attrs
class APIKey:
    """An authorization key for a Faculty API.

    Parameters
    ----------
    id : uuid.UUID
    label : str
    material : str
    enabled : bool
    """

    id = attrib()
    label = attrib()
    material = attrib()
    enabled = attrib()


class _APIKeySchema(BaseSchema):
    id = fields.UUID(data_key="keyId", required=True)
    label = fields.String(required=True)
    material = fields.String(required=True)
    enabled = fields.Boolean(required=True)


class APIClient(BaseClient):
    """Client for the Faculty API service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("api")

    Parameters
    ----------
    url : str
        The URL of the api service.
    session : faculty.session.Session
        The session to use to make requests.
    """

    SERVICE_NAME = "aperture"

    def list_api_keys(self, project_id, api_id):
        """List the API keys for a given API.

        Parameters
        ----------
        project_id : uuid.UUID
            The ID of the project containing the API.
        api_id : uuid.UUID
            The ID of the API to list keys for.

        Returns
        -------
        list[APIKey]
            The API keys for the given API.
        """
        endpoint = "/project/{}/api/{}/key".format(project_id, api_id)
        return self._get(endpoint, _APIKeySchema(many=True))
