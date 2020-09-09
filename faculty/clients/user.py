# Copyright 2018-2020 Faculty Science Limited
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
Manage Faculty users.
"""


from collections import namedtuple
from enum import Enum

from marshmallow import fields, post_load
from marshmallow_enum import EnumField

from faculty.clients.base import BaseSchema, BaseClient


class GlobalRole(Enum):
    """Enumeration of global roles a user can have in a Faculty deployment."""

    BASIC_USER = "global-basic-user"
    FULL_USER = "global-full-user"
    ADMIN = "global-admin"


User = namedtuple(
    "User",
    [
        "id",
        "username",
        "full_name",
        "email",
        "created_at",
        "enabled",
        "global_roles",
        "is_system",
    ],
)


class UserClient(BaseClient):
    """Client for the Faculty user service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("user")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    """

    _SERVICE_NAME = "flock"

    def get_user(self, user_id):
        """Get information about a user.

        Parameters
        ----------
        user_id : uuid.UUID
            The ID of the user.

        Returns
        -------
        User
            The retrieved user.
        """
        endpoint = "/user/{}".format(user_id)
        return self._get(endpoint, _UserSchema())

    def get_all_users(self, is_system=None, enabled=None):
        """Get all users in the deployment.

        Parameters
        ----------
        is_system : bool, optional
            If provided, filter users by their status as 'system' or 'human'
            user. ``is_system=True`` will return only system users, and
            ``is_system=False`` will return only human users.
        enabled : bool, optional
            If provided, filter users by their enabled/disabled status.
            ``enabled=True`` will return only enabled users, and
            ``enabled=False`` will return only disabled users.

        Returns
        -------
        List[User]
            The matching users.
        """
        params = {}
        if is_system is not None:
            params["isSystem"] = "true" if is_system else "false"
        if enabled is not None:
            params["isDisabled"] = "false" if enabled else "true"
        endpoint = "/users"
        return self._get(endpoint, _UserSchema(many=True), params=params)

    def set_global_roles(self, user_id, global_roles):
        """Set the global roles for a user.

        Parameters
        ----------
        user_id : uuid.UUID
            The ID of the user to update.
        global_roles : List[str]
            The new global roles for the user.

        Returns
        -------
        User
            The updated user.
        """
        endpoint = "/user/{}/roles".format(user_id)
        return self._put(endpoint, _UserSchema(), json={"roles": global_roles})


class _UserSchema(BaseSchema):

    id = fields.UUID(data_key="userId", required=True)
    username = fields.Str(required=True)
    full_name = fields.Str(data_key="fullName", missing=None)
    email = fields.Str(required=True)
    created_at = fields.DateTime(data_key="createdAt", required=True)
    enabled = fields.Boolean(required=True)
    global_roles = fields.List(
        EnumField(GlobalRole, by_value=True),
        data_key="globalRoles",
        missing=None,
    )
    is_system = fields.Boolean(data_key="isSystem", required=True)

    @post_load
    def make_user(self, data):
        return User(**data)
