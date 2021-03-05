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
Manage invitations to Faculty and new user creation.
"""

from faculty.clients.base import BaseClient
from faculty.clients.user import GlobalRole


class InvitationClient(BaseClient):
    """Client for the Faculty user creation and invitation service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("invitation")

    Parameters
    ----------
    url : str
        The URL of the user creation and invitation service.
    session : faculty.session.Session
        The session to use to make requests.
    """

    SERVICE_NAME = "ivory"

    def invite_user(self, email, global_roles=None):
        """Invite a new user to Faculty by email

        Parameters
        ----------
        email: str
            Email address to send the invitation to.
        global_roles: List[faculty.clients.user.GlobalRole]
            Global roles to assign to the new user after accepting the invite.
            Defaults to a basic user.
        """
        if global_roles is None:
            global_roles = [GlobalRole.BASIC_USER]
        payload = {
            "email": email,
            "globalRoles": [r.value for r in global_roles],
        }
        self._post_raw("/admin/invitation", json=payload)
