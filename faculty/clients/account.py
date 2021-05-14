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
Manage Faculty user accounts.
"""

from attr import attrs, attrib
from marshmallow import fields, post_load

from faculty.clients.base import BaseSchema, BaseClient


@attrs
class Account:
    """A user account in Faculty.

    Parameters
    ----------
    user_id : uuid.UUID
        The user ID of the account.
    username : str
        The account's username.
    email : str
        The email address associated with the account.
    """

    user_id = attrib()
    username = attrib()
    email = attrib()


@attrs
class _AuthenticationResponse:
    account = attrib()


class AccountClient(BaseClient):
    """Client for the Faculty account service.

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("account")

    Parameters
    ----------
    url : str
        The URL of the account service.
    session : faculty.session.Session
        The session to use to make requests.
    """

    SERVICE_NAME = "hudson"

    def authenticated_account(self):
        """Get information on the account used to authenticate this session.

        Returns
        -------
        Account
            The account used to authenticate this session.
        """
        data = self._get("/authenticate", _AuthenticationResponseSchema())
        return data.account

    def authenticated_user_id(self):
        """Get the user ID of the account used to authenticate this session.

        Returns
        -------
        uuid.UUID
            The user ID used to authenticate this session.
        """
        return self.authenticated_account().user_id

    def get(self, user_id):
        """Get an account by its user ID.

        Parameters
        ----------
        user_id : uuid.UUID
            The ID of the account to get.

        Returns
        -------
        Account
            The account.
        """
        endpoint = "/user/{}".format(user_id)
        return self._get(endpoint, _AccountSchema())


class _AccountSchema(BaseSchema):
    user_id = fields.UUID(data_key="userId", required=True)
    username = fields.String(required=True)
    email = fields.String(required=True)

    @post_load
    def make_account(self, data, **kwargs):
        return Account(**data)


class _AuthenticationResponseSchema(BaseSchema):
    account = fields.Nested(_AccountSchema, required=True)

    @post_load
    def make_authentication_response(self, data, **kwargs):
        return _AuthenticationResponse(**data)
