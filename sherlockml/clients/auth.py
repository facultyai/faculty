# Copyright 2018 ASI Data Science
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


from collections import namedtuple
from datetime import datetime, timedelta

import pytz
import requests
from requests.auth import AuthBase


AccessToken = namedtuple("AccessToken", ["token", "expires_at"])


class AccessTokenClient(object):
    """Client for getting access tokens for accessing SherlockML services."""

    def __init__(self, hudson_url):
        self._session = requests.Session()
        self.hudson_url = hudson_url

    def get_access_token(self, client_id, client_secret):
        endpoint = "{}/access_token".format(self.hudson_url)
        payload = {
            "client_id": client_id,
            "client_secret": client_secret,
            "grant_type": "client_credentials",
        }
        response = self._session.post(endpoint, json=payload)
        response.raise_for_status()
        body = response.json()

        token = body["access_token"]
        now = datetime.now(tz=pytz.utc)
        expires_at = now + timedelta(seconds=body["expires_in"])

        return AccessToken(token, expires_at)


class SherlockMLAuth(AuthBase):
    """Requests auth implementation for accessing SherlockML services.

    Parameters
    ----------
    auth_service_url : str
        The URL of the SherlockML authentication service
    client_id : str
        The SherlockML client ID to use for authentication
    client_secret : str
        The client secret associated with the client ID

    To perform an authenticated request against a SherlockML service, first
    construct an instance of this class:

    >>> auth = SherlockMLAuth('https://hudson.services.example.sherlockml.net',
                              your_client_id, your_client_secret)

    then pass it as the ``auth`` argument when making a request with
    ``requests``:

    >>> import requests
    >>> requests.get('https://servicename.services.example.sherlockml.net',
                     auth=auth)

    You can also set it as the ``auth`` attribute on a
    :class:`requests.Session`, so that subsequent requests will be
    authenticated automatically:

    >>> import requests
    >>> session = requests.Session()
    >>> session.auth = auth
    """

    def __init__(self, auth_service_url, client_id, client_secret):
        self.auth_service_url = auth_service_url
        self.client_id = client_id
        self.client_secret = client_secret
        self.access_token = None

    def _get_token(self):
        client = AccessTokenClient(self.auth_service_url)
        return client.get_access_token(self.client_id, self.client_secret)

    def __call__(self, request):

        if self.access_token is None:
            self.access_token = self._get_token()
        if self.access_token.expires_at < datetime.now(tz=pytz.utc):
            self.access_token = self._get_token()

        header_content = "Bearer {}".format(self.access_token.token)
        request.headers["Authorization"] = header_content

        return request
