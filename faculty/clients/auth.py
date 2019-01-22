# Copyright 2018-2019 ASI Data Science
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


class _AccessTokenCache(object):
    def __init__(self):
        self._store = {}

    def get(self, profile):
        access_token = self._store.get(profile)
        utc_now = datetime.now(tz=pytz.utc)
        if access_token is None or access_token.expires_at < utc_now:
            return None
        else:
            return access_token

    def add(self, profile, access_token):
        self._store[profile] = access_token


_ACCESS_TOKEN_CACHE = _AccessTokenCache()


def _get_access_token(profile):

    url = "{}://hudson.{}/access_token".format(
        profile.protocol, profile.domain
    )
    payload = {
        "client_id": profile.client_id,
        "client_secret": profile.client_secret,
        "grant_type": "client_credentials",
    }

    response = requests.post(url, json=payload)
    response.raise_for_status()

    body = response.json()

    token = body["access_token"]
    now = datetime.now(tz=pytz.utc)
    expires_at = now + timedelta(seconds=body["expires_in"])

    return AccessToken(token, expires_at)


def _get_access_token_cached(profile):
    access_token = _ACCESS_TOKEN_CACHE.get(profile)
    if access_token is None:
        access_token = _get_access_token(profile)
        _ACCESS_TOKEN_CACHE.add(profile, access_token)
    return access_token


class FacultyAuth(AuthBase):
    """Requests auth implementation for accessing Faculty services.

    Parameters
    ----------
    profile : faculty.config.Profile
        The profile (Faculty domain, protocol and client credentials) to use

    To perform an authenticated request against a Faculty service, first
    construct an instance of this class:

    >>> from faculty.config import resolve_profile
    >>> from faculty.clients.auth import FacultyAuth
    >>> profile = resolve_profile()
    >>> auth = FacultyAuth(profile)

    then pass it as the ``auth`` argument when making a request with
    ``requests``:

    >>> import requests
    >>> requests.get('https://servicename.services.example.my.faculty.ai',
                     auth=auth)

    You can also set it as the ``auth`` attribute on a
    :class:`requests.Session`, so that subsequent requests will be
    authenticated automatically:

    >>> import requests
    >>> session = requests.Session()
    >>> session.auth = auth
    """

    def __init__(self, profile):
        self.profile = profile

    def __call__(self, request):
        access_token = _get_access_token_cached(self.profile)

        header_content = "Bearer {}".format(access_token.token)
        request.headers["Authorization"] = header_content

        return request
