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
Provides a session for connecting to Faculty services.
"""


from datetime import datetime, timedelta

import pytz
import requests
import urllib

import faculty.config
from faculty.session.accesstoken import AccessToken, AccessTokenMemoryCache


class Session:
    """A session for connecting to Faculty services.

    This session encapsultates the configuration (address and authentication
    credentials) for connecting to a Faculty deployment, plus the caching of
    access tokens used for authenticating requests to services.

    Parameters
    ----------
    profile : faculty.config.Profile
        The profile to use with this session.
    access_token_cache : faculty.config.accesstoken.AccessTokenMemoryCache or \
            faculty.config.accesstoken.AccessTokenFileSystemCache
        A cache for keeping access tokens in this session.
    """

    def __init__(self, profile, access_token_cache):
        self.profile = profile
        self.access_token_cache = access_token_cache

    def access_token(self):
        """Get an access token for authenticating a request.

        If a valid token is in the cache, it will be returned. Otherwise, a new
        token is retrieved.

        Returns
        -------
        AccessToken
            A valid access token.
        """
        access_token = self.access_token_cache.get(self.profile)
        if access_token is None:
            access_token = _get_access_token(self.profile)
            self.access_token_cache.add(self.profile, access_token)
        return access_token

    def service_url(self, service_name, endpoint=""):
        """Determine the URL of a Faculty service endpoint.

        Parameters
        ----------
        service_name : str
            The name of the service to make a request to.
        endpoint : str, optional
            The endpoint to generate a URL for. If not provided, the root
            endpoint will be used.

        Returns
        -------
        str
            The resolved URL.
        """
        return _service_url(self.profile, service_name, endpoint)


_SESSION_CACHE = {}


def get_session(
    access_token_cache=None,
    credentials_path=None,
    profile_name=None,
    domain=None,
    protocol=None,
    client_id=None,
    client_secret=None,
):
    """Get a Faculty session for a given configuration.

    Sessions returned by this function are cached. If called multiple times
    with the same arguments, the same session instance will be returned.

    Configuration settings are resolved as described in
    :func:`faculty.config.resolve_profile`.

    Parameters
    ----------
    access_token_cache : faculty.config.accesstoken.AccessTokenMemoryCache or \
            faculty.config.accesstoken.AccessTokenFileSystemCache
        A cache for keeping access tokens in this session. The default is a
        :class:`faculty.config.accesstoken.AccessTokenMemoryCache`.
    credentials_path : str or pathlib.Path, optional
        The path of the credentials file to load.
    profile_name : str, optional
        The name of the profile to load from the credentials file.
    domain : str, optional
        The domain name where Faculty services are hosted.
    protocol : str, optional
        The protocol to use when making requests to Faculty services.
    client_id : str, optional
        The OAuth client ID to authenticate requests with.
    client_secret : str, optional
        The OAuth client secret to authenticate requests with.

    Returns
    -------
    Session
        The resulting Faculty session.
    """
    key = (
        access_token_cache,
        credentials_path,
        profile_name,
        domain,
        protocol,
        client_id,
        client_secret,
    )
    try:
        session = _SESSION_CACHE[key]
    except KeyError:
        profile = faculty.config.resolve_profile(
            credentials_path=credentials_path,
            profile_name=profile_name,
            domain=domain,
            protocol=protocol,
            client_id=client_id,
            client_secret=client_secret,
        )
        access_token_cache = access_token_cache or AccessTokenMemoryCache()
        session = Session(profile, access_token_cache)
        _SESSION_CACHE[key] = session
    return session


def _service_url(profile, service, endpoint=""):
    host = "{}.{}".format(service, profile.domain)
    url_parts = (profile.protocol, host, endpoint, None, None)
    return urllib.parse.urlunsplit(url_parts)


def _get_access_token(profile):
    url = _service_url(profile, "hudson", "access_token")
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
