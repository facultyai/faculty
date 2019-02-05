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


from datetime import datetime, timedelta

import pytz
import requests
from six.moves import urllib

import faculty.config
from faculty.session.accesstoken import AccessToken, AccessTokenMemoryCache


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


class Session(object):
    def __init__(self, profile, access_token_cache):
        self.profile = profile
        self.access_token_cache = access_token_cache

    def access_token(self):
        access_token = self.access_token_cache.get(self.profile)
        if access_token is None:
            access_token = _get_access_token(self.profile)
            self.access_token_cache.add(self.profile, access_token)
        return access_token

    def service_url(self, service_name, endpoint=""):
        return _service_url(self.profile, service_name, endpoint)


_SESSION_CACHE = {}


def get_session(access_token_cache=None, **kwargs):
    key = tuple(kwargs.items()) + (access_token_cache,)
    try:
        session = _SESSION_CACHE[key]
    except KeyError:
        profile = faculty.config.resolve_profile(**kwargs)
        access_token_cache = access_token_cache or AccessTokenMemoryCache()
        session = Session(profile, access_token_cache)
        _SESSION_CACHE[key] = session
    return session
