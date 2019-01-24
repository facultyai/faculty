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


import os
import json
import errno
from datetime import datetime, timedelta
from collections import namedtuple

import requests
import pytz
from marshmallow import Schema, fields, post_load, ValidationError
from six.moves import urllib

import faculty.config


AccessToken = namedtuple("AccessToken", ["token", "expires_at"])


class AccessTokenSchema(Schema):
    token = fields.String(required=True)
    expires_at = fields.DateTime(data_key="expiresAt", required=True)

    @post_load
    def make_access_token(self, data):
        return AccessToken(**data)


class AccessTokenStore(object):
    def __init__(self, tokens=None):
        self.tokens = tokens or {}

    @staticmethod
    def _hash_profile(profile):
        return str(hash(profile))

    def __getitem__(self, profile):
        return self.tokens[self._hash_profile(profile)]

    def __setitem__(self, profile, access_token):
        self.tokens[self._hash_profile(profile)] = access_token

    def get(self, profile):
        try:
            return self[profile]
        except KeyError:
            return None


class AccessTokenStoreSchema(Schema):
    tokens = fields.Dict(
        keys=fields.String(),
        values=fields.Nested(AccessTokenSchema),
        required=True,
    )

    @post_load
    def make_access_token_store(self, data):
        return AccessTokenStore(**data)


def _is_valid_access_token(access_token_or_none):
    if access_token_or_none is None:
        return False
    else:
        return access_token_or_none.expires_at >= datetime.now(tz=pytz.utc)


class MemoryAccessTokenCache(object):
    def __init__(self):
        self._store = AccessTokenStore()

    def get(self, profile):
        access_token = self._store.get(profile)
        return access_token if _is_valid_access_token(access_token) else None

    def add(self, profile, access_token):
        self._store[profile] = access_token


def _ensure_directory_exists(path, mode):
    try:
        os.makedirs(path, mode=mode)
    except OSError as e:
        if e.errno == errno.EEXIST:
            # Directory already exists
            pass
        else:
            raise


class FileSystemAccessTokenCache(object):
    def __init__(self, cache_path):
        self.cache_path = str(cache_path)
        self._store = None

    def _load_from_disk(self):
        try:
            with open(self.cache_path, "r") as fp:
                data = json.load(fp)
            self._store = AccessTokenStoreSchema().load(data)
        except IOError as e:
            if e.errno == errno.ENOENT:
                # File does not exist - initialise empty store
                self._store = AccessTokenStore()
            else:
                raise
        except (ValueError, ValidationError):
            # File is of invalid format - reset with empty store
            self._store = AccessTokenStore()

    def _persist_to_disk(self):
        dirname = os.path.dirname(self.cache_path)
        _ensure_directory_exists(dirname, mode=0o700)
        data = AccessTokenStoreSchema().dump(self._store)
        with open(self.cache_path, "w") as fp:
            json.dump(data, fp, separators=(",", ":"))

    def get(self, profile):
        if self._store is None:
            self._load_from_disk()
        access_token = self._store.get(profile)
        return access_token if _is_valid_access_token(access_token) else None

    def add(self, profile, access_token):
        if self._store is None:
            self._load_from_disk()
        self._store[profile] = access_token
        self._persist_to_disk()


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
        access_token_cache = access_token_cache or MemoryAccessTokenCache()
        session = Session(profile, access_token_cache)
        _SESSION_CACHE[key] = session
    return session
