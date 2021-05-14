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
Retrieve and cache access tokens for authenticating with Faculty services.
"""


import os
import json
import errno
from datetime import datetime
from collections import namedtuple

import pytz
from marshmallow import Schema, fields, post_load, ValidationError


AccessToken = namedtuple("AccessToken", ["token", "expires_at"])


class AccessTokenMemoryCache:
    """An in-memory cache for access tokens."""

    def __init__(self):
        self._store = _AccessTokenStore()

    def get(self, profile):
        """Get an access token from the cache.

        Parameters
        ----------
        profile : faculty.config.Profile
            The profile which the access token corresponds to.

        Returns
        -------
        Optional[AccessToken]
            The access token, or None if it is not present or is invalid.
        """
        access_token = self._store.get(profile)
        return access_token if _is_valid_access_token(access_token) else None

    def add(self, profile, access_token):
        """Insert an access token into the cache.

        Parameters
        ----------
        profile : faculty.config.Profile
            The profile which the access token corresponds to.
        access_token : AccessToken
            The access token to cache.
        """
        self._store[profile] = access_token


class AccessTokenFileSystemCache:
    """A disk-persisted cache for access tokens.

    Parameters
    ----------
    cache_path : str or pathlib.Path, optional
        Pass to override the default cache location.
    """

    def __init__(self, cache_path=None):
        if cache_path is None:
            self.cache_path = _default_token_cache_path()
        else:
            self.cache_path = str(cache_path)
        self._store = None

    def get(self, profile):
        """Get an access token from the cache.

        Parameters
        ----------
        profile : faculty.config.Profile
            The profile which the access token corresponds to.

        Returns
        -------
        Optional[AccessToken]
            The access token, or None if it is not present or is invalid.
        """
        if self._store is None:
            self._load_from_disk()
        access_token = self._store.get(profile)
        return access_token if _is_valid_access_token(access_token) else None

    def add(self, profile, access_token):
        """Insert an access token into the cache.

        Parameters
        ----------
        profile : faculty.config.Profile
            The profile which the access token corresponds to.
        access_token : AccessToken
            The access token to cache.
        """
        if self._store is None:
            self._load_from_disk()
        self._store[profile] = access_token
        self._persist_to_disk()

    def _load_from_disk(self):
        try:
            with open(self.cache_path, "r") as fp:
                data = json.load(fp)
            self._store = _AccessTokenStoreSchema().load(data)
        except IOError as e:
            if e.errno == errno.ENOENT:
                # File does not exist - initialise empty store
                self._store = _AccessTokenStore()
            else:
                raise
        except (ValueError, ValidationError):
            # File is of invalid format - reset with empty store
            self._store = _AccessTokenStore()

    def _persist_to_disk(self):
        dirname = os.path.dirname(self.cache_path)
        _ensure_directory_exists(dirname, mode=0o700)
        data = _AccessTokenStoreSchema().dump(self._store)
        with open(self.cache_path, "w") as fp:
            json.dump(data, fp, separators=(",", ":"))


class _AccessTokenSchema(Schema):
    token = fields.String(required=True)
    expires_at = fields.DateTime(data_key="expiresAt", required=True)

    @post_load
    def make_access_token(self, data, **kwargs):
        return AccessToken(**data)


class _AccessTokenStore:
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


class _AccessTokenStoreSchema(Schema):
    tokens = fields.Dict(
        keys=fields.String(),
        values=fields.Nested(_AccessTokenSchema),
        required=True,
    )

    @post_load
    def make_access_token_store(self, data, **kwargs):
        return _AccessTokenStore(**data)


def _is_valid_access_token(access_token_or_none):
    if access_token_or_none is None:
        return False
    else:
        return access_token_or_none.expires_at >= datetime.now(tz=pytz.utc)


def _default_token_cache_path():
    xdg_cache_home = os.environ.get("XDG_CACHE_HOME")
    if not xdg_cache_home:
        xdg_cache_home = os.path.expanduser("~/.cache")
    return os.path.join(xdg_cache_home, "faculty", "token-cache.json")


def _ensure_directory_exists(path, mode):
    try:
        os.makedirs(path, mode=mode)
    except OSError as e:
        if e.errno == errno.EEXIST:
            # Directory already exists
            pass
        else:
            raise
