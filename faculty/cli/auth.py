"""Authenticate with Faculty."""

# Copyright 2016-2018 ASI Data Science
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

import json
import os
import time
import uuid

import requests

import faculty.cli.config
import faculty.cli.version


class AuthenticationError(Exception):
    """Exception for authentication errors."""

    pass


def _token_cache_path():
    """Return the path to a credentials file."""
    xdg_cache_dir = os.environ.get("XDG_CACHE_DIR")

    if not xdg_cache_dir:
        xdg_cache_dir = os.path.expanduser("~/.cache")

    return os.path.join(xdg_cache_dir, "faculty", "token-cache.json")


def _raise_on_hudson_error(response, valid_status_codes=None):
    """Retrieve a description of a Hudson error."""
    if valid_status_codes is None:
        valid_status_codes = [200]

    if response.status_code not in valid_status_codes:
        try:
            json_response = response.json()
            error = json_response.get("error", "")
            error_description = json_response.get("error_description", "")
        except Exception:  # pylint: disable=broad-except
            error = ""
            error_description = ""
        raise AuthenticationError(
            "Failed to authenticate with Faculty: {} {}".format(
                error, error_description
            )
        )


class TokenCache(object):
    """Disk-persisted cache for Faculty access tokens."""

    def __init__(self):
        self._cache_path = _token_cache_path()
        self.load()

    def load(self):
        """Load the cache from disk."""
        try:
            with open(self._cache_path, "r") as fp:
                self._store = json.load(fp)
        except Exception:  # pylint: disable=broad-except
            self._store = {}

    def commit(self):
        """Commit the cache to disk."""
        try:
            os.makedirs(os.path.dirname(self._cache_path), mode=0o700)
        except OSError:
            pass
        try:
            with open(self._cache_path, "w") as fp:
                json.dump(self._store, fp, separators=(",", ":"))
        except Exception:  # pylint: disable=broad-except
            pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.commit()

    def add(self, domain, client_id, token, expires_at):
        if domain not in self._store:
            self._store[domain] = {}
        self._store[domain][client_id] = (token, expires_at)

    def get(self, domain, client_id):
        try:
            token, expires_at = self._store[domain][client_id]
            if expires_at is None or expires_at < time.time():
                return None
            return token
        except KeyError:
            return None


class Session(object):
    """Session with the Faculty authentication service."""

    def __init__(self, url, profile):
        self._session = requests.Session()
        self.url = url
        self.profile = profile
        self._user_id = None

    def _get_token(self):
        url = self.url + "/access_token"
        payload = {
            "client_id": self.profile.client_id,
            "client_secret": self.profile.client_secret,
            "grant_type": "client_credentials",
        }
        resp = self._session.post(url, data=payload)
        _raise_on_hudson_error(resp)
        body = resp.json()
        token = body["access_token"]
        expires_at = time.time() + float(body["expires_in"])
        return token, expires_at

    @property
    def token(self):
        """Get an authentication token."""
        with TokenCache() as cache:
            token = cache.get(self.profile.domain, self.profile.client_id)
            if token is None:
                token, expires_at = self._get_token()
                cache.add(
                    self.profile.domain,
                    self.profile.client_id,
                    token,
                    expires_at,
                )
        return token

    def auth_headers(self):
        """Return HTTP Authorization headers."""
        return {"Authorization": "Bearer {}".format(self.token)}

    def _get_account(self):
        url = self.url + "/authenticate"
        headers = {"User-Agent": faculty.cli.version.user_agent()}
        headers.update(self.auth_headers())
        resp = self._session.get(url, headers=headers)
        _raise_on_hudson_error(resp)
        body = resp.json()
        return body["account"]

    @property
    def user_id(self):
        """Return ID of authenticated user."""
        if self._user_id is None:
            self._user_id = uuid.UUID(self._get_account()["userId"])
        return self._user_id

    @property
    def username(self):
        """Return name of authenticated user."""
        return self._get_account()["username"]


_hudson_session = None


def _get_session():
    global _hudson_session
    if _hudson_session is None:
        profile = faculty.cli.config.get_profile()
        url = faculty.cli.config.hudson_url()
        _hudson_session = Session(url, profile)
    return _hudson_session


def token():
    """Get authentication token."""
    session = _get_session()
    return session.token


def auth_headers():
    """Get authentication headers."""
    session = _get_session()
    return session.auth_headers()


def user_id():
    """Get session user ID."""
    session = _get_session()
    return session.user_id


def username():
    """Get session user name."""
    session = _get_session()
    return session.username


def credentials_valid(profile):
    """Determine if the given credentials are valid for a given domain."""
    url = "{}://hudson.{}".format(profile.protocol, profile.domain)
    session = Session(url, profile)
    try:
        session.token
    except faculty.cli.auth.AuthenticationError:
        return False
    return True
