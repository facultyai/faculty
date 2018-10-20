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

import requests
import marshmallow
from six.moves import urllib

from sherlockml.clients.auth import SherlockMLAuth


class BadResponseStatus(Exception):
    def __init__(self, response):
        self.response = response


class Unauthorized(BadResponseStatus):
    pass


class NotFound(BadResponseStatus):
    pass


class InvalidResponse(Exception):
    pass


def _service_url(profile, service, endpoint=""):
    host = "{}.{}".format(service, profile.domain)
    url_parts = (profile.protocol, host, endpoint, None, None)
    return urllib.parse.urlunsplit(url_parts)


def _check_status(response):
    if response.status_code == 401:
        raise Unauthorized(response)
    elif response.status_code == 404:
        raise NotFound(response)
    elif response.status_code >= 400:
        raise BadResponseStatus(response)


def _deserialise_response(schema, response):
    try:
        response_json = response.json()
    except ValueError:
        raise InvalidResponse("response body was not valid JSON")

    try:
        data = schema.load(response_json)
    except marshmallow.ValidationError:
        # TODO: log validation errors and possibly include them in raised
        # exception
        raise InvalidResponse("response content did not match expected format")

    return data


class BaseClient(object):

    SERVICE_NAME = None

    def __init__(self, profile):
        if self.SERVICE_NAME is None:
            raise RuntimeError(
                "must set SERVICE_NAME in subclasses of BaseClient"
            )
        self.profile = profile
        self._http_session_cache = None

    @property
    def http_session(self):
        if self._http_session_cache is None:
            self._http_session_cache = requests.Session()
            self._http_session_cache.auth = SherlockMLAuth(
                _service_url(self.profile, "hudson"),
                self.profile.client_id,
                self.profile.client_secret,
            )
        return self._http_session_cache

    def _request(self, method, endpoint, check_status=True, *args, **kwargs):
        url = _service_url(self.profile, self.SERVICE_NAME, endpoint)
        response = self.http_session.request(method, url, *args, **kwargs)
        if check_status:
            _check_status(response)
        return response

    def _get_raw(self, endpoint, *args, **kwargs):
        return self._request("GET", endpoint, *args, **kwargs)

    def _get(self, endpoint, schema, **kwargs):
        response = self._get_raw(endpoint, **kwargs)
        return _deserialise_response(schema, response)

    def _post_raw(self, endpoint, *args, **kwargs):
        return self._request("POST", endpoint, *args, **kwargs)

    def _post(self, endpoint, schema, **kwargs):
        response = self._post_raw(endpoint, **kwargs)
        return _deserialise_response(schema, response)

    def _put_raw(self, endpoint, *args, **kwargs):
        return self._request("PUT", endpoint, *args, **kwargs)

    def _put(self, endpoint, schema, **kwargs):
        response = self._put_raw(endpoint, **kwargs)
        return _deserialise_response(schema, response)

    def _delete_raw(self, endpoint, *args, **kwargs):
        return self._request("DELETE", endpoint, *args, **kwargs)

    def _delete(self, endpoint, schema, **kwargs):
        response = self._delete_raw(endpoint, **kwargs)
        return _deserialise_response(schema, response)
