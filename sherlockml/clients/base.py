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
from marshmallow import Schema, fields, ValidationError
from six.moves import urllib

from sherlockml.clients.auth import SherlockMLAuth


class InvalidResponse(Exception):
    pass


class HTTPError(Exception):
    def __init__(self, response, error=None):
        self.response = response
        self.error = error


class BadRequest(HTTPError):
    pass


class Unauthorized(HTTPError):
    pass


class Forbidden(HTTPError):
    pass


class NotFound(HTTPError):
    pass


class MethodNotAllowed(HTTPError):
    pass


class Conflict(HTTPError):
    pass


class InternalServerError(HTTPError):
    pass


class BadGateway(HTTPError):
    pass


class ServiceUnavailable(HTTPError):
    pass


class GatewayTimeout(HTTPError):
    pass


def _service_url(profile, service, endpoint=""):
    host = "{}.{}".format(service, profile.domain)
    url_parts = (profile.protocol, host, endpoint, None, None)
    return urllib.parse.urlunsplit(url_parts)


HTTP_ERRORS = {
    400: BadRequest,
    401: Unauthorized,
    403: Forbidden,
    404: NotFound,
    405: MethodNotAllowed,
    409: Conflict,
    500: InternalServerError,
    502: BadGateway,
    503: ServiceUnavailable,
    504: GatewayTimeout,
}


class ErrorSchema(Schema):
    error = fields.String(required=True)


def _extract_error(response):
    try:
        data = response.json()
        return ErrorSchema().load(data)["error"]
    except (ValueError, ValidationError):
        return None


def _check_status(response):
    if response.status_code >= 400:
        cls = HTTP_ERRORS.get(response.status_code, HTTPError)
        raise cls(response, _extract_error(response))


def _deserialise_response(schema, response):
    try:
        response_json = response.json()
    except ValueError:
        raise InvalidResponse("response body was not valid JSON")

    try:
        data = schema.load(response_json)
    except ValidationError:
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
