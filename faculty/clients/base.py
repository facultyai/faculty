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
Common functionality of Faculty service clients.
"""


import requests

from marshmallow import Schema, fields, ValidationError, EXCLUDE
from attr import attrs, attrib

from faculty.clients.auth import FacultyAuth


class HttpError(Exception):
    """An HTTP error occurred.

    Parameters
    ----------
    response : requests.Response
        The HTTP response.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    def __init__(self, response, error=None, error_code=None):
        self.response = response
        self.error = error
        self.error_code = error_code


HTTPError = HttpError  # For backwards compatiblity


class BadRequest(HttpError):
    """A 400 Bad Request HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


class Unauthorized(HttpError):
    """A 401 Unauthorized HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


class Forbidden(HttpError):
    """A 403 Forbidden HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


class NotFound(HttpError):
    """A 404 Not Found HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


class MethodNotAllowed(HttpError):
    """A 405 Method Not Allowed HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


class Conflict(HttpError):
    """A 409 Conflict HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


class InternalServerError(HttpError):
    """A 500 Internal Server Error HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


class BadGateway(HttpError):
    """A 502 Bad Gateway HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


class ServiceUnavailable(HttpError):
    """A 503 Service Unavailable HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


class GatewayTimeout(HttpError):
    """A 504 Gateway Timeout HTTP response was returned.

    Parameters
    ----------
    response : requests.Response
        The HTTP respponse.
    error : str, optional
        A descriptive error message returned by the server.
    error_code : str, optional
        A key returned by the server identifying the specific cause of the
        error.
    """

    pass


@attrs
class ServerSentEventMessage(object):
    """Server sent event message.

    Parameters
    ----------
    id : uuid.UUID
        The ID of the server sent event message.
    event : str
        The type of server sent event message.
    data : str
        The server sent event message data.

    """

    id = attrib()
    event = attrib()
    data = attrib()


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


class BaseClient(object):
    """Base class with core functionality for Faculty service clients."""

    SERVICE_NAME = None

    def __init__(self, url, session):
        self.url = url
        self.session = session
        self._http_session_cache = None

    @property
    def http_session(self):
        """A requests session with authentication against Faculy services."""
        if self._http_session_cache is None:
            self._http_session_cache = requests.Session()
            self._http_session_cache.auth = FacultyAuth(self.session)
        return self._http_session_cache

    def _request(self, method, endpoint, check_status=True, *args, **kwargs):
        """Perform an HTTP request.

        This method should not be called from subclasses directly. Instead,
        call one of the HTTP verb-specific methods. If it does not exist yet
        for the HTTP method you need, contribute it.
        """
        endpoint_url = self.url.rstrip("/") + "/" + endpoint.lstrip("/")
        response = self.http_session.request(
            method, endpoint_url, *args, **kwargs
        )
        if check_status:
            _check_status(response)
        return response

    def _get_raw(self, endpoint, *args, **kwargs):
        """Perform a GET request and return the requests response object."""
        return self._request("GET", endpoint, *args, **kwargs)

    def _get(self, endpoint, schema, **kwargs):
        """Perform a GET request and parse the response."""
        response = self._get_raw(endpoint, **kwargs)
        return _deserialise_response(schema, response)

    def _post_raw(self, endpoint, *args, **kwargs):
        """Perform a POST request and return the requests response object."""
        return self._request("POST", endpoint, *args, **kwargs)

    def _post(self, endpoint, schema, **kwargs):
        """Perform a POST request and parse the response."""
        response = self._post_raw(endpoint, **kwargs)
        return _deserialise_response(schema, response)

    def _put_raw(self, endpoint, *args, **kwargs):
        """Perform a PUT request and return the requests response object."""
        return self._request("PUT", endpoint, *args, **kwargs)

    def _put(self, endpoint, schema, **kwargs):
        """Perform a PUT request and parse the response."""
        response = self._put_raw(endpoint, **kwargs)
        return _deserialise_response(schema, response)

    def _patch_raw(self, endpoint, *args, **kwargs):
        """Perform a PATCH request and return the requests response object."""
        return self._request("PATCH", endpoint, *args, **kwargs)

    def _patch(self, endpoint, schema, **kwargs):
        """Perform a PATCH request and parse the response."""
        response = self._patch_raw(endpoint, **kwargs)
        return _deserialise_response(schema, response)

    def _delete_raw(self, endpoint, *args, **kwargs):
        """Perform a DELETE request and return the requests response object."""
        return self._request("DELETE", endpoint, *args, **kwargs)

    def _delete(self, endpoint, schema, **kwargs):
        """Perform a DELETE request and parse the response."""
        response = self._delete_raw(endpoint, **kwargs)
        return _deserialise_response(schema, response)

    def _stream_server_sent_events(self, endpoint):
        """Stream from a SSE endpoint.

        Parameters
        ----------
        endpoint : str
            HTTP request endpoint.

        Yields
        ------
        ServerSentEventMessage
        """
        response = self._get_raw(endpoint, stream=True)

        def sse_stream_iterator():
            buf = []
            for line in response.iter_lines(decode_unicode=True):
                if not line.strip():
                    yield _sse_message_from_lines(buf)
                    buf = []
                else:
                    buf.append(line)

        with response:
            yield from sse_stream_iterator()


class BaseSchema(Schema):
    """Base class for marshmallow schemas in this library."""

    class Meta:
        unknown = EXCLUDE


class _ErrorSchema(BaseSchema):
    error = fields.String(missing=None)
    error_code = fields.String(data_key="errorCode", missing=None)


def _check_status(response):
    if response.status_code >= 400:
        cls = HTTP_ERRORS.get(response.status_code, HttpError)
        try:
            data = _ErrorSchema().load(response.json())
        except (ValueError, ValidationError):
            data = {}
        raise cls(response, data.get("error"), data.get("error_code"))


def _deserialise_response(schema, response):
    response_json = response.json()
    return schema.load(response_json)


def _sse_message_from_lines(lines):
    """Parses server sent event stream.

    Parameters
    ----------
    lines : List[str]
        Lines from server sent event endpoint.

    Returns
    -------
    ServerSentEventMessage
    """
    id = None
    event = None
    data = []
    for line in lines:
        if line.startswith("id:"):
            id = int(line[3:].strip())
        elif line.startswith("event:"):
            event = line[6:].strip()
        elif line.startswith("data:"):
            data.append(line[5:].strip())
        else:
            raise ValueError("unexpected sse line: {}".format(line))

    return ServerSentEventMessage(id, event, "\n".join(data))
