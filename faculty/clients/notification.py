# Copyright 2020 Faculty Science Limited
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
Consume notifications from the Faculty frontend.
"""

import json

import sseclient

from faculty.clients.base import BaseClient


class NotificationClient(BaseClient):
    """Client to listen on notifications from the Faculty frontend.

    Either build this client with a session directly:

    >>> from faculty.clients.notification import NotificationClient
    >>> session = faculty.session.get_session()
    >>> notification_client = NotificationClient(session,
    >>> protocol="https", host="my-domain.my.faculty.ai")

    or use the :func:`faculty.client` helper function to create the client
    with default values:

    >>> client = faculty.client("notification")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
    protocol : str
        Protocol to use for requests to the frontend. (`http` when used from
        within the platform via internal DNS or `https` when using external
        domain name.)
    """

    _SERVICE_NAME = "frontend"

    def user_updates(self, user_id):
        """Get notification events for the given user.

        Parameters
        ----------
        user_id : uuid.UUID
            ID of the user to get updates for.

        Returns
        -------
        generator
            Server-sent events
        """
        endpoint = "api/updates/user/{}".format(user_id)
        response = self._get_raw(endpoint, stream=True)
        client = sseclient.SSEClient(response)
        return client.events()

    def check_publish_template_result(self, events, project_id):
        """Handle results of a template publishing operation.

        Only returns when success or failure events are received for the given
        source project_id. Events with other project IDs are ignored.

        Parameters
        ----------
        project_id : uuid.UUID
            The project from which the template was published.
        events : generator
            The value that was returned from
            :func:`faculty.clients.notification.NotificationClient.user_updates`
        """
        for e in events:
            body = json.loads(e.data)
            if body["sourceProjectId"] == str(project_id):
                if e.event == "@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED":
                    msg = _extract_publishing_error_msg(body)
                    raise TemplatePublishingError(msg)
                elif e.event == "@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_COMPLETED":
                    return


def _extract_publishing_error_msg(error_body):
    try:
        code = error_body["errorCode"]
        if code in {"name_conflict", "unexpected_error"}:
            return error_body["error"]
        elif code == "template_rendering_error":
            errors = error_body["errors"]
            msg = "Failed to render the template with default parameters:"
            for e in errors:
                msg += "\n\t{} in file {}".format(e["error"], e["path"])
            return msg
        else:
            return "Unexpected error code received"
    except KeyError:
        return "Unexpected server response"


class TemplatePublishingError(Exception):
    pass
