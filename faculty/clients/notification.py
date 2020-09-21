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

    Either build this client with a session directly, or use the
    :func:`faculty.client` helper function:

    >>> client = faculty.client("notification")

    Parameters
    ----------
    session : faculty.session.Session
        The session to use to make requests
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

    def publish_template_notifications(self, user_id, source_project_id):
        """Get notification events of a template publishing operation.

        Only returns when success or failure events are received for the given
        source project_id. Events with other project IDs are ignored.

        Parameters
        ----------
        user_id : uuid.UUID
            The user who started the publish operation.
        source_project_id : uuid.UUID
            The project from which the template was published.
        """

        def is_publishing_event(event):
            if not event.event.startswith("@SSE/PROJECT_TEMPLATE_PUBLISH"):
                return False
            body = json.loads(event.data)
            return body.get("sourceProjectId") == str(source_project_id)

        return PublishTemplateNotifications(
            filter(is_publishing_event, self.user_updates(user_id))
        )

    def add_to_project_from_dir_notifications(
        self, user_id, target_project_id
    ):
        """Get notifications of a "add to project from directory" operation.

        Only returns when success or failure events are received for the given
        target project_id. Events with other project IDs are ignored.

        Parameters
        ----------
        user_id : uuid.UUID
            The user who started the publish operation.
        target_project_id : uuid.UUID
            The project from which the template was published.
        """

        def is_publishing_event(event):
            if not event.event.startswith(
                "@SSE/PROJECT_TEMPLATE_APPLY_FROM_DIRECTORY_ADD_TO_PROJECT"
            ):
                return False
            body = json.loads(event.data)
            return body.get("projectId") == str(target_project_id)

        return AddTemplateToProjectFromDirectoryNotifications(
            filter(is_publishing_event, self.user_updates(user_id))
        )


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


class PublishTemplateNotifications:
    """
    Wrapper around notification events from template publishing.

    Parameters
    ----------
    events : generator
            Publishing events. Should be already filtered for a project and
            user IDs.
    """

    def __init__(self, events):
        self.events = events

    def wait_for_completion(self):
        """Block until template publishing completes or fails."""
        for e in self.events:
            if e.event == "@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED":
                body = json.loads(e.data)
                msg = _extract_publishing_error_msg(body)
                raise TemplatePublishingError(msg)
            elif e.event == "@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_COMPLETED":
                return


class AddTemplateToProjectFromDirectoryNotifications:
    """
    Wrapper around notification events from the "add template to project from
    directory" operation

    Parameters
    ----------
    events : generator
            Events from the "add to project from directory" operation.
            Should be already filtered for a project and user IDs.
    """

    def __init__(self, events):
        self.events = events

    def wait_for_completion(self):
        """Block until template publishing completes or fails."""
        for e in self.events:
            if (
                e.event == "@SSE/PROJECT_TEMPLATE_APPLY_FROM_DIRECTORY"
                "_ADD_TO_PROJECT_FAILED"
            ):
                json.loads(e.data)
                # TODO backend needs to return rendering errors here
                msg = "Failed to apply template to directory"
                raise TemplatePublishingError(msg)
            elif (
                e.event == "@SSE/PROJECT_TEMPLATE_APPLY_FROM_DIRECTORY"
                "_ADD_TO_PROJECT_COMPLETED"
            ):
                return
