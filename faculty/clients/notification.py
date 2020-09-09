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
Interact with the Faculty frontend.
"""

import json

from six.moves import urllib
import sseclient

from faculty.clients.base import BaseClient


class NotificationClient(BaseClient):
    """
    TODO
    from faculty.clients.notification import NotificationClient
    session = faculty.session.get_session()
    notification_client = NotificationClient(session,
    protocol="https", host="gollum.platform.asidata.science")
    """

    def __init__(self, session, protocol=None, host=None):
        self.protocol = protocol or session.profile.protocol
        self.host = host or "frontend.{}".format(session.profile.domain)
        super(NotificationClient, self).__init__(session)

    def _service_url(self, endpoint):
        url_parts = (self.profile.protocol, self.host, endpoint, None, None)
        return urllib.parse.urlunsplit(url_parts)

    def user_updates(self, user_id):
        endpoint = "api/updates/user/{}".format(user_id)
        response = self._get_raw(endpoint, stream=True)
        client = sseclient.SSEClient(response)
        return client.events()

    def check_publish_template_result(self, events, project_id):
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
