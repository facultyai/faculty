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


import json
import uuid

import pytest
import sseclient
from sseclient import Event

from faculty.clients.notification import (
    NotificationClient,
    TemplatePublishingError,
)


PROJECT_ID = uuid.uuid4()
USER_ID_STRING = "3024d586-6a4a-4fc1-98a9-c6135b163f17"
USER_ID = uuid.UUID(USER_ID_STRING)


def test_user_updates(mocker):
    mock_response = mocker.Mock()
    mocker.patch.object(
        NotificationClient, "_get_raw", return_value=mock_response
    )
    mock_sse_client = mocker.Mock()
    mock_events = mocker.Mock()
    mock_sse_client.events = mocker.Mock(return_value=mock_events)
    mocker.patch.object(sseclient, "SSEClient", return_value=mock_sse_client)

    client = NotificationClient(mocker.Mock())
    client.user_updates(USER_ID)

    NotificationClient._get_raw.assert_called_once_with(
        "api/updates/user/{}".format(USER_ID_STRING), stream=True
    )
    sseclient.SSEClient.assert_called_once_with(mock_response)
    mock_sse_client.events.assert_called_once_with()


def test_wait_for_completion_success(mocker):
    def events():
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_COMPLETED",
            data=json.dumps({"sourceProjectId": str(PROJECT_ID)}),
        )

    user_updates_mock = mocker.patch.object(
        NotificationClient, "user_updates", return_value=events()
    )
    client = NotificationClient(mocker.Mock())

    publish_notifications = client.publish_template_notifications(
        USER_ID, PROJECT_ID
    )
    publish_notifications.wait_for_completion()
    user_updates_mock.assert_called_once_with(USER_ID)


def test_publish_template_notifications_filter(mocker):
    def events():
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED",
            data=json.dumps({"sourceProjectId": "other project ID"}),
        )
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED",
            data=json.dumps({"sourceProjectId": str(PROJECT_ID)}),
        )
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_COMPLETED",
            data=json.dumps({"sourceProjectId": "other project ID"}),
        )
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_COMPLETED",
            data=json.dumps({"sourceProjectId": str(PROJECT_ID)}),
        )
        yield Event(
            event="@SSE/OTHER",
            data=json.dumps({"sourceProjectId": str(PROJECT_ID)}),
        )

    user_updates_mock = mocker.patch.object(
        NotificationClient, "user_updates", return_value=events()
    )
    client = NotificationClient(mocker.Mock())

    publish_notifications = client.publish_template_notifications(
        USER_ID, PROJECT_ID
    )
    event_data = [(e.event, e.data) for e in publish_notifications.events]

    assert event_data == [
        (
            "@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED",
            json.dumps({"sourceProjectId": str(PROJECT_ID)}),
        ),
        (
            "@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_COMPLETED",
            json.dumps({"sourceProjectId": str(PROJECT_ID)}),
        ),
    ]
    user_updates_mock.assert_called_once_with(USER_ID)


@pytest.mark.parametrize("error_code", ["name_conflict", "unexpected_error"])
def test_wait_for_completion_errors(mocker, error_code):
    def events():
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED",
            data=json.dumps(
                {
                    "sourceProjectId": str(PROJECT_ID),
                    "errorCode": error_code,
                    "error": "dummy error message",
                }
            ),
        )

    user_updates_mock = mocker.patch.object(
        NotificationClient, "user_updates", return_value=events()
    )
    client = NotificationClient(mocker.Mock())
    publish_notifications = client.publish_template_notifications(
        USER_ID, PROJECT_ID
    )
    with pytest.raises(TemplatePublishingError, match="dummy error message"):
        publish_notifications.wait_for_completion()
    user_updates_mock.assert_called_once_with(USER_ID)


def test_wait_for_completion_rendering_errors(mocker):
    def events():
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED",
            data=json.dumps(
                {
                    "sourceProjectId": str(PROJECT_ID),
                    "errorCode": "template_rendering_error",
                    "errors": [
                        {"error": "Unexpected key { abc }", "path": "a.py"},
                        {"error": "Unexpected key { abc }", "path": "a/b.py"},
                    ],
                }
            ),
        )

    user_updates_mock = mocker.patch.object(
        NotificationClient, "user_updates", return_value=events()
    )
    client = NotificationClient(mocker.Mock())
    publish_notifications = client.publish_template_notifications(
        USER_ID, PROJECT_ID
    )
    expected_message = """Failed to render the template with default parameters:
\tUnexpected key { abc } in file a.py
\tUnexpected key { abc } in file a/b.py"""
    with pytest.raises(TemplatePublishingError, match=expected_message):
        publish_notifications.wait_for_completion()
    user_updates_mock.assert_called_once_with(USER_ID)


def test_wait_for_completion_rendering_unexpected_error_code(mocker):
    def events():
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED",
            data=json.dumps(
                {
                    "sourceProjectId": str(PROJECT_ID),
                    "errorCode": "unkown error code",
                }
            ),
        )

    user_updates_mock = mocker.patch.object(
        NotificationClient, "user_updates", return_value=events()
    )
    client = NotificationClient(mocker.Mock())
    publish_notifications = client.publish_template_notifications(
        USER_ID, PROJECT_ID
    )
    with pytest.raises(
        TemplatePublishingError, match="Unexpected error code received"
    ):
        publish_notifications.wait_for_completion()
    user_updates_mock.assert_called_once_with(USER_ID)


def test_wait_for_completion_rendering_no_error_code(mocker):
    def events():
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_PUBLISH_NEW_FAILED",
            data=json.dumps({"sourceProjectId": str(PROJECT_ID)}),
        )

    user_updates_mock = mocker.patch.object(
        NotificationClient, "user_updates", return_value=events()
    )
    client = NotificationClient(mocker.Mock())
    publish_notifications = client.publish_template_notifications(
        USER_ID, PROJECT_ID
    )
    with pytest.raises(
        TemplatePublishingError, match="Unexpected server response"
    ):
        publish_notifications.wait_for_completion()
    user_updates_mock.assert_called_once_with(USER_ID)


def test_add_to_project_from_dir_notifications_filter(mocker):
    def events():
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_APPLY_FROM_DIRECTORY"
            "_ADD_TO_PROJECT_FAILED",
            data=json.dumps({"projectId": "other project ID"}),
        )
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_APPLY_FROM_DIRECTORY"
            "_ADD_TO_PROJECT_FAILED",
            data=json.dumps({"projectId": str(PROJECT_ID)}),
        )
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_APPLY_FROM_DIRECTORY"
            "_ADD_TO_PROJECT_COMPLETED",
            data=json.dumps({"projectId": "other project ID"}),
        )
        yield Event(
            event="@SSE/PROJECT_TEMPLATE_APPLY_FROM_DIRECTORY"
            "_ADD_TO_PROJECT_COMPLETED",
            data=json.dumps({"projectId": str(PROJECT_ID)}),
        )
        yield Event(
            event="@SSE/OTHER", data=json.dumps({"projectId": str(PROJECT_ID)})
        )

    user_updates_mock = mocker.patch.object(
        NotificationClient, "user_updates", return_value=events()
    )
    client = NotificationClient(mocker.Mock())

    publish_notifications = client.add_to_project_from_dir_notifications(
        USER_ID, PROJECT_ID
    )
    event_data = [(e.event, e.data) for e in publish_notifications.events]

    assert event_data == [
        (
            "@SSE/PROJECT_TEMPLATE_APPLY_FROM_DIRECTORY"
            "_ADD_TO_PROJECT_FAILED",
            json.dumps({"projectId": str(PROJECT_ID)}),
        ),
        (
            "@SSE/PROJECT_TEMPLATE_APPLY_FROM_DIRECTORY"
            "_ADD_TO_PROJECT_COMPLETED",
            json.dumps({"projectId": str(PROJECT_ID)}),
        ),
    ]
    user_updates_mock.assert_called_once_with(USER_ID)
