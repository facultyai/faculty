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

from faculty.clients.invitation import InvitationClient
from faculty.clients.user import GlobalRole


def test_invite_user(mocker):
    mocker.patch.object(InvitationClient, "_post_raw")

    client = InvitationClient(mocker.Mock(), mocker.Mock())
    client.invite_user(
        "test@email.xyz",
        global_roles=[
            GlobalRole.BASIC_USER,
            GlobalRole.FULL_USER,
            GlobalRole.ADMIN,
        ],
    )

    InvitationClient._post_raw.assert_called_once_with(
        "/admin/invitation",
        json={
            "email": "test@email.xyz",
            "globalRoles": [
                "global-basic-user",
                "global-full-user",
                "global-admin",
            ],
        },
    )


def test_invite_user_default_global_roles(mocker):
    mocker.patch.object(InvitationClient, "_post_raw")

    client = InvitationClient(mocker.Mock(), mocker.Mock())
    client.invite_user("test@email.xyz")

    InvitationClient._post_raw.assert_called_once_with(
        "/admin/invitation",
        json={"email": "test@email.xyz", "globalRoles": ["global-basic-user"]},
    )
