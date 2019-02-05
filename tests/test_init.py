# Copyright 2018-2019 Faculty Science Limited
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


import faculty


def test_client(mocker):
    get_session_mock = mocker.patch("faculty.session.get_session")
    for_resource_mock = mocker.patch("faculty.clients.for_resource")

    options = {"foo": "bar"}
    faculty.client("test-resource", **options)

    get_session_mock.assert_called_once_with(**options)
    for_resource_mock.assert_called_once_with("test-resource")

    returned_session = get_session_mock.return_value
    returned_class = for_resource_mock.return_value
    returned_class.assert_called_once_with(returned_session)
