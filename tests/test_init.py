# Copyright 2018-2019 ASI Data Science
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
    session_get_mock = mocker.patch("faculty.session.Session.get")
    for_resource_mock = mocker.patch("faculty.clients.for_resource")

    options = {"foo": "bar"}
    faculty.client("test-resource", **options)

    session_get_mock.assert_called_once_with(**options)
    for_resource_mock.assert_called_once_with("test-resource")

    returned_session = session_get_mock.return_value
    returned_class = for_resource_mock.return_value
    returned_class.assert_called_once_with(returned_session)
