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


from datetime import datetime

from dateutil.tz import UTC  # type: ignore

from sherlockml.clients.workspace import FileNodeSchema, File


FILE = File(
    path="/path/to/test-directory/test-file",
    name="test-file",
    last_modified=datetime(2017, 8, 9, 14, 21, 13, tzinfo=UTC),
    size=906,
)

FILE_BODY = {
    "path": FILE.path,
    "name": FILE.name,
    "last_modified": "2017-08-09T14:21:13Z",
    "size": FILE.size,
    "type": "file",
}


def test_file_node_schema_file():
    assert FileNodeSchema().load(FILE_BODY) == FILE
