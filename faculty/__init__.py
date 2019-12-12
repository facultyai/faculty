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


import faculty.session
import faculty.clients


def client(
    resource,
    credentials_path=None,
    profile_name=None,
    domain=None,
    protocol=None,
    client_id=None,
    client_secret=None,
    access_token_cache=None,
):
    session = faculty.session.get_session(
        credentials_path=credentials_path,
        profile_name=profile_name,
        domain=domain,
        protocol=protocol,
        client_id=client_id,
        client_secret=client_secret,
        access_token_cache=access_token_cache,
    )
    client_class = faculty.clients.for_resource(resource)
    return client_class(session)
