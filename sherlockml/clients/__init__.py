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


from sherlockml.clients.user import UserClient
from sherlockml.clients.project import ProjectClient
from sherlockml.clients.server import ServerClient
from sherlockml.clients.cluster import ClusterClient
from sherlockml.clients.secret import SecretClient


CLIENT_FOR_RESOURCE = {
    "user": UserClient,
    "project": ProjectClient,
    "server": ServerClient,
    "cluster": ClusterClient,
    "secret": SecretClient,
}


def for_resource(resource):
    try:
        return CLIENT_FOR_RESOURCE[resource]
    except KeyError:
        raise ValueError(
            "unsupported resource {}, choose one of {}".format(
                resource, set(CLIENT_FOR_RESOURCE.keys())
            )
        )
