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


from faculty.clients.account import AccountClient
from faculty.clients.cluster import ClusterClient
from faculty.clients.environment import EnvironmentClient
from faculty.clients.experiment import ExperimentClient
from faculty.clients.job import JobClient
from faculty.clients.log import LogClient
from faculty.clients.project import ProjectClient
from faculty.clients.report import ReportClient
from faculty.clients.secret import SecretClient
from faculty.clients.server import ServerClient
from faculty.clients.user import UserClient
from faculty.clients.workspace import WorkspaceClient


CLIENT_FOR_RESOURCE = {
    "account": AccountClient,
    "cluster": ClusterClient,
    "environment": EnvironmentClient,
    "experiment": ExperimentClient,
    "log": LogClient,
    "job": JobClient,
    "project": ProjectClient,
    "report": ReportClient,
    "secret": SecretClient,
    "server": ServerClient,
    "user": UserClient,
    "workspace": WorkspaceClient,
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
