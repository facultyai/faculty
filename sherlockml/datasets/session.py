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

import os
import time

import boto3

import sherlockml


class SherlockMLDatasetsError(Exception):
    pass


def project_id_from_environment():
    try:
        project_id = os.environ["SHERLOCKML_PROJECT_ID"]
    except KeyError:
        raise SherlockMLDatasetsError(
            "No SHERLOCKML_PROJECT_ID in environment - set the project ID "
            "explicitly to use outside of SherlockML"
        )
    return project_id


SECRETS_CACHE_TTL = 10


class DatasetsSession(object):
    def __init__(self):
        self.secret_client = sherlockml.client("secret")
        self.bucket_cache = {}
        self.secrets_cache = {}

    def bucket(self, project_id):
        if project_id is None:
            project_id = project_id_from_environment()
        if project_id not in self.bucket_cache:
            secrets = self.secret_client.datasets_secrets(project_id)
            self.bucket_cache[project_id] = secrets.bucket
        return self.bucket_cache[project_id]

    def _get_verified_secrets(self, project_id):
        secrets = self.secret_client.datasets_secrets(project_id)
        tries = 1
        while not secrets.verified:
            if tries >= 30:
                raise ValueError("Secrets not verified after 60 seconds")
            time.sleep(2)
            secrets = self.secret_client.datasets_secrets(project_id)
            tries += 1
        return secrets

    def _cached_secrets(self, project_id):
        if (
            project_id not in self.secrets_cache
            or self.secrets_cache[project_id][1] + SECRETS_CACHE_TTL
            < time.time()
        ):
            secrets = self._get_verified_secrets(project_id)
            self.secrets_cache[project_id] = secrets, time.time()
        return self.secrets_cache[project_id][0]

    def s3_client(self, project_id=None):

        if project_id is None:
            project_id = project_id_from_environment()

        secrets = self._cached_secrets(project_id)

        boto_session = boto3.session.Session(
            aws_access_key_id=secrets.access_key,
            aws_secret_access_key=secrets.secret_key,
            region_name="eu-west-1",
        )

        return boto_session.client("s3")


DATASETS_SESSION = None


def get():
    global DATASETS_SESSION
    if DATASETS_SESSION is None:
        DATASETS_SESSION = DatasetsSession()
    return DATASETS_SESSION
