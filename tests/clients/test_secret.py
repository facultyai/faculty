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


import pytest
from marshmallow import ValidationError

from sherlockml.clients.secret import DatasetsSecrets, DatasetsSecretsSchema


TEST_SECRETS = DatasetsSecrets(
    bucket="test-bucket",
    access_key="test-access-key",
    secret_key="test-secret-key",
    verified=True,
)

TEST_SECRETS_BODY = {
    "bucket": TEST_SECRETS.bucket,
    "access_key": TEST_SECRETS.access_key,
    "secret_key": TEST_SECRETS.secret_key,
    "verified": TEST_SECRETS.verified,
}


def test_datasets_secrets_schema():
    assert DatasetsSecretsSchema().load(TEST_SECRETS_BODY) == TEST_SECRETS


def test_datasets_secrets_invalid():
    with pytest.raises(ValidationError):
        DatasetsSecretsSchema().load({})
