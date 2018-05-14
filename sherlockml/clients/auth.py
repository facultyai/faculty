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


from collections import namedtuple
from datetime import datetime, timedelta

import requests
import pytz


AccessToken = namedtuple('AccessToken', ['token', 'expires_at'])


class AccessTokenClient(object):
    """Client for getting access tokens for accessing SherlockML services."""

    def __init__(self, hudson_url):
        self._session = requests.Session()
        self.hudson_url = hudson_url

    def get_access_token(self, client_id, client_secret):
        endpoint = '{}/access_token'.format(self.hudson_url)
        payload = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials'
        }
        response = self._session.post(endpoint, json=payload)
        response.raise_for_status()
        body = response.json()

        token = body['access_token']
        now = datetime.now(tz=pytz.utc)
        expires_at = now + timedelta(seconds=body['expires_in'])

        return AccessToken(token, expires_at)
