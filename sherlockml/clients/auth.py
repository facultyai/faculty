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


from datetime import datetime, timedelta
import pytz
from collections import namedtuple

import requests


AccessToken = namedtuple('AccessToken', ['token', 'expires_at'])


class HudsonClient(object):
    """Session with the SherlockML authentication service."""

    def __init__(self, url):
        self._session = requests.Session()
        self.url = url

    def get_access_token(self, client_id, client_secret):
        url = '{}/access_token'.format(self.url)
        payload = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'client_credentials'
        }
        response = self._session.post(url, json=payload)
        response.raise_for_status()
        body = response.json()

        token = body['access_token']
        now = datetime.now(tz=pytz.utc)
        expires_at = now + timedelta(seconds=body['expires_in'])

        return AccessToken(token, expires_at)
