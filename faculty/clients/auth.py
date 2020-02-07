# Copyright 2018-2020 Faculty Science Limited
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


from requests.auth import AuthBase


class FacultyAuth(AuthBase):
    """Requests auth implementation for accessing Faculty services.

    Parameters
    ----------
    session : faculty.session.Session
        The Faculty session to authenticate with

    To perform an authenticated request against a Faculty service, first
    construct an instance of this class with a session:

    >>> import faculty.session
    >>> session = faculty.session.get_session()
    >>> auth = FacultyAuth(session)

    then pass it as the ``auth`` argument when making a request with
    ``requests``:

    >>> import requests
    >>> requests.get(
            'https://servicename.services.example.my.faculty.ai',
            auth=auth
        )

    You can also set it as the ``auth`` attribute on a
    :class:`requests.Session`, so that subsequent requests will be
    authenticated automatically:

    >>> import requests
    >>> session = requests.Session()
    >>> session.auth = auth
    """

    def __init__(self, session):
        self.session = session

    def __call__(self, request):
        access_token = self.session.access_token()

        header_content = "Bearer {}".format(access_token.token)
        request.headers["Authorization"] = header_content

        return request
