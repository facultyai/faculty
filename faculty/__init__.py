# Copyright 2018-2021 Faculty Science Limited
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
    """Construct a client for a Faculty resource.

    Parameters
    ----------
    resource : str
        The resource to construct a client for.
    credentials_path : str or pathlib.Path, optional
        The path of a file to load Faculty credentials from.
    profile_name : str, optional
        The name of the profile to read from the credentials file. The default
        is 'default'.
    domain : str, optional
        The domain to access Faculty services at. This is provided when you
        generate credentials in the platform.
    protocol : str, optional
        Either 'http' or 'https' (the default).
    client_id : str, optional
        The Faculty client ID to user. This is provided when you generate
        credentials in the platform.
    client_secret : str, optional
        The Faculty client secret to use. This is provided when you generate
        credentials in the platform.
    access_token_cache : faculty.session.accesstoken.AccessTokenMemoryCache or
    faculty.session.accesstoken.AccessTokenMemoryCache, optional
        Set the access token cache used. The default is an
        AccessTokenMemoryCache.

    Examples
    --------
    To construct a client loading default configuration and credentials (from
    ~/.config/faculty/credentials or from environment variables), simply call
    this function with the resource type as a string:

    >>> faculty.client("account")
    <faculty.clients.account.AccountClient object at 0x10d4b7fd0>

    To load a different configuration file, or load a profile from the file
    other than 'default', pass the `credentials_path` or `profile_name`
    arguments respectively:

    >>> faculty.client(
    ...     "account",
    ...     credentials_path="other/credentials",
    ...     profile_name="custom",
    ... )
    <faculty.clients.account.AccountClient object at 0x10e447550>

    To set any of the domain, protocol, client ID or client secret, pass their
    respective arguments. These always take higher precendence than values read
    from configuration files or environment variables:

    >>> faculty.client(
    ...     "account",
    ...     domain="services.faculty.myorganisation.com",
    ...     protocol="http",
    ...     client_id="d047dad1-175b-4810-84b1-c0ff6bbcf456",
    ...     client_secret="Vk4a1FgSKlTplkK0GkToAdRTdsoU4XHuwCMQ",
    ... )
    <faculty.clients.account.AccountClient object at 0x10e4472b0>
    """

    client_class = faculty.clients.for_resource(resource)

    if client_class.SERVICE_NAME is None:
        raise ValueError(
            "Cannot infer URL for resource {} - its client does not define a "
            "service name".format(resource)
        )

    session = faculty.session.get_session(
        credentials_path=credentials_path,
        profile_name=profile_name,
        domain=domain,
        protocol=protocol,
        client_id=client_id,
        client_secret=client_secret,
        access_token_cache=access_token_cache,
    )

    url = session.service_url(client_class.SERVICE_NAME)

    return client_class(url, session)
