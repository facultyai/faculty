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

"""
Load library configuration from files, environment variables or code.
"""


import os
import warnings
from collections import namedtuple

from six.moves.configparser import ConfigParser, NoSectionError, NoOptionError


Profile = namedtuple(
    "Profile", ["domain", "protocol", "client_id", "client_secret"]
)

DEFAULT_PROFILE = "default"
DEFAULT_DOMAIN = "services.cloud.my.faculty.ai"
DEFAULT_PROTOCOL = "https"


def load(path):
    """Read the Faculty configuration from a file.

    Parameters
    ----------
    path : str or pathlib.Path
        The path of the file to load configuration from.

    Returns
    -------
    Dict[str, Profile]
        The profiles loaded from the file, keyed by their names.
    """

    parser = ConfigParser()
    parser.read(str(path))

    def _get(section, option):
        try:
            return parser.get(section, option)
        except (NoSectionError, NoOptionError):
            return None

    profiles = {}

    for section in parser.sections():
        profiles[section] = Profile(
            domain=_get(section, "domain"),
            protocol=_get(section, "protocol"),
            client_id=_get(section, "client_id"),
            client_secret=_get(section, "client_secret"),
        )

    return profiles


def load_profile(path, profile):
    """Read a single profile from a file.

    Parameters
    ----------
    path : str or pathlib.Path
        The path of the file to load the profile from.
    profile : str
        The name of the profile to load.

    Returns
    -------
    Profile
        The loaded profile. If the requested profile is not present, an empty
        profile (with all None values) is returned.
    """
    profiles = load(path)
    try:
        return profiles[profile]
    except KeyError:
        return Profile(None, None, None, None)


def _default_credentials_path():

    xdg_config_home = os.environ.get("XDG_CONFIG_HOME")
    if not xdg_config_home:
        xdg_config_home = os.path.expanduser("~/.config")

    default_path = os.path.join(xdg_config_home, "faculty", "credentials")
    legacy_path = os.path.join(xdg_config_home, "sherlockml", "credentials")

    if not os.path.exists(default_path) and os.path.exists(legacy_path):
        template = (
            "Reading credentials from {legacy_path}. Credentials at this path "
            "are deprecated - please migrate by moving them to {default_path}."
        )
        warnings.warn(
            template.format(legacy_path=legacy_path, default_path=default_path)
        )
        return legacy_path
    else:
        return default_path


def _get_deprecated_env_var(key, expected_key):
    value = os.getenv(key)

    if value:
        template = (
            "The environment variable {key} is deprecated. "
            "Please migrate by using {expected_key}."
        )
        warnings.warn(template.format(key=key, expected_key=expected_key))

    return value


def resolve_credentials_path(credentials_path=None):
    """Determine which credentials file to load.

    This function implements the order of precendence in which the path of the
    credentials file can be configured. This order is (highed priority first):

    * The path passed to this function
    * The environment variable ``FACULTY_CREDENTIALS_PATH``
    * ``~/.config/faculty/credentials``

    The last path will be relative to the XDG home directory, when this is
    configured.
    """
    return (
        credentials_path
        or os.getenv("FACULTY_CREDENTIALS_PATH")
        or _get_deprecated_env_var(
            "SHERLOCKML_CREDENTIALS_PATH", "FACULTY_CREDENTIALS_PATH"
        )
        or _default_credentials_path()
    )


class CredentialsError(RuntimeError):
    """An error was encourntered when loading Faculty credentials."""

    pass


def _raise_credentials_error(type_):
    raise CredentialsError("No {} found".format(type_))


def resolve_profile(
    credentials_path=None,
    profile_name=None,
    domain=None,
    protocol=None,
    client_id=None,
    client_secret=None,
):
    """Resolve all sources of configuration to load a Faculty profile.

    This function implements the order of precendence in which configuration
    entries are determined from files, environment variables and code.

    Configuration entries are determined in this order of priority (highest
    first):

    * The value passed to this function
    * The value set in an environment variable
    * The value read from a configuration file

    The logic for determining the configuration file is described in
    :func:`resolve_credentials_path`.

    The profile read from the configuration file will be, in order of priority:

    * The value passed to this function
    * The value set in the environment variable ``FACULTY_PROFILE``
    * ``default``

    Parameters
    ----------
    credentials_path : str or pathlib.Path, optional
        The path of the credentials file to load. Can also be set with the
        environment variable ``FACULTY_CREDENTIALS_PATH``.
    profile_name : str, optional
        The name of the profile to load from the credentials file. Can also be
        set with the environment variable ``FACULTY_PROFILE``.
    domain : str, optional
        The domain name where Faculty services are hosted. Can also be set with
        the environment variable ``FACULTY_DOMAIN``.
    protocol : str, optional
        The protocol to use when making requests to Faculty services. Can also
        be set with the environment variable ``FACULTY_PROTOCOL``.
    client_id : str, optional
        The OAuth client ID to authenticate requests with. Can also be set with
        the environment variable ``FACULTY_CLIENT_ID``.
    client_secret : str, optional
        The OAuth client secret to authenticate requests with. Can also be set
        with the environment variable ``FACULTY_CLIENT_SECRET``.

    Returns
    -------
    Profile
        The resolved Faculty profile.
    """

    resolved_profile_name = (
        profile_name
        or os.getenv("FACULTY_PROFILE")
        or _get_deprecated_env_var("SHERLOCKML_PROFILE", "FACULTY_PROFILE")
        or DEFAULT_PROFILE
    )

    profile = load_profile(
        resolve_credentials_path(credentials_path), resolved_profile_name
    )

    resolved_domain = (
        domain
        or os.getenv("FACULTY_DOMAIN")
        or _get_deprecated_env_var("SHERLOCKML_DOMAIN", "FACULTY_DOMAIN")
        or profile.domain
        or DEFAULT_DOMAIN
    )

    resolved_protocol = (
        protocol
        or os.getenv("FACULTY_PROTOCOL")
        or _get_deprecated_env_var("SHERLOCKML_PROTOCOL", "FACULTY_PROTOCOL")
        or profile.protocol
        or DEFAULT_PROTOCOL
    )

    resolved_client_id = (
        client_id
        or os.getenv("FACULTY_CLIENT_ID")
        or _get_deprecated_env_var("SHERLOCKML_CLIENT_ID", "FACULTY_CLIENT_ID")
        or profile.client_id
        or _raise_credentials_error("client_id")
    )

    resolved_client_secret = (
        client_secret
        or os.getenv("FACULTY_CLIENT_SECRET")
        or _get_deprecated_env_var(
            "SHERLOCKML_CLIENT_SECRET", "FACULTY_CLIENT_SECRET"
        )
        or profile.client_secret
        or _raise_credentials_error("client_secret")
    )

    return Profile(
        domain=resolved_domain,
        protocol=resolved_protocol,
        client_id=resolved_client_id,
        client_secret=resolved_client_secret,
    )
