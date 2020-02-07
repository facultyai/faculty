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
    """Read the Faculty configuration from a file."""

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
    """Read a Faculty profile from a file."""
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
    return (
        credentials_path
        or os.getenv("FACULTY_CREDENTIALS_PATH")
        or _get_deprecated_env_var(
            "SHERLOCKML_CREDENTIALS_PATH", "FACULTY_CREDENTIALS_PATH"
        )
        or _default_credentials_path()
    )


class CredentialsError(RuntimeError):
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
