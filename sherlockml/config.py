import os
from collections import namedtuple
from functools import wraps

from six.moves.configparser import ConfigParser, NoSectionError, NoOptionError


Profile = namedtuple(
    'Profile',
    ['domain', 'protocol', 'client_id', 'client_secret']
)

DEFAULT_PROFILE = 'default'
DEFAULT_DOMAIN = 'sherlockml.com'
DEFAULT_PROTOCOL = 'https'


def load(path):
    """Read the SherlockML configuration from a file."""

    parser = ConfigParser()
    parser.read(str(path))

    def _get(section, option, fallback=None):
        try:
            return parser.get(section, option)
        except (NoSectionError, NoOptionError):
            return fallback

    profiles = {}

    for section in parser.sections():
        profiles[section] = Profile(
            domain=_get(section, 'domain', DEFAULT_DOMAIN),
            protocol=_get(section, 'protocol', DEFAULT_PROTOCOL),
            client_id=_get(section, 'client_id'),
            client_secret=_get(section, 'client_secret')
        )

    return profiles


def env_override(environment_variable):
    """Override the return value of a function when an env variable is set."""
    def decorator(func):
        @wraps(func)
        def wrapped(*args, **kwargs):
            env_value = os.environ.get(environment_variable)
            return env_value or func(*args, **kwargs)
        return wrapped
    return decorator


@env_override('SHERLOCKML_CONFIGURATION')
def _configuration_path():
    xdg_config_home = os.environ.get('XDG_CONFIG_HOME')
    if not xdg_config_home:
        xdg_config_home = os.path.expanduser('~/.config')
    return os.path.join(xdg_config_home, 'sherlockml', 'configuration')


def _default_profile():
    config = load(_configuration_path())
    return config['default']


@env_override('SHERLOCKML_DOMAIN')
def domain():
    """Return the domain for the default profile."""
    return _default_profile().domain


@env_override('SHERLOCKML_PROTOCOL')
def protocol():
    """Return the protocol for the default profile."""
    return _default_profile().protocol


@env_override('SHERLOCKML_CLIENT_ID')
def client_id():
    """Return the client ID for the default profile."""
    return _default_profile().client_id


@env_override('SHERLOCKML_CLIENT_SECRET')
def client_secret():
    """Return the client secret for the default profile."""
    return _default_profile().client_secret
