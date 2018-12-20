import os

import pytest

from faculty import config


SAMPLE_CONFIG_CONTENT = """
[default]
domain = test.domain.com
protocol = test-protocol
client_id = test-client-id
client_secret = test-client-secret

[empty profile]
"""

DEFAULT_PROFILE = config.Profile(
    domain="test.domain.com",
    protocol="test-protocol",
    client_id="test-client-id",
    client_secret="test-client-secret",
)
OTHER_PROFILE = config.Profile(
    domain="other.domain.com",
    protocol="other-protocol",
    client_id="other-client-id",
    client_secret="other-client-secret",
)
CREDENTIALS_ONLY_PROFILE = config.Profile(
    domain=None,
    protocol=None,
    client_id="test-client-id",
    client_secret="test-client-secret",
)
PROFILE_WITHOUT_ID = config.Profile(
    domain="test.domain.com",
    protocol="test-protocol",
    client_id=None,
    client_secret="test-client-secret",
)
PROFILE_WITHOUT_SECRET = config.Profile(
    domain="test.domain.com",
    protocol="test-protocol",
    client_id="test-client-id",
    client_secret=None,
)
EMPTY_PROFILE = config.Profile(
    domain=None, protocol=None, client_id=None, client_secret=None
)

SAMPLE_CONFIG = {"default": DEFAULT_PROFILE, "empty profile": EMPTY_PROFILE}


def test_load(tmpdir):
    file = tmpdir.join("config")
    file.write(SAMPLE_CONFIG_CONTENT)
    assert config.load(file) == SAMPLE_CONFIG


def test_load_missing():
    assert config.load("does-not-exist") == {}


@pytest.mark.parametrize(
    "profile_name, expected_profile",
    [("default", DEFAULT_PROFILE), ("missing profile", EMPTY_PROFILE)],
)
def test_load_profile(mocker, profile_name, expected_profile):
    mocker.patch("faculty.config.load", return_value=SAMPLE_CONFIG)

    assert config.load_profile("test/path", profile_name) == expected_profile

    config.load.assert_called_once_with("test/path")


def test_default_credentials_path(mocker):
    mocker.patch.dict(os.environ, {"HOME": "/foo/bar"})
    expected_path = "/foo/bar/.config/sherlockml/credentials"
    assert config.default_credentials_path() == expected_path


def test_default_credentials_path_xdg_home(mocker):
    mocker.patch.dict(os.environ, {"XDG_CONFIG_HOME": "/xdg/home"})
    expected_path = "/xdg/home/sherlockml/credentials"
    assert config.default_credentials_path() == expected_path


def test_resolve_profile(mocker):
    mocker.patch("faculty.config.load_profile", return_value=DEFAULT_PROFILE)
    mocker.patch("faculty.config.default_credentials_path")

    assert config.resolve_profile() == DEFAULT_PROFILE

    config.load_profile.assert_called_once_with(
        config.default_credentials_path.return_value, "default"
    )


def test_resolve_profile_credentials_path_override(mocker):
    mocker.patch("faculty.config.load_profile", return_value=DEFAULT_PROFILE)

    profile = config.resolve_profile(credentials_path="test/path")
    assert profile == DEFAULT_PROFILE

    config.load_profile.assert_called_once_with("test/path", "default")


@pytest.mark.parametrize(
    "environment_variable",
    ["FACULTY_CREDENTIALS_PATH", "SHERLOCKML_CREDENTIALS_PATH"],
)
def test_resolve_profile_credentials_path_env(mocker, environment_variable):
    mocker.patch("faculty.config.load_profile", return_value=DEFAULT_PROFILE)
    path = "/path/to/credentials"
    mocker.patch.dict(os.environ, {environment_variable: path})

    assert config.resolve_profile() == DEFAULT_PROFILE

    config.load_profile.assert_called_once_with(path, "default")


def test_resolve_profile_credentials_path_env_faculty_precedence(mocker):
    mocker.patch("faculty.config.load_profile")
    path = "/faculty/credentials"
    mocker.patch.dict(
        os.environ,
        {
            "FACULTY_CREDENTIALS_PATH": path,
            "SHERLOCKML_CREDENTIALS_PATH": "ignored",
        },
    )
    config.resolve_profile()
    config.load_profile.assert_called_once_with(path, "default")


def test_resolve_profile_profile_name_override(mocker):
    mocker.patch("faculty.config.load_profile", return_value=OTHER_PROFILE)
    mocker.patch("faculty.config.default_credentials_path")

    profile = config.resolve_profile(profile_name="other")
    assert profile == OTHER_PROFILE

    config.load_profile.assert_called_once_with(
        config.default_credentials_path.return_value, "other"
    )


@pytest.mark.parametrize(
    "environment_variable", ["FACULTY_PROFILE", "SHERLOCKML_PROFILE"]
)
def test_resolve_profile_profile_name_env(mocker, environment_variable):
    mocker.patch("faculty.config.load_profile", return_value=OTHER_PROFILE)
    mocker.patch("faculty.config.default_credentials_path")
    mocker.patch.dict(os.environ, {environment_variable: "other"})

    assert config.resolve_profile() == OTHER_PROFILE

    config.load_profile.assert_called_once_with(
        config.default_credentials_path.return_value, "other"
    )


def test_resolve_profile_profile_name_env_faculty_precendence(mocker):
    mocker.patch("faculty.config.load_profile", return_value=OTHER_PROFILE)
    mocker.patch("faculty.config.default_credentials_path")
    mocker.patch.dict(
        os.environ,
        {"FACULTY_PROFILE": "other", "SHERLOCKML_PROFILE": "ignored"},
    )
    config.resolve_profile()
    config.load_profile.assert_called_once_with(
        config.default_credentials_path.return_value, "other"
    )


def test_resolve_profile_overrides(mocker):
    mocker.patch("faculty.config.load_profile", return_value=DEFAULT_PROFILE)
    profile = config.resolve_profile(
        domain="other.domain.com",
        protocol="other-protocol",
        client_id="other-client-id",
        client_secret="other-client-secret",
    )
    assert profile == OTHER_PROFILE


@pytest.mark.parametrize("prefix", ["FACULTY", "SHERLOCKML"])
def test_resolve_profile_env(mocker, prefix):
    mocker.patch("faculty.config.load_profile", return_value=DEFAULT_PROFILE)
    mocker.patch.dict(
        os.environ,
        {
            "{}_DOMAIN".format(prefix): "other.domain.com",
            "{}_PROTOCOL".format(prefix): "other-protocol",
            "{}_CLIENT_ID".format(prefix): "other-client-id",
            "{}_CLIENT_SECRET".format(prefix): "other-client-secret",
        },
    )
    assert config.resolve_profile() == OTHER_PROFILE


def test_resolve_profile_env_faculty_precedence(mocker):
    mocker.patch("faculty.config.load_profile", return_value=DEFAULT_PROFILE)
    mocker.patch.dict(
        os.environ,
        {
            "FACULTY_DOMAIN": "other.domain.com",
            "FACULTY_PROTOCOL": "other-protocol",
            "FACULTY_CLIENT_ID": "other-client-id",
            "FACULTY_CLIENT_SECRET": "other-client-secret",
            "SHERLOCKML_DOMAIN": "ignored",
            "SHERLOCKML_PROTOCOL": "ignored",
            "SHERLOCKML_CLIENT_ID": "ignored",
            "SHERLOCKML_CLIENT_SECRET": "ignored",
        },
    )
    assert config.resolve_profile() == OTHER_PROFILE


def test_resolve_profile_defaults(mocker):
    mocker.patch(
        "faculty.config.load_profile", return_value=CREDENTIALS_ONLY_PROFILE
    )
    profile = config.resolve_profile()
    assert profile == config.Profile(
        domain=config.DEFAULT_DOMAIN,
        protocol=config.DEFAULT_PROTOCOL,
        client_id="test-client-id",
        client_secret="test-client-secret",
    )


def test_resolve_profile_missing_client_id(mocker):
    mocker.patch(
        "faculty.config.load_profile", return_value=PROFILE_WITHOUT_ID
    )
    with pytest.raises(config.CredentialsError, match="client_id"):
        config.resolve_profile()


def test_resolve_profile_missing_client_secret(mocker):
    mocker.patch(
        "faculty.config.load_profile", return_value=PROFILE_WITHOUT_SECRET
    )
    with pytest.raises(config.CredentialsError, match="client_secret"):
        config.resolve_profile()
