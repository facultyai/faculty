import pytest

import faculty.config

import faculty.cli.config


PROFILE = faculty.config.Profile(
    domain="services.subdomain.sherlockml.net",
    protocol="protocol",
    client_id="client id",
    client_secret="client secret",
)


@pytest.fixture
def mock_profile(mocker):
    mocker.patch("faculty.config.resolve_profile", return_value=PROFILE)


def test_casebook_url(mock_profile):
    assert (
        faculty.cli.config.casebook_url()
        == "protocol://casebook.services.subdomain.sherlockml.net"
    )


def test_hudson_url(mock_profile):
    assert (
        faculty.cli.config.hudson_url()
        == "protocol://hudson.services.subdomain.sherlockml.net"
    )


def test_galleon_url(mock_profile):
    assert (
        faculty.cli.config.galleon_url()
        == "protocol://galleon.services.subdomain.sherlockml.net"
    )


def test_baskerville_url(mock_profile):
    assert (
        faculty.cli.config.baskerville_url()
        == "protocol://baskerville.services.subdomain.sherlockml.net"
    )
