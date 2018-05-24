import pytest

from sherlockml import config


SAMPLE_CONFIG_CONTENT = """
[default]
deployment = A Deployment

[A Deployment]
domain = test.domain.com
protocol = test-protocol
client_id = test-client-id
client_secret = test-client-secret

[Deployment with Defaults]
"""

SAMPLE_CONFIG = config.Config(
    default_deployment='A Deployment',
    deployments={
        'A Deployment': config.Deployment(
            domain='test.domain.com',
            protocol='test-protocol',
            client_id='test-client-id',
            client_secret='test-client-secret'
        ),
        'Deployment with Defaults': config.Deployment(
            domain='sherlockml.com',
            protocol='https',
            client_id=None,
            client_secret=None
        )
    }
)


@pytest.fixture
def sample_config(tmpdir):
    file = tmpdir.join('config')
    file.write(SAMPLE_CONFIG_CONTENT)
    return file


def test_load(sample_config):
    assert config.load(sample_config) == SAMPLE_CONFIG


def test_load_missing():
    assert config.load('does-not-exist') == config.Config(None, {})


@pytest.mark.parametrize(
    'override_value, expected_return_value',
    [('override', 'override'), ('', 'normal'), (None, 'normal')]
)
def test_env_override(monkeypatch, override_value, expected_return_value):

    if override_value is not None:
        monkeypatch.setenv('EXAMPLE', override_value)

    @config.env_override('EXAMPLE')
    def test():
        return 'normal'

    assert test() == expected_return_value
