import sherlockml


def test_client(mocker):
    profile_mock = mocker.patch('sherlockml.config.resolve_profile')
    for_resource_mock = mocker.patch('sherlockml.clients.for_resource')

    config_overrides = {'foo': 'bar'}
    sherlockml.client('test-resource', **config_overrides)

    profile_mock.assert_called_once_with(**config_overrides)
    for_resource_mock.assert_called_once_with('test-resource')

    returned_profile = profile_mock.return_value
    returned_class = for_resource_mock.return_value
    returned_class.assert_called_once_with(returned_profile)
