import pytest

import sherlockml.clients
from sherlockml.clients.user import UserClient


def test_for_resource():
    assert sherlockml.clients.for_resource('user') is UserClient


def test_for_resource_missing():
    with pytest.raises(ValueError):
        sherlockml.clients.for_resource('missing')
