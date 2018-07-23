from sherlockml.clients.user import UserClient
from sherlockml.clients.project import ProjectClient
from sherlockml.clients.server import ServerClient


CLIENT_FOR_RESOURCE = {
    'user': UserClient,
    'project': ProjectClient,
    'server': ServerClient
}


def for_resource(resource):
    try:
        return CLIENT_FOR_RESOURCE[resource]
    except KeyError:
        raise ValueError(
            'unsupported resource {}, choose one of {}'.format(
                resource, set(CLIENT_FOR_RESOURCE.keys())
            )
        )
