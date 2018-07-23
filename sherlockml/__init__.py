import sherlockml.config
import sherlockml.clients


def client(resource, *args, **kwargs):
    profile = sherlockml.config.resolve_profile(*args, **kwargs)
    client_class = sherlockml.clients.for_resource(resource)
    return client_class(profile)
