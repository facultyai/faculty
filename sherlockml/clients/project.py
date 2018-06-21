from collections import namedtuple

from marshmallow import fields, post_load

from sherlockml.clients.base import BaseSchema, BaseClient


Project = namedtuple('Project', ['id', 'name', 'owner_id'])


class ProjectSchema(BaseSchema):

    id = fields.UUID(required=True)
    name = fields.Str(required=True)
    owner_id = fields.UUID(required=True)

    @post_load
    def make_project(self, data):
        return Project(**data)


class ProjectClient(BaseClient):

    SERVICE_NAME = 'casebook'

    def list_accessible_by_user(self, user_id):
        endpoint = '/user/{}'.format(user_id)
        return self._get(endpoint, ProjectSchema(many=True))
