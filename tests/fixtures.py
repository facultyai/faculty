import uuid

from faculty.clients.project import Project


USER_ID = uuid.uuid4()
PROJECT = Project(id=uuid.uuid4(), name="test-project", owner_id=USER_ID)
