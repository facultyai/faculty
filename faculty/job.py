# Copyright 2018-2019 Faculty Science Limited
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import uuid

from attr import attrs, attrib

import faculty
from faculty.project import Project


@attrs
class Job(object):

    id = attrib()
    project = attrib()
    name = attrib()

    @classmethod
    def get(cls, job_name_or_id, project=None):
        if not isinstance(project, Project):
            project = Project.get(project)
        job_client = faculty.client("job")
        try:
            job_id = uuid.UUID(job_name_or_id)
            job = job_client.get(project.id, job_id)
        except ValueError:
            jobs = job_client.list(project.id)
            matching_jobs = [job for job in jobs if job.name == job_name_or_id]
            [job] = matching_jobs
        return cls(id=job.id, project=project, name=job.metadata.name)

    @classmethod
    def list(cls, project=None):
        if not isinstance(project, Project):
            project = Project.get(project)
        job_client = faculty.client("job")
        jobs = job_client.list(project.id)
        return [
            cls(id=job.id, project=project, name=job.metadata.name)
            for job in jobs
        ]

    def run(self, **parameter_values):
        job_client = faculty.client("job")
        job_client.create_run(self.project.id, self.id, [parameter_values])

    def run_array(self, *parameter_value_sets):
        job_client = faculty.client("job")
        job_client.create_run(self.project.id, self.id, parameter_value_sets)
