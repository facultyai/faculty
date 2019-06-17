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


from attr import attrs, attrib
import pandas

from faculty.session import get_session
from faculty._util.resolvers import resolve_project_id
from faculty.clients.experiment import ExperimentClient


class QueryResult(object):
    def __init__(self, iterable):
        self.iterable = iterable

    def __iter__(self):
        return iter(self.iterable)


class ExperimentRunQueryResult(QueryResult):
    def as_dataframe(self):
        records = []
        for run in self:
            row = {
                "Experiment ID": run.experiment_id,
                "Run ID": run.id,
                "Status": run.status.value,
                "Started At": run.started_at,
            }
            for metric in run.metrics:
                row[metric.key] = row[metric.value]
            records.append(row)
        return pandas.DataFrame(records)


@attrs
class ExperimentRun(object):
    id = attrib()
    run_number = attrib()
    experiment_id = attrib()
    name = attrib()
    parent_run_id = attrib()
    artifact_location = attrib()
    status = attrib()
    started_at = attrib()
    ended_at = attrib()
    deleted_at = attrib()
    tags = attrib()
    params = attrib()
    metrics = attrib()

    @classmethod
    def _from_client_model(cls, client_object):
        return cls(**client_object._asdict())

    @classmethod
    def query(cls, project=None, filter=None, sort=None, **session_config):

        session = get_session(**session_config)
        project_id = resolve_project_id(session, project)

        def _get_runs():
            client = ExperimentClient(session)

            response = client.query_runs(project_id, filter, sort)
            # return map(cls._from_client_model, response.runs)
            yield from map(cls._from_client_model, response.runs)

            while response.pagination.next is not None:
                response = client.query_runs(
                    project_id,
                    filter,
                    sort,
                    start=response.pagination.next.start,
                    limit=response.pagination.next.limit,
                )
                yield from map(cls._from_client_model, response.runs)

        return ExperimentRunQueryResult(list(_get_runs()))
