from attr import attrs, attrib
import pandas

import faculty  # TODO: Avoid possible circular imports


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
    def query(cls, project_id, filter=None, sort=None):
        def get_runs():
            client = faculty.client("experiment")

            response = client.query_runs(project_id, filter, sort)
            print(response)
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

        # Open question:
        # Should we evalutate the entire set of runs before returning the
        # result, or is it ok to have them lazily evaluated
        return ExperimentRunQueryResult(get_runs())
