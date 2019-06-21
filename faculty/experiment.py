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

from faculty.clients.experiment import (
    ComparisonOperator,
    DeletedAtFilter,
    ExperimentIdFilter,
    MetricFilter,
    ParamFilter,
    RunIdFilter,
    TagFilter,
)


@attrs
class ExperimentRun(object):
    """A single run of an experiment."""

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
        """Query the platform for experiment runs.

        Parameters
        ----------
        project : str, UUID, or None
            The name or ID of a project. If ``None`` is passed (the default),
            the project will be inferred from the runtime context.
        filter : a filter object from ``faculty.clients.experiment``
            Condition(s) to filter experiment runs by. ``FilterBy`` provides a
            convenience interface for constructing filter objects.
        sort : a sequence of sort objects from ``faculty.clients.experiment``
            Condition(s) to sort experiment runs by.
        **session_config
            Configuration options to build the session with.

        Returns
        -------
        ExperimentRunList

        Examples
        --------
        Get all experiment runs in the current project:

        >>> ExperimentRun.query()
        ExperimentRunList([ExperimentRun(...)])

        Get all experiment runs in a named project:

        >>> ExperimentRun.query("my project")
        ExperimentRunList([ExperimentRun(...)])

        Filter experiment runs by experiment ID:

        >>> ExperimentRun.query(filter=FilterBy.experiment_id() == 2)
        ExperimentRunList([ExperimentRun(...)])

        Filter experiment runs by a more complex condition:

        >>> filter = (
        ...     FilterBy.experiment_id().one_of([2, 3, 4]) &
        ...     (FilterBy.metric("accuracy") > 0.9) &
        ...     (FilterBy.param("alpha") < 0.3)
        ... )
        >>> ExperimentRun.query("my project", filter)
        ExperimentRunList([ExperimentRun(...)])
        """

        session = get_session(**session_config)
        project_id = resolve_project_id(session, project)

        def _get_runs():
            client = ExperimentClient(session)

            response = client.query_runs(project_id, filter, sort)
            for run in response.runs:
                yield cls._from_client_model(run)

            while response.pagination.next is not None:
                response = client.query_runs(
                    project_id,
                    filter,
                    sort,
                    start=response.pagination.next.start,
                    limit=response.pagination.next.limit,
                )
                for run in response.runs:
                    yield cls._from_client_model(run)

        return ExperimentRunList(_get_runs())


class ExperimentRunList(list):
    """A list of experiment runs.

    This collection is a subclass of ``list``, and so supports all its
    functionality, but adds the ``as_dataframe`` method which returns a
    representation of the contained ExperimentRuns as a ``pandas.DataFrame``.
    """

    def __repr__(self):
        return "{}({})".format(
            self.__class__.__name__, super(ExperimentRunList, self).__repr__()
        )

    def as_dataframe(self):
        """Get the experiment runs as a pandas DataFrame.

        Returns
        -------
        pandas.DataFrame
        """

        records = []
        for run in self:
            row = {
                ("experiment_id", ""): run.experiment_id,
                ("run_id", ""): run.id,
                ("run_number", ""): run.run_number,
                ("status", ""): run.status.value,
                ("started_at", ""): run.started_at,
                ("ended_at", ""): run.ended_at,
            }
            for param in run.params:
                row[("params", param.key)] = param.value
            for metric in run.metrics:
                row[("metrics", metric.key)] = metric.value
            records.append(row)

        df = pandas.DataFrame(records)
        df.columns = pandas.MultiIndex.from_tuples(df.columns)

        # Reorder columns and return
        column_order = [
            "experiment_id",
            "run_id",
            "run_number",
            "status",
            "started_at",
            "ended_at",
        ]
        if "params" in df.columns:
            column_order.append("params")
        if "metrics" in df.columns:
            column_order.append("metrics")
        return df[column_order]


class _FilterBuilder(object):
    def __init__(self, constructor, *constructor_args):
        self.constructor = constructor
        self.constructor_args = constructor_args

    def _build(self, *args):
        return self.constructor(*(self.constructor_args + args))

    def defined(self, value=True):
        return self._build(ComparisonOperator.DEFINED, value)

    def __eq__(self, value):
        return self._build(ComparisonOperator.EQUAL_TO, value)

    def __ne__(self, value):
        return self._build(ComparisonOperator.NOT_EQUAL_TO, value)

    def __gt__(self, value):
        return self._build(ComparisonOperator.GREATER_THAN, value)

    def __ge__(self, value):
        return self._build(ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, value)

    def __lt__(self, value):
        return self._build(ComparisonOperator.LESS_THAN, value)

    def __le__(self, value):
        return self._build(ComparisonOperator.LESS_THAN_OR_EQUAL_TO, value)

    def one_of(self, values):
        try:
            first, remaining = values[0], values[1:]
        except IndexError:
            raise ValueError("Must provide at least one value")
        filter = self == first
        for val in remaining:
            filter |= self == val
        return filter


class FilterBy(object):
    @staticmethod
    def experiment_id():
        """Filter by experiment ID.

        Examples
        --------
        Get runs for experiment 4:

        >>> FilterBy.experiment_id() == 4
        """
        return _FilterBuilder(ExperimentIdFilter)

    @staticmethod
    def run_id():
        """Filter by run ID.

        Examples
        --------
        Get the run with a specified ID:

        >>> FilterBy.run_id() == "945f1d96-9937-4b95-aa3f-addcdd1c8749"
        """
        return _FilterBuilder(RunIdFilter)

    @staticmethod
    def deleted_at():
        """Filter by run deletion time.

        Examples
        --------
        Get runs deleted more than ten minutes ago:

        >>> from datetime import datetime, timedelta
        >>> FilterBy.deleted_at() <  datetime.now() - timedelta(minutes=10)

        Get non-deleted runs:

        >>> FilterBy.deleted_at() == None
        """
        return _FilterBuilder(DeletedAtFilter)

    @staticmethod
    def tag(key):
        """Filter by run tag.

        Examples
        --------
        Get runs with a particular tag:

        >>> FilterBy.tag("key") == "value"

        Get runs where a tag is set, with any value:

        >>> FilterBy.tag("key") != None
        """
        return _FilterBuilder(TagFilter, key)

    @staticmethod
    def param(key):
        """Filter by parameter.

        Examples
        --------
        Get runs with a particular parameter value:

        >>> FilterBy.param("key") == "value"

        Params also support filtering by numeric value:

        >>> FilterBy.param("alpha") > 0.2
        """
        return _FilterBuilder(ParamFilter, key)

    @staticmethod
    def metric(key):
        """Filter by metric.

        Examples
        --------
        Get runs with matching metric values:

        >>> FilterBy.metric("accuracy") > 0.9

        To filter a range of values, combine them with ``&``:

        >>> (
        ...     (FilterBy.metric("accuracy") > 0.8 ) &
        ...     (FilterBy.metric("accuracy") > 0.9)
        ... )
        """
        return _FilterBuilder(MetricFilter, key)
