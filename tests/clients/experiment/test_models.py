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

import pytest

from faculty.clients.experiment._models import (
    ComparisonOperator,
    CompoundFilter,
    DeletedAtFilter,
    ExperimentIdFilter,
    LogicalOperator,
    MetricFilter,
    ParamFilter,
    ProjectIdFilter,
    RunIdFilter,
    TagFilter,
)


SINGLE_FILTERS = [
    ProjectIdFilter(ComparisonOperator.EQUAL_TO, uuid.uuid4()),
    ExperimentIdFilter(ComparisonOperator.NOT_EQUAL_TO, 4),
    RunIdFilter(ComparisonOperator.EQUAL_TO, uuid.uuid4()),
    DeletedAtFilter(ComparisonOperator.DEFINED, False),
    TagFilter("key", ComparisonOperator.EQUAL_TO, "value"),
    ParamFilter("key", ComparisonOperator.NOT_EQUAL_TO, "value"),
    ParamFilter("key", ComparisonOperator.GREATER_THAN, 0.3),
    MetricFilter("key", ComparisonOperator.LESS_THAN_OR_EQUAL_TO, 0.6),
]
AND_FILTER = CompoundFilter(
    LogicalOperator.AND,
    [
        ExperimentIdFilter(ComparisonOperator.EQUAL_TO, 4),
        ParamFilter("key", ComparisonOperator.GREATER_THAN_OR_EQUAL_TO, 0.4),
    ],
)
OR_FILTER = CompoundFilter(
    LogicalOperator.OR,
    [
        ExperimentIdFilter(ComparisonOperator.EQUAL_TO, 4),
        ExperimentIdFilter(ComparisonOperator.EQUAL_TO, 5),
    ],
)


@pytest.mark.parametrize("left", SINGLE_FILTERS + [OR_FILTER])
@pytest.mark.parametrize("right", SINGLE_FILTERS + [OR_FILTER])
def test_non_mergable_and(left, right):
    assert (left & right) == CompoundFilter(LogicalOperator.AND, [left, right])


@pytest.mark.parametrize("left", SINGLE_FILTERS + [AND_FILTER])
@pytest.mark.parametrize("right", SINGLE_FILTERS + [AND_FILTER])
def test_non_mergable_or(left, right):
    assert (left | right) == CompoundFilter(LogicalOperator.OR, [left, right])


@pytest.mark.parametrize("right", SINGLE_FILTERS)
def test_left_mergeable_and(right):
    assert (AND_FILTER & right) == CompoundFilter(
        LogicalOperator.AND, AND_FILTER.conditions + [right]
    )


@pytest.mark.parametrize("right", SINGLE_FILTERS)
def test_left_mergeable_or(right):
    assert (OR_FILTER | right) == CompoundFilter(
        LogicalOperator.OR, OR_FILTER.conditions + [right]
    )


@pytest.mark.parametrize("left", SINGLE_FILTERS)
def test_right_mergeable_and(left):
    assert (left & AND_FILTER) == CompoundFilter(
        LogicalOperator.AND, [left] + AND_FILTER.conditions
    )


@pytest.mark.parametrize("left", SINGLE_FILTERS)
def test_right_mergeable_or(left):
    assert (left | OR_FILTER) == CompoundFilter(
        LogicalOperator.OR, [left] + OR_FILTER.conditions
    )


def test_fully_mergable_and():
    assert (AND_FILTER & AND_FILTER) == CompoundFilter(
        LogicalOperator.AND, AND_FILTER.conditions + AND_FILTER.conditions
    )


def test_fully_mergable_or():
    assert (OR_FILTER | OR_FILTER) == CompoundFilter(
        LogicalOperator.OR, OR_FILTER.conditions + OR_FILTER.conditions
    )
