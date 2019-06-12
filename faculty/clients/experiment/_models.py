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


from collections import namedtuple
from enum import Enum


class LifecycleStage(Enum):
    ACTIVE = "active"
    DELETED = "deleted"


class ExperimentRunStatus(Enum):
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"
    SCHEDULED = "scheduled"
    KILLED = "killed"


Page = namedtuple("Page", ["start", "limit"])
Pagination = namedtuple("Pagination", ["start", "size", "previous", "next"])

Metric = namedtuple("Metric", ["key", "value", "timestamp", "step"])
Param = namedtuple("Param", ["key", "value"])
Tag = namedtuple("Tag", ["key", "value"])

Experiment = namedtuple(
    "Experiment",
    [
        "id",
        "name",
        "description",
        "artifact_location",
        "created_at",
        "last_updated_at",
        "deleted_at",
    ],
)


ExperimentRun = namedtuple(
    "ExperimentRun",
    [
        "id",
        "run_number",
        "experiment_id",
        "name",
        "parent_run_id",
        "artifact_location",
        "status",
        "started_at",
        "ended_at",
        "deleted_at",
        "tags",
        "params",
        "metrics",
    ],
)


class ComparisonOperator(Enum):
    DEFINED = "defined"
    EQUAL_TO = "eq"
    NOT_EQUAL_TO = "ne"
    LESS_THAN = "lt"
    LESS_THAN_OR_EQUAL_TO = "le"
    GREATER_THAN = "gt"
    GREATER_THAN_OR_EQUAL_TO = "ge"


ProjectIdFilter = namedtuple("ProjectIdFilter", ["operator", "value"])
ExperimentIdFilter = namedtuple("ExperimentIdFilter", ["operator", "value"])
RunIdFilter = namedtuple("RunIdFilter", ["operator", "value"])
DeletedAtFilter = namedtuple("DeletedAtFilter", ["operator", "value"])
TagFilter = namedtuple("TagFilter", ["key", "operator", "value"])
ParamFilter = namedtuple("ParamFilter", ["key", "operator", "value"])
MetricFilter = namedtuple("MetricFilter", ["key", "operator", "value"])


class LogicalOperator(Enum):
    AND = "and"
    OR = "or"


CompoundFilter = namedtuple("CompoundFilter", ["operator", "conditions"])


class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"


StartedAtSort = namedtuple("StartedAtSort", ["order"])
RunNumberSort = namedtuple("RunNumberSort", ["order"])
DurationSort = namedtuple("DurationSort", ["order"])
TagSort = namedtuple("TagSort", ["key", "order"])
ParamSort = namedtuple("ParamSort", ["key", "order"])
MetricSort = namedtuple("MetricSort", ["key", "order"])

RunQuery = namedtuple("RunQuery", ["filter", "sort", "page"])

MetricDataPoint = namedtuple("Metric", ["value", "timestamp", "step"])
MetricHistory = namedtuple(
    "MetricHistory", ["original_size", "subsampled", "key", "history"]
)

ListExperimentRunsResponse = namedtuple(
    "ListExperimentRunsResponse", ["runs", "pagination"]
)
DeleteExperimentRunsResponse = namedtuple(
    "DeleteExperimentRunsResponse", ["deleted_run_ids", "conflicted_run_ids"]
)
RestoreExperimentRunsResponse = namedtuple(
    "RestoreExperimentRunsResponse", ["restored_run_ids", "conflicted_run_ids"]
)
