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


from marshmallow import fields, post_load, pre_dump, ValidationError
from marshmallow_enum import EnumField

from faculty._oneofschema import OneOfSchema
from faculty.clients.base import BaseSchema
from faculty.clients.experiment._models import (
    ComparisonOperator,
    DeleteExperimentRunsResponse,
    Experiment,
    ExperimentRun,
    ExperimentRunStatus,
    ListExperimentRunsResponse,
    LogicalOperator,
    Metric,
    MetricDataPoint,
    MetricHistory,
    Page,
    Pagination,
    Param,
    RestoreExperimentRunsResponse,
    SortOrder,
    Tag,
)


class _OptionalField(fields.Field):
    """Wrap another field, passing through Nones."""

    def __init__(self, nested, *args, **kwargs):
        self.nested = nested
        super(_OptionalField, self).__init__(*args, **kwargs)

    def _deserialize(self, value, *args, **kwargs):
        if value is None:
            return None
        else:
            return self.nested._deserialize(value, *args, **kwargs)

    def _serialize(self, value, *args, **kwargs):
        if value is None:
            return None
        else:
            return self.nested._serialize(value, *args, **kwargs)


class _OneOfSchemaWithoutType(OneOfSchema):
    def dump(self, *args, **kwargs):
        data = super(_OneOfSchemaWithoutType, self).dump(*args, **kwargs)
        # Remove the type field added by marshmallow-oneofschema
        return {k: v for k, v in data.items() if k != "type"}


class PageSchema(BaseSchema):
    start = fields.Integer(required=True)
    limit = fields.Integer(required=True)

    @post_load
    def make_page(self, data):
        return Page(**data)


class PaginationSchema(BaseSchema):
    start = fields.Integer(required=True)
    size = fields.Integer(required=True)
    previous = fields.Nested(PageSchema, missing=None)
    next = fields.Nested(PageSchema, missing=None)

    @post_load
    def make_pagination(self, data):
        return Pagination(**data)


class MetricSchema(BaseSchema):
    key = fields.String(required=True)
    value = fields.Float(required=True)
    timestamp = fields.DateTime(required=True)
    step = fields.Integer(required=True)

    @post_load
    def make_metric(self, data):
        return Metric(**data)


class ParamSchema(BaseSchema):
    key = fields.String(required=True)
    value = fields.String(required=True)

    @post_load
    def make_param(self, data):
        return Param(**data)


class TagSchema(BaseSchema):
    key = fields.String(required=True)
    value = fields.String(required=True)

    @post_load
    def make_tag(self, data):
        return Tag(**data)


class ExperimentSchema(BaseSchema):
    id = fields.Integer(data_key="experimentId", required=True)
    name = fields.String(required=True)
    description = fields.String(required=True)
    artifact_location = fields.String(
        data_key="artifactLocation", required=True
    )
    created_at = fields.DateTime(data_key="createdAt", required=True)
    last_updated_at = fields.DateTime(data_key="lastUpdatedAt", required=True)
    deleted_at = fields.DateTime(data_key="deletedAt", missing=None)

    @post_load
    def make_experiment(self, data):
        return Experiment(**data)


class ExperimentRunSchema(BaseSchema):
    id = fields.UUID(data_key="runId", required=True)
    run_number = fields.Integer(data_key="runNumber", required=True)
    experiment_id = fields.Integer(data_key="experimentId", required=True)
    name = fields.String(required=True)
    parent_run_id = fields.UUID(data_key="parentRunId", missing=None)
    artifact_location = fields.String(
        data_key="artifactLocation", required=True
    )
    status = EnumField(ExperimentRunStatus, by_value=True, required=True)
    started_at = fields.DateTime(data_key="startedAt", required=True)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)
    deleted_at = fields.DateTime(data_key="deletedAt", missing=None)
    tags = fields.Nested(TagSchema, many=True, required=True)
    params = fields.Nested(ParamSchema, many=True, required=True)
    metrics = fields.Nested(MetricSchema, many=True, required=True)

    @post_load
    def make_experiment_run(self, data):
        return ExperimentRun(**data)


# Schemas for payloads sent to API:


class ExperimentRunDataSchema(BaseSchema):
    metrics = fields.List(fields.Nested(MetricSchema))
    params = fields.List(fields.Nested(ParamSchema))
    tags = fields.List(fields.Nested(TagSchema))


class ExperimentRunInfoSchema(BaseSchema):
    status = EnumField(ExperimentRunStatus, by_value=True, required=True)
    ended_at = fields.DateTime(data_key="endedAt", missing=None)


class ListExperimentRunsResponseSchema(BaseSchema):
    pagination = fields.Nested(PaginationSchema, required=True)
    runs = fields.Nested(ExperimentRunSchema, many=True, required=True)

    @post_load
    def make_list_runs_response_schema(self, data):
        return ListExperimentRunsResponse(**data)


class CreateRunSchema(BaseSchema):
    name = fields.String()
    parent_run_id = fields.UUID(data_key="parentRunId")
    started_at = fields.DateTime(data_key="startedAt")
    artifact_location = fields.String(data_key="artifactLocation")
    tags = fields.Nested(TagSchema, many=True, required=True)


class _ParamFilterValueField(fields.Field):
    """Field that passes through strings or numbers."""

    default_error_messages = {
        "unsupported_type": "Param values must be of type str, int or float."
    }

    def _serialize(self, value, attr, obj, **kwargs):
        if isinstance(value, str):
            field = fields.String()
        elif isinstance(value, int) or isinstance(value, float):
            field = fields.Number()
        else:
            self.fail("unsupported_type")
        return field._serialize(value, attr, obj, **kwargs)


class _FilterValueField(fields.Field):
    def __init__(self, other_field_type, *args, **kwargs):
        self.other_field_type = other_field_type
        super(_FilterValueField, self).__init__(*args, **kwargs)

    def _serialize(self, value, attr, obj, **kwargs):
        if obj.operator == ComparisonOperator.DEFINED:
            field_cls = fields.Boolean
        else:
            field_cls = self.other_field_type
        return field_cls()._serialize(value, attr, obj, **kwargs)


def _validate_discrete(operator):
    if operator not in {
        ComparisonOperator.DEFINED,
        ComparisonOperator.EQUAL_TO,
        ComparisonOperator.NOT_EQUAL_TO,
    }:
        raise ValidationError({"operator": "Not a discrete operator."})


class _ProjectIdFilterSchema(BaseSchema):
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.UUID)
    by = fields.Constant("projectId", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        _validate_discrete(obj.operator)
        return obj


class _ExperimentIdFilterSchema(BaseSchema):
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.Integer)
    by = fields.Constant("experimentId", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        _validate_discrete(obj.operator)
        return obj


class _RunIdFilterSchema(BaseSchema):
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.UUID)
    by = fields.Constant("runId", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        _validate_discrete(obj.operator)
        return obj


class _DeletedAtFilterSchema(BaseSchema):
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.DateTime)
    by = fields.Constant("deletedAt", dump_only=True)


class _TagFilterSchema(BaseSchema):
    key = fields.String()
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.String)
    by = fields.Constant("tag", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        _validate_discrete(obj.operator)
        return obj


class _ParamFilterSchema(BaseSchema):
    key = fields.String()
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(_ParamFilterValueField)
    by = fields.Constant("param", dump_only=True)

    @pre_dump
    def check_operator(self, obj):
        if isinstance(obj.value, str):
            _validate_discrete(obj.operator)
        return obj


class _MetricFilterSchema(BaseSchema):
    key = fields.String()
    operator = EnumField(ComparisonOperator, by_value=True)
    value = _FilterValueField(fields.Float)
    by = fields.Constant("metric", dump_only=True)


class _CompoundFilterSchema(BaseSchema):
    operator = EnumField(LogicalOperator, by_value=True)
    conditions = fields.List(fields.Nested("FilterSchema"))


class FilterSchema(_OneOfSchemaWithoutType):
    type_schemas = {
        "ProjectIdFilter": _ProjectIdFilterSchema,
        "ExperimentIdFilter": _ExperimentIdFilterSchema,
        "RunIdFilter": _RunIdFilterSchema,
        "DeletedAtFilter": _DeletedAtFilterSchema,
        "TagFilter": _TagFilterSchema,
        "ParamFilter": _ParamFilterSchema,
        "MetricFilter": _MetricFilterSchema,
        "CompoundFilter": _CompoundFilterSchema,
    }


class _StartedAtSortSchema(BaseSchema):
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("startedAt", dump_only=True)


class _RunNumberSortSchema(BaseSchema):
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("runNumber", dump_only=True)


class _DurationSortSchema(BaseSchema):
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("duration", dump_only=True)


class _TagSortSchema(BaseSchema):
    key = fields.String()
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("tag", dump_only=True)


class _ParamSortSchema(BaseSchema):
    key = fields.String()
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("param", dump_only=True)


class _MetricSortSchema(BaseSchema):
    key = fields.String()
    order = EnumField(SortOrder, by_value=True)
    by = fields.Constant("metric", dump_only=True)


class SortSchema(_OneOfSchemaWithoutType):
    type_schemas = {
        "StartedAtSort": _StartedAtSortSchema,
        "RunNumberSort": _RunNumberSortSchema,
        "DurationSort": _DurationSortSchema,
        "TagSort": _TagSortSchema,
        "ParamSort": _ParamSortSchema,
        "MetricSort": _MetricSortSchema,
    }


class RunQuerySchema(BaseSchema):
    filter = _OptionalField(fields.Nested(FilterSchema))
    sort = fields.List(fields.Nested(SortSchema))
    page = fields.Nested(PageSchema, missing=None)


# Schemas for responses returned from API:


class DeleteExperimentRunsResponseSchema(BaseSchema):
    deleted_run_ids = fields.List(
        fields.UUID(), data_key="deletedRunIds", required=True
    )
    conflicted_run_ids = fields.List(
        fields.UUID(), data_key="conflictedRunIds", required=True
    )

    @post_load
    def make_delete_runs_response(self, data):
        return DeleteExperimentRunsResponse(**data)


class RestoreExperimentRunsResponseSchema(BaseSchema):
    restored_run_ids = fields.List(
        fields.UUID(), data_key="restoredRunIds", required=True
    )
    conflicted_run_ids = fields.List(
        fields.UUID(), data_key="conflictedRunIds", required=True
    )

    @post_load
    def make_restore_runs_response(self, data):
        return RestoreExperimentRunsResponse(**data)


class _MetricDataPointSchema(BaseSchema):
    """Deserialise a data point from the metric history endpoint.

    This schema is written with the expectation that it is not used alongside
    the metric subsampling feature, which can result in null timestamp or step,
    or a non-integer step.
    """

    value = fields.Float(required=True)
    timestamp = fields.DateTime(required=True)
    step = fields.Integer(required=True)

    @post_load
    def make_metric(self, data):
        return MetricDataPoint(**data)


class MetricHistorySchema(BaseSchema):
    original_size = fields.Integer(data_key="originalSize", required=True)
    subsampled = fields.Boolean(required=True)
    key = fields.String(required=True)
    history = fields.Nested(_MetricDataPointSchema, many=True, required=True)

    @post_load
    def make_history(self, data):
        return MetricHistory(**data)
