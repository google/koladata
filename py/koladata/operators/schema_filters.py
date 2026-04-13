# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Schema filter operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils

M = arolla.M
P = arolla.P
constraints = arolla.optools.constraints


@optools.add_to_registry(via_cc_operator_package=True)
@optools.as_backend_operator(
    'kd.schema_filters.apply_filter',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.schema_filter),
    ],
)
def apply_filter(x, schema_filter):  # pylint: disable=unused-argument
  """Returns a DataSlice with a new DataBag, where only parts of data are kept.

  The parts of data to keep are specified by the `schema_filter`.
  If `x` is a DataSlice, both data and schema in the result would be filtered.
  If `x` is a schema, the result would be a schema with filtered attributes.
  In both cases, all the ObjectIds are preserved.

  Filter is essentially a schema that also can contain a number of special
  values:
    - kd.schema_filters.ANY_SCHEMA_FILTER matches any schema or data.
    - kd.schema_filters.ANY_PRIMITIVE_FILTER matches any primitive data.

  Examples:
    1.
    x = kd.new(f=ds([1, 2]), g=ds([3, 4]), bar=kd.obj(x=ds(['a', 'b'])))
    filter = kd.schema(f=kd.INT32, bar=kd.schema(x=kd.STRING))
    kd.schema_filters.apply_filter(x, filter)
      -> kd.new(f=ds([1, 2]), bar=kd.obj(x=ds(['a', 'b'])))

    2.
    x = kd.schema.list_schema(kd.schema.new_schema(a=kd.INT32, b=kd.FLOAT32))
    filter = kd.schema.list_schema(kd.schema(a=kd.INT32))
    kd.schema_filters.apply_filter(x, filter)
      -> kd.schema.list_schema(kd.schema(a=kd.INT32))

    3.
    x = kd.new(foo=1, bar=kd.new(a=1, b=2))
    filter = kd.schema.new_schema(bar=kd.schema_filters.ANY_SCHEMA_FILTER)
    kd.schema_filters.apply_filter(x, filter)
      -> kd.new(kd.new(a=1, b=2))

  Args:
    x: The DataSlice or schema.
    schema_filter: The filter schema.

  Returns:
    A DataSlice with the subset of the structure of `x`, filtered according to
    the `schema_filter`.
  """
  raise NotImplementedError('implemented in the backend')
