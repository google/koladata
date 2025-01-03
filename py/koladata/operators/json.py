# Copyright 2024 Google LLC
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

"""JSON DataSlice operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import view_overloads as _
from koladata.types import qtypes

P = arolla.P


# TODO: Add utility ops to set `__koladata_json_object_keys__`
# and `__koladata_json_object_values__`.
@optools.add_to_registry(aliases=['kde.to_json'])
@optools.as_backend_operator(
    'kde.json.to_json',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.indent),
        qtype_utils.expect_data_slice(P.ensure_ascii),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def to_json(
    x,  # pylint: disable=unused-argument
    /,
    *,
    indent=None,  # pylint: disable=unused-argument
    ensure_ascii=True,  # pylint: disable=unused-argument
):
  r"""Converts `x` to a DataSlice of JSON strings.

  Data with STRING, numeric, MASK, BOOLEAN, LIST, STRING-key DICT, and entity
  schemas are allowed, along with OBJECT schemas that resolve to those schemas.
  Itemid cycles are not allowed.

  Missing DataSlice items in the input are missing in the result. Missing values
  inside of lists/entities/etc. are encoded as JSON `null`, except for
  `kd.missing`, which is encoded as `false`.

  For example:

    kd.to_json(None) -> kd.str(None)
    kd.to_json(kd.missing) -> kd.str(None)
    kd.to_json(kd.present) -> 'true'
    kd.to_json(True) -> 'true'
    kd.to_json(kd.slice([1, None, 3])) -> ['1', None, '3']
    kd.to_json(kd.list([1, None, 3])) -> '[1, null, 3]'
    kd.to_json(kd.dict({'a': 1, 'b':'2'}) -> '{"a": 1, "b": "2"}'
    kd.to_json(kd.new(a=1, b='2')) -> '{"a": 1, "b": "2"}'
    kd.to_json(kd.new(x=None)) -> '{"x": null}'
    kd.to_json(kd.new(x=kd.missing)) -> '{"x": false}'

  Integers are always stored exactly in decimal. Finite floating point values
  are formatted similar to python format string `%.17g`, except that a decimal
  point and at least one decimal digit are always present if the format doesn't
  use scientific notation. This appears to match the behavior of python json.

  Non-finite floating point values are stored as the strings "inf", "-inf" and
  "nan". This differs from python json, which emits non-standard JSON tokens
  `Infinity` and `NaN`. This also differs from javascript, which stores these
  values as `null`, which would be ambiguous with Koda missing values. There is
  unfortunately no standard way to express these values in JSON.

  The `indent` and `ensure_ascii` arguments control JSON formatting:
  - If `indent` is negative, then the JSON is formatted without any whitespace.
  - If `indent` is None (the default), the JSON is formatted with a single
    padding space only after ',' and ':' and no other whitespace.
  - If `indent` is zero or positive, the JSON is pretty-printed, with that
    number of spaces used for indenting each level.
  - If `ensure_ascii` is True (the default) then all non-ASCII code points in
    strings will be escaped, and the result strings will be ASCII-only.
    Otherwise, they will be left as-is.

  For example:

    kd.to_json(kd.list([1, 2, 3]), indent=-1) -> '[1,2,3]'
    kd.to_json(kd.list([1, 2, 3]), indent=2) -> '[\n  1,\n  2,\n  3\n]'

    kd.to_json('✨', ensure_ascii=True) -> '"\\u2728"'
    kd.to_json('✨', ensure_ascii=False) -> '"✨"'

  Args:
    x: The DataSlice to convert.
    indent: An INT32 DataItem that describes how the result should be indented.
    ensure_ascii: A BOOLEAN DataItem that controls non-ASCII escaping.
  """
  raise NotImplementedError('implemented in the backend')
