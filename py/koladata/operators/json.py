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

"""JSON DataSlice operators."""

from arolla import arolla
from koladata.operators import optools
from koladata.operators import qtype_utils
from koladata.operators import view_overloads as _
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

P = arolla.P


# TODO: Possibly add itemid argument to match from_proto.
@optools.add_to_registry(aliases=['kd.from_json'])
@optools.as_backend_operator(
    'kd.json.from_json',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.schema),
        qtype_utils.expect_data_slice(P.default_number_schema),
        qtype_utils.expect_data_slice(P.on_invalid),
        qtype_utils.expect_data_slice(P.keys_attr),
        qtype_utils.expect_data_slice(P.values_attr),
    ],
    deterministic=False
)
def from_json(
    x,  # pylint: disable=unused-argument
    /,
    schema=schema_constants.OBJECT,  # pylint: disable=unused-argument
    default_number_schema=schema_constants.OBJECT,  # pylint: disable=unused-argument
    *,
    # Hack: deterministic=False and DispatchOperator currently don't mix well,
    # so use an empty 1D slice as an arbitrary marker that on_invalid is unset
    # instead of the conventional arolla.unspecified().
    on_invalid=data_slice.DataSlice.from_vals([]),  # pylint: disable=unused-argument
    keys_attr='json_object_keys',  # pylint: disable=unused-argument
    values_attr='json_object_values',  # pylint: disable=unused-argument
):
  """Parses a DataSlice `x` of JSON strings.

  The result will have the same shape as `x`, and missing items in `x` will be
  missing in the result. The result will use a new immutable DataBag.

  If `schema` is OBJECT (the default), the schema is inferred from the JSON
  data, and the result will have an OBJECT schema. The decoded data will only
  have BOOLEAN, numeric, STRING, LIST[OBJECT], and entity schemas, corresponding
  to JSON primitives, arrays, and objects.

  If `default_number_schema` is OBJECT (the default), then the inferred schema
  of each JSON number will be INT32, INT64, or FLOAT32, depending on its value
  and on whether it contains a decimal point or exponent, matching the combined
  behavior of python json and `kd.from_py`. Otherwise, `default_number_schema`
  must be a numeric schema, and the inferred schema of all JSON numbers will be
  that schema.

  For example:

    kd.from_json(None) -> kd.obj(None)
    kd.from_json('null') -> kd.obj(None)
    kd.from_json('true') -> kd.obj(True)
    kd.from_json('[true, false, null]') -> kd.obj([True, False, None])
    kd.from_json('[1, 2.0]') -> kd.obj([1, 2.0])
    kd.from_json('[1, 2.0]', kd.OBJECT, kd.FLOAT64)
      -> kd.obj([kd.float64(1.0), kd.float64(2.0)])

  JSON objects parsed using an OBJECT schema will record the object key order on
  the attribute specified by `keys_attr` as a LIST[STRING], and also redundantly
  record a copy of the object values as a parallel LIST on the attribute
  specified by `values_attr`. If there are duplicate keys, the last value is the
  one stored on the Koda object attribute. If a key conflicts with `keys_attr`
  or `values_attr`, it is only available in the `values_attr` list. These
  behaviors can be disabled by setting `keys_attr` and/or `values_attr` to None.

  For example:

    kd.from_json('{"a": 1, "b": "y", "c": null}') ->
        kd.obj(a=1.0, b='y', c=None,
               json_object_keys=kd.list(['a', 'b', 'c']),
               json_object_values=kd.list([1.0, 'y', None]))
    kd.from_json('{"a": 1, "b": "y", "c": null}',
                 keys_attr=None, values_attr=None) ->
        kd.obj(a=1.0, b='y', c=None)
    kd.from_json('{"a": 1, "b": "y", "c": null}',
                 keys_attr='my_keys', values_attr='my_values') ->
        kd.obj(a=1.0, b='y', c=None,
               my_keys=kd.list(['a', 'b', 'c']),
               my_values=kd.list([1.0, 'y', None]))
    kd.from_json('{"a": 1, "a": 2", "a": 3}') ->
        kd.obj(a=3.0,
               json_object_keys=kd.list(['a', 'a', 'a']),
               json_object_values=kd.list([1.0, 2.0, 3.0]))
    kd.from_json('{"json_object_keys": ["x", "y"]}') ->
        kd.obj(json_object_keys=kd.list(['json_object_keys']),
               json_object_values=kd.list([["x", "y"]]))

  If `schema` is explicitly specified, it is used to validate the JSON data,
  and the result DataSlice will have `schema` as its schema.

  OBJECT schemas inside subtrees of `schema` are allowed, and will use the
  inference behavior described above.

  Primitive schemas in `schema` will attempt to cast any JSON primitives using
  normal Koda explicit casting rules, and if those fail, using the following
  additional rules:
  - BYTES will accept JSON strings containing base64 (RFC 4648 section 4)

  If entity schemas in `schema` have attributes matching `keys_attr` and/or
  `values_attr`, then the object key and/or value order (respectively) will be
  recorded as lists on those attributes, similar to the behavior for OBJECT
  described above. These attributes must have schemas LIST[STRING] and
  LIST[T] (for a T compatible with the contained values) if present.

  For example:

    kd.from_json('null', kd.MASK) -> kd.missing
    kd.from_json('null', kd.STRING) -> kd.str(None)
    kd.from_json('123', kd.INT32) -> kd.int32(123)
    kd.from_json('123', kd.FLOAT32) -> kd.int32(123.0)
    kd.from_json('"123"', kd.STRING) -> kd.string('123')
    kd.from_json('"123"', kd.INT32) -> kd.int32(123)
    kd.from_json('"123"', kd.FLOAT32) -> kd.float32(123.0)
    kd.from_json('"MTIz"', kd.BYTES) -> kd.bytes(b'123')
    kd.from_json('"inf"', kd.FLOAT32) -> kd.float32(float('inf'))
    kd.from_json('"1e100"', kd.FLOAT32) -> kd.float32(float('inf'))
    kd.from_json('[1, 2, 3]', kd.list_schema(kd.INT32)) -> kd.list([1, 2, 3])
    kd.from_json('{"a": 1}', kd.schema.new_schema(a=kd.INT32)) -> kd.new(a=1)
    kd.from_json('{"a": 1}', kd.dict_schema(kd.STRING, kd.INT32)
      -> kd.dict({"a": 1})

    kd.from_json('{"b": 1, "a": 2}',
                 kd.new_schema(
                     a=kd.INT32, json_object_keys=kd.list_schema(kd.STRING))) ->
      kd.new(a=1, json_object_keys=kd.list(['b', 'a', 'c']))
    kd.from_json('{"b": 1, "a": 2, "c": 3}',
                 kd.new_schema(a=kd.INT32,
                               json_object_keys=kd.list_schema(kd.STRING),
                               json_object_values=kd.list_schema(kd.OBJECT))) ->
      kd.new(a=1, c=3.0,
             json_object_keys=kd.list(['b', 'a', 'c']),
             json_object_values=kd.list([1, 2.0, 3.0]))

  In general:

    `kd.to_json(kd.from_json(x))` is equivalent to `x`, ignoring differences in
    JSON number formatting and padding.

    `kd.from_json(kd.to_json(x), kd.get_schema(x))` is equivalent to `x` if `x`
    has a concrete (no OBJECT) schema, ignoring differences in Koda itemids.
    In other words, `to_json` doesn't capture the full information of `x`, but
    the original schema of `x` has enough additional information to recover it.

  Args:
    x: A DataSlice of STRING containing JSON strings to parse.
    schema: A SCHEMA DataItem containing the desired result schema. Defaults to
      kd.OBJECT.
    default_number_schema: A SCHEMA DataItem containing a numeric schema, or
      None to infer all number schemas using python-boxing-like rules.
    on_invalid: If specified, a DataItem to use in the result wherever the
      corresponding JSON string in `x` was invalid. If unspecified, any invalid
      JSON strings in `x` will cause an operator error.
    keys_attr: A STRING DataItem that controls which entity attribute is used to
      record json object key order, if it is present on the schema.
    values_attr: A STRING DataItem that controls which entity attribute is used
      to record json object values, if it is present on the schema.

  Returns:
    A DataSlice with the same shape as `x` and schema `schema`.
  """
  raise NotImplementedError('implemented in the backend')


@optools.add_to_registry(aliases=['kd.to_json'])
@optools.as_backend_operator(
    'kd.json.to_json',
    qtype_constraints=[
        qtype_utils.expect_data_slice(P.x),
        qtype_utils.expect_data_slice(P.indent),
        qtype_utils.expect_data_slice(P.ensure_ascii),
        qtype_utils.expect_data_slice(P.keys_attr),
        qtype_utils.expect_data_slice(P.values_attr),
    ],
    qtype_inference_expr=qtypes.DATA_SLICE,
)
def to_json(
    x,  # pylint: disable=unused-argument
    /,
    *,
    indent=None,  # pylint: disable=unused-argument
    ensure_ascii=True,  # pylint: disable=unused-argument
    keys_attr='json_object_keys',  # pylint: disable=unused-argument
    values_attr='json_object_values',  # pylint: disable=unused-argument
):
  r"""Converts `x` to a DataSlice of JSON strings.

  The following schemas are allowed:
  - STRING, BYTES, INT32, INT64, FLOAT32, FLOAT64, MASK, BOOLEAN
  - LIST[T] where T is an allowed schema
  - DICT{K, V} where K is one of {STRING, BYTES, INT32, INT64}, and V is an
    allowed schema
  - Entity schemas where all attribute values have allowed schemas
  - OBJECT schemas resolving to allowed schemas

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

  Koda BYTES values are converted to base64 strings (RFC 4648 section 4).

  Integers are always stored exactly in decimal. Finite floating point values
  are formatted similar to python format string `%.17g`, except that a decimal
  point and at least one decimal digit are always present if the format doesn't
  use scientific notation. This appears to match the behavior of python json.

  Non-finite floating point values are stored as the strings "inf", "-inf" and
  "nan". This differs from python json, which emits non-standard JSON tokens
  `Infinity` and `NaN`. This also differs from javascript, which stores these
  values as `null`, which would be ambiguous with Koda missing values. There is
  unfortunately no standard way to express these values in JSON.

  By default, JSON objects are written with keys in sorted order. However, it is
  also possible to control the key order of JSON objects using the `keys_attr`
  argument. If an entity has the attribute specified by `keys_attr`, then that
  attribute must have schema LIST[STRING], and the JSON object will have exactly
  the key order specified in that list, including duplicate keys.

  To write duplicate JSON object keys with different values, use `values_attr`
  to designate an attribute to hold a parallel list of values to write.

  For example:

    kd.to_json(kd.new(x=1, y=2)) -> '{"x": 2, "y": 1}'
    kd.to_json(kd.new(x=1, y=2, json_object_keys=kd.list(['y', 'x'])))
      -> '{"y": 2, "x": 1}'
    kd.to_json(kd.new(x=1, y=2, foo=kd.list(['y', 'x'])), keys_attr='foo')
      -> '{"y": 2, "x": 1}'
    kd.to_json(kd.new(x=1, y=2, z=3, json_object_keys=kd.list(['x', 'z', 'x'])))
      -> '{"x": 1, "z": 3, "x": 1}'

    kd.to_json(kd.new(json_object_keys=kd.list(['x', 'z', 'x']),
                      json_object_values=kd.list([1, 2, 3])))
      -> '{"x": 1, "z": 2, "x": 3}'
    kd.to_json(kd.new(a=kd.list(['x', 'z', 'x']), b=kd.list([1, 2, 3])),
               keys_attr='a', values_attr='b')
      -> '{"x": 1, "z": 2, "x": 3}'


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
    keys_attr: A STRING DataItem that controls which entity attribute controls
      json object key order, or None to always use sorted order. Defaults to
      `json_object_keys`.
    values_attr: A STRING DataItem that can be used with `keys_attr` to give
      full control over json object contents. Defaults to `json_object_values`.
  """
  raise NotImplementedError('implemented in the backend')
