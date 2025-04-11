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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE
OBJECT = schema_constants.OBJECT


QTYPES = frozenset([
    (
        DATA_SLICE,
        DATA_SLICE,
        DATA_SLICE,
        DATA_SLICE,
        DATA_SLICE,
        DATA_SLICE,
        qtypes.NON_DETERMINISTIC_TOKEN,
        DATA_SLICE,
    ),
])


class JsonFromJsonTest(parameterized.TestCase):

  @parameterized.parameters(
      # schema OBJECT, primitives
      (ds(None), {}, ds(None, OBJECT)),
      (ds([None]), {}, ds([None], OBJECT)),
      (
          ds(None, schema_constants.STRING),
          {},
          ds(None, OBJECT),
      ),
      (ds('null'), {}, ds(None, OBJECT)),
      (ds('true'), {}, ds(True, OBJECT)),
      (ds('false'), {}, ds(False, OBJECT)),
      (ds('0'), {}, ds(0, OBJECT)),
      (ds('1'), {}, ds(1, OBJECT)),
      (ds('-1'), {}, ds(-1, OBJECT)),
      (ds('10000000000'), {}, ds(10000000000, OBJECT)),
      (ds('-10000000000'), {}, ds(-10000000000, OBJECT)),
      # Note: this is actually stored as INT64 -1, we're checking that the
      # wrapping behavior of from_json and python boxing is the same.
      (ds('18446744073709551615'), {}, ds(18446744073709551615, OBJECT)),
      (ds('0.0'), {}, ds(0.0, OBJECT)),
      (ds('1.0'), {}, ds(1.0, OBJECT)),
      (ds('-1.0'), {}, ds(-1.0, OBJECT)),
      (ds('""'), {}, ds('', OBJECT)),
      (ds('"abc"'), {}, ds('abc', OBJECT)),
      (
          ds([
              None,
              'null',
              'true',
              'false',
              '0',
              '1',
              '-1',
              '0.0',
              '1.0',
              '-1.0',
              '""',
              '"abc"',
          ]),
          {},
          ds(
              [None, None, True, False, 0, 1, -1, 0.0, 1.0, -1.0, '', 'abc'],
              OBJECT,
          ),
      ),
      (ds([['1'], ['2', '3']]), {}, ds([[1], [2, 3]], OBJECT)),
      # schema OBJECT, primitives, default_number_schema != OBJECT
      (
          ds('1'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.FLOAT64},
          ds(ds(1.0, schema_constants.FLOAT64), OBJECT),
      ),
      (
          ds('-1'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.FLOAT64},
          ds(ds(-1.0, schema_constants.FLOAT64), OBJECT),
      ),
      (
          ds('1.0'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.FLOAT64},
          ds(ds(1.0, schema_constants.FLOAT64), OBJECT),
      ),
      (
          ds('1'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.FLOAT32},
          ds(ds(1.0, schema_constants.FLOAT32), OBJECT),
      ),
      (
          ds('-1'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.FLOAT32},
          ds(ds(-1.0, schema_constants.FLOAT32), OBJECT),
      ),
      (
          ds('1.0'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.FLOAT32},
          ds(ds(1.0, schema_constants.FLOAT32), OBJECT),
      ),
      (
          ds('1'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.INT64},
          ds(ds(1, schema_constants.INT64), OBJECT),
      ),
      (
          ds('-1'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.INT64},
          ds(ds(-1, schema_constants.INT64), OBJECT),
      ),
      (
          ds('1'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.INT32},
          ds(ds(1, schema_constants.INT32), OBJECT),
      ),
      (
          ds('-1'),
          {'schema': OBJECT, 'default_number_schema': schema_constants.INT32},
          ds(ds(-1, schema_constants.INT32), OBJECT),
      ),
      # MASK schema
      (ds('null'), {'schema': schema_constants.MASK}, mask_constants.missing),
      (ds('false'), {'schema': schema_constants.MASK}, mask_constants.missing),
      (ds('true'), {'schema': schema_constants.MASK}, mask_constants.present),
      # BOOLEAN schema
      (
          ds('null'),
          {'schema': schema_constants.BOOLEAN},
          ds(None, schema_constants.BOOLEAN),
      ),
      (
          ds('false'),
          {'schema': schema_constants.BOOLEAN},
          ds(False),
      ),
      (
          ds('true'),
          {'schema': schema_constants.BOOLEAN},
          ds(True),
      ),
      (
          ds('0'),
          {'schema': schema_constants.BOOLEAN},
          ds(False),
      ),
      (
          ds('1'),
          {'schema': schema_constants.BOOLEAN},
          ds(True),
      ),
      (
          ds('0.0'),
          {'schema': schema_constants.BOOLEAN},
          ds(False),
      ),
      (
          ds('0.1'),
          {'schema': schema_constants.BOOLEAN},
          ds(True),
      ),
      # STRING schema
      (
          ds('null'),
          {'schema': schema_constants.STRING},
          ds(None, schema_constants.STRING),
      ),
      (ds('""'), {'schema': schema_constants.STRING}, ds('')),
      (ds('"abc"'), {'schema': schema_constants.STRING}, ds('abc')),
      (ds('"\u1234"'), {'schema': schema_constants.STRING}, ds('\u1234')),
      (ds('"\\u1234"'), {'schema': schema_constants.STRING}, ds('\u1234')),
      (ds('1234'), {'schema': schema_constants.STRING}, ds('1234')),
      (ds('1234.5'), {'schema': schema_constants.STRING}, ds('1234.5')),
      # BYTES schema
      (ds('""'), {'schema': schema_constants.BYTES}, ds(b'')),
      (ds('"YWJjZA=="'), {'schema': schema_constants.BYTES}, ds(b'abcd')),
      # FLOAT64 schema
      (
          ds('null'),
          {'schema': schema_constants.FLOAT64},
          ds(None, schema_constants.FLOAT64),
      ),
      (
          ds('0'),
          {'schema': schema_constants.FLOAT64},
          ds(0.0, schema_constants.FLOAT64),
      ),
      (
          ds('-1'),
          {'schema': schema_constants.FLOAT64},
          ds(-1.0, schema_constants.FLOAT64),
      ),
      (
          ds('0.5'),
          {'schema': schema_constants.FLOAT64},
          ds(0.5, schema_constants.FLOAT64),
      ),
      (
          ds('"0.5"'),
          {'schema': schema_constants.FLOAT64},
          ds(0.5, schema_constants.FLOAT64),
      ),
      (
          ds('"inf"'),
          {'schema': schema_constants.FLOAT64},
          ds(float('inf'), schema_constants.FLOAT64),
      ),
      (
          ds('"-inf"'),
          {'schema': schema_constants.FLOAT64},
          ds(float('-inf'), schema_constants.FLOAT64),
      ),
      (
          ds('"nan"'),
          {'schema': schema_constants.FLOAT64},
          ds(float('nan'), schema_constants.FLOAT64),
      ),
      # FLOAT32 schema
      (
          ds('null'),
          {'schema': schema_constants.FLOAT32},
          ds(None, schema_constants.FLOAT32),
      ),
      (
          ds('0'),
          {'schema': schema_constants.FLOAT32},
          ds(0.0, schema_constants.FLOAT32),
      ),
      (
          ds('-1'),
          {'schema': schema_constants.FLOAT32},
          ds(-1.0, schema_constants.FLOAT32),
      ),
      (
          ds('0.5'),
          {'schema': schema_constants.FLOAT32},
          ds(0.5, schema_constants.FLOAT32),
      ),
      (
          ds('"0.5"'),
          {'schema': schema_constants.FLOAT32},
          ds(0.5, schema_constants.FLOAT32),
      ),
      (
          ds('"inf"'),
          {'schema': schema_constants.FLOAT32},
          ds(float('inf'), schema_constants.FLOAT32),
      ),
      (
          ds('"-inf"'),
          {'schema': schema_constants.FLOAT32},
          ds(float('-inf'), schema_constants.FLOAT32),
      ),
      (
          ds('"nan"'),
          {'schema': schema_constants.FLOAT32},
          ds(float('nan'), schema_constants.FLOAT32),
      ),
      # INT32 schema
      (
          ds('null'),
          {'schema': schema_constants.INT32},
          ds(None, schema_constants.INT32),
      ),
      (
          ds('0'),
          {'schema': schema_constants.INT32},
          ds(0.0, schema_constants.INT32),
      ),
      (
          ds('-1'),
          {'schema': schema_constants.INT32},
          ds(-1.0, schema_constants.INT32),
      ),
      (
          ds('"0"'),
          {'schema': schema_constants.INT32},
          ds(0, schema_constants.INT32),
      ),
      # INT64 schema
      (
          ds('null'),
          {'schema': schema_constants.INT64},
          ds(None, schema_constants.INT64),
      ),
      (
          ds('0'),
          {'schema': schema_constants.INT64},
          ds(0.0, schema_constants.INT64),
      ),
      (
          ds('-1'),
          {'schema': schema_constants.INT64},
          ds(-1.0, schema_constants.INT64),
      ),
      (
          ds('"0"'),
          {'schema': schema_constants.INT64},
          ds(0, schema_constants.INT64),
      ),
      (
          ds('18446744073709551615'),
          {'schema': schema_constants.INT64},
          ds(18446744073709551615, schema_constants.INT64),
      ),
      # on_invalid
      (ds('not valid json'), {'on_invalid': None}, ds(None, OBJECT)),
      (ds('not valid json'), {'on_invalid': 'ERROR'}, ds('ERROR', OBJECT)),
  )
  def test_eval_ignore_bag(self, x, kwargs, expected_result):
    result = expr_eval.eval(kde.json.from_json(x, **kwargs))
    self.assertIsNone(result.get_bag().is_mutable().to_py())
    testing.assert_equal(
        result.get_schema().no_bag(), expected_result.get_schema().no_bag()
    )
    testing.assert_equal(result.no_bag(), expected_result)

  def test_on_invalid_adoption(self):
    on_invalid = fns.obj(x=1)
    result = expr_eval.eval(
        kde.json.from_json('not valid json', on_invalid=on_invalid)
    )
    self.assertEqual(result.to_py(), on_invalid.to_py())

  @parameterized.parameters(
      # schema OBJECT, array
      (ds('[]'), {}, []),
      (ds('[[], [[[]], []]]'), {}, [[], [[[]], []]]),
      (
          ds('[null, false, true, 1, 2, 3, 0.0, 1.0, -1.0, "", "abc"]'),
          {},
          [None, False, True, 1, 2, 3, 0.0, 1.0, -1.0, '', 'abc'],
      ),
      # LIST schema, array
      (ds('[]'), {'schema': fns.list_schema(schema_constants.INT32)}, []),
      (
          ds('[null]'),
          {'schema': fns.list_schema(schema_constants.INT32)},
          [None],
      ),
      (
          ds('[1, null, 3]'),
          {'schema': fns.list_schema(schema_constants.INT32)},
          [1, None, 3],
      ),
      # schema OBJECT, object
      (ds('{}'), {}, {'json_object_keys': [], 'json_object_values': []}),
      (ds('{}'), {'values_attr': None}, {'json_object_keys': []}),
      (ds('{}'), {'keys_attr': None, 'values_attr': None}, {}),
      (
          ds('{"a": 1, "b": "2", "c": null, "a": 4.0}'),
          {},
          {
              'a': 4.0,
              'b': '2',
              'c': None,
              'json_object_keys': ['a', 'b', 'c', 'a'],
              'json_object_values': [1, '2', None, 4.0],
          },
      ),
      (
          ds('{"a": 1, "b": "2", "c": null, "a": 4.0}'),
          {'values_attr': None},
          {
              'a': 4.0,
              'b': '2',
              'c': None,
              'json_object_keys': ['a', 'b', 'c', 'a'],
          },
      ),
      (
          ds('{"a": 1, "b": "2", "c": null, "a": 4.0}'),
          {'keys_attr': None, 'values_attr': None},
          {'a': 4.0, 'b': '2', 'c': None},
      ),
      (
          ds('{"a": 1, "b": "2", "c": null, "a": 4.0}'),
          {'keys_attr': 'c', 'values_attr': 'b'},
          {
              'a': 4.0,
              'b': [1, '2', None, 4.0],
              'c': ['a', 'b', 'c', 'a'],
          },
      ),
      # entity schema, object
      (
          ds('{}'),
          {'schema': fns.schema.new_schema(a=schema_constants.INT32)},
          {'a': None},
      ),
      (
          ds('{"a": 1, "b": 2}'),
          {'schema': fns.schema.new_schema(a=schema_constants.INT32)},
          {'a': 1},
      ),
      (
          ds('{"a": 1, "b": 2}'),
          {
              'schema': fns.schema.new_schema(
                  a=schema_constants.INT32,
                  json_object_keys=fns.list_schema(schema_constants.STRING),
              )
          },
          {'a': 1, 'json_object_keys': ['a', 'b']},
      ),
      (
          ds('{"a": 1, "b": 2}'),
          {
              'schema': fns.schema.new_schema(
                  a=schema_constants.FLOAT32,
                  json_object_keys=fns.list_schema(schema_constants.STRING),
                  json_object_values=fns.list_schema(schema_constants.OBJECT),
              )
          },
          {
              'a': 1.0,
              'json_object_keys': ['a', 'b'],
              'json_object_values': [1.0, 2],
          },
      ),
      # DICT schema, object
      (
          ds('{}'),
          {
              'schema': fns.dict_schema(
                  key_schema=schema_constants.STRING,
                  value_schema=schema_constants.INT32,
              )
          },
          {},
      ),
      (
          ds('{"a": 1, "b": 2}'),
          {
              'schema': fns.dict_schema(
                  key_schema=schema_constants.STRING,
                  value_schema=schema_constants.INT32,
              )
          },
          {'a': 1, 'b': 2},
      ),
      (
          ds('{"0": 1, "3": 2}'),
          {
              'schema': fns.dict_schema(
                  key_schema=schema_constants.STRING,
                  value_schema=schema_constants.INT32,
              )
          },
          {'0': 1, '3': 2},
      ),
      (
          ds('{"0": 1, "3": 2}'),
          {
              'schema': fns.dict_schema(
                  key_schema=schema_constants.INT32,
                  value_schema=schema_constants.INT32,
              )
          },
          {0: 1, 3: 2},
      ),
      (
          ds('{"a": {"b": 1}, "c": 2}'),
          {},
          {
              'a': {
                  'b': 1,
                  'json_object_keys': ['b'],
                  'json_object_values': [1],
              },
              'c': 2,
              'json_object_keys': ['a', 'c'],
              # Note: in the Koda data, this is an alias to the values of "a"
              # and "c", so there is no duplication.
              'json_object_values': [
                  {
                      'b': 1,
                      'json_object_keys': ['b'],
                      'json_object_values': [1],
                  },
                  2,
              ],
          },
      ),
      (
          ds('{"a": {"b": 1}, "c": 2}'),
          {
              'schema': fns.schema.new_schema(
                  a=fns.schema.new_schema(
                      b=schema_constants.INT32,
                      json_object_keys=fns.list_schema(schema_constants.STRING),
                      json_object_values=fns.list_schema(
                          schema_constants.OBJECT
                      ),
                  ),
                  c=schema_constants.INT32,
                  json_object_keys=fns.list_schema(schema_constants.STRING),
                  json_object_values=fns.list_schema(schema_constants.OBJECT),
              )
          },
          {
              'a': {
                  'b': 1,
                  'json_object_keys': ['b'],
                  'json_object_values': [1],
              },
              'c': 2,
              'json_object_keys': ['a', 'c'],
              # Note: in the Koda data, this is an alias to the values of "a"
              # and "c", so there is no duplication.
              'json_object_values': [
                  {
                      'b': 1,
                      'json_object_keys': ['b'],
                      'json_object_values': [1],
                  },
                  2,
              ],
          },
      ),
      (
          ds('{"a": {"b": 1}, "c": 2}'),
          {
              'schema': fns.schema.new_schema(
                  a=fns.dict_schema(
                      schema_constants.STRING, schema_constants.INT32
                  ),
                  c=schema_constants.INT32,
                  json_object_keys=fns.list_schema(schema_constants.STRING),
                  json_object_values=fns.list_schema(schema_constants.OBJECT),
              )
          },
          {
              'a': {'b': 1},
              'c': 2,
              'json_object_keys': ['a', 'c'],
              # Note: in the Koda data, this is an alias to the values of "a"
              # and "c", so there is no duplication.
              'json_object_values': [{'b': 1}, 2],
          },
      ),
      (
          ds('{"a": {"b": 1}, "c": 2}'),
          {
              'schema': fns.schema.new_schema(
                  a=fns.schema.new_schema(b=schema_constants.INT64),
                  c=schema_constants.INT32,
              )
          },
          {'a': {'b': 1}, 'c': 2},
      ),
      (
          ds('{"a": {"b": 1}, "c": 2}'),
          {'schema': fns.schema.new_schema(a=OBJECT, c=schema_constants.INT32)},
          {
              'a': {
                  'b': 1,
                  'json_object_keys': ['b'],
                  'json_object_values': [1],
              },
              'c': 2,
          },
      ),
      (
          ds('{"a": {"b": 1}, "c": 2}'),
          {'schema': fns.dict_schema(schema_constants.STRING, OBJECT)},
          {
              'a': {
                  'b': 1,
                  'json_object_keys': ['b'],
                  'json_object_values': [1],
              },
              'c': 2,
          },
      ),
  )
  def test_eval_structure(self, x, kwargs, expected_py_result):
    result = expr_eval.eval(kde.json.from_json(x, **kwargs))
    if 'schema' in kwargs:
      self.assertEqual(result.get_schema(), kwargs['schema'])
    py_result = fns.to_pytree(result, max_depth=-1)
    self.assertEqual(py_result, expected_py_result)

  def test_eval_dataslice(self):
    json_input = ds(['{"a": 1}', '{"a": 2}'])
    result = expr_eval.eval(kde.json.from_json(json_input))
    testing.assert_equal(result.a.no_bag(), ds([1, 2], schema_constants.OBJECT))

    result = expr_eval.eval(
        kde.json.from_json(
            json_input, schema=fns.schema.new_schema(a=schema_constants.INT32)
        )
    )
    testing.assert_equal(result.a.no_bag(), ds([1, 2]))

  def test_values_attr_causes_schema_embed(self):
    # If values_attr is present on a schema, then it should cause all
    # non-primitive values to have embedded schemas, even if they have
    # non-OBJECT schemas. This is necessary because they must be stored in a
    # LIST[OBJECT] in the values attr.
    schema = fns.schema.new_schema(
        a=fns.dict_schema(schema_constants.STRING, schema_constants.INT32),
        b=fns.list_schema(schema_constants.INT32),
        c=fns.schema.new_schema(
            y=schema_constants.STRING, w=schema_constants.INT32
        ),
        d=schema_constants.INT32,
        json_object_keys=fns.list_schema(schema_constants.STRING),
        json_object_values=fns.list_schema(schema_constants.OBJECT),
    )
    result = kde.json.from_json(
        ds('{"a": {"x": 1}, "b": [2, 3], "c": {"y": "z", "w": 4}, "d": 5}'),
        schema=schema,
    ).eval()

    testing.assert_equal(result.get_schema().no_bag(), schema.no_bag())
    testing.assert_equal(
        result.json_object_values[:].get_obj_schema().no_bag(),
        ds([
            schema.a.no_bag(),
            schema.b.no_bag(),
            schema.c.no_bag(),
            schema.d.no_bag(),
        ]),
    )

  def test_json_parse_error(self):
    with self.assertRaisesRegex(ValueError, 'json parse error at position 2'):
      _ = kde.json.from_json('{?}').eval()

    with self.assertRaisesRegex(ValueError, 'json parse error at position 2'):
      _ = kde.json.from_json('[').eval()

    with self.assertRaisesRegex(ValueError, 'json parse error at position 1'):
      _ = kde.json.from_json('/* comments not allowed */').eval()

  def test_slice_one_element_error(self):
    with self.assertRaisesRegex(ValueError, 'json parse error at position 2'):
      _ = kde.json.from_json(ds(['[]', '[', '[]'])).eval()

  @parameterized.parameters(
      ('false', schema_constants.NONE),
      ('0', schema_constants.NONE),
      ('0', schema_constants.MASK),
      ('-1', schema_constants.NONE),
      ('-1', schema_constants.MASK),
      ('0.0', schema_constants.NONE),
      ('0.0', schema_constants.MASK),
      ('""', schema_constants.NONE),
      ('""', schema_constants.MASK),
      ('""', schema_constants.BOOLEAN),
      # non-exhaustive
      ('[]', schema_constants.INT32),
      ('[]', fns.dict_schema(schema_constants.STRING, schema_constants.OBJECT)),
      ('{}', schema_constants.INT32),
      ('{}', fns.list_schema(schema_constants.INT32)),
      ('{}', fns.dict_schema(schema_constants.BYTES, schema_constants.OBJECT)),
  )
  def test_invalid_schema_error(self, value, schema):
    with self.assertRaisesRegex(ValueError, 'json .* invalid for '):
      _ = kde.json.from_json(value, schema).eval()

  @parameterized.parameters(
      ('10000000000', schema_constants.INT32),
      ('-10000000000', schema_constants.INT32),
  )
  def test_out_of_range_error(self, value, schema):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(f'cannot cast int64{{{value}}} to int32'),
    ):
      _ = kde.json.from_json(value, schema).eval()

  @parameterized.parameters(
      ('""', schema_constants.FLOAT64),
      ('""', schema_constants.FLOAT32),
      ('""', schema_constants.INT64),
      ('""', schema_constants.INT32),
      ('"aaa"', schema_constants.FLOAT64),
      ('"aaa"', schema_constants.FLOAT32),
      ('"aaa"', schema_constants.INT64),
      ('"aaa"', schema_constants.INT32),
      ('"0.0"', schema_constants.INT64),
      ('"0.0"', schema_constants.INT32),
  )
  def test_string_number_parse_error(self, value, schema):
    with self.assertRaisesRegex(
        ValueError, 'kd.json.from_json: unable to parse '
    ):
      _ = kde.json.from_json(value, schema).eval()

  def test_x_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.json.from_json: argument `x` must be a slice of STRING, got a slice'
        ' of OBJECT containing INT32 values',
    ):
      _ = kde.json.from_json(fns.obj(1)).eval()

    with self.assertRaisesRegex(
        ValueError,
        'kd.json.from_json: argument `x` must be a slice of STRING, got a slice'
        ' of OBJECT containing INT32 values',
    ):
      _ = kde.json.from_json(ds([fns.obj(1)])).eval()

    with self.assertRaisesRegex(
        ValueError,
        'kd.json.from_json: argument `x` must be a slice of STRING, got a slice'
        ' of BYTES',
    ):
      _ = kde.json.from_json(ds(b'[]')).eval()

  def test_default_number_schema_arg_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected default_number_schema to be OBJECT or a numeric primitive'
        ' schema',
    ):
      _ = kde.json.from_json(
          '1', default_number_schema=schema_constants.STRING
      ).eval()

  def test_on_invalid_arg_error(self):
    with self.assertRaisesRegex(ValueError, 'on_invalid must be a DataItem'):
      _ = kde.json.from_json('1', on_invalid=ds([1, 2])).eval()

    with self.assertRaisesRegex(ValueError, 'no common schema'):
      _ = kde.json.from_json('1', on_invalid=fns.new().get_itemid()).eval()

  def test_values_attr_arg_error(self):
    with self.assertRaisesRegex(
        ValueError, 'values_attr must be None if keys_attr is None'
    ):
      _ = kde.json.from_json(ds([]), keys_attr=None).eval()

  @parameterized.parameters(
      ('[null, false, true]',),
      ('{"z": 1, "y": "2", "x": null, "z": 4.0, "w": []}',),
      # Even works when the input collides with keys_attr and values_attr.
      ('{"json_object_keys": [7, 100], "json_object_values": {"a": 2}}',),
  )
  def test_json_roundtrip(self, value):
    self.assertEqual(
        kde.json.to_json(kde.json.from_json(value)).eval(), ds(value)
    )

  @parameterized.parameters(
      mask_constants.present,
      mask_constants.missing,
      ds(None),
      ds(None, schema_constants.INT32),
      ds(True),
      ds(False),
      ds(1, schema_constants.INT32),
      ds(1, schema_constants.INT64),
      ds(1, schema_constants.FLOAT32),
      ds(1, schema_constants.FLOAT64),
      ds('abc'),
      (fns.list(['abc', 'def']),),
      (fns.dict({'abc': 'def'}),),
      (fns.new({'abc': 'def', 'g': 123}),),
      (fns.dict({'abc': fns.dict({'def': 'ghi'})}),),
  )
  def test_koda_roundtrip(self, x):
    x1 = kde.json.from_json(kde.json.to_json(x), kde.get_schema(x)).eval()
    self.assertEqual(fns.to_py(x, max_depth=-1), fns.to_py(x1, max_depth=-1))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.json.from_json,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        max_arity=7,
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.json.from_json, kde.from_json))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.json.from_json(I.x)))


if __name__ == '__main__':
  absltest.main()
