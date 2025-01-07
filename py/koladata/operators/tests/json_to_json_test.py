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


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class JsonToJsonTest(parameterized.TestCase):

  @parameterized.parameters(
      (mask_constants.missing, {}, ds(None, schema=schema_constants.STRING)),
      (ds(arolla.unit()), {}, ds('true')),
      (ds(True), {}, ds('true')),
      (ds(False), {}, ds('false')),
      (ds([1, 2, 3]), {}, ds(['1', '2', '3'])),
      (ds([1, None, 3]), {}, ds(['1', None, '3'])),
      (ds(['a', '\n', '\\']), {}, ds(['"a"', '"\\n"', '"\\\\"'])),
      (ds(123, schema_constants.INT32), {}, ds('123')),
      (ds(123, schema_constants.INT64), {}, ds('123')),
      (ds(1.25), {}, ds('1.25')),
      (ds(1.25, schema_constants.FLOAT32), {}, ds('1.25')),
      (ds(1.25, schema_constants.FLOAT64), {}, ds('1.25')),
      (ds(1.0), {}, ds('1.0')),
      (ds(0.0), {}, ds('0.0')),
      (ds(-0.0), {}, ds('-0.0')),
      (ds(float('inf')), {}, ds('"inf"')),
      (ds(float('-inf')), {}, ds('"-inf"')),
      (ds(float('nan')), {}, ds('"nan"')),
      (fns.list([]), {}, ds('[]')),
      (fns.list([mask_constants.present]), {}, ds('[true]')),
      (fns.list([mask_constants.missing]), {}, ds('[false]')),
      (fns.list([None]), {}, ds('[null]')),
      (fns.list([1, 2, 3]), {}, ds('[1, 2, 3]')),
      (
          fns.list([[1], [2, 3]]),
          {},
          ds('[[1], [2, 3]]'),
      ),
      (
          fns.dict({}),
          {},
          ds('{}'),
      ),
      (
          fns.dict({'a': 1, 'b': '2'}),
          {},
          ds('{"a": 1, "b": "2"}'),
      ),
      (
          fns.new(a=1, b='2', c=mask_constants.missing),
          {},
          ds('{"a": 1, "b": "2", "c": false}'),
      ),
      (
          fns.obj(a=1, b='2', c=None),
          {},
          ds('{"a": 1, "b": "2", "c": null}'),
      ),
      (
          fns.new(a=ds([1, 2]), b=ds([3, None])),
          {},
          ds([
              '{"a": 1, "b": 3}',
              '{"a": 2, "b": null}',
          ]),
      ),
      (
          fns.obj(a=ds([1, 2]), b=ds([3, None])),
          {},
          ds([
              '{"a": 1, "b": 3}',
              '{"a": 2, "b": null}',
          ]),
      ),
      # indent != None
      (fns.list([1, 2, 3]), {'indent': -1}, ds('[1,2,3]')),
      (fns.list([1, 2, 3]), {'indent': 0}, ds('[\n1,\n2,\n3\n]')),
      (fns.list([1, 2, 3]), {'indent': 2}, ds('[\n  1,\n  2,\n  3\n]')),
      (
          fns.dict({'a': 1, 'b': 2}),
          {'indent': -1},
          ds('{"a":1,"b":2}'),
      ),
      (
          fns.dict({'a': 1, 'b': 2}),
          {'indent': 0},
          ds('{\n"a": 1,\n"b": 2\n}'),
      ),
      (
          fns.dict({'a': 1, 'b': 2}),
          {'indent': 2},
          ds('{\n  "a": 1,\n  "b": 2\n}'),
      ),
      (fns.list([[]]), {'indent': -1}, ds('[[]]')),
      (fns.list([[]]), {'indent': 0}, ds('[\n[]\n]')),
      (fns.list([[]]), {'indent': 2}, ds('[\n  []\n]')),
      # ensure_ascii
      (ds('♣'), {}, ds('"\\u2663"')),
      (ds('♣'), {'ensure_ascii': False}, ds('"♣"')),
      # keys_attr
      (
          fns.new(
              my_keys=fns.list(['b', 'a', 'c']),
              a=1,
              b=2,
              c=3,
          ),
          {'keys_attr': 'my_keys'},
          ds('{"b": 2, "a": 1, "c": 3}'),
      ),
      (
          fns.new(
              my_keys=fns.list(['b', 'a', 'c']),
              a=1,
              b=2,
              c=fns.new(
                  my_keys=fns.list(['x', 'z', 'y']),
                  x=10,
                  y=11,
                  z=12,
              ),
          ),
          {'keys_attr': 'my_keys'},
          ds('{"b": 2, "a": 1, "c": {"x": 10, "z": 12, "y": 11}}'),
      ),
      (
          fns.new(
              my_keys=fns.list(['b', 'a', 'c', 'a']),
              a=1,
              b=2,
              c=3,
          ),
          {'keys_attr': 'my_keys'},
          ds('{"b": 2, "a": 1, "c": 3, "a": 1}'),
      ),
      # keys_attr and values_attr
      (
          fns.new(
              my_keys=fns.list(['b', 'a', 'c', 'a']),
              my_values=fns.list([1, 2, 3, 4]),
          ),
          {'keys_attr': 'my_keys', 'values_attr': 'my_values'},
          ds('{"b": 1, "a": 2, "c": 3, "a": 4}'),
      ),
      (
          fns.new(
              my_keys=fns.list(['b', 'a', 'c', 'a']),
              my_values=fns.list([
                  fns.obj(a=1, b=2),
                  fns.obj(my_keys=['y', 'x'], x=1, y=2),
                  fns.obj(my_values=[3, 4], a=1, b=2),
                  fns.obj(
                      my_keys=['y', 'x'], my_values=[5, 6], a=1, b=2, x=3, y=4
                  ),
              ]),
          ),
          {'keys_attr': 'my_keys', 'values_attr': 'my_values'},
          ds(
              '{'
              '"b": {"a": 1, "b": 2}, '
              '"a": {"y": 2, "x": 1}, '
              '"c": {"a": 1, "b": 2}, '
              '"a": {"y": 5, "x": 6}'
              '}'
          ),
      ),
      # keys_attr disabled
      (
          fns.new(
              json_object_keys=fns.list(['y', 'x']),
              json_object_values=fns.list([3, 4]),
              x=1,
              y=2,
          ),
          {'keys_attr': None},
          ds(
              '{"json_object_keys": ["y", "x"], "json_object_values": [3, 4],'
              ' "x": 1, "y": 2}'
          ),
      ),
      # values_attr disabled
      (
          fns.new(
              json_object_keys=fns.list(['y', 'x']),
              json_object_values=fns.list([3, 4]),
              x=1,
              y=2,
          ),
          {'values_attr': None},
          ds('{"y": 2, "x": 1}'),
      ),
  )
  def test_eval(self, x, kwargs, expected_result):
    testing.assert_equal(
        expr_eval.eval(kde.json.to_json(x, **kwargs)),
        expected_result,
    )

  def test_eval_duplicate_itemid_no_cycle(self):
    y = fns.new(x=1)
    x = fns.new(a=y, b=y)
    testing.assert_equal(
        expr_eval.eval(kde.json.to_json(x)),
        ds('{"a": {"x": 1}, "b": {"x": 1}}'),
    )

  def test_error_invalid_schema(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported schema BYTES for json serialization'
    ):
      _ = expr_eval.eval(kde.json.to_json(ds(b'a')))

    with self.assertRaisesRegex(
        ValueError, 'unsupported schema BYTES for json serialization'
    ):
      _ = expr_eval.eval(
          kde.json.to_json(ds(b'a', schema=schema_constants.OBJECT))
      )

    with self.assertRaisesRegex(
        ValueError, 'unsupported schema SCHEMA for json serialization'
    ):
      _ = expr_eval.eval(kde.json.to_json(ds(schema_constants.INT32)))

    with self.assertRaisesRegex(
        ValueError, 'unsupported schema SCHEMA for json serialization'
    ):
      _ = expr_eval.eval(
          kde.json.to_json(fns.new_schema(a=schema_constants.INT32))
      )

    with self.assertRaisesRegex(
        ValueError, 'unsupported schema ITEMID for json serialization'
    ):
      _ = expr_eval.eval(kde.json.to_json(fns.new().as_itemid()))

    with self.assertRaisesRegex(
        ValueError, 'unsupported schema EXPR for json serialization'
    ):
      _ = expr_eval.eval(kde.json.to_json(ds(arolla.quote(I.x))))

  def test_error_itemid_cycle(self):
    db = fns.bag()
    x = fns.new(db=db)
    x.x = x
    with self.assertRaisesRegex(
        ValueError, 'cycle detected in json serialization at '
    ):
      _ = expr_eval.eval(kde.json.to_json(x))

  def test_error_invalid_dict_key_schema(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported dict key schema BYTES for json serialization'
    ):
      _ = expr_eval.eval(kde.json.to_json(fns.dict({b'a': 'b'})))

    with self.assertRaisesRegex(
        ValueError, 'unsupported dict key dtype BYTES for json serialization'
    ):
      _ = expr_eval.eval(
          kde.json.to_json(
              fns.dict(
                  {b'a': 'b'},
                  schema=fns.dict_schema(
                      schema_constants.OBJECT, schema_constants.STRING
                  ),
              )
          )
      )

  def test_error_invalid_json_key_list(self):
    with self.assertRaisesRegex(
        ValueError, 'expected json key list to contain STRING, got INT32'
    ):
      _ = expr_eval.eval(
          kde.json.to_json(fns.new(keys=fns.list([1])), keys_attr='keys')
      )

    with self.assertRaisesRegex(
        ValueError, 'expected json key list not to have missing items'
    ):
      _ = expr_eval.eval(
          kde.json.to_json(
              fns.new(keys=fns.list(['a', None])), keys_attr='keys'
          )
      )

  def test_error_invalid_json_value_list(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected json key list and json value list to have the same length,'
        ' got 2 and 3',
    ):
      _ = expr_eval.eval(
          kde.json.to_json(
              fns.new(
                  keys=fns.list(['a', 'b']),
                  values=fns.list([1, 2, 3]),
              ),
              keys_attr='keys',
              values_attr='values',
          )
      )

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.printf,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        max_arity=3,
    )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.json.to_json, kde.to_json))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.json.to_json(I.x)))
    self.assertTrue(
        view.has_koda_view(
            kde.json.to_json(I.x, indent=I.indent, ensure_ascii=I.ensure_ascii)
        )
    )


if __name__ == '__main__':
  absltest.main()
