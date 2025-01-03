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
      (
          mask_constants.missing,
          None,
          True,
          ds(None, schema=schema_constants.STRING),
      ),
      (ds(arolla.unit()), None, True, ds('true')),
      (ds(True), None, True, ds('true')),
      (ds(False), None, True, ds('false')),
      (ds([1, 2, 3]), None, True, ds(['1', '2', '3'])),
      (ds([1, None, 3]), None, True, ds(['1', None, '3'])),
      (ds(['a', '\n', '\\']), None, True, ds(['"a"', '"\\n"', '"\\\\"'])),
      (ds(123, schema_constants.INT32), None, True, ds('123')),
      (ds(123, schema_constants.INT64), None, True, ds('123')),
      (ds(1.25), None, True, ds('1.25')),
      (ds(1.25, schema_constants.FLOAT32), None, True, ds('1.25')),
      (ds(1.25, schema_constants.FLOAT64), None, True, ds('1.25')),
      (ds(1.0), None, True, ds('1.0')),
      (ds(0.0), None, True, ds('0.0')),
      (ds(-0.0), None, True, ds('-0.0')),
      (ds(float('inf')), None, True, ds('"inf"')),
      (ds(float('-inf')), None, True, ds('"-inf"')),
      (ds(float('nan')), None, True, ds('"nan"')),
      (fns.list([]), None, True, ds('[]')),
      (fns.list([mask_constants.present]), None, True, ds('[true]')),
      (fns.list([mask_constants.missing]), None, True, ds('[false]')),
      (fns.list([None]), None, True, ds('[null]')),
      (fns.list([1, 2, 3]), None, True, ds('[1, 2, 3]')),
      (
          fns.list([[1], [2, 3]]),
          None,
          True,
          ds('[[1], [2, 3]]'),
      ),
      (
          fns.dict({}),
          None,
          True,
          ds('{}'),
      ),
      (
          fns.dict({'a': 1, 'b': '2'}),
          None,
          True,
          ds('{"a": 1, "b": "2"}'),
      ),
      (
          fns.new(a=1, b='2', c=mask_constants.missing),
          None,
          True,
          ds('{"a": 1, "b": "2", "c": false}'),
      ),
      (
          fns.obj(a=1, b='2', c=None),
          None,
          True,
          ds('{"a": 1, "b": "2", "c": null}'),
      ),
      (
          fns.new(a=ds([1, 2]), b=ds([3, None])),
          None,
          True,
          ds([
              '{"a": 1, "b": 3}',
              '{"a": 2, "b": null}',
          ]),
      ),
      (
          fns.obj(a=ds([1, 2]), b=ds([3, None])),
          None,
          True,
          ds([
              '{"a": 1, "b": 3}',
              '{"a": 2, "b": null}',
          ]),
      ),
      # indent != None
      (fns.list([1, 2, 3]), -1, True, ds('[1,2,3]')),
      (fns.list([1, 2, 3]), 0, True, ds('[\n1,\n2,\n3\n]')),
      (fns.list([1, 2, 3]), 2, True, ds('[\n  1,\n  2,\n  3\n]')),
      (
          fns.dict({'a': 1, 'b': 2}),
          -1,
          True,
          ds('{"a":1,"b":2}'),
      ),
      (
          fns.dict({'a': 1, 'b': 2}),
          0,
          True,
          ds('{\n"a": 1,\n"b": 2\n}'),
      ),
      (
          fns.dict({'a': 1, 'b': 2}),
          2,
          True,
          ds('{\n  "a": 1,\n  "b": 2\n}'),
      ),
      (fns.list([[]]), -1, True, ds('[[]]')),
      (fns.list([[]]), 0, True, ds('[\n[]\n]')),
      (fns.list([[]]), 2, True, ds('[\n  []\n]')),
      # ensure_ascii
      (ds('♣'), None, True, ds('"\\u2663"')),
      (ds('♣'), None, False, ds('"♣"')),
      # __koladata_json_object_keys__
      (
          fns.new(
              __koladata_json_object_keys__=fns.list(['b', 'a', 'c']),
              a=1,
              b=2,
              c=3,
          ),
          None,
          True,
          ds('{"b": 2, "a": 1, "c": 3}'),
      ),
      (
          fns.new(
              __koladata_json_object_keys__=fns.list(['b', 'a', 'c']),
              a=1,
              b=2,
              c=fns.new(
                  __koladata_json_object_keys__=fns.list(['x', 'z', 'y']),
                  x=10,
                  y=11,
                  z=12,
              ),
          ),
          None,
          True,
          ds('{"b": 2, "a": 1, "c": {"x": 10, "z": 12, "y": 11}}'),
      ),
      (
          fns.new(
              __koladata_json_object_keys__=fns.list(['b', 'a', 'c', 'a']),
              a=1,
              b=2,
              c=3,
          ),
          None,
          True,
          ds('{"b": 2, "a": 1, "c": 3, "a": 1}'),
      ),
      # __koladata_json_object_values__
      (
          fns.new(
              __koladata_json_object_keys__=fns.list(['b', 'a', 'c', 'a']),
              __koladata_json_object_values__=fns.list([1, 2, 3, 4]),
          ),
          None,
          True,
          ds('{"b": 1, "a": 2, "c": 3, "a": 4}'),
      ),
  )
  def test_eval(self, x, indent, ensure_ascii, expected_result):
    testing.assert_equal(
        expr_eval.eval(
            kde.json.to_json(x, indent=indent, ensure_ascii=ensure_ascii)
        ),
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
          kde.json.to_json(fns.new(__koladata_json_object_keys__=fns.list([1])))
      )

    with self.assertRaisesRegex(
        ValueError, 'expected json key list not to have missing items'
    ):
      _ = expr_eval.eval(
          kde.json.to_json(
              fns.new(
                  __koladata_json_object_keys__=fns.list(
                      ['a', None]
                  )
              )
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
                  __koladata_json_object_keys__=fns.list(['a', 'b']),
                  __koladata_json_object_values__=fns.list([1, 2, 3]),
              )
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
