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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from arolla.jagged_shape import jagged_shape as arolla_jagged_shape
from koladata.expr import expr_eval
from koladata.functions import functions as fns
# Register kde ops for e.g. jagged_shape.create_shape().
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import dict_item
from koladata.types import jagged_shape
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class DictShapedTest(parameterized.TestCase):

  def test_mutability(self):
    shape = jagged_shape.create_shape([3])
    self.assertFalse(fns.dict_shaped(shape).is_mutable())

  def test_no_kv(self):
    shape = jagged_shape.create_shape([3])
    x = fns.dict_shaped(shape).fork_bag()
    self.assertIsInstance(x, data_slice.DataSlice)
    x['a'] = ds([1, 2, 3])
    testing.assert_equal(
        x['a'], ds([1, 2, 3], schema_constants.OBJECT).with_bag(x.get_bag())
    )

  def test_no_kv_scalar(self):
    shape = jagged_shape.create_shape()
    x = fns.dict_shaped(shape).fork_bag()
    self.assertIsInstance(x, dict_item.DictItem)
    x['a'] = 57
    testing.assert_equal(
        x['a'], ds(57, schema_constants.OBJECT).with_bag(x.get_bag())
    )

  def test_with_dict_kv(self):
    x = fns.dict_shaped(jagged_shape.create_shape(), {'foo': 57, 'bar': 42})
    testing.assert_dicts_keys_equal(x, ds(['foo', 'bar']))
    testing.assert_equal(x['foo'], ds(57).with_bag(x.get_bag()))

    with self.assertRaisesRegex(
        ValueError,
        'cannot create a DataSlice of dicts from a Python dictionary',
    ):
      fns.dict_shaped(jagged_shape.create_shape([3]), {'foo': 57, 'bar': 42})

  def test_with_scalar_kv(self):
    x = fns.dict_shaped(jagged_shape.create_shape(), 'foo', 57)
    testing.assert_dicts_keys_equal(x, ds(['foo']))
    testing.assert_equal(x['foo'], ds(57).with_bag(x.get_bag()))

  def test_with_kv(self):
    shape = ds([[0, 0], [0]]).get_shape()
    x = fns.dict_shaped(
        shape,
        ds(['a', 'b']),
        ds([1, 2]),
    )
    testing.assert_dicts_keys_equal(x, ds([[['a'], ['a']], [['b']]]))
    testing.assert_equal(x['a'], ds([[1, 1], [None]]).with_bag(x.get_bag()))
    testing.assert_equal(x['b'], ds([[None, None], [2]]).with_bag(x.get_bag()))

  def test_adopt_values(self):
    shape = ds([[0, 0], [0]]).get_shape()
    dct = fns.dict('a', 7)
    dct2 = fns.dict_shaped(shape, 'obj', dct)

    testing.assert_equal(
        dct2['obj']['a'],
        ds([[7, 7], [7]], schema_constants.INT32).with_bag(dct2.get_bag()),
    )

  def test_adopt_schema(self):
    shape = ds([[0, 0], [0]]).get_shape()
    dict_schema = kde.dict_schema(
        schema_constants.STRING, kde.uu_schema(a=schema_constants.INT32)
    ).eval()
    dct = fns.dict_shaped(shape, schema=dict_schema)

    testing.assert_equal(
        dct[ds(None)].a.no_bag(),
        ds([[None, None], [None]], schema_constants.INT32)
    )

  @parameterized.parameters(
      dict(
          keys=None,
          values=None,
          key_schema=None,
          value_schema=None,
          schema=None,
          expected_key_schema=schema_constants.OBJECT,
          expected_value_schema=schema_constants.OBJECT,
      ),
      dict(
          keys=None,
          values=None,
          key_schema=schema_constants.INT32,
          value_schema=schema_constants.OBJECT,
          schema=None,
          expected_key_schema=schema_constants.INT32,
          expected_value_schema=schema_constants.OBJECT,
      ),
      # Deduce schema from keys and values.
      dict(
          keys=ds([[1, 2], [3]]),
          values=ds([[1, 'foo'], [3]]),
          key_schema=None,
          value_schema=None,
          schema=None,
          expected_key_schema=schema_constants.INT32,
          expected_value_schema=schema_constants.OBJECT,
      ),
      dict(
          keys=ds([[1, 'foo'], [3]]),
          values=ds([[1, 'foo'], [3]]),
          key_schema=None,
          value_schema=None,
          schema=None,
          expected_key_schema=schema_constants.OBJECT,
          expected_value_schema=schema_constants.OBJECT,
      ),
      # Both schema and keys / values provided, do casting.
      dict(
          keys=ds([[1, 2], [3]]),
          values=ds([[1, 2], [3]]),
          key_schema=schema_constants.INT64,
          value_schema=schema_constants.OBJECT,
          schema=None,
          expected_key_schema=schema_constants.INT64,
          expected_value_schema=schema_constants.OBJECT,
      ),
      dict(
          keys=None,
          values=None,
          key_schema=None,
          value_schema=None,
          schema=kde.dict_schema(
              key_schema=schema_constants.INT32,
              value_schema=schema_constants.OBJECT,
          ).eval(),
          expected_key_schema=schema_constants.INT32,
          expected_value_schema=schema_constants.OBJECT,
      ),
      # Both schema and keys / values provided, do casting.
      dict(
          keys=ds([[1, 2], [3]]),
          values=ds([[1, 2], [3]]),
          key_schema=None,
          value_schema=None,
          schema=kde.dict_schema(
              key_schema=schema_constants.INT64,
              value_schema=schema_constants.OBJECT,
          ).eval(),
          expected_key_schema=schema_constants.INT64,
          expected_value_schema=schema_constants.OBJECT,
      ),
  )
  def test_schema(
      self,
      keys,
      values,
      key_schema,
      value_schema,
      schema,
      expected_key_schema,
      expected_value_schema,
  ):
    shape = ds([[0, 0], [0]]).get_shape()
    result_schema = fns.dict_shaped(
        shape,
        items_or_keys=keys,
        key_schema=key_schema,
        values=values,
        value_schema=value_schema,
        schema=schema,
    ).get_schema()
    testing.assert_equal(
        result_schema.get_attr('__keys__').no_bag(),
        expected_key_schema,
    )
    testing.assert_equal(
        result_schema.get_attr('__values__').no_bag(),
        expected_value_schema,
    )

  def test_schema_arg_error(self):
    shape = ds([[0, 0], [0]]).get_shape()
    dict_schema = kde.dict_schema(
        key_schema=schema_constants.INT64, value_schema=schema_constants.OBJECT
    ).eval()
    with self.assertRaisesRegex(
        ValueError, 'either a dict schema or key/value schemas, but not both'
    ):
      fns.dict_shaped(
          shape,
          key_schema=schema_constants.INT64,
          schema=dict_schema,
      )

  def test_wrong_arg_types(self):
    shape = ds([[0, 0], [0]]).get_shape()
    with self.assertRaisesRegex(
        TypeError, 'expecting key_schema to be a DataSlice, got int'
    ):
      fns.dict_shaped(shape, key_schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting value_schema to be a DataSlice, got int'
    ):
      fns.dict_shaped(shape, value_schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got int'
    ):
      fns.dict_shaped(shape, schema=42)

  def test_itemid(self):
    itemid = expr_eval.eval(
        kde.allocation.new_dictid_shaped_as(ds([[1, 1], [1]]))
    )
    x = fns.dict_shaped(itemid.get_shape(), 'a', 42, itemid=itemid)
    testing.assert_dicts_keys_equal(x, ds([[['a'], ['a']], [['a']]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    triple = fns.new(non_existent=42)
    itemid = fns.dict_shaped(ds([[1, 1], [1]]).get_shape(), 'a', triple)

    # Successful.
    x = fns.dict_shaped(itemid.get_shape(), itemid=itemid.get_itemid())
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        AttributeError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_errors(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting shape to be a JaggedShape, got int'
    ):
      fns.dict_shaped(4)  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        TypeError, 'expecting shape to be a JaggedShape, got .*DataBag'
    ):
      fns.dict_shaped(fns.mutable_bag())
    with self.assertRaisesRegex(
        TypeError,
        'expecting shape to be a JaggedShape, got JaggedArrayShape',
    ):
      # Using JaggedArrayShape, instead of JaggedDenseArrayShape
      shape = arolla_jagged_shape.JaggedArrayShape.from_edges(
          arolla.types.ArrayEdge.from_sizes(arolla.array([3]))
      )
      fns.dict_shaped(shape)  # pytype: disable=wrong-arg-types

  def test_alias(self):
    self.assertIs(fns.dict_shaped, fns.dicts.shaped)


if __name__ == '__main__':
  absltest.main()
