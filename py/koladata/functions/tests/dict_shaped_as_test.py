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
from koladata.expr import expr_eval
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class DictShapedAsTest(parameterized.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.dict_shaped_as(ds([1, 2, 3])).is_mutable())

  def test_no_kv(self):
    x = fns.dict_shaped_as(ds([1, 2, 3])).fork_bag()
    self.assertIsInstance(x, data_slice.DataSlice)
    x['a'] = ds([1, 2, 3])
    testing.assert_equal(
        x['a'], ds([1, 2, 3], schema_constants.OBJECT).with_bag(x.get_bag())
    )

  def test_with_dict_kv(self):
    x = fns.dict_shaped_as(ds(1), {'foo': 57, 'bar': 42})
    testing.assert_dicts_keys_equal(x, ds(['foo', 'bar']))
    testing.assert_equal(x['foo'], ds(57).with_bag(x.get_bag()))

    with self.assertRaisesRegex(
        ValueError,
        'cannot create a DataSlice of dicts from a Python dictionary',
    ):
      fns.dict_shaped_as(ds([1, 2, 3]), {'foo': 57, 'bar': 42})

  def test_with_kv(self):
    x = fns.dict_shaped_as(
        ds([[0, 0], [0]]),
        ds(['a', 'b']),
        ds([1, 2]),
    )
    testing.assert_dicts_keys_equal(x, ds([[['a'], ['a']], [['b']]]))
    testing.assert_equal(x['a'], ds([[1, 1], [None]]).with_bag(x.get_bag()))
    testing.assert_equal(x['b'], ds([[None, None], [2]]).with_bag(x.get_bag()))

  def test_key_schema_arg(self):
    x = fns.dict_shaped_as(
        ds([[0, 0], [0]]),
        key_schema=schema_constants.INT32,
    )
    testing.assert_equal(
        x.get_schema().get_attr('__keys__').with_bag(None),
        schema_constants.INT32,
    )

  def test_value_schema_arg(self):
    x = fns.dict_shaped_as(
        ds([[0, 0], [0]]),
        value_schema=schema_constants.OBJECT,
    )
    testing.assert_equal(
        x.get_schema().get_attr('__values__').with_bag(None),
        schema_constants.OBJECT,
    )

  def test_schema(self):
    x = fns.dict_shaped_as(
        ds([[0, 0], [0]]),
        schema=fns.dict_schema(schema_constants.INT32, schema_constants.OBJECT),
    )
    testing.assert_equal(
        x.get_schema().get_attr('__keys__').with_bag(None),
        schema_constants.INT32,
    )
    testing.assert_equal(
        x.get_schema().get_attr('__values__').with_bag(None),
        schema_constants.OBJECT,
    )

  def test_itemid(self):
    itemid = expr_eval.eval(
        kde.allocation.new_dictid_shaped_as(ds([[1, 1], [1]]))
    )
    x = fns.dict_shaped_as(itemid, 'a', 42, itemid=itemid)
    testing.assert_dicts_keys_equal(x, ds([[['a'], ['a']], [['a']]]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_adopt_values(self):
    dct = fns.dict('a', 7)
    dct2 = fns.dict_shaped_as(ds([[0, 0], [0]]), 'obj', dct)

    testing.assert_equal(
        dct2['obj']['a'],
        ds([[7, 7], [7]], schema_constants.INT32).with_bag(dct2.get_bag()),
    )

  def test_adopt_schema(self):
    dict_schema = fns.dict_schema(
        schema_constants.STRING, fns.uu_schema(a=schema_constants.INT32)
    )
    dct = fns.dict_shaped_as(ds([[0, 0], [0]]), schema=dict_schema)

    testing.assert_equal(
        dct[ds(None)].a.no_bag(),
        ds([[None, None], [None]], schema_constants.INT32)
    )

  def test_alias(self):
    self.assertIs(fns.dict_shaped_as, fns.dicts.shaped_as)


if __name__ == '__main__':
  absltest.main()
