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
from koladata import kd
from koladata.expr import expr_eval
from koladata.functions import functions as fns
# Register kde ops for e.g. jagged_shape.create_shape().
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import list_item
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class ListShapedAsTest(parameterized.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.list_shaped_as(ds([1, None])).is_mutable())

  def test_item(self):
    l = fns.list_shaped_as(ds(1))
    self.assertIsInstance(l, list_item.ListItem)
    testing.assert_equal(
        l[:], ds([], schema_constants.OBJECT).with_bag(l.get_bag())
    )

  def test_item_with_items(self):
    l = fns.list_shaped_as(ds(1), [1, 2])
    self.assertIsInstance(l, list_item.ListItem)
    testing.assert_equal(l[:], ds([1, 2]).with_bag(l.get_bag()))

  def test_slice(self):
    l = fns.list_shaped_as(ds([['a', 'b'], ['c']])).fork_bag()
    self.assertIsInstance(l, data_slice.DataSlice)
    testing.assert_equal(
        l[:],
        ds([[[], []], [[]]], schema_constants.OBJECT).with_bag(l.get_bag()),
    )
    l.append(1)
    testing.assert_equal(
        l[:],
        ds([[[1], [1]], [[1]]], schema_constants.OBJECT).with_bag(l.get_bag()),
    )

  def test_itemid(self):
    itemid = expr_eval.eval(kde.allocation.new_listid_shaped_as(ds([1, 1])))
    x = fns.list_shaped_as(itemid, ds([['a', 'b'], ['c']]), itemid=itemid)
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], ['c']]))

  def test_item_schema_arg(self):
    testing.assert_equal(
        fns.list_shaped_as(
            ds([['a', 'b'], ['c']]),
            items=[[1, 2], [3]],
            item_schema=schema_constants.FLOAT32,
        )
        .get_schema()
        .get_attr('__items__')
        .with_bag(None),
        schema_constants.FLOAT32,
    )

  def test_schema_arg(self):
    testing.assert_equal(
        fns.list_shaped_as(
            ds([['a', 'b'], ['c']]),
            items=[[1, 2], [3]],
            schema=kde.list_schema(item_schema=schema_constants.FLOAT32).eval(),
        )
        .get_schema()
        .get_attr('__items__')
        .with_bag(None),
        schema_constants.FLOAT32,
    )

  def test_list_preserves_mixed_types(self):
    testing.assert_equal(
        fns.list_shaped_as(
            ds([1, 2.0]), [1, 2.0], item_schema=schema_constants.OBJECT,
        )[:].no_bag(),
        kd.implode(
            data_slice.DataSlice.from_vals(
                [[1], [2.0]], schema_constants.OBJECT
            ),
        )[:].no_bag(),
    )
    testing.assert_equal(
        fns.list_shaped_as(
            ds([1, 2.0]),
            [1, 2.0],
            schema=kde.list_schema(schema_constants.OBJECT).eval(),
        )[:].no_bag(),
        kd.implode(
            data_slice.DataSlice.from_vals(
                [[1], [2.0]], schema_constants.OBJECT
            ),
        )[:].no_bag(),
    )

  def test_adopt_values(self):
    lst = fns.implode(ds([[1, 2], [3]]))
    lst2 = fns.list_shaped_as(ds([0, 0]), lst)

    testing.assert_equal(
        lst2[:][:],
        ds([[[1, 2]], [[3]]], schema_constants.INT32).with_bag(lst2.get_bag()),
    )

  def test_adopt_schema(self):
    list_schema = kde.list_schema(
        kde.uu_schema(a=schema_constants.INT32)
    ).eval()
    lst = fns.list_shaped_as(ds([0, 0]), schema=list_schema)

    testing.assert_equal(
        lst[:].a.no_bag(), ds([[], []], schema_constants.INT32)
    )

  def test_alias(self):
    self.assertIs(fns.list_shaped_as, fns.lists.shaped_as)


if __name__ == '__main__':
  absltest.main()
