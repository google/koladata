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
from koladata import kd
from koladata.expr import expr_eval
from koladata.functions import functions as fns
# Register kde ops for e.g. jagged_shape.create_shape().
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import list_item
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class ListShapedTest(parameterized.TestCase):

  def test_mutability(self):
    shape = jagged_shape.create_shape()
    self.assertFalse(fns.list_shaped(shape).is_mutable())

  def test_item(self):
    shape = jagged_shape.create_shape()
    l = fns.list_shaped(shape).fork_bag()
    self.assertIsInstance(l, list_item.ListItem)
    testing.assert_equal(
        l[:], ds([], schema_constants.OBJECT).with_bag(l.get_bag())
    )
    l.append(1)
    testing.assert_equal(
        l[:], ds([1], schema_constants.OBJECT).with_bag(l.get_bag())
    )

  def test_item_with_items(self):
    shape = jagged_shape.create_shape()
    l = fns.list_shaped(shape, [1, 2]).fork_bag()
    self.assertIsInstance(l, list_item.ListItem)
    testing.assert_equal(l[:], ds([1, 2]).with_bag(l.get_bag()))
    l.append(3)
    testing.assert_equal(l[:], ds([1, 2, 3]).with_bag(l.get_bag()))

  def test_slice(self):
    shape = jagged_shape.create_shape([2], [2, 1])
    l = fns.list_shaped(shape).fork_bag()
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

  def test_slice_with_scalar_items(self):
    shape = jagged_shape.create_shape([2], [2, 1])
    l = fns.list_shaped(shape, 1).fork_bag()
    self.assertIsInstance(l, data_slice.DataSlice)
    testing.assert_equal(l[:], ds([[[1], [1]], [[1]]]).with_bag(l.get_bag()))
    l.append(2)
    testing.assert_equal(
        l[:], ds([[[1, 2], [1, 2]], [[1, 2]]]).with_bag(l.get_bag())
    )

  def test_slice_with_slice_items(self):
    shape = jagged_shape.create_shape([2], [2, 1])
    l = fns.list_shaped(shape, ds([[[1, 2], [3, 4]], [[5, 6]]])).fork_bag()
    self.assertIsInstance(l, data_slice.DataSlice)
    testing.assert_equal(
        l[:], ds([[[1, 2], [3, 4]], [[5, 6]]]).with_bag(l.get_bag())
    )
    l.append(7)
    testing.assert_equal(
        l[:], ds([[[1, 2, 7], [3, 4, 7]], [[5, 6, 7]]]).with_bag(l.get_bag())
    )

  def test_itemid(self):
    itemid = expr_eval.eval(kde.allocation.new_listid_shaped_as(ds([1, 1])))
    x = fns.list_shaped(
        itemid.get_shape(), ds([['a', 'b'], ['c']]), itemid=itemid
    )
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], ['c']]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    triple = fns.new(non_existent=42)
    itemid = fns.implode(ds([[[triple], []], [[]]]))

    # Successful.
    x = fns.list_shaped(itemid.get_shape(), itemid=itemid.get_itemid())
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            "attribute 'non_existent' is missing"
        ),
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_adopt_values(self):
    shape = jagged_shape.create_shape([2])
    lst = fns.implode(ds([[1, 2], [3]]))
    lst2 = fns.list_shaped(shape, lst)

    testing.assert_equal(
        lst2[:][:],
        ds([[[1, 2]], [[3]]], schema_constants.INT32).with_bag(lst2.get_bag()),
    )

  def test_adopt_schema(self):
    shape = jagged_shape.create_shape([2])
    list_schema = kde.list_schema(
        kde.uu_schema(a=schema_constants.INT32)
    ).eval()
    lst = fns.list_shaped(shape, schema=list_schema)

    testing.assert_equal(
        lst[:].a.no_bag(), ds([[], []], schema_constants.INT32)
    )

  def test_list_preserves_mixed_types(self):
    shape = jagged_shape.create_shape([2])

    testing.assert_equal(
        fns.list_shaped(
            shape, [1, 2.0], item_schema=schema_constants.OBJECT,
        )[:].no_bag(),
        kd.implode(
            data_slice.DataSlice.from_vals(
                [[1], [2.0]], schema_constants.OBJECT
            ),
        )[:].no_bag(),
    )
    testing.assert_equal(
        fns.list_shaped(
            shape,
            [1, 2.0],
            schema=kde.list_schema(schema_constants.OBJECT).eval(),
        )[:].no_bag(),
        kd.implode(
            data_slice.DataSlice.from_vals(
                [[1], [2.0]], schema_constants.OBJECT
            ),
        )[:].no_bag(),
    )

  def test_wrong_shape(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting shape to be a JaggedShape, got int'
    ):
      fns.list_shaped(57)  # pytype: disable=wrong-arg-types

  def test_imcompatible_shape(self):
    shape = jagged_shape.create_shape([2], [2, 1])
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      fns.list_shaped(shape, [1, 2, 3])

  @parameterized.parameters(
      # No schema, no list items provided.
      (None, None, None, schema_constants.OBJECT),
      # Schema, but no list items provided.
      (None, schema_constants.OBJECT, None, schema_constants.OBJECT),
      (None, schema_constants.INT64, None, schema_constants.INT64),
      # Deduce schema from list items.
      ([[1, 2], [3]], None, None, schema_constants.INT32),
      ([[1, 'foo'], [3]], None, None, schema_constants.OBJECT),
      (
          ds([[1, 'foo'], [3]], schema=schema_constants.OBJECT),
          None,
          None,
          schema_constants.OBJECT,
      ),
      # Both schema and list items provided.
      ([[1, 2], [3]], schema_constants.INT64, None, schema_constants.INT64),
      ([[1, 2], [3]], schema_constants.OBJECT, None, schema_constants.OBJECT),
      (
          None,
          None,
          kde.list_schema(item_schema=schema_constants.INT64).eval(),
          schema_constants.INT64,
      ),
      (
          [[1, 2], [3]],
          None,
          kde.list_schema(item_schema=schema_constants.INT64).eval(),
          schema_constants.INT64,
      ),
  )
  def test_schema(self, items, item_schema, schema, expected_item_schema):
    shape = jagged_shape.create_shape([2], [2, 1])
    testing.assert_equal(
        fns.list_shaped(
            shape,
            items=items,
            item_schema=item_schema,
            schema=schema,
        )
        .get_schema()
        .get_attr('__items__')
        .no_bag(),
        expected_item_schema,
    )

  def test_schema_arg_error(self):
    shape = jagged_shape.create_shape([2], [2, 1])
    list_schema = kde.list_schema(item_schema=schema_constants.INT64).eval()
    with self.assertRaisesRegex(
        ValueError, 'either a list schema or item schema, but not both'
    ):
      fns.list_shaped(
          shape,
          item_schema=schema_constants.INT64,
          schema=list_schema,
      )

  def test_wrong_arg_types(self):
    shape = ds([[0, 0], [0]]).get_shape()
    with self.assertRaisesRegex(
        TypeError, 'expecting item_schema to be a DataSlice, got int'
    ):
      fns.list_shaped(shape, item_schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got int'
    ):
      fns.list_shaped(shape, schema=42)

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for list items is incompatible.

Expected schema for list items: BYTES
Assigned schema for list items: INT32""",
    ):
      fns.list_shaped(
          jagged_shape.create_shape([2], [2, 1]),
          items=[[1, 2], [3]],
          item_schema=schema_constants.BYTES,
      )

  def test_alias(self):
    self.assertIs(fns.list_shaped, fns.lists.shaped)


if __name__ == '__main__':
  absltest.main()
