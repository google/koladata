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
from koladata import kd
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import list_item
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class ListLikeTest(parameterized.TestCase):

  def test_mutability(self):
    self.assertFalse(fns.list_like(ds([1, None])).is_mutable())
    self.assertTrue(fns.list_like(ds([1, None]), db=fns.bag()).is_mutable())

  def test_item(self):
    l = fns.list_like(ds(1)).fork_bag()
    self.assertIsInstance(l, list_item.ListItem)
    testing.assert_equal(
        l[:], ds([], schema_constants.OBJECT).with_bag(l.get_bag())
    )
    l.append(1)
    testing.assert_equal(
        l[:], ds([1], schema_constants.OBJECT).with_bag(l.get_bag())
    )

  def test_item_with_items(self):
    l = fns.list_like(ds(1), [1, 2]).fork_bag()
    self.assertIsInstance(l, list_item.ListItem)
    testing.assert_equal(l[:], ds([1, 2]).with_bag(l.get_bag()))
    l.append(3)
    testing.assert_equal(l[:], ds([1, 2, 3]).with_bag(l.get_bag()))

  def test_empty_item(self):
    l = fns.list_like(ds(None)).fork_bag()
    testing.assert_equal(l.no_bag(), ds(None, l.get_schema().no_bag()))
    l.append(1)
    testing.assert_equal(l[:].no_bag(), ds([], schema_constants.OBJECT))

  def test_slice(self):
    l = fns.list_like(ds([[1, None], [3]])).fork_bag()
    self.assertIsInstance(l, data_slice.DataSlice)
    testing.assert_equal(
        l[:],
        ds([[[], []], [[]]], schema_constants.OBJECT).with_bag(l.get_bag()),
    )
    l.append(1)
    testing.assert_equal(
        l[:],
        ds([[[1], []], [[1]]], schema_constants.OBJECT).with_bag(l.get_bag()),
    )

  def test_all_missing_slice(self):
    l = fns.list_like(ds([[None, None], [None]])).fork_bag()
    self.assertIsInstance(l, data_slice.DataSlice)
    testing.assert_equal(
        l[:],
        ds([[[], []], [[]]], schema_constants.OBJECT).with_bag(l.get_bag()),
    )
    l.append(1)
    testing.assert_equal(
        l[:],
        ds([[[], []], [[]]], schema_constants.OBJECT).with_bag(l.get_bag()),
    )

  def test_slice_with_scalar_items(self):
    l = fns.list_like(ds([[1, None], [3]]), 1).fork_bag()
    self.assertIsInstance(l, data_slice.DataSlice)
    testing.assert_equal(l[:], ds([[[1], []], [[1]]]).with_bag(l.get_bag()))
    l.append(2)
    testing.assert_equal(
        l[:], ds([[[1, 2], []], [[1, 2]]]).with_bag(l.get_bag())
    )

  def test_slice_with_slice_items(self):
    l = fns.list_like(
        ds([[1, None], [3]]), ds([[[1, 2], [3, 4]], [[5, 6]]])
    ).fork_bag()
    self.assertIsInstance(l, data_slice.DataSlice)
    testing.assert_equal(
        l[:], ds([[[1, 2], []], [[5, 6]]]).with_bag(l.get_bag())
    )
    l.append(7)
    testing.assert_equal(
        l[:], ds([[[1, 2, 7], []], [[5, 6, 7]]]).with_bag(l.get_bag())
    )

  def test_adopt_values(self):
    lst = fns.implode(ds([[1, 2], [3]]))
    lst2 = fns.list_like(ds([None, 0]), lst)

    testing.assert_equal(
        lst2[:][:],
        ds([[], [[3]]], schema_constants.INT32).with_bag(lst2.get_bag()),
    )

  def test_adopt_schema(self):
    list_schema = fns.list_schema(fns.uu_schema(a=schema_constants.INT32))
    lst = fns.list_like(ds([None, 0]), schema=list_schema)

    testing.assert_equal(
        lst[:].a.no_bag(), ds([[], []], schema_constants.INT32)
    )

  def test_list_preserves_mixed_types(self):
    db = fns.bag()

    testing.assert_equal(
        fns.list_like(
            ds([1, 2.0]), [1, 2.0], item_schema=schema_constants.OBJECT, db=db
        )[:],
        kd.implode(
            data_slice.DataSlice.from_vals(
                [[1], [2.0]], schema_constants.OBJECT
            ),
            db=db,
        )[:],
    )
    testing.assert_equal(
        fns.list_like(
            ds([1, 2.0]),
            [1, 2.0],
            schema=fns.list_schema(schema_constants.OBJECT),
            db=db,
        )[:],
        kd.implode(
            data_slice.DataSlice.from_vals(
                [[1], [2.0]], schema_constants.OBJECT
            ),
            db=db,
        )[:],
    )

  def test_itemid_dataitem(self):
    itemid = expr_eval.eval(kde.allocation.new_listid())
    input_arg = ds(['a'])

    with self.subTest('present DataItem and present itemid'):
      x = fns.list_like(ds(1), input_arg, itemid=itemid)
      testing.assert_equal(
          x,
          itemid.with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('missing DataItem and missing itemid'):
      x = fns.list_like(ds(None), input_arg, itemid=(itemid & None))
      self.assertTrue(x.is_empty())

    with self.subTest('missing DataItem and present itemid'):
      x = fns.list_like(ds(None), input_arg, itemid=itemid)
      self.assertTrue(x.is_empty())

    with self.subTest('present DataItem and missing itemid'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` only has 0 present items but 1 are required',
      ):
        _ = fns.list_like(ds(1), input_arg, itemid=(itemid & None))

  def test_itemid_dataslice(self):
    id1 = expr_eval.eval(kde.allocation.new_listid())
    id2 = expr_eval.eval(kde.allocation.new_listid())
    id3 = expr_eval.eval(kde.allocation.new_listid())
    input_arg = ds([['a'], ['b'], ['c']])

    with self.subTest('full DataSlice and full itemid'):
      x = fns.list_like(ds([1, 1, 1]), input_arg, itemid=ds([id1, id2, id3]))
      testing.assert_equal(
          x,
          ds([id1, id2, id3]).with_schema(x.get_schema()).with_bag(x.get_bag()),
      )

    with self.subTest('full DataSlice and sparse itemid'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` only has 2 present items but 3 are required',
      ):
        _ = fns.list_like(
            ds([1, 1, 1]),
            input_arg,
            itemid=ds([id1, None, id3]),
        )

    with self.subTest('full DataSlice and full itemid with duplicates'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` cannot have duplicate ItemIds',
      ):
        _ = fns.list_like(ds([1, 1, 1]), input_arg, itemid=ds([id1, id2, id1]))

    with self.subTest('sparse DataSlice and sparse itemid'):
      x = fns.list_like(
          ds([1, None, 1]),
          input_arg,
          itemid=ds([id1, None, id3]),
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest(
        'sparse DataSlice and sparse itemid with sparsity mismatch'
    ):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` and `shape_and_mask_from` must have the same sparsity',
      ):
        _ = fns.list_like(
            ds([1, None, 1]), input_arg, itemid=ds([id1, id2, None])
        )

    with self.subTest('sparse DataSlice and full itemid'):
      x = fns.list_like(
          ds([1, None, 1]),
          input_arg,
          itemid=ds([id1, id2, id3]),
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

    with self.subTest('sparse DataSlice and full itemid with duplicates'):
      with self.assertRaisesRegex(
          ValueError,
          '`itemid` cannot have duplicate ItemIds',
      ):
        _ = fns.list_like(
            ds([1, None, 1]),
            input_arg,
            itemid=ds([id1, id1, id1]),
        )

    with self.subTest(
        'sparse DataSlice and full itemid with unused duplicates'
    ):
      x = fns.list_like(
          ds([1, None, 1]),
          input_arg,
          itemid=ds([id1, id1, id3]),
      )
      testing.assert_equal(
          x,
          ds([id1, None, id3])
          .with_schema(x.get_schema())
          .with_bag(x.get_bag()),
      )

  def test_itemid_from_different_bag(self):
    triple = fns.new(non_existent=42)
    itemid = fns.implode(ds([[[triple], []], [[]]]))

    # Successful.
    x = fns.list_like(ds([[1, None], [1]]), itemid=itemid.get_itemid())
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, 'attribute \'non_existent\' is missing'
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_bag_arg(self):
    db = fns.bag()
    shape_and_mask_from = ds([[1, None], [2]])
    items = ds([[[1], [2]], [[3]]])
    l = fns.list_like(shape_and_mask_from, items)
    with self.assertRaises(AssertionError):
      testing.assert_equal(l.get_bag(), db)

    l = fns.list_like(shape_and_mask_from, items, db=db)
    testing.assert_equal(l.get_bag(), db)

  def test_wrong_shape_and_mask_from(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting shape_and_mask_from to be a DataSlice, got int'
    ):
      fns.list_like(57)  # pytype: disable=wrong-arg-types
    with self.assertRaisesRegex(
        TypeError,
        'expecting shape_and_mask_from to be a DataSlice, got .*DataBag',
    ):
      fns.list_like(fns.bag())  # pytype: disable=wrong-arg-types

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      fns.list_like(ds([[1, None], [2]]), [1, 2, 3])

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
          fns.list_schema(item_schema=schema_constants.INT64),
          schema_constants.INT64,
      ),
      (
          [[1, 2], [3]],
          None,
          fns.list_schema(item_schema=schema_constants.INT64),
          schema_constants.INT64,
      ),
  )
  def test_schema(self, items, item_schema, schema, expected_item_schema):
    mask_and_shape = ds([[1, None], [3]])
    testing.assert_equal(
        fns.list_like(
            mask_and_shape,
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
    mask_and_shape = ds([[1, None], [3]])
    list_schema = fns.list_schema(item_schema=schema_constants.INT64)
    with self.assertRaisesRegex(
        ValueError, 'either a list schema or item schema, but not both'
    ):
      fns.list_like(
          mask_and_shape,
          item_schema=schema_constants.INT64,
          schema=list_schema,
      )

  def test_wrong_arg_types(self):
    mask_and_shape = ds([[1, None], [3]])
    with self.assertRaisesRegex(
        TypeError, 'expecting item_schema to be a DataSlice, got int'
    ):
      fns.list_like(mask_and_shape, item_schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got int'
    ):
      fns.list_like(mask_and_shape, schema=42)

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for List item is incompatible.

Expected schema for List item: BYTES
Assigned schema for List item: INT32""",
    ):
      fns.list_like(
          ds([[1, None], [3]]),
          items=[[1, 2], [3]],
          item_schema=schema_constants.BYTES,
      )

  def test_alias(self):
    self.assertIs(fns.list_like, fns.lists.like)


if __name__ == '__main__':
  absltest.main()
