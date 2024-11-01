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
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import list_item
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


# TODO: Add more tests with merging, etc. Also some from
# data_bag_test.
class ListTest(parameterized.TestCase):

  def test_empty(self):
    l = fns.list()
    self.assertIsInstance(l, list_item.ListItem)
    testing.assert_equal(l[:], ds([]).with_bag(l.get_bag()))
    l.append(1)
    testing.assert_equal(
        l[:], ds([1], schema_constants.OBJECT).with_bag(l.get_bag())
    )

  def test_list_with_uuid(self):
    db = fns.bag()
    testing.assert_equal(
        fns.list(itemid=kd.uuid_for_list(seed='seed', a=ds(1)), db=db),
        fns.list(itemid=kd.uuid_for_list(seed='seed', a=ds(1)), db=db),
    )

  def test_item_with_values(self):
    l = fns.list([1, 2, 3])
    self.assertIsInstance(l, list_item.ListItem)
    testing.assert_equal(l[:], ds([1, 2, 3]).with_bag(l.get_bag()))

  def test_item_with_nested_values(self):
    l = fns.list([[1, 2], [3]])
    self.assertIsInstance(l, list_item.ListItem)
    nested_l = l[:]
    self.assertIsInstance(nested_l, data_slice.DataSlice)
    testing.assert_equal(nested_l[:], ds([[1, 2], [3]]).with_bag(l.get_bag()))

    nested_l_0 = l[0]
    nested_l_1 = l[1]
    self.assertIsInstance(nested_l_0, list_item.ListItem)
    self.assertIsInstance(nested_l_1, list_item.ListItem)
    testing.assert_equal(nested_l_0[:], ds([1, 2]).with_bag(l.get_bag()))
    testing.assert_equal(nested_l_1[:], ds([3]).with_bag(l.get_bag()))

  def test_slice_with_data_slice_input(self):
    l = fns.list(data_slice.DataSlice.from_vals([[1, 2], [3]]))
    self.assertIsInstance(l, data_slice.DataSlice)
    testing.assert_equal(l[:], ds([[1, 2], [3]]).with_bag(l.get_bag()))

  def test_itemid(self):
    itemid = kde.allocation.new_listid_shaped_as._eval(ds([1, 1]))  # pylint: disable=protected-access
    x = fns.list(ds([['a', 'b'], ['c']]), itemid=itemid)
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], ['c']]))
    testing.assert_equal(x.no_bag().get_itemid(), itemid)

  def test_itemid_from_different_bag(self):
    triple = fns.new(non_existent=42)
    itemid = fns.list([triple])

    # Successful.
    x = fns.list([1, 2, 3], itemid=itemid.get_itemid())
    # ITEMID's triples are stripped in the new DataBag.
    with self.assertRaisesRegex(
        ValueError, 'attribute \'non_existent\' is missing'
    ):
      _ = triple.with_bag(x.get_bag()).non_existent

  def test_bag_arg(self):
    db = fns.bag()
    l = fns.list([1, 2, 3])
    with self.assertRaises(AssertionError):
      testing.assert_equal(l.get_bag(), db)

    l = fns.list([1, 2, 3], db=db)
    testing.assert_equal(l.get_bag(), db)

  def test_repr(self):
    self.assertRegex(
        repr(fns.list([1, 2, 3])), r'DataItem\(.*, schema: .*, bag_id: .*\)'
    )

  def test_create_list_merge_bags(self):
    lst = fns.list()
    lst.append(7)
    nested_lst = fns.list()
    nested_lst.append(lst)
    nested_lst.append(lst)
    lst2 = fns.list(items=nested_lst[:])
    testing.assert_equal(
        lst2[0][0], ds(7, schema_constants.OBJECT).with_bag(lst2.get_bag())
    )
    testing.assert_equal(
        lst2[1][0], ds(7, schema_constants.OBJECT).with_bag(lst2.get_bag())
    )

    l = fns.list([fns.list([1, 2]), fns.list([3, 4])])
    testing.assert_equal(l[:][:], ds([[1, 2], [3, 4]]).with_bag(l.get_bag()))

  @parameterized.parameters(
      # No schema, no list items provided.
      (None, None, None, schema_constants.OBJECT),
      # Schema, but no list items provided.
      (None, schema_constants.OBJECT, None, schema_constants.OBJECT),
      (None, schema_constants.INT64, None, schema_constants.INT64),
      (None, schema_constants.ANY, None, schema_constants.ANY),
      # Deduce schema from list items.
      ([1, 2, 3], None, None, schema_constants.INT32),
      ([1, 'foo'], None, None, schema_constants.OBJECT),
      (
          ds([[1, 'foo'], [3]], schema=schema_constants.ANY),
          None,
          None,
          schema_constants.ANY,
      ),
      (
          [[1, 2], [3]],
          None,
          None,
          fns.list(item_schema=schema_constants.INT32).get_schema().no_bag(),
      ),
      # Both schema and list items provided.
      ([1, 2, 3], schema_constants.INT64, None, schema_constants.INT64),
      ([1, 2, 3], schema_constants.ANY, None, schema_constants.ANY),
      ([1, 2, 3], schema_constants.OBJECT, None, schema_constants.OBJECT),
      (
          None,
          None,
          fns.list_schema(item_schema=schema_constants.INT64),
          schema_constants.INT64,
      ),
      (
          [1, 2, 3],
          None,
          fns.list_schema(item_schema=schema_constants.INT64),
          schema_constants.INT64,
      ),
  )
  def test_schema(self, items, item_schema, schema, expected_item_schema):
    testing.assert_equal(
        fns.list(items=items, item_schema=item_schema, schema=schema)
        .get_schema()
        .get_attr('__items__')
        .no_bag(),
        expected_item_schema,
    )

  def test_schema_arg_error(self):
    list_schema = fns.list_schema(item_schema=schema_constants.INT64)
    with self.assertRaisesRegex(
        ValueError, 'either a list schema or item schema, but not both'
    ):
      fns.list(item_schema=schema_constants.INT64, schema=list_schema)

  def test_wrong_arg_types(self):
    with self.assertRaisesRegex(
        TypeError, 'expecting item_schema to be a DataSlice, got int'
    ):
      fns.list(item_schema=42)
    with self.assertRaisesRegex(
        TypeError, 'expecting schema to be a DataSlice, got int'
    ):
      fns.list(schema=42)

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for List item is incompatible.

Expected schema for List item: BYTES
Assigned schema for List item: INT32""",
    ):
      fns.list([[1, 2], [3]], item_schema=schema_constants.BYTES)


if __name__ == '__main__':
  absltest.main()
