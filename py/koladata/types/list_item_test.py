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

"""Tests for list_item."""

import inspect
import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
# Register kde ops for e.g. jagged_shape.create_shape().
from koladata.operators import kde_operators as _
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_item
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import list_item

ds = data_slice.DataSlice.from_vals


class ListItemTest(parameterized.TestCase):

  def test_qvalue(self):
    self.assertTrue(issubclass(list_item.ListItem, arolla.QValue))
    self.assertTrue(issubclass(list_item.ListItem, data_slice.DataSlice))
    self.assertTrue(issubclass(list_item.ListItem, data_item.DataItem))
    l = data_bag.DataBag.empty().list()
    self.assertIsInstance(l, list_item.ListItem)
    self.assertIsInstance(l, arolla.QValue)

  def test_hash(self):
    db = data_bag.DataBag.empty()
    py_lists = [
        None,
        [1, 2, 3],
        [[1, 2], [3, 4]],
    ]
    for py_list_1, py_list_2 in itertools.combinations(py_lists, 2):
      self.assertNotEqual(hash(db.list(py_list_1)), hash(db.list(py_list_2)))

  def test_db(self):
    db = data_bag.DataBag.empty()
    testing.assert_equal(db.list([1, 2, 3]).db, db)

  def test_get_shape(self):
    l = data_bag.DataBag.empty().list([1, 2, 3])
    testing.assert_equal(l.get_shape(), jagged_shape.create_shape())

  def test_len(self):
    db = data_bag.DataBag.empty()

    l1 = db.list()
    self.assertEmpty(l1, 0)

    l2 = db.list([2, 1, 0])
    self.assertLen(l2, 3)

  def test_pop(self):
    db = data_bag.DataBag.empty()
    l = db.list([1, 2, 3, 4, 5, 6])
    testing.assert_equal(l.pop(), ds(6).with_db(db))
    testing.assert_equal(l[:], ds([1, 2, 3, 4, 5]).with_db(db))
    testing.assert_equal(l.pop(0), ds(1).with_db(db))
    testing.assert_equal(l[:], ds([2, 3, 4, 5]).with_db(db))
    testing.assert_equal(l.pop(-2), ds(4).with_db(db))
    testing.assert_equal(l[:], ds([2, 3, 5]).with_db(db))
    testing.assert_equal(l.pop(ds(1)), ds(3).with_db(db))
    testing.assert_equal(l[:], ds([2, 5]).with_db(db))

    with self.assertRaisesWithLiteralMatch(
        IndexError, "List index out of range: list size 2 vs index 2"
    ):
      l.pop(2)

    with self.assertRaisesWithLiteralMatch(
        IndexError, "List index out of range: list size 2 vs index -3"
    ):
      l.pop(-3)

    with self.assertRaisesWithLiteralMatch(
        ValueError, "'a' cannot be interpreted as an integer"
    ):
      l.pop("a")

  def test_iter(self):
    db = data_bag.DataBag.empty()
    l = db.list([1, "2", 1.1])
    self.assertTrue(inspect.isgenerator(iter(l)))
    self.assertEqual([i.internal_as_py() for i in l], l[:].internal_as_py())

  @parameterized.named_parameters(
      (
          "int32",
          [1, 2, 3],
          "List[1, 2, 3]",
          (
              r"DataItem\(List\[1, 2, 3\], schema: LIST\[INT32\], bag_id:"
              r" \$\w{4}\)"
          ),
      ),
      (
          "int64",
          [arolla.int64(1), arolla.int64(2), arolla.int64(3)],
          "List[1, 2, 3]",
          (
              r"DataItem\(List\[1, 2, 3\], schema: LIST\[INT64\], bag_id:"
              r" \$\w{4}\)"
          ),
      ),
      (
          "float32",
          [0.618, 114.514],
          "List[0.618, 114.514]",
          (
              r"DataItem\(List\[0.618, 114.514\], schema: LIST\[FLOAT32\],"
              r" bag_id: \$\w{4}\)"
          ),
      ),
      (
          "float64",
          [arolla.float64(0.618), arolla.float64(114.514)],
          "List[0.618, 114.514]",
          (
              r"DataItem\(List\[0.618, 114.514\], schema:"
              r" LIST\[FLOAT64\], bag_id: \$\w{4}\)"
          ),
      ),
      (
          "boolean",
          [True, False],
          "List[True, False]",
          (
              r"DataItem\(List\[True, False\], schema: LIST\[BOOLEAN\], bag_id:"
              r" \$\w{4}\)"
          ),
      ),
      (
          "mask",
          [arolla.unit(), arolla.optional_unit(None)],
          "List[present, None]",
          (
              r"DataItem\(List\[present, None\], schema: LIST\[MASK\],"
              r" bag_id: \$\w{4}\)"
          ),
      ),
      (
          "text",
          ["a", "b"],
          "List['a', 'b']",
          (
              r"DataItem\(List\['a', 'b'\], schema: LIST\[TEXT\], bag_id:"
              r" \$\w{4}\)"
          ),
      ),
      (
          "bytes",
          [b"a", b"b"],
          "List[b'a', b'b']",
          (
              r"DataItem\(List\[b'a', b'b'\], schema: LIST\[BYTES\], bag_id:"
              r" \$\w{4}\)"
          ),
      ),
      (
          "empty",
          [],
          "List[]",
          r"DataItem\(List\[\], schema: LIST\[OBJECT\], bag_id: \$\w{4}\)",
      ),
      (
          "mixed_data",
          [1, "abc", True, arolla.float64(1.1)],
          "List[1, 'abc', True, 1.1]",
          (
              r"DataItem\(List\[1, 'abc', True, 1.1\], schema:"
              r" LIST\[OBJECT\], bag_id: \$\w{4}\)"
          ),
      ),
      (
          "nested",
          [[1, 2], [3, 4, 5]],
          "List[List[1, 2], List[3, 4, 5]]",
          (
              r"DataItem\(List\[List\[1, 2\], List\[3, 4, 5\]\], schema:"
              r" LIST\[LIST\[INT32\]\], bag_id: \$\w{4}\)"
          ),
      ),
  )
  def test_str_and_repr(self, x, expected_str, expected_repr):
    db = data_bag.DataBag.empty()
    self.assertEqual(str(db.list(x)), expected_str)
    self.assertRegex(repr(db.list(x)), expected_repr)


if __name__ == "__main__":
  absltest.main()
