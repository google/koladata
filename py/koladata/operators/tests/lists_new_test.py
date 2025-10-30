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
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals

db = data_bag.DataBag.empty_mutable()


class ListsListTest(parameterized.TestCase):

  @parameterized.parameters(
      ([1, 2, 3], ds([1, 2, 3])),
      ([1, 2, 3.0], ds([1, 2, 3.0])),
      ([[1, 2], [3, 4, 5]], ds(db.list([db.list([1, 2]), db.list([3, 4, 5])]))),
      (((1, 2), (3, 4, 5)), ds(db.list([db.list([1, 2]), db.list([3, 4, 5])]))),
      (kde.slice([1, 2, 3]), ds([1, 2, 3])),
  )
  def test_eval(self, items, expected_items):
    res = expr_eval.eval(kde.lists.new(items))
    testing.assert_equal(
        res.explode(-1).no_bag(), expected_items.explode(-1).no_bag()
    )

  @parameterized.parameters(
      (arolla.unspecified(), ds([], schema=schema_constants.OBJECT)),
      ([], ds([], schema=schema_constants.NONE)),
  )
  def test_eval_empty(self, items, expected_items):
    res = expr_eval.eval(kde.lists.new(items))
    testing.assert_equal(res[:].no_bag(), expected_items.no_bag())

  def test_eval_expr_argument(self):
    res = expr_eval.eval(
        kde.lists.new([[I.x0, I.x1], [I.x2]]), x0=0, x1=1, x2=2
    )
    testing.assert_equal(res.explode(-1).no_bag(), ds([[0, 1], [2]]))

  @parameterized.parameters(
      ([], schema_constants.INT32, ds([], schema_constants.INT32)),
      ([1, 2, 3], schema_constants.FLOAT32, ds([1.0, 2.0, 3.0])),
  )
  def test_eval_with_item_schema(self, items, item_schema, expected_items):
    res = expr_eval.eval(kde.lists.new(items, item_schema=item_schema))
    testing.assert_equal(res[:].no_bag(), expected_items)

  def test_eval_with_schema(self):
    schema = kde.list_schema(item_schema=schema_constants.INT64).eval()
    res = expr_eval.eval(kde.lists.new([1, 2, 3], schema=schema))
    testing.assert_equal(res[:].no_bag(), ds([1, 2, 3], schema_constants.INT64))

  # TODO: Re-enable once the bug is fixed.
  # def test_eval_nested_list_schema(self):
  #   schema = kde.list_schema(kde.list_schema(schema_constants.INT32)).eval()
  #   res = expr_eval.eval(kde.lists.new([[1, 2], [3]], schema=schema))
  #   testing.assert_equal(
  #       res[:].no_bag(), ds([1, 2, 3], schema_constants.INT32))

  @parameterized.parameters(
      ([1, 2, 3],),
      ([[1, 2], [3]],),
  )
  def test_itemid(self, items):
    itemid = expr_eval.eval(kde.allocation.new_listid())
    res = expr_eval.eval(kde.lists.new(items, itemid=itemid))
    testing.assert_equal(res.get_itemid().no_bag(), itemid)

  def test_schema_as_expr(self):
    res = expr_eval.eval(
        kde.lists.new(I.items, item_schema=I.schema),
        items=ds([1, 2, 3]),
        schema=schema_constants.INT64,
    )
    testing.assert_equal(res[:].no_bag(), ds([1, 2, 3], schema_constants.INT64))

    res = expr_eval.eval(
        kde.lists.new([1, 2, 3], item_schema=I.schema),
        schema=schema_constants.INT64,
    )
    testing.assert_equal(res[:].no_bag(), ds([1, 2, 3], schema_constants.INT64))

    res = expr_eval.eval(
        kde.lists.new(I.items, schema=I.schema),
        items=ds([1, 2, 3]),
        schema=kde.list_schema(item_schema=schema_constants.INT64).eval(),
    )
    testing.assert_equal(res[:].no_bag(), ds([1, 2, 3], schema_constants.INT64))

    res = expr_eval.eval(
        kde.lists.new([1, 2, 3], schema=I.schema),
        schema=kde.list_schema(item_schema=schema_constants.INT64).eval(),
    )
    testing.assert_equal(res[:].no_bag(), ds([1, 2, 3], schema_constants.INT64))

  def test_eval_data_slice_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.list does not accept DataSlice as an input',
    ):
      _ = expr_eval.eval(kde.lists.new(ds([1, 2, 3])))

  def test_cast_error_item_schema(self):
    x = [mask_constants.missing, mask_constants.present]
    with self.assertRaisesRegex(
        ValueError,
        'the schema for list items is incompatible',
    ):
      expr_eval.eval(kde.lists.new(x, item_schema=schema_constants.INT32))

  def test_cast_error_schema(self):
    x = [mask_constants.missing, mask_constants.present]
    schema = kde.list_schema(item_schema=schema_constants.INT32).eval()
    with self.assertRaisesRegex(
        ValueError,
        'the schema for list items is incompatible',
    ):
      expr_eval.eval(kde.lists.new(x, schema=schema))

  def test_schema_not_a_list_schema(self):
    with self.assertRaisesRegex(
        ValueError, 'expected List schema for get_item_schema'
    ):
      expr_eval.eval(kde.lists.new([1, 2, 3], schema=schema_constants.INT32))

  def test_item_schema_and_schema(self):
    schema = kde.list_schema(item_schema=schema_constants.FLOAT64).eval()
    with self.assertRaisesRegex(
        ValueError,
        'either a list schema or item schema, but not both',
    ):
      _ = expr_eval.eval(
          kde.lists.new(
              [1.6, 2.2], item_schema=schema_constants.INT64, schema=schema
          )
      )

  def test_no_explicit_casting(self):
    with self.assertRaisesRegex(
        ValueError,
        'the schema for list items is incompatible',
    ):
      expr_eval.eval(
          kde.lists.new([3.14, 2.71], item_schema=schema_constants.INT32)
      )

  def test_expr_argument_inside_list_with_cast(self):
    x = [1, I.x, 3]
    with self.assertRaisesRegex(
        ValueError,
        'the schema for list items is incompatible',
    ):
      _ = expr_eval.eval(
          kde.lists.new(x, item_schema=schema_constants.INT64), x=2.1
      )

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.new, kde.list))

  def test_repr(self):
    self.assertEqual(
        repr(kde.lists.new(I.x)),
        'kd.lists.new(I.x, unspecified, unspecified, unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
