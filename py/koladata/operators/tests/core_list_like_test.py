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

"""Tests for kde.core.list_like operator."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.functions import functions as fns
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
bag = data_bag.DataBag.empty


class ListLikeTest(parameterized.TestCase):

  @parameterized.parameters(
      ([ds(None)], dict()),
      ([ds(1)], dict()),
      ([ds(1)], dict(items=ds([2]))),
      ([ds([[1, None], [3]])], dict()),
      ([ds([[1, None], [3]])], dict(item_schema=schema_constants.OBJECT)),
      ([ds([[1, None], [3]])], dict(item_schema=schema_constants.INT64)),
      ([ds([[1, None], [3]])], dict(item_schema=schema_constants.ANY)),
      ([ds([[1, None], [3]])], dict(items=ds([[1, 2], [3]]))),
      ([ds([[1, None], [3]])], dict(items=ds([[1, 'foo'], [3]]))),
      (
          [ds([[1, None], [3]])],
          dict(items=ds([[1, 'foo'], [3]], schema=schema_constants.ANY)),
      ),
      (
          [ds([[1, None], [3]])],
          dict(
              items=ds([[1, 'foo'], [3]], schema=schema_constants.OBJECT),
              item_schema=schema_constants.ANY,
          ),
      ),
      (
          [ds([[1, None], [3]])],
          dict(
              schema=bag().list_schema(item_schema=schema_constants.INT64),
          ),
      ),
      (
          [ds([[1, None], [3]])],
          dict(
              items=ds([[1, 2], [3]]),
              schema=bag().list_schema(item_schema=schema_constants.INT64),
          ),
      ),
      (
          [ds([[1, None], [3]])],
          dict(
              itemid=bag().list_like(ds([[0, 0], [0]])).as_itemid()
          ),
      ),
  )
  def test_value(self, args, kwargs):
    actual = expr_eval.eval(kde.core.list_like(*args, **kwargs))
    expected = bag().list_like(*args, **kwargs)
    testing.assert_equivalent(
        actual.get_schema().get_attr('__items__').no_db(),
        expected.get_schema().get_attr('__items__').no_db(),
    )
    self.assertEqual(actual.to_py(), expected.to_py())

  def test_itemid(self):
    itemid = kde.allocation.new_listid_shaped_as._eval(ds([1, 1]))  # pylint: disable=protected-access
    x = expr_eval.eval(
        kde.core.list_like(
            ds([1, None]), items=ds([['a', 'b'], ['c']]), itemid=itemid
        )
    )
    testing.assert_equal(x[:].no_db(), ds([['a', 'b'], []]))
    testing.assert_equal(x.no_db().as_itemid(), itemid & kde.has._eval(x))  # pylint: disable=protected-access

  def test_db_is_immutable(self):
    lst = expr_eval.eval(kde.core.list_like(ds([[1, None], [3]])))
    self.assertFalse(lst.is_mutable())

  def test_wrong_shape_and_mask_from(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got shape_and_mask_from: DATA_BAG',
    ):
      expr_eval.eval(kde.core.list_like(fns.bag()))

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      expr_eval.eval(
          kde.core.list_like(ds([[1, None], [2]]), items=ds([1, 2, 3]))
      )

  def test_schema_arg_error(self):
    mask_and_shape = ds([[1, None], [3]])
    list_schema = fns.list_schema(item_schema=schema_constants.INT64)
    with self.assertRaisesRegex(
        ValueError, 'either a list schema or item schema, but not both'
    ):
      expr_eval.eval(kde.core.list_like(
          mask_and_shape,
          item_schema=schema_constants.INT64,
          schema=list_schema,
      ))

  def test_wrong_arg_types(self):
    mask_and_shape = ds([[1, None], [3]])
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.list_like(mask_and_shape, item_schema=42))
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.core.list_like(mask_and_shape, schema=42))

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        r"""the schema for List item is incompatible.

Expected schema for List item: BYTES
Assigned schema for List item: INT32""",
    ):
      expr_eval.eval(kde.core.list_like(
          ds([[1, None], [3]]),
          items=ds([[1, 2], [3]]),
          item_schema=schema_constants.BYTES,
      ))

  def test_non_determinism(self):
    x = ds([[1, None], [3]])
    items = ds([[1, 2], [3]])
    res_1 = expr_eval.eval(kde.core.list_like(x, items=items))
    res_2 = expr_eval.eval(kde.core.list_like(x, items=items))
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_equal(res_1[:].no_db(), res_2[:].no_db())

    expr = kde.core.list_like(x, items=items)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(res_1.db.fingerprint, res_2.db.fingerprint)
    testing.assert_equal(res_1[:].no_db(), res_2[:].no_db())


if __name__ == '__main__':
  absltest.main()
