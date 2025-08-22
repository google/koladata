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

"""Tests for kde.lists.shaped_as operator."""

import itertools

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
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde
bag = data_bag.DataBag.empty_mutable

QTYPE_SIGNATURES = list(
    (
        qtypes.DATA_SLICE, *args, qtypes.NON_DETERMINISTIC_TOKEN,
        qtypes.DATA_SLICE
    )
    for args in itertools.product(
        [qtypes.DATA_SLICE, arolla.UNSPECIFIED], repeat=4
    )
)


class ListShapedAsTest(parameterized.TestCase):

  @parameterized.parameters(
      ([ds(None)], dict()),
      ([ds([])], dict()),
      ([ds([1])], dict(items=ds([2]))),
      ([ds([[None, 2], [3]])], dict()),
      (
          [ds([1, 2])],
          dict(item_schema=schema_constants.OBJECT),
      ),
      (
          [ds([1, 2])],
          dict(item_schema=schema_constants.INT64),
      ),
      ([ds([1, 2])], dict(items=ds([[1, 2], [3]]))),
      (
          [ds([1, 2])],
          dict(items=ds([[1, 'foo'], [3]])),
      ),
      (
          [ds([1, 2])],
          dict(
              schema=bag().list_schema(item_schema=schema_constants.INT64),
          ),
      ),
      (
          [ds([1, 2])],
          dict(
              items=ds([[1, 2], [3]]),
              schema=bag().list_schema(item_schema=schema_constants.INT64),
          ),
      ),
      (
          [ds([1, 2])],
          dict(itemid=fns.list_shaped_as(ds([1, 2])).get_itemid()),
      ),
  )
  def test_value(self, args, kwargs):
    actual = expr_eval.eval(kde.lists.shaped_as(*args, **kwargs))
    expected = fns.list_shaped_as(*args, **kwargs)
    testing.assert_equal(actual[:].no_bag(), expected[:].no_bag())

  def test_itemid(self):
    itemid = expr_eval.eval(kde.allocation.new_listid_shaped_as(ds([1, 1])))
    x = expr_eval.eval(
        kde.lists.shaped_as(
            ds([1, 2]),
            items=ds([['a', 'b'], ['c']]),
            itemid=itemid,
        )
    )
    testing.assert_equal(x[:].no_bag(), ds([['a', 'b'], ['c']]))
    testing.assert_equal(
        x.no_bag().get_itemid(), itemid & expr_eval.eval(kde.has(x))
    )

  def test_db_is_immutable(self):
    lst = expr_eval.eval(kde.lists.shaped_as(ds([1, 2])))
    self.assertFalse(lst.is_mutable())

  def test_adopt_values(self):
    lst = kde.lists.implode(ds([[1, 2], [3]])).eval()
    lst2 = kde.lists.shaped_as(ds([0, 0]), lst).eval()

    testing.assert_equal(
        lst2[:][:],
        ds([[[1, 2]], [[3]]], schema_constants.INT32).with_bag(lst2.get_bag()),
    )

  def test_adopt_schema(self):
    list_schema = kde.schema.list_schema(
        kde.uu_schema(a=schema_constants.INT32)
    ).eval()
    lst = kde.lists.shaped_as(ds([0, 0]), schema=list_schema).eval()

    testing.assert_equal(
        lst[:].a.no_bag(), ds([[], []], schema_constants.INT32)
    )

  def test_wrong_shape_and_mask_from(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected DATA_SLICE, got shape_from: DATA_BAG',
    ):
      expr_eval.eval(kde.lists.shaped_as(fns.mutable_bag()))

  def test_incompatible_shape(self):
    with self.assertRaisesRegex(ValueError, 'cannot be expanded'):
      expr_eval.eval(kde.lists.shaped_as(ds([1, 2]), items=ds([1, 2, 3])))

  def test_schema_arg_error(self):
    list_schema = kde.list_schema(item_schema=schema_constants.INT64)
    with self.assertRaisesRegex(
        ValueError, 'either a list schema or item schema, but not both'
    ):
      expr_eval.eval(
          kde.lists.shaped_as(
              ds([[None, 2], [3]]),
              item_schema=schema_constants.INT64,
              schema=list_schema,
          )
      )

  def test_wrong_arg_types(self):
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(
          kde.lists.shaped_as(ds([[None, 2], [3]]), item_schema=42)
      )
    with self.assertRaisesRegex(
        ValueError, "schema's schema must be SCHEMA, got: INT32"
    ):
      expr_eval.eval(kde.lists.shaped_as(ds([[None, 2], [3]]), schema=42))

  def test_schema_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        r"""the schema for list items is incompatible.

Expected schema for list items: BYTES
Assigned schema for list items: INT32""",
    ):
      expr_eval.eval(
          kde.lists.shaped_as(
              ds([1, 2]),
              items=ds([[1, 2], [3]]),
              item_schema=schema_constants.BYTES,
          )
      )

  def test_non_determinism(self):
    shape = ds([1, 2])
    items = ds([[1, 2], [3]])
    res_1 = expr_eval.eval(kde.lists.shaped_as(shape, items=items))
    res_2 = expr_eval.eval(kde.lists.shaped_as(shape, items=items))
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

    expr = kde.lists.shaped_as(shape, items=items)
    res_1 = expr_eval.eval(expr)
    res_2 = expr_eval.eval(expr)
    self.assertNotEqual(
        res_1.get_bag().fingerprint, res_2.get_bag().fingerprint
    )
    testing.assert_equal(res_1[:].no_bag(), res_2[:].no_bag())

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.lists.shaped_as,
        QTYPE_SIGNATURES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.shaped_as(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.lists.shaped_as, kde.list_shaped_as)
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.lists.shaped_as(I.x, I.y)),
        'kd.lists.shaped_as(I.x, I.y, item_schema=unspecified,'
        ' schema=unspecified, itemid=unspecified)',
    )


if __name__ == '__main__':
  absltest.main()
