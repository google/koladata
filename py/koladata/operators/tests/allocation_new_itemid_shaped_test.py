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

"""Tests for allocation.new_itemid_shaped."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import qtypes
from koladata.types import schema_constants

bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


# TODO: Test non-determinism when it gets implemented.
class AllocationNewItemIdShapedTest(parameterized.TestCase):

  @parameterized.parameters(
      (jagged_shape.create_shape([2]), ds([42, 42])),
      (jagged_shape.create_shape([2], [3, 1]), ds([[42, 42, 42], [42]])),
      (jagged_shape.create_shape(), ds(42)),
  )
  def test_eval(self, shape, attr):
    itemid = expr_eval.eval(kde.allocation.new_itemid_shaped(shape))
    testing.assert_equal(itemid.get_schema(), schema_constants.ITEMID)
    entity = itemid.with_db(bag())
    entity = entity.with_schema(entity.db.new_schema(a=schema_constants.INT32))
    entity.a = 42
    testing.assert_equal(entity.a, attr.with_db(entity.db))

  def test_new_alloc_ids(self):
    shape = jagged_shape.create_shape([2])
    itemid = expr_eval.eval(kde.allocation.new_itemid_shaped(shape))
    testing.assert_equal(
        itemid, expr_eval.eval(kde.allocation.new_itemid_shaped(shape))
    )
    arolla.abc.clear_caches()
    with self.assertRaises(AssertionError):
      testing.assert_equal(
          itemid, expr_eval.eval(kde.allocation.new_itemid_shaped(shape))
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.allocation.new_itemid_shaped,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.JAGGED_SHAPE, qtypes.DATA_SLICE)]),
    )

  def test_view(self):
    shape = jagged_shape.create_shape([2])
    self.assertTrue(
        view.has_data_slice_view(kde.allocation.new_itemid_shaped(shape))
    )

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(
            kde.allocation.new_itemid_shaped, kde.new_itemid_shaped
        )
    )


if __name__ == '__main__':
  absltest.main()