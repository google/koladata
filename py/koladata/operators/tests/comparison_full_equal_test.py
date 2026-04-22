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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
present = mask_constants.present
missing = mask_constants.missing
bag = data_bag.DataBag.empty_mutable
DATA_SLICE = qtypes.DATA_SLICE

ENTITY_1 = bag().new()


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class ComparisonFullEqualTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([1, None, 3, 2]), ds([1, None, 3, 2]), present),
      (ds([1, 1, 3, 2]), ds([1, None, 3, 2]), missing),
      (ds(['a', 'b', 1, None, 1.5]), ds(['a', 'b', 1, None, 1.5]), present),
      (ds(['a', 'b', 1, 2, 1.5]), ds(['a', 'b', 1, None, 1.5]), missing),
      (ds([ENTITY_1, None]), ds([ENTITY_1, None]), present),
      (ds([ENTITY_1, ENTITY_1]), ds([ENTITY_1, None]), missing),
      # Broadcasting
      (ds([2, 2, 2]), 1, missing),
      (ds([1, None, 1]), 1, missing),
      (ds([1, 1, 1]), 1, present),
      # Scalar input
      (ds(1), ds(1), present),
      (ds(1), ds(2), missing),
      (ds(1), ds(1, schema_constants.INT64), present),
      (ds(1), ds(1.0), present),
      (ds('a'), ds('a'), present),
      (ds('a'), ds('b'), missing),
      (ds('a'), ds(b'a'), missing),
      (ds('a'), ds('a', schema_constants.OBJECT), present),
      (ds(1), ds(arolla.missing()), missing),
      (ds(arolla.missing()), ds(arolla.missing()), present),
      (
          ds(None, schema_constants.INT32),
          ds(None, schema_constants.INT32),
          present,
      ),
      (ENTITY_1, ENTITY_1, present),
  )
  def test_eval(self, lhs, rhs, expected):
    result = kd.comparison.full_equal(lhs, rhs)
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.comparison.full_equal,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_incompatible_schemas(self):
    testing.assert_equal(kd.comparison.full_equal(ENTITY_1, ds(1)), missing)

    db = data_bag.DataBag.empty_mutable()
    testing.assert_equal(
        kd.comparison.full_equal(db.new(x=1), db.new()), missing
    )
    testing.assert_equal(
        kd.comparison.full_equal(db.new(x=1), db.obj()), missing
    )
    testing.assert_equal(
        kd.comparison.full_equal(
            db.new(x=1), db.obj().with_schema(schema_constants.ITEMID)
        ),
        missing,
    )

  def test_incompatible_shapes(self):
    testing.assert_equal(
        kd.comparison.full_equal(ds([1, 2]), ds([1, 2, 3])), missing
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.comparison.full_equal(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.comparison.full_equal, kde.full_equal)
    )


if __name__ == '__main__':
  absltest.main()
