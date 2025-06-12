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
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes

I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals


class SlicesIsInTest(parameterized.TestCase):

  @parameterized.parameters(
      # DataItem.
      (ds('a'), ds('a'), mask_constants.present),
      (ds('a'), ds('b'), mask_constants.missing),
      (ds(42), ds('b'), mask_constants.missing),
      (ds(42), ds(None), mask_constants.missing),
      # DataSlice.
      (ds(b'xyz'), ds([b'axy', b'1234']), mask_constants.missing),
      (
          ds(b'xyz'),
          ds([b'axy', b'1234', None, None, b'xyz']),
          mask_constants.present,
      ),
      (ds(42), ds([1, 2, None, 34]), mask_constants.missing),
      (ds(42), ds([42, 1, 2, None, 34]), mask_constants.present),
      # Auto-boxed DataItem.
      (42, ds([42, 1, 2, None, 34]), mask_constants.present),
      (ds(float('nan')), ds([1.0, 2.0, float('nan')]), mask_constants.missing),
      (
          ds(float('nan')),
          ds([1.0, 2.0, float('nan'), float('nan')]),
          mask_constants.missing,
      ),
      # Mixed.
      (ds('a'), ds(['b', 'c', 42]), mask_constants.missing),
      (ds('a'), ds(['b', 'c', None, 42, 'a']), mask_constants.present),
      (ds(42), ds(['b', 'c', None, 42, 'a']), mask_constants.present),
      # Objects.
      (
          bag().uu(a=42, b=b'xyz'),
          bag().uu(a=ds([1, 2, 3]), b=ds([b'xyz', b'xyz', b'xyz'])),
          mask_constants.missing,
      ),
      (
          bag().uu(a=42, b=b'xyz'),
          bag().uu(a=ds([1, 2, 42]), b=ds([b'xyz', b'xyz', b'xyz'])),
          mask_constants.present,
      ),
      # None is not in None(s).
      (ds(None), ds([None, None]), mask_constants.missing),
      # Multi-dim.
      (ds(42), ds([[1, 2, 3], [4, 5]]), mask_constants.missing),
      (ds(42), ds([[1, 42, 3], [42, 5]]), mask_constants.present),
  )
  def test_eval(self, x, y, result):
    testing.assert_equal(expr_eval.eval(kde.slices.isin(x, y)), result)

  def test_x_not_an_item(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.slices.isin: argument `x` must be a DataItem',
    ):
      expr_eval.eval(kde.slices.isin(ds([1, 2, 3]), ds([1, 2, 3])))

  def test_x_not_a_ds(self):
    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got x: DATA_BAG'
    ):
      kde.slices.isin(bag(), ds([1, 2, 3]))
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'object with unsupported type: dict'
    ):
      kde.slices.isin({'a': 42}, ds([1, 2, 3]))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.isin,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.DATA_SLICE, qtypes.DATA_SLICE, qtypes.DATA_SLICE)]),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.isin(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.isin, kde.isin))


if __name__ == '__main__':
  absltest.main()
