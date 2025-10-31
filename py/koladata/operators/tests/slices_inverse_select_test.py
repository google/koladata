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

import re

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.testdata import slices_inverse_select_testdata
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class SlicesInverseSelectTest(parameterized.TestCase):

  @parameterized.parameters(*slices_inverse_select_testdata.TEST_CASES)
  def test_eval(self, values, fltr, expected):
    result = expr_eval.eval(kde.slices.inverse_select(values, fltr))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.inverse_select,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_invalid_type_error(self):
    with self.assertRaisesRegex(
        ValueError,
        'kd.slices.inverse_select: the schema of the fltr DataSlice should only'
        ' be Object or Mask',
    ):
      expr_eval.eval(kde.slices.inverse_select(ds([1]), ds([1, 2])))

    with self.assertRaisesRegex(
        ValueError,
        'kd.slices.inverse_select: fltr argument must have all items of MASK'
        ' dtype',
    ):
      expr_eval.eval(
          kde.slices.inverse_select(
              ds([1]), ds([1, 2], schema_constants.OBJECT)
          )
      )

  def test_invalid_shape_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.inverse_select: the rank of the ds and fltr DataSlice'
            ' must be the same. Got rank(ds): 0, rank(fltr): 1'
        ),
    ):
      expr_eval.eval(
          kde.slices.inverse_select(ds(1), ds([arolla.present(), None]))
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.inverse_select: the shape of `ds` and the shape of the'
            ' present elements in `fltr` do not match: JaggedShape(2) vs'
            ' JaggedShape(1)'
        ),
    ):
      expr_eval.eval(
          kde.slices.inverse_select(
              ds([1, 2]), ds([arolla.present(), None, None])
          )
      )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.inverse_select(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.slices.inverse_select, kde.inverse_select)
    )
    self.assertTrue(
        optools.equiv_to_op(kde.slices.reverse_select, kde.inverse_select)
    )
    self.assertTrue(optools.equiv_to_op(kde.reverse_select, kde.inverse_select))


if __name__ == '__main__':
  absltest.main()
