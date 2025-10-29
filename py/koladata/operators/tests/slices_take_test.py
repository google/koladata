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
from koladata.operators.tests.testdata import slices_take_testdata
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([(DATA_SLICE, DATA_SLICE, DATA_SLICE)])


class SlicesTakeTest(parameterized.TestCase):

  @parameterized.parameters(*slices_take_testdata.TEST_CASES)
  def test_eval(self, x, indices, expected):
    result = expr_eval.eval(kde.take(x, indices))
    testing.assert_equal(result, expected)

  def test_data_item_input_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape('kd.slices.take: DataItem is not supported.'),
    ):
      expr_eval.eval(kde.take(ds(1), ds(0)))

  def test_incompatible_shape_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.take: DataSlice with shape=JaggedShape(2) is not'
            ' compatible with shape=JaggedShape(3); kd.take requires'
            ' shape(x)[:-1] to be compatible with shape(indices)'
        ),
    ):
      expr_eval.eval(kde.take(ds([[1], [2, 3]]), ds([1, 2, 3])))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.take: DataSlice with shape=JaggedShape(2, [1, 2]) is not'
            ' compatible with shape=JaggedShape(3); kd.take requires'
            ' shape(x)[:-1] to be compatible with shape(indices)'
        ),
    ):
      expr_eval.eval(kde.take(ds([[[1]], [[2], [3]]]), ds([1, 2, 3])))

  def test_wrong_dtype_error(self):
    with self.assertRaisesWithPredicateMatch(
        ValueError,
        arolla.testing.any_cause_message_regex(
            re.escape(
                'unsupported narrowing cast to INT64 for the given STRING'
                ' DataSlice'
            )
        ),
    ) as cm:
      expr_eval.eval(kde.take(ds([[1], [2, 3]]), ds(['1', '2'])))
    self.assertRegex(
        str(cm.exception),
        'kd.slices.take: the provided indices must contain only integers',
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.take,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.take(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.take, kde.take))
    self.assertTrue(optools.equiv_to_op(kde.slices.at, kde.take))
    self.assertTrue(optools.equiv_to_op(kde.at, kde.take))


if __name__ == '__main__':
  absltest.main()
