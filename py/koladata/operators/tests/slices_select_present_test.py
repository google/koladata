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

import re

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
from koladata.types import data_slice
from koladata.types import qtypes


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, qtypes.NON_DETERMINISTIC_TOKEN, DATA_SLICE),
])


class SlicesSelectPresentTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          ds([[1, 2, None, 4], [None, None], [7, 8, 9]]),
          ds([[1, 2, 4], [], [7, 8, 9]]),
      ),
      (ds([[[[None, 1]]]]), ds([[[[1]]]])),
      (ds([[[[1]]]]), ds([[[[1]]]])),
  )
  def test_eval(self, values, expected):
    result = expr_eval.eval(kde.slices.select_present(values))
    testing.assert_equal(result, expected)

  def test_select_on_data_item_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.select: cannot select from DataItem because its size is'
            ' always 1. Consider calling .flatten() beforehand'
            ' to convert it to a 1-dimensional DataSlice'
        ),
    ):
      expr_eval.eval(kde.slices.select_present(ds(1)))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.select_present,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.select_present(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.slices.select_present, kde.select_present)
    )


if __name__ == '__main__':
  absltest.main()
