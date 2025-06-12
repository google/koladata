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
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
])


class SlicesRankTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([1, 2, None, 3]), ds(1, schema_constants.INT64)),
      (ds([[1, 2], [None, 3]]), ds(2, schema_constants.INT64)),
      (ds([[['foo'], [2]], [[None, 2.1]]]), ds(3, schema_constants.INT64)),
      (ds(None), ds(0, schema_constants.INT64)),
      (ds(42), ds(0, schema_constants.INT64)),
  )
  def test_eval(self, *args_and_expected):
    args, expected_value = args_and_expected[:-1], args_and_expected[-1]
    result = expr_eval.eval(kde.slices.get_ndim(*args))
    testing.assert_equal(result, expected_value)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.get_ndim,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.get_ndim(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.get_ndim, kde.get_ndim))


if __name__ == '__main__':
  absltest.main()
