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
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class SlicesCollapseTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([None]), ds(None)),
      (ds([1, None, 'b']), ds(None, schema_constants.OBJECT)),
      (ds(['a', 'b', 1, 1, None]), ds(None, schema_constants.OBJECT)),
      (ds([[1, None], [2, 2], [3, 4], [None, None]]), ds([1, 2, None, None])),
      (ds([[[1], ['a', 'a']], [['b', 2]]]), ds([[1, 'a'], [None]])),
  )
  def test_eval_one_arg(self, x, expected):
    result = expr_eval.eval(kde.slices.collapse(x))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      (
          ds([[1, None], [2, 2], [3, 4], [None, None]]),
          arolla.unspecified(),
          ds([1, 2, None, None]),
      ),
      (
          ds([[1, None], [2, 2], [3, 4], [None, None]]),
          ds(0),
          ds([[1, None], [2, 2], [3, 4], [None, None]]),
      ),
      (
          ds([[1, None], [2, 2], [3, 4], [None, None]]),
          ds(1),
          ds([1, 2, None, None]),
      ),
      (
          ds([[1, None], [2, 2], [3, 4], [None, None]]),
          ds(2),
          ds(None, schema_constants.INT32),
      ),
      (ds([[1, None], [1, 1], [1, 1], [None, None]]), ds(2), ds(1)),
  )
  def test_eval_two_args(self, x, ndim, expected):
    result = expr_eval.eval(kde.slices.collapse(x, ndim))
    testing.assert_equal(result, expected)

  def test_data_item_input_error(self):
    x = ds(1)
    with self.assertRaisesRegex(
        ValueError,
        re.escape('kd.slices.collapse: DataItem is not supported'),
    ):
      expr_eval.eval(kde.slices.collapse(x))

  @parameterized.parameters(-1, 2)
  def test_out_of_bounds_ndim_error(self, ndim):
    x = ds([1, 2, 3])
    with self.assertRaisesRegex(ValueError, 'expected 0 <= ndim <= rank'):
      expr_eval.eval(kde.slices.collapse(x, ndim))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.collapse,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.collapse(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.slices.collapse, kde.collapse))


if __name__ == '__main__':
  absltest.main()
