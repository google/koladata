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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, arolla.UNSPECIFIED, DATA_SLICE),
])


class SlicesInverseMappingTest(parameterized.TestCase):

  @parameterized.parameters(
      # x.ndim = 0
      (ds(0), 0, ds(0)),
      # x.ndim = 1
      (ds([0, 0, None, 0]), 0, ds([0, 0, None, 0])),
      (ds([0, 2, None, 1]), 1, ds([0, 3, 1, None])),
      # x.ndim = 2
      (ds([[0, None, 0], [None, 0]]), 0, ds([[0, None, 0], [None, 0]])),
      (ds([[1, 2, 0], [1, None]]), 1, ds([[2, 0, 1], [None, 0]])),
      (ds([[1, 2, 0], [3, None]]), 2, ds([[2, 0, 1], [3, None]])),
      # descending and ndim as DataItems
      (ds([0, 2, None, 1]), ds(1), ds([0, 3, 1, None])),
      # OBJECT
      (
          ds([0, 2, None, 1], schema_constants.OBJECT),
          1,
          ds([0, 3, 1, None], schema_constants.OBJECT),
      ),
      # INT64
      (
          ds([0, 2, None, 1], schema_constants.INT64),
          1,
          ds([0, 3, 1, None], schema_constants.INT64),
      ),
      # empty x
      (ds([], schema_constants.INT64), 1, ds([], schema_constants.INT64)),
      # Empty and unknown inputs.
      (
          ds([[None, None], [None]], schema_constants.OBJECT),
          1,
          ds([[None, None], [None]], schema_constants.OBJECT),
      ),
      (
          ds([[None, None], [None]]),
          1,
          ds([[None, None], [None]]),
      ),
      (
          ds([[None, None], [None]], schema_constants.INT64),
          1,
          ds([[None, None], [None]], schema_constants.INT64),
      ),
  )
  def test_eval(self, x, ndim, expected):
    result = expr_eval.eval(kde.slices.inverse_mapping(x, ndim=ndim))
    testing.assert_equal(result, expected)

  @parameterized.parameters(
      # x.ndim = 1
      (ds([0, 2, None, 1]), ds([0, 3, 1, None])),
      # x.ndim = 2
      (ds([[1, 2, 0], [1, None]]), ds([[2, 0, 1], [None, 0]])),
      (ds([None, None, None]), ds([None, None, None])),
      (
          ds([None, None, None], schema_constants.INT64),
          ds([None, None, None], schema_constants.INT64),
      ),
  )
  def test_eval_with_ndim_unspecified(self, x, expected):
    result = expr_eval.eval(kde.slices.inverse_mapping(x))
    testing.assert_equal(result, expected)

  def test_out_of_bounds_ndim_error(self):
    x = ds([0, 3, 6])
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 0 <= ndim <= rank')
    ):
      expr_eval.eval(kde.slices.inverse_mapping(x, ndim=-1))
    with self.assertRaisesRegex(
        ValueError, re.escape('expected 0 <= ndim <= rank')
    ):
      expr_eval.eval(kde.slices.inverse_mapping(x, ndim=2))

  def test_invalid_permutation_error(self):
    with self.assertRaisesRegex(ValueError, 'invalid permutation'):
      expr_eval.eval(kde.slices.inverse_mapping(ds([1, 3, 0])))

    with self.assertRaisesRegex(ValueError, 'invalid permutation'):
      expr_eval.eval(
          kde.slices.inverse_mapping(ds([[1, 2, 0], [1, None]]), ndim=2)
      )

  def test_invalid_type_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.slices.inverse_mapping: argument `x` must be a slice of integer'
            ' values, got a slice of FLOAT32'
        ),
    ):
      expr_eval.eval(
          kde.slices.inverse_mapping(ds([0.0, 2.0, None, 1.0]), ndim=1)
      )

  def test_qtype_signatures(self):
    # Limit the allowed qtypes and a random QType to speed up the test.
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.slices.inverse_mapping,
            possible_qtypes=(
                arolla.UNSPECIFIED,
                qtypes.DATA_SLICE,
                arolla.INT64,
            ),
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.slices.inverse_mapping(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.slices.inverse_mapping, kde.inverse_mapping)
    )


if __name__ == '__main__':
  absltest.main()
