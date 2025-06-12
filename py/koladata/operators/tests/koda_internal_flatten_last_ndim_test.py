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

"""Tests for koda_internal.flatten_last_ndim."""

import itertools

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import jagged_shape as jagged_shape_ops
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import jagged_shape
from koladata.types import literal_operator
from koladata.types import qtypes


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
create_shape = jagged_shape.create_shape


class FlattenTest(parameterized.TestCase):

  def test_qtype_signatures(self):
    input_qtypes = itertools.product(
        [qtypes.JAGGED_SHAPE, qtypes.DATA_SLICE],
        [arolla.UNSPECIFIED, arolla.INT32, arolla.INT64, qtypes.DATA_SLICE],
    )
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            jagged_shape_ops.flatten_last_ndim,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        tuple((x, ndim, x) for x, ndim in input_qtypes),
    )

  @parameterized.parameters(
      (create_shape(), arolla.unspecified(), create_shape()),
      (create_shape(2, 3), arolla.unspecified(), create_shape(2, 3)),
      (create_shape(2, 3, 4), ds(1), create_shape(2, 3, 4)),
      (create_shape(2, 3, 4), ds(2), create_shape(2, 12)),
      (create_shape(2, 3, 4), ds(3), create_shape(24)),
      (create_shape(2, 3, 4), arolla.int32(2), create_shape(2, 12)),
      (create_shape(2, 3, 4), arolla.int64(2), create_shape(2, 12)),
      (
          ds([[[1, 2], [3]], [[4, 5]]]),
          arolla.unspecified(),
          ds([[[1, 2], [3]], [[4, 5]]]),
      ),
      (ds([[[1, 2], [3]], [[4, 5]]]), ds(2), ds([[1, 2, 3], [4, 5]])),
      (ds([[[1, 2], [3]], [[4, 5]]]), arolla.int32(2), ds([[1, 2, 3], [4, 5]])),
      (ds([[[1, 2], [3]], [[4, 5]]]), arolla.int64(2), ds([[1, 2, 3], [4, 5]])),
  )
  def test_eval(self, shape, ndim, expected):
    actual_value = expr_eval.eval(
        jagged_shape_ops.flatten_last_ndim(shape, ndim)
    )
    testing.assert_equal(actual_value, expected)

  def test_multidim_ndim_error(self):
    with self.assertRaisesRegex(ValueError, 'expected rank 0, but got rank=2'):
      expr_eval.eval(
          jagged_shape_ops.flatten_last_ndim(create_shape(2), ds([[1]]))
      )

  def test_non_int_ndim_error(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to INT64'
    ):
      expr_eval.eval(
          jagged_shape_ops.flatten_last_ndim(create_shape(2), ds(1.0))
      )

  @parameterized.parameters(-1, 2)
  def test_out_of_bounds_ndim_error(self, ndim):
    with self.assertRaisesRegex(ValueError, 'expected 0 <= ndim <= rank'):
      expr_eval.eval(jagged_shape_ops.flatten_last_ndim(create_shape(2), ndim))

  def test_boxing(self):
    testing.assert_equal(
        jagged_shape_ops.flatten_last_ndim(create_shape(), 1),
        arolla.abc.bind_op(
            jagged_shape_ops.flatten_last_ndim,
            literal_operator.literal(create_shape()),
            literal_operator.literal(ds(1)),
        ),
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(jagged_shape_ops.flatten_last_ndim(I.x, I.ndim))
    )


if __name__ == '__main__':
  absltest.main()
