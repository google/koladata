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
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class AssertionAssertPresentScalarTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1), schema_constants.INT32),
      (ds(1, schema=schema_constants.INT64), schema_constants.INT64),
      (ds(1), schema_constants.FLOAT32),
      (ds(1, schema_constants.OBJECT), schema_constants.FLOAT32),
  )
  def test_eval(self, x, dtype):
    result = expr_eval.eval(
        kde.assertion.assert_present_scalar('unused', x, dtype)
    )
    testing.assert_equal(result, x)

  @parameterized.parameters(
      (
          ds(None),
          schema_constants.INT32,
          'argument `foo` must be an item holding INT32, got missing',
      ),
      (
          ds(None, schema_constants.OBJECT),
          schema_constants.INT32,
          'argument `foo` must be an item holding INT32, got missing',
      ),
      (
          ds([1]),
          schema_constants.INT32,
          (
              'argument `foo` must be an item holding INT32, got a slice of'
              ' rank 1 > 0'
          ),
      ),
      (
          ds(1),
          schema_constants.MASK,
          'argument `foo` must be an item holding MASK, got an item of INT32',
      ),
  )
  def test_assertion_fails(self, x, dtype, message):
    with self.assertRaisesRegex(ValueError, re.escape(message)):  # pylint: disable=g-error-prone-assert-raises
      expr_eval.eval(kde.assertion.assert_present_scalar('foo', x, dtype))

  def test_invalid_dtype(self):
    with self.assertRaisesRegex(  # pylint: disable=g-error-prone-assert-raises
        ValueError,
        'primitive schema must contain a primitive DType',
    ):
      expr_eval.eval(
          kde.assertion.assert_present_scalar(
              'unused', ds(1), schema_constants.OBJECT
          )
      )

    with self.assertRaisesRegex(  # pylint: disable=g-error-prone-assert-raises
        ValueError,
        "primitive schema's schema must be SCHEMA",
    ):
      expr_eval.eval(
          kde.assertion.assert_present_scalar('unused', ds(1), ds(1))
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.assertion.assert_present_scalar,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.assertion.assert_present_scalar(I.ds, I.dtype, I.message)
        )
    )


if __name__ == '__main__':
  absltest.main()
