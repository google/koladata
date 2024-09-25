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

"""Tests for kde.assertion.assert_ds_has_primitives_of."""

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
    (DATA_SLICE, DATA_SLICE, arolla.TEXT, DATA_SLICE),
])


class AssertionAssertDtypeIsTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(1), schema_constants.INT32, 'unused'),
      (
          ds(None, dtype=schema_constants.INT32),
          schema_constants.INT32,
          'unused',
      ),
      (ds(1, dtype=schema_constants.INT64), schema_constants.INT64, 'unused'),
      (ds([1, 2, 3]), schema_constants.INT32, 'unused'),
      (
          ds([1, 2, 3], schema_constants.OBJECT),
          schema_constants.INT32,
          'unused',
      ),
      (ds([], schema_constants.OBJECT), schema_constants.INT32, 'unused'),
      (
          ds([None, None, None], schema_constants.OBJECT),
          schema_constants.INT32,
          'unused',
      ),
      (ds([1, 2, 3]).as_any(), schema_constants.INT32, 'unused'),
      (ds([]).as_any(), schema_constants.INT32, 'unused'),
      (ds([None, None, None]).as_any(), schema_constants.INT32, 'unused'),
  )
  def test_eval(self, x, dtype, message):
    result = expr_eval.eval(
        kde.assertion.assert_ds_has_primitives_of(x, dtype, message)
    )
    testing.assert_equal(result, x)

  @parameterized.parameters(
      (ds(1), schema_constants.INT64),
      (ds([1, 2, 3]), schema_constants.INT64),
      (
          ds([1, 2.0, 3], schema_constants.OBJECT),
          schema_constants.INT32,
      ),
      (ds([1, 2.0, 3]).as_any(), schema_constants.INT32),
      (bag().list(), schema_constants.INT32),
  )
  def test_assertion_fails(self, x, dtype):
    with self.assertRaisesRegex(
        ValueError,
        'assert_ds_has_primitives_of fails',
    ):
      expr_eval.eval(
          kde.assertion.assert_ds_has_primitives_of(
              x, dtype, 'assert_ds_has_primitives_of fails'
          )
      )

  def test_invalid_dtype(self):
    with self.assertRaisesRegex(
        ValueError,
        'primitive schema must contain a primitive DType',
    ):
      expr_eval.eval(
          kde.assertion.assert_ds_has_primitives_of(
              ds(1), schema_constants.OBJECT, 'unused'
          )
      )

    with self.assertRaisesRegex(
        ValueError,
        "primitive schema's schema must be SCHEMA",
    ):
      expr_eval.eval(
          kde.assertion.assert_ds_has_primitives_of(ds(1), ds(1), 'unused')
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.assertion.assert_ds_has_primitives_of,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_data_slice_view(
            kde.assertion.assert_ds_has_primitives_of(I.ds, I.dtype, I.message)
        )
    )


if __name__ == '__main__':
  absltest.main()
