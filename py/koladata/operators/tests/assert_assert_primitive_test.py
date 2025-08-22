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
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty_mutable
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class AssertionAssertPrimitiveTest(parameterized.TestCase):

  @parameterized.parameters(
      ('unused', ds(1), schema_constants.INT32),
      (
          'unused',
          ds(None, schema=schema_constants.INT32),
          schema_constants.INT32,
      ),
      ('unused', ds(1, schema=schema_constants.INT64), schema_constants.INT64),
      ('unused', ds(1), schema_constants.FLOAT32),
      ('unused', ds([1, 2, 3]), schema_constants.INT32),
      (
          'unused',
          ds([1, 2, 3], schema_constants.OBJECT),
          schema_constants.INT32,
      ),
      ('unused', ds([], schema_constants.OBJECT), schema_constants.INT32),
      (
          'unused',
          ds([None, None, None], schema_constants.OBJECT),
          schema_constants.INT32,
      ),
      (
          'unused',
          ds([1.0, 2], schema_constants.OBJECT),
          schema_constants.FLOAT32,
      ),
      (
          'unused',
          ds([None, 2], schema_constants.OBJECT),
          schema_constants.FLOAT32,
      ),
      ('unused', ds(None, schema_constants.NONE), schema_constants.MASK),
      ('unused', ds(None, schema_constants.OBJECT), schema_constants.MASK),
  )
  def test_eval(self, arg_name, x, dtype):
    result = expr_eval.eval(
        kde.assertion.assert_primitive(arg_name, x, dtype)
    )
    testing.assert_equal(result, x)

  @parameterized.parameters(
      (ds(1), schema_constants.MASK),
      (ds([1, 2, 3], schema_constants.INT64), schema_constants.INT32),
      (
          ds([1, 2.0, 3], schema_constants.OBJECT),
          schema_constants.INT32,
      ),
      (bag().list(), schema_constants.INT32),
  )
  def test_assertion_fails(self, x, dtype):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            f'argument `foo` must be a slice of {dtype}, got a slice of'
            f' {x.get_schema()}'
        ),
    ):
      expr_eval.eval(kde.assertion.assert_primitive('foo', x, dtype))

  def test_invalid_dtype(self):
    with self.assertRaisesRegex(
        ValueError,
        'primitive schema must contain a primitive DType',
    ):
      expr_eval.eval(
          kde.assertion.assert_primitive(
              'unused', ds(1), schema_constants.OBJECT
          )
      )

    with self.assertRaisesRegex(
        ValueError,
        "primitive schema's schema must be SCHEMA",
    ):
      expr_eval.eval(
          kde.assertion.assert_primitive('unused', ds(1), ds(1))
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.assertion.assert_primitive,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(
        view.has_koda_view(
            kde.assertion.assert_primitive(I.ds, I.dtype, I.message)
        )
    )


if __name__ == '__main__':
  absltest.main()
