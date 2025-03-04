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

"""Tests for kde.schema.to_float64.

Extensive testing is done in C++.
"""

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
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer("I")
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class SchemaToFloat64Test(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.OBJECT), ds(None, schema_constants.FLOAT64)),
      (ds(1), ds(1.0, schema_constants.FLOAT64)),
      (ds(1, schema_constants.INT64), ds(1.0, schema_constants.FLOAT64)),
      (ds(1.5), ds(1.5, schema_constants.FLOAT64)),
      (ds(1.5, schema_constants.OBJECT), ds(1.5, schema_constants.FLOAT64)),
      (ds(True), ds(1.0, schema_constants.FLOAT64)),
      (ds("1"), ds(1.0, schema_constants.FLOAT64)),
      (ds(b"1"), ds(1.0, schema_constants.FLOAT64)),
      (
          ds([None], schema_constants.OBJECT),
          ds([None], schema_constants.FLOAT64),
      ),
      (ds([1]), ds([1.0], schema_constants.FLOAT64)),
      (ds([1], schema_constants.INT64), ds([1.0], schema_constants.FLOAT64)),
      (ds([1.5]), ds([1.5], schema_constants.FLOAT64)),
      (
          ds([1, 2.5], schema_constants.OBJECT),
          ds([1.0, 2.5], schema_constants.FLOAT64),
      ),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.schema.to_float64(x))
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      ds(arolla.quote(arolla.L.x)), ds(arolla.present())
  )
  def test_not_castable_error(self, value):
    with self.assertRaisesRegex(
        ValueError, f"unsupported schema: {value.get_schema()}"
    ):
      expr_eval.eval(kde.schema.to_float64(value))

  def test_not_castable_internal_value(self):
    x = ds(arolla.present(), schema_constants.OBJECT)
    with self.assertRaisesRegex(ValueError, "cannot cast MASK to FLOAT64"):
      expr_eval.eval(kde.schema.to_float64(x))

  def test_not_parseable_error(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("unable to parse FLOAT64: 'foo'")
    ):
      expr_eval.eval(kde.schema.to_float64(ds("foo")))

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.to_float64(1),
        arolla.abc.bind_op(
            kde.schema.to_float64, literal_operator.literal(ds(1))
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.to_float64,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.to_float64(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.schema.to_float64, kde.to_float64))


if __name__ == "__main__":
  absltest.main()
