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

"""Tests for koda_to_arolla_bool."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals
kde_internal = kde_operators.internal


class KodaToArollaBooleanTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(arolla.boolean(True)), arolla.boolean(True)),
      (ds(arolla.boolean(False)), arolla.boolean(False)),
      (ds(arolla.boolean(True), schema_constants.OBJECT), arolla.boolean(True)),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(
        expr_eval.eval(kde_internal.to_arolla_boolean(I.x), x=x), expected
    )

  def test_unsupported_schema_error(self):
    x = data_slice.DataSlice.from_vals(arolla.int64(1))
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to BOOLEAN'
    ):
      expr_eval.eval(kde_internal.to_arolla_boolean(x))

  def test_unsupported_dtype_error(self):
    x = data_slice.DataSlice.from_vals(arolla.unit(), schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to BOOLEAN'
    ):
      expr_eval.eval(kde_internal.to_arolla_boolean(x))

  def test_non_dataitem_error(self):
    x = data_slice.DataSlice.from_vals([True])
    with self.assertRaisesRegex(ValueError, 'expected rank 0, but got rank=1'):
      expr_eval.eval(kde_internal.to_arolla_boolean(x))

  def test_missing_value_error(self):
    x = data_slice.DataSlice.from_vals(arolla.optional_boolean(None))
    with self.assertRaisesRegex(ValueError, 'expected a present value'):
      expr_eval.eval(kde_internal.to_arolla_boolean(x))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde_internal.to_arolla_boolean,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.DATA_SLICE, arolla.BOOLEAN)]),
    )

  def test_view(self):
    self.assertFalse(view.has_koda_view(kde_internal.to_arolla_boolean(I.x)))


if __name__ == '__main__':
  absltest.main()
