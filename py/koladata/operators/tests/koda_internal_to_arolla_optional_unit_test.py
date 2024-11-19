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

"""Tests for koda_to_arolla_optional_unit."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import arolla_bridge
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
ds = data_slice.DataSlice.from_vals


class KodaToArollaOptionalUnitTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(mask_constants.present), arolla.present()),
      (ds(mask_constants.missing), arolla.missing()),
      (ds(mask_constants.present, schema_constants.ANY), arolla.present()),
      (ds(mask_constants.present, schema_constants.OBJECT), arolla.present()),
      (ds(None), arolla.missing()),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(
        expr_eval.eval(arolla_bridge.to_arolla_optional_unit(I.x), x=x),
        expected,
    )

  def test_unsupported_schema_error(self):
    x = data_slice.DataSlice.from_vals(arolla.int64(1))
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to MASK'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_optional_unit(x))

  def test_unsupported_dtype_error(self):
    x = data_slice.DataSlice.from_vals(True, schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to MASK'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_optional_unit(x))

  def test_non_dataitem_error(self):
    x = data_slice.DataSlice.from_vals([True])
    with self.assertRaisesRegex(ValueError, 'expected rank 0, but got rank=1'):
      expr_eval.eval(arolla_bridge.to_arolla_optional_unit(x))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            arolla_bridge.to_arolla_optional_unit,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.DATA_SLICE, arolla.OPTIONAL_UNIT)]),
    )

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(arolla_bridge.to_arolla_optional_unit(I.x))
    )


if __name__ == '__main__':
  absltest.main()
