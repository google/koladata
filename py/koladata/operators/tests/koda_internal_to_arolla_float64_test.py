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

"""Tests for koda_to_arolla_float64."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import arolla_bridge
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class KodaToArollaFloat64Test(parameterized.TestCase):

  @parameterized.parameters(
      (ds(arolla.int32(1)), arolla.float64(1.0)),
      (ds(arolla.int64(1)), arolla.float64(1.0)),
      (ds(arolla.float32(1.0)), arolla.float64(1.0)),
      (ds(arolla.float64(1.0)), arolla.float64(1.0)),
      (ds(arolla.float32(1.0), schema_constants.OBJECT), arolla.float64(1.0)),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(
        expr_eval.eval(arolla_bridge.to_arolla_float64(I.x), x=x), expected
    )

  def test_non_dataitem_error(self):
    x = ds([1])
    with self.assertRaisesRegex(ValueError, 'expected rank 0, but got rank=1'):
      expr_eval.eval(arolla_bridge.to_arolla_float64(x))

  def test_unsupported_schema_error(self):
    x = ds(arolla.unit())
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to FLOAT64'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_float64(x))

  def test_unsupported_dtype_error(self):
    x = ds(arolla.unit(), schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to FLOAT64'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_float64(x))

  def test_missing_value_error(self):
    x = ds(arolla.optional_float64(None))
    with self.assertRaisesRegex(ValueError, 'expected a present value'):
      expr_eval.eval(arolla_bridge.to_arolla_float64(x))

  def test_unsupported_entity(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      expr_eval.eval(arolla_bridge.to_arolla_float64(bag().new(x=1.0)))

  def test_unsupported_object(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      expr_eval.eval(arolla_bridge.to_arolla_float64(bag().obj(x=1.0)))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            arolla_bridge.to_arolla_float64,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.DATA_SLICE, arolla.FLOAT64)]),
    )

  def test_view(self):
    self.assertFalse(view.has_koda_view(arolla_bridge.to_arolla_float64(I.x)))


if __name__ == '__main__':
  absltest.main()
