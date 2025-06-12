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
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals


class KodaToArollaTextTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('abc'), arolla.text('abc')),
      (ds('abc', schema_constants.OBJECT), arolla.text('abc')),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(
        expr_eval.eval(arolla_bridge.to_arolla_text(I.x), x=x), expected
    )

  def test_non_dataitem_error(self):
    with self.assertRaisesRegex(ValueError, 'expected rank 0, but got rank=1'):
      expr_eval.eval(arolla_bridge.to_arolla_text(ds(['abc'])))

  def test_boxing(self):
    testing.assert_equal(
        arolla_bridge.to_arolla_text('abc'),
        arolla.abc.bind_op(
            arolla_bridge.to_arolla_text,
            literal_operator.literal(ds('abc')),
        ),
    )

  def test_bind_time_evaluation(self):
    # Allows it to be used in operators that require literal inputs.
    expr = arolla_bridge.to_arolla_text(ds('abc'))
    testing.assert_equal(expr.qvalue, arolla.text('abc'))

  def test_unsupported_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to STRING'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_text(ds(b'abc')))

  def test_unsupported_dtype_error(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to STRING'
    ):
      expr_eval.eval(
          arolla_bridge.to_arolla_text(ds(b'abc', schema_constants.OBJECT))
      )

  def test_missing_value_error(self):
    with self.assertRaisesRegex(ValueError, 'expected a present value'):
      expr_eval.eval(
          arolla_bridge.to_arolla_text(ds(None, schema_constants.STRING))
      )

  def test_unsupported_entity(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      expr_eval.eval(arolla_bridge.to_arolla_text(bag().new(x='abc')))

  def test_unsupported_object(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      expr_eval.eval(arolla_bridge.to_arolla_text(bag().obj(x='abc')))

  def test_non_data_slice_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got x: BYTES'
    ):
      arolla.eval(arolla_bridge.to_arolla_text(arolla.bytes(b'foobar')))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            arolla_bridge.to_arolla_text,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.DATA_SLICE, arolla.TEXT)]),
    )

  def test_view(self):
    self.assertFalse(view.has_koda_view(arolla_bridge.to_arolla_text(I.x)))


if __name__ == '__main__':
  absltest.main()
