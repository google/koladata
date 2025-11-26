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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import literal_operator
from koladata.types import schema_constants


I = input_container.InputContainer('I')
bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kde_internal = kde_operators.internal
kd_internal = eager_op_utils.operators_container(
    top_level_arolla_container=kde_internal
)


class KodaToArollaTextTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('abc'), arolla.text('abc')),
      (ds('abc', schema_constants.OBJECT), arolla.text('abc')),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(kd_internal.to_arolla_text(x), expected)

  def test_non_dataitem_error(self):
    with self.assertRaisesRegex(ValueError, 'expected rank 0, but got rank=1'):
      kd_internal.to_arolla_text(ds(['abc']))

  def test_boxing(self):
    testing.assert_equal(
        kde_internal.to_arolla_text('abc'),
        arolla.abc.bind_op(
            kde_internal.to_arolla_text, literal_operator.literal(ds('abc'))
        ),
    )

  def test_bind_time_evaluation(self):
    # Allows it to be used in operators that require literal inputs.
    expr = kde_internal.to_arolla_text(ds('abc'))
    testing.assert_equal(expr.qvalue, arolla.text('abc'))

  def test_unsupported_schema_error(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to STRING'
    ):
      kd_internal.to_arolla_text(ds(b'abc'))

  def test_unsupported_dtype_error(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to STRING'
    ):
      kd_internal.to_arolla_text(ds(b'abc', schema_constants.OBJECT))

  def test_missing_value_error(self):
    with self.assertRaisesRegex(ValueError, 'expected a present value'):
      kd_internal.to_arolla_text(ds(None, schema_constants.STRING))

  def test_unsupported_entity(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      kd_internal.to_arolla_text(bag().new(x='abc'))

  def test_unsupported_object(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      kd_internal.to_arolla_text(bag().obj(x='abc'))

  def test_non_data_slice_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got x: BYTES'
    ):
      kd_internal.to_arolla_text(arolla.bytes(b'foobar'))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde_internal.to_arolla_text,
        [(qtypes.DATA_SLICE, arolla.TEXT)],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertFalse(view.has_koda_view(kde_internal.to_arolla_text(I.x)))


if __name__ == '__main__':
  absltest.main()
