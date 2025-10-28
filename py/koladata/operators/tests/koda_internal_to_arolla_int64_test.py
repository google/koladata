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
koda_internal = kde_operators.internal
# TODO: b/454825280 - Disambiguate kd_internal from koda_internal, here and in
# all other tests.
kd_internal = eager_op_utils.operators_container(
    'koda_internal',
    top_level_arolla_container=arolla.unsafe_operators_container(),
)


class KodaToArollaInt64Test(parameterized.TestCase):

  @parameterized.parameters(
      (data_slice.DataSlice.from_vals(arolla.int32(1)), arolla.int64(1)),
      (data_slice.DataSlice.from_vals(arolla.int64(1)), arolla.int64(1)),
      (
          data_slice.DataSlice.from_vals(
              arolla.int32(1), schema_constants.OBJECT
          ),
          arolla.int64(1),
      ),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(kd_internal.to_arolla_int64(x), expected)

  def test_non_dataitem_error(self):
    x = data_slice.DataSlice.from_vals([1])
    with self.assertRaisesRegex(ValueError, 'expected rank 0, but got rank=1'):
      kd_internal.to_arolla_int64(x)

  def test_boxing(self):
    testing.assert_equal(
        koda_internal.to_arolla_int64(1),
        arolla.abc.bind_op(
            koda_internal.to_arolla_int64, literal_operator.literal(ds(1))
        ),
    )

  def test_bind_time_evaluation(self):
    # Allows it to be used in operators that require literal inputs.
    expr = koda_internal.to_arolla_int64(ds(1))
    testing.assert_equal(expr.qvalue, arolla.int64(1))

  def test_unsupported_schema_error(self):
    x = data_slice.DataSlice.from_vals(arolla.unit())
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to INT64'
    ):
      koda_internal.to_arolla_int64(x)

  def test_unsupported_dtype_error(self):
    x = data_slice.DataSlice.from_vals(arolla.unit(), schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to INT64'
    ):
      koda_internal.to_arolla_int64(x)

  def test_missing_value_error(self):
    x = data_slice.DataSlice.from_vals(arolla.optional_int64(None))
    with self.assertRaisesRegex(ValueError, 'expected a present value'):
      koda_internal.to_arolla_int64(x)

  def test_unsupported_entity(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      kd_internal.to_arolla_int64(bag().new(x=1))

  def test_unsupported_object(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      kd_internal.to_arolla_int64(bag().obj(x=1))

  def test_non_data_slice_error(self):
    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got x: BYTES'
    ):
      kd_internal.to_arolla_int64(arolla.bytes(b'foobar'))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        koda_internal.to_arolla_int64,
        [(qtypes.DATA_SLICE, arolla.INT64)],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertFalse(view.has_koda_view(koda_internal.to_arolla_int64(I.x)))


if __name__ == '__main__':
  absltest.main()
