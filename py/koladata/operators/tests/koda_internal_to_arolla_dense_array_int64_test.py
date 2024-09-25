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

"""Tests for koda_internal.to_arolla_dense_array_int64."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
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
OBJECT = schema_constants.OBJECT
INT32 = schema_constants.INT32
INT64 = schema_constants.INT64


class KodaToArollaDenseArrayInt64Test(parameterized.TestCase):

  @parameterized.parameters(
      # Flat values.
      (ds([], INT32), arolla.dense_array_int64([])),
      (ds([1], INT32), arolla.dense_array_int64([1])),
      (ds([1], INT64), arolla.dense_array_int64([1])),
      (ds([None], INT32), arolla.dense_array_int64([None])),
      (ds([None], INT64), arolla.dense_array_int64([None])),
      (ds([1], INT32).as_any(), arolla.dense_array_int64([1])),
      (ds([1], INT64).as_any(), arolla.dense_array_int64([1])),
      (ds([1], INT32).with_schema(OBJECT), arolla.dense_array_int64([1])),
      (ds([1], INT64).with_schema(OBJECT), arolla.dense_array_int64([1])),
      # Empty and unknown.
      (ds([None]).with_schema(INT64), arolla.dense_array_int64([None])),
      (ds([None]), arolla.dense_array_int64([None])),
      (ds([None], OBJECT), arolla.dense_array_int64([None])),
      # Mixed.
      (ds([1, arolla.int64(2)], OBJECT), arolla.dense_array_int64([1, 2])),
      # Scalars.
      (ds(None, INT32), arolla.dense_array_int64([None])),
      (ds(1, INT32), arolla.dense_array_int64([1])),
      (ds(1, INT64), arolla.dense_array_int64([1])),
      (ds(1, INT32).as_any(), arolla.dense_array_int64([1])),
      (ds(1, INT64).as_any(), arolla.dense_array_int64([1])),
      (ds(1, INT32).with_schema(OBJECT), arolla.dense_array_int64([1])),
      (ds(1, INT64).with_schema(OBJECT), arolla.dense_array_int64([1])),
      (ds(None).with_schema(INT64), arolla.dense_array_int64([None])),
      # Multidim values.
      (ds([[], []], INT32), arolla.dense_array_int64([])),
      (ds([[1], [2]], INT32), arolla.dense_array_int64([1, 2])),
      (ds([[1], [2]], INT64), arolla.dense_array_int64([1, 2])),
      (ds([[None], [None]], INT32), arolla.dense_array_int64([None, None])),
      (ds([[None], [None]], INT64), arolla.dense_array_int64([None, None])),
      (
          ds([[1], [2]], INT32).as_any(),
          arolla.dense_array_int64([1, 2]),
      ),
      (
          ds([[1], [2]], INT64).as_any(),
          arolla.dense_array_int64([1, 2]),
      ),
      (
          ds([[1], [2]], INT32).with_schema(OBJECT),
          arolla.dense_array_int64([1, 2]),
      ),
      (
          ds([[1], [2]], INT64).with_schema(OBJECT),
          arolla.dense_array_int64([1, 2]),
      ),
      (
          ds([[None], [None]]).with_schema(INT64),
          arolla.dense_array_int64([None, None]),
      ),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(
        expr_eval.eval(arolla_bridge.to_arolla_dense_array_int64(I.x), x=x),
        expected,
    )

  def test_unsupported_schema_error(self):
    x = data_slice.DataSlice.from_vals([arolla.unit()])
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to INT64'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_int64(x))

  def test_unsupported_dtype_error(self):
    x = data_slice.DataSlice.from_vals([arolla.unit()], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to INT64'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_int64(x))

  def test_unsupported_entity(self):
    with self.assertRaisesRegex(exceptions.KodaError, 'common schema'):
      expr_eval.eval(
          arolla_bridge.to_arolla_dense_array_int64(bag().new(x=[1]))
      )

  def test_unsupported_object(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to INT64'
    ):
      expr_eval.eval(
          arolla_bridge.to_arolla_dense_array_int64(bag().obj(x=[1]))
      )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            arolla_bridge.to_arolla_dense_array_int64,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.DATA_SLICE, arolla.DENSE_ARRAY_INT64)]),
    )

  def test_view(self):
    self.assertTrue(
        view.has_basic_koda_view(arolla_bridge.to_arolla_dense_array_int64(I.x))
    )


if __name__ == '__main__':
  absltest.main()
