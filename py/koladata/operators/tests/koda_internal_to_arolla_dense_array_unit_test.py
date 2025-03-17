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

"""Tests for koda_internal.to_arolla_dense_array_unit."""

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
OBJECT = schema_constants.OBJECT
MASK = schema_constants.MASK


class KodaToArollaDenseArrayUnitTest(parameterized.TestCase):

  @parameterized.parameters(
      # Flat values.
      (ds([], MASK), arolla.dense_array_unit([])),
      (ds([arolla.unit()], MASK), arolla.dense_array_unit([True])),
      (ds([None]), arolla.dense_array_unit([None])),
      (ds([None], MASK), arolla.dense_array_unit([None])),
      (
          ds([arolla.unit()], MASK).with_schema(OBJECT),
          arolla.dense_array_unit([True]),
      ),
      # Empty and unknown.
      (ds([None]).with_schema(MASK), arolla.dense_array_unit([None])),
      # Scalars.
      (ds(None, MASK), arolla.dense_array_unit([None])),
      (ds(arolla.unit(), MASK), arolla.dense_array_unit([True])),
      (
          ds(arolla.unit(), MASK).with_schema(OBJECT),
          arolla.dense_array_unit([True]),
      ),
      (ds(None).with_schema(MASK), arolla.dense_array_unit([None])),
      # Multidim values.
      (ds([[], []], MASK), arolla.dense_array_unit([])),
      (
          ds([[arolla.unit()], [None]], MASK),
          arolla.dense_array_unit([True, None]),
      ),
      (ds([[None], [None]], MASK), arolla.dense_array_unit([None, None])),
      (
          ds([[arolla.unit()], [None]], MASK).with_schema(OBJECT),
          arolla.dense_array_unit([True, None]),
      ),
      (
          ds([[None], [None]]).with_schema(MASK),
          arolla.dense_array_unit([None, None]),
      ),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(
        expr_eval.eval(arolla_bridge.to_arolla_dense_array_unit(I.x), x=x),
        expected,
    )

  def test_unsupported_schema_error(self):
    x = data_slice.DataSlice.from_vals([1])
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to MASK'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_unit(x))

  def test_unsupported_dtype_error(self):
    x = data_slice.DataSlice.from_vals([1], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to MASK'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_unit(x))

  def test_unsupported_entity(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_unit(bag().new(x=[1])))

  def test_unsupported_object(self):
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to MASK'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_unit(bag().obj(x=[1])))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            arolla_bridge.to_arolla_dense_array_unit,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        frozenset([(qtypes.DATA_SLICE, arolla.DENSE_ARRAY_UNIT)]),
    )

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(arolla_bridge.to_arolla_dense_array_unit(I.x))
    )


if __name__ == '__main__':
  absltest.main()
