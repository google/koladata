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

"""Tests for koda_internal.to_arolla_dense_array_text."""

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
STRING = schema_constants.STRING


class KodaToArollaDenseArrayTextTest(parameterized.TestCase):

  @parameterized.parameters(
      # Flat values.
      (ds([], STRING), arolla.dense_array_text([])),
      (ds([None]), arolla.dense_array_text([None])),
      (ds([None], STRING), arolla.dense_array_text([None])),
      (ds([None], OBJECT), arolla.dense_array_text([None])),
      (ds(['foo']), arolla.dense_array_text(['foo'])),
      (ds(['foo'], OBJECT), arolla.dense_array_text(['foo'])),
      # Scalars.
      (ds(None, STRING), arolla.dense_array_text([None])),
      (ds(None, OBJECT), arolla.dense_array_text([None])),
      (ds('foo'), arolla.dense_array_text(['foo'])),
      (ds('foo', OBJECT), arolla.dense_array_text(['foo'])),
      # Multidim values.
      (ds([[], []], STRING), arolla.dense_array_text([])),
      (ds([['foo'], [None]]), arolla.dense_array_text(['foo', None])),
      (ds([[None], [None]], STRING), arolla.dense_array_text([None, None])),
      (ds([['foo'], [None]], OBJECT), arolla.dense_array_text(['foo', None])),
      (ds([[None], [None]], OBJECT), arolla.dense_array_text([None, None])),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(
        expr_eval.eval(arolla_bridge.to_arolla_dense_array_text(I.x), x=x),
        expected,
    )

  def test_unsupported_schema_error(self):
    x = data_slice.DataSlice.from_vals([1])
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to STRING'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_text(x))

  def test_unsupported_dtype_error(self):
    x = data_slice.DataSlice.from_vals([1], schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'unsupported narrowing cast to STRING'
    ):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_text(x))

  def test_unsupported_entity(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_text(bag().new(x=[1])))

  def test_unsupported_object(self):
    with self.assertRaisesRegex(ValueError, 'common schema'):
      expr_eval.eval(arolla_bridge.to_arolla_dense_array_text(bag().obj(x=[1])))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            arolla_bridge.to_arolla_dense_array_text,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((qtypes.DATA_SLICE, arolla.DENSE_ARRAY_TEXT),),
    )

  def test_view(self):
    self.assertFalse(
        view.has_koda_view(arolla_bridge.to_arolla_dense_array_text(I.x))
    )


if __name__ == '__main__':
  absltest.main()
