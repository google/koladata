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

"""Tests for kde.schema.to_str.

Extensive testing is done in C++.
"""

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
from koladata.types import literal_operator
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


class SchemaToStrTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.BYTES), ds(None, schema_constants.STRING)),
      (ds(b'foo'), ds("b'foo'")),
      (ds('foo'), ds('foo')),
      (ds(1), ds('1')),
      (ds(1, schema_constants.INT64), ds('1')),
      (ds(1.5), ds('1.5')),
      (ds(1.5, schema_constants.FLOAT64), ds('1.5')),
      (ds('αβγ'.encode('utf-8')), ds("b'αβγ'")),
      (ds(b'foo', schema_constants.OBJECT), ds("b'foo'")),
      (ds([None], schema_constants.BYTES), ds([None], schema_constants.STRING)),
      (ds([b'foo']), ds(["b'foo'"])),
      (ds(['foo']), ds(['foo'])),
      (ds([1]), ds(['1'])),
      (ds([1], schema_constants.INT64), ds(['1'])),
      (ds([1.5]), ds(['1.5'])),
      (ds([1.5], schema_constants.FLOAT64), ds(['1.5'])),
      (ds([b'foo'], schema_constants.OBJECT), ds(["b'foo'"])),
      (ds([b'foo', 'bar'], schema_constants.OBJECT), ds(["b'foo'", 'bar'])),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.schema.to_str(x))
    testing.assert_equal(res, expected)

  @parameterized.parameters(
      ds(None, schema_constants.EXPR), ds(arolla.quote(I.x))
  )
  def test_not_castable_error(self, value):
    with self.assertRaisesRegex(
        ValueError,
        f'casting a DataSlice with schema {value.get_schema()} to STRING is not'
        ' supported',
    ):
      expr_eval.eval(kde.schema.to_str(value))

  def test_not_castable_internal_value(self):
    x = ds(arolla.quote(I.x), schema_constants.OBJECT)
    with self.assertRaisesRegex(
        ValueError, 'casting data of type EXPR to STRING is not supported'
    ):
      expr_eval.eval(kde.schema.to_str(x))

  def test_boxing(self):
    testing.assert_equal(
        kde.schema.to_str(b'foo'),
        arolla.abc.bind_op(
            kde.schema.to_str, literal_operator.literal(ds(b'foo'))
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.schema.to_str,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.to_str(I.x)))


if __name__ == '__main__':
  absltest.main()
