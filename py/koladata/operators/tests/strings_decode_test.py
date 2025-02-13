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

"""Tests for kde.strings.decode.

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


class StringsDecodeTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.BYTES), ds(None, schema_constants.STRING)),
      (ds(b'foo'), ds('foo')),
      (ds('foo'), ds('foo')),
      (ds('αβγ'.encode('utf-8')), ds('αβγ')),
      (ds(b'foo', schema_constants.OBJECT), ds('foo')),
      (ds([None], schema_constants.BYTES), ds([None], schema_constants.STRING)),
      (ds([b'foo']), ds(['foo'])),
      (ds(['foo']), ds(['foo'])),
      (ds([b'foo'], schema_constants.OBJECT), ds(['foo'])),
      (ds([b'foo', 'bar'], schema_constants.OBJECT), ds(['foo', 'bar'])),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.strings.decode(x))
    testing.assert_equal(res, expected)

  def test_invalid_utf8_error(self):
    x = ds(b'\xff')
    with self.assertRaisesRegex(ValueError, 'invalid UTF-8'):
      expr_eval.eval(kde.strings.decode(x))

  @parameterized.parameters(
      ds(None, schema_constants.INT32), ds(1), ds(arolla.present())
  )
  def test_not_castable_error(self, value):
    with self.assertRaisesRegex(
        ValueError, f'unsupported schema: {value.get_schema()}'
    ):
      expr_eval.eval(kde.strings.decode(value))

  def test_not_castable_internal_value(self):
    x = ds(1, schema_constants.OBJECT)
    with self.assertRaisesRegex(ValueError, 'cannot cast INT32 to STRING'):
      expr_eval.eval(kde.strings.decode(x))

  def test_boxing(self):
    testing.assert_equal(
        kde.strings.decode(b'foo'),
        arolla.abc.bind_op(
            kde.strings.decode, literal_operator.literal(ds(b'foo'))
        ),
    )

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.strings.decode,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        ((DATA_SLICE, DATA_SLICE),),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.decode(I.x)))


if __name__ == '__main__':
  absltest.main()
