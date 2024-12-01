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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.exceptions import exceptions
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
])


class StringsEncodeBase64Test(parameterized.TestCase):

  @parameterized.parameters(
      (ds(None, schema_constants.BYTES), ds(None, schema_constants.STRING)),
      (ds(None, schema_constants.ANY), ds(None, schema_constants.STRING)),
      (ds(None, schema_constants.OBJECT), ds(None, schema_constants.STRING)),
      (ds(b''), ds('')),
      (ds(b'foo'), ds('Zm9v')),
      (ds(b'aaaa'), ds('YWFhYQ==')),
      (ds(b'foo', schema_constants.ANY), ds('Zm9v')),
      (ds(b'foo', schema_constants.OBJECT), ds('Zm9v')),
      (ds([None], schema_constants.BYTES), ds([None], schema_constants.STRING)),
      (ds([b'foo']), ds(['Zm9v'])),
      (ds([b'foo'], schema_constants.ANY), ds(['Zm9v'])),
      (ds([b'foo', b'bar'], schema_constants.ANY), ds(['Zm9v', 'YmFy'])),
  )
  def test_eval(self, x, expected):
    res = expr_eval.eval(kde.strings.encode_base64(x))
    testing.assert_equal(res, expected)

  def test_schema_error(self):
    with self.assertRaisesWithLiteralMatch(
        exceptions.KodaError,
        'kd.strings.encode_base64: argument `x` must be a slice of BYTES, got a'
        ' slice of STRING',
    ):
      _ = kde.strings.encode_base64(ds('a')).eval()

  def test_dtype_error(self):
    with self.assertRaisesWithLiteralMatch(
        exceptions.KodaError,
        'kd.strings.encode_base64: argument `x` must be a slice of BYTES, got a'
        ' slice of OBJECT with an item of type STRING',
    ):
      _ = kde.strings.encode_base64(ds('a', schema_constants.OBJECT)).eval()

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.encode_base64,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.encode_base64(I.x)))


if __name__ == '__main__':
  absltest.main()
