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

import re

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

present = arolla.present()
missing = arolla.missing()

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsContainsTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('foo'), ds('oo'), ds(present)),
      (ds('foo'), ds('zoo'), ds(missing)),
      (ds(b'foo'), ds(b'oo'), ds(present)),
      (ds(b'foo'), ds(b'zoo'), ds(missing)),
      (ds(['foo', 'zoo', 'bar']), ds('oo'), ds([present, present, missing])),
      (
          ds([b'foo', b'zoo', b'bar']),
          ds(b'oo'),
          ds([present, present, missing]),
      ),
      (
          ds([['foo', 'zoo'], ['bar']]),
          ds(['oo', 'ar']),
          ds([[present, present], [present]]),
      ),
      (
          ds([['foo', 'zoo'], ['bar']]),
          ds(['zo', 'lo']),
          ds([[missing, present], [missing]]),
      ),
      (ds('foo'), ds(None), ds(missing)),
      (
          ds('foo'),
          ds(None, schema_constants.OBJECT),
          ds(missing),
      ),
      (
          ds(['foo'], schema_constants.ANY),
          ds('foo'),
          ds([present]),
      ),
      # Empty and unknown.
      (ds([None, None]), ds([None, None]), ds([missing, missing])),
      (
          ds([None, None], schema_constants.STRING),
          ds(None, schema_constants.STRING),
          ds([missing, missing]),
      ),
      (
          ds([None, None], schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds([missing, missing]),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds([missing, missing]),
      ),
      (ds([None, None]), ds('abc'), ds([missing, missing])),
  )
  def test_eval(self, s, substr, expected):
    result = expr_eval.eval(kde.strings.contains(s, substr))
    testing.assert_equal(result, expected)

  def test_incompatible_types_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        # TODO: Make errors Koda friendly.
        re.escape('unsupported argument types (TEXT,BYTES)'),
    ):
      expr_eval.eval(kde.strings.contains(ds('foo'), ds(b'f')))

  def test_another_incompatible_types_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        # TODO: Make errors Koda friendly.
        re.escape('unsupported argument types (DENSE_ARRAY_TEXT,INT32)'),
    ):
      expr_eval.eval(
          kde.strings.contains(ds([None], schema_constants.STRING), ds(123))
      )

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError, 'DataSlice with mixed types is not supported'
    ):
      expr_eval.eval(kde.strings.contains(ds('foo'), ds([1, 'fo'])))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.contains,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.contains(I.x, I.y)))


if __name__ == '__main__':
  absltest.main()
