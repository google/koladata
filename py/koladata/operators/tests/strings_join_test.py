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

import re

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
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


# Signatures of 'kd.strings.join' at a maximum arity of 3:
QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsJoinTest(parameterized.TestCase):

  @parameterized.parameters(
      ([ds('foo')], ds('foo')),
      ([ds(b'foo')], ds(b'foo')),
      ([ds('foo'), ds(' bar')], ds('foo bar')),
      ([ds(b'foo'), ds(b' bar')], ds(b'foo bar')),
      ([ds(['a', 'b']), ds([' c', ' d'])], ds(['a c', 'b d'])),
      ([ds([b'a', b'b']), ds([b' c', b' d'])], ds([b'a c', b'b d'])),
      ([ds(['a', 'b']), ds(' c'), ds([' d', ' e'])], ds(['a c d', 'b c e'])),
      (
          [ds('a'), ds(['b', 'c']), ds([['d', 'e'], ['f']])],
          ds([['abd', 'abe'], ['acf']]),
      ),
      ([ds('foo'), ds(None)], ds(None, schema_constants.STRING)),
      (
          [ds('foo'), ds(None, schema_constants.OBJECT)],
          ds(None, schema_constants.OBJECT),
      ),
      (
          [ds(['foo'], schema_constants.OBJECT), ds('bar')],
          ds(['foobar'], schema_constants.OBJECT),
      ),
      # Empty and unknown.
      ([ds([None, None])], ds([None, None])),
      (
          [ds([None, None], schema_constants.STRING)],
          ds([None, None], schema_constants.STRING),
      ),
      (
          [ds([None, None], schema_constants.BYTES)],
          ds([None, None], schema_constants.BYTES),
      ),
      (
          [ds([None, None], schema_constants.OBJECT)],
          ds([None, None], schema_constants.OBJECT),
      ),
      (
          [ds([None, None]), ds('abc')],
          ds([None, None], schema_constants.STRING),
      ),
  )
  def test_eval(self, args, expected):
    result = expr_eval.eval(kde.strings.join(*args))
    testing.assert_equal(result, expected)

  def test_no_operands_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected at least one argument'
        ),
    ):
      expr_eval.eval(kde.strings.join())

  def test_incompatible_types_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.strings.join: mixing STRING and BYTES arguments is not allowed,'
            ' but `slices[0]` contains STRING and `slices[1]` contains BYTES'
        ),
    ):
      expr_eval.eval(kde.strings.join(ds('foo'), ds(b' bytes')))

  def test_another_incompatible_types_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.strings.join: argument `slices[1]` must be a slice of either'
            ' STRING or BYTES, got a slice of INT32'
        ),
    ):
      expr_eval.eval(kde.strings.join(ds([None]), ds(123)))

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'kd.strings.join: argument `slices[1]` must be a slice of either'
            ' STRING or BYTES, got a slice of OBJECT containing INT32 and'
            ' STRING values'
        ),
    ):
      expr_eval.eval(kde.strings.join(ds('foo '), ds([1, 'bar'])))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.join,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        max_arity=3,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.join(I.x)))


if __name__ == '__main__':
  absltest.main()
