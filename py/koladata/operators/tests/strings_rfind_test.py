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
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class StringsRfindTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds('foo'), ds('oo'), ds(1, schema_constants.INT64)),
      (ds('foo'), ds('zoo'), ds(None, schema_constants.INT64)),
      (ds(b'foo'), ds(b'oo'), ds(1, schema_constants.INT64)),
      (ds(b'foo'), ds(b'zoo'), ds(None, schema_constants.INT64)),
      (
          ds(['foo', 'zoo', 'bar']),
          ds('oo'),
          ds([1, 1, None], schema_constants.INT64),
      ),
      (
          ds([b'foo', b'zoo', b'bar']),
          ds(b'oo'),
          ds([1, 1, None], schema_constants.INT64),
      ),
      (
          ds([['foo', 'bzoo'], ['bari']]),
          ds(['oo', 'i']),
          ds([[1, 2], [3]], schema_constants.INT64),
      ),
      (
          ds('foo'),
          ds(None, schema_constants.STRING),
          ds(None, schema_constants.INT64),
      ),
      (
          ds(['foo'], schema_constants.OBJECT),
          ds('foo'),
          ds([0], schema_constants.INT64),
      ),
      # Empty and unknown.
      (
          ds([None, None]),
          ds([None, None]),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None], schema_constants.STRING),
          ds(None, schema_constants.STRING),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None], schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds([None, None], schema_constants.INT64),
      ),
      (ds([None, None]), ds('abc'), ds([None, None], schema_constants.INT64)),
  )
  def test_two_args(self, s, substr, expected):
    result = expr_eval.eval(kde.strings.rfind(s, substr))
    testing.assert_equal(result, expected)

  def test_eval_two_args_wrong_types(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.rfind: argument `substr` must be a slice of either'
            ' STRING or BYTES, got a slice of INT32'
        ),
    ):
      expr_eval.eval(kde.strings.rfind(None, 123))

    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.rfind: mixing STRING and BYTES arguments is not'
            ' allowed, but `x` contains STRING and `substr` contains BYTES'
        ),
    ):
      expr_eval.eval(kde.strings.rfind('a', b'a'))

  @parameterized.parameters(
      (ds('fooaaoo'), ds('oo'), ds(0), ds(5, schema_constants.INT64)),
      (ds('fooaaoo'), ds('oo'), ds(3), ds(5, schema_constants.INT64)),
      (ds('fooaa'), ds('oo'), ds(3), ds(None, schema_constants.INT64)),
      (ds(b'fooaaoo'), ds(b'oo'), ds(3), ds(5, schema_constants.INT64)),
      (
          ds(['fooaaoo', 'zoo', 'bar']),
          ds('oo'),
          ds(1),
          ds([5, 1, None], schema_constants.INT64),
      ),
      (
          ds(['fooaaoo', 'zoo', 'bar']),
          ds('oo'),
          ds(2),
          ds([5, None, None], schema_constants.INT64),
      ),
      (
          ds(['fooaaoo', 'zoo', 'bar']),
          ds('oo'),
          ds(-3),
          ds([5, 1, None], schema_constants.INT64),
      ),
      (
          ds(['fooaaoo', 'zoo', 'bar']),
          ds('oo'),
          ds([2, 1, 0]),
          ds([5, 1, None], schema_constants.INT64),
      ),
      (
          ds([['foo', 'bzboo'], ['barioooooi']]),
          ds(['oo', 'i']),
          ds([2, 4]),
          ds([[None, 3], [9]], schema_constants.INT64),
      ),
      (
          ds('foo'),
          ds('o', schema_constants.STRING),
          ds(None),
          ds(2, schema_constants.INT64),
      ),
      (
          ds(['foo'], schema_constants.OBJECT),
          ds('foo'),
          ds(1),
          ds([None], schema_constants.INT64),
      ),
      # Empty and unknown.
      (
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None], schema_constants.STRING),
          ds(None, schema_constants.STRING),
          ds(None, schema_constants.INT64),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None], schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds(None, schema_constants.INT64),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None]),
          ds('abc'),
          ds(1),
          ds([None, None], schema_constants.INT64),
      ),
  )
  def test_three_args(self, s, substr, start, expected):
    result = expr_eval.eval(kde.strings.rfind(s, substr, start))
    testing.assert_equal(result, expected)

  def test_three_args_wrong_types(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.rfind: argument `start` must be a slice of integer'
            ' values, got a slice of STRING'
        ),
    ):
      expr_eval.eval(kde.strings.rfind(None, None, 'foo'))

  @parameterized.parameters(
      (ds('fooaaoo'), ds('oo'), ds(0), ds(3), ds(1, schema_constants.INT64)),
      (ds('fooaaoo'), ds('oo'), ds(0), ds(2), ds(None, schema_constants.INT64)),
      (ds('fooaaoo'), ds('oo'), ds(0), ds(None), ds(5, schema_constants.INT64)),
      (ds('fooaaoo'), ds('oo'), ds(0), ds(-2), ds(1, schema_constants.INT64)),
      (ds('fooaaoo'), ds('oo'), ds(0), ds(1), ds(None, schema_constants.INT64)),
      (
          ds('fooaaoo'),
          ds('oo'),
          ds(0),
          ds(-5),
          ds(None, schema_constants.INT64),
      ),
      (
          ds(['fooaaoo', 'zbboo', 'oobar']),
          ds('oo'),
          ds(1),
          ds(3),
          ds([1, None, None], schema_constants.INT64),
      ),
      (
          ds([['foo', 'bzboo', 'bzbooa'], ['barioooooi']]),
          ds(['oo', 'i']),
          ds([2, 4]),
          ds([-1, None]),
          ds([[None, None, 3], [9]], schema_constants.INT64),
      ),
      # Empty and unknown.
      (
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
          ds([None, None]),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None], schema_constants.STRING),
          ds(None, schema_constants.STRING),
          ds(None, schema_constants.INT64),
          ds(None, schema_constants.INT64),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None], schema_constants.BYTES),
          ds(None, schema_constants.BYTES),
          ds(None, schema_constants.INT64),
          ds(None, schema_constants.INT64),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None], schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds(None, schema_constants.OBJECT),
          ds([None, None], schema_constants.INT64),
      ),
      (
          ds([None, None]),
          ds('abc'),
          ds(1),
          ds(3),
          ds([None, None], schema_constants.INT64),
      ),
  )
  def test_four_args(self, s, substr, start, end, expected):
    result = expr_eval.eval(kde.strings.rfind(s, substr, start, end))
    testing.assert_equal(result, expected)

  def test_incompatible_types_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.rfind: mixing STRING and BYTES arguments is not'
            ' allowed, but `x` contains STRING and `substr` contains BYTES'
        ),
    ):
      expr_eval.eval(kde.strings.rfind(ds('foo'), ds(b'f')))

  def test_another_incompatible_types_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        re.escape(
            'kd.strings.rfind: argument `substr` must be a slice of either'
            ' STRING or BYTES, got a slice of INT32'
        ),
    ):
      expr_eval.eval(
          kde.strings.rfind(ds([None], schema_constants.STRING), ds(123))
      )

  def test_mixed_slice_error(self):
    with self.assertRaisesRegex(
        exceptions.KodaError,
        'kd.strings.rfind: argument `substr` must be a slice of either STRING'
        ' or BYTES, got a slice of OBJECT containing INT32 and STRING values',
    ):
      expr_eval.eval(kde.strings.rfind(ds('foo'), ds([1, 'fo'])))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.strings.rfind,
        QTYPES,
        # Limit the allowed qtypes to speed up the test.
        possible_qtypes=(
            arolla.UNSPECIFIED,
            qtypes.DATA_SLICE,
            arolla.INT64,
            arolla.BYTES,
        ),
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.strings.rfind(I.a, I.b)))


if __name__ == '__main__':
  absltest.main()
