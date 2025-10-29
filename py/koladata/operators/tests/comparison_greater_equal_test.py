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
from koladata.operators import optools
from koladata.operators.tests.testdata import comparison_greater_equal_testdata
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
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class ComparisonGreaterEqualTest(parameterized.TestCase):

  @parameterized.parameters(*comparison_greater_equal_testdata.TEST_CASES)
  def test_eval(self, x, y, expected):
    result = expr_eval.eval(kde.comparison.greater_equal(I.x, I.y), x=x, y=y)
    testing.assert_equal(result, expected)

  def test_qtype_difference(self):
    x = ds([1, 2, 3])
    y = ds(['a', 'b', 'c'])
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r"""kd.comparison.greater_equal: arguments do not contain values castable to a common primitive schema, but have the common non-primitive schema OBJECT.

Schema for `y`: STRING
Schema for `x`: INT32"""
        ),
    ):
      expr_eval.eval(kde.comparison.greater_equal(I.x, I.y), x=x, y=y)

  def test_unordered_types(self):
    empty = ds([None, None])
    schemas = ds([None, schema_constants.BOOLEAN])
    with self.assertRaisesRegex(
        ValueError,
        'kd.comparison.greater_equal: argument `x` must be a slice of orderable'
        ' values, got a slice of SCHEMA',
    ):
      expr_eval.eval(kde.comparison.greater_equal(I.x, I.y), x=schemas, y=empty)
    with self.assertRaisesRegex(
        ValueError,
        'kd.comparison.greater_equal: argument `y` must be a slice of orderable'
        ' values, got a slice of SCHEMA',
    ):
      expr_eval.eval(kde.comparison.greater_equal(I.x, I.y), x=empty, y=schemas)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.comparison.greater_equal,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.comparison.greater_equal(I.x, I.y)), 'I.x >= I.y')
    self.assertEqual(repr(kde.greater_equal(I.x, I.y)), 'I.x >= I.y')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.comparison.greater_equal(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.comparison.greater_equal, kde.greater_equal)
    )


if __name__ == '__main__':
  absltest.main()
