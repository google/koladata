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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.testdata import comparison_equal_testdata
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')

kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class ComparisonEqualTest(parameterized.TestCase):

  @parameterized.parameters(*comparison_equal_testdata.TEST_CASES)
  def test_eval(self, lhs, rhs, expected):
    result = kd.comparison.equal(lhs, rhs)
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.comparison.equal,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_raises_on_incompatible_schemas(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r"""kd.comparison.equal: arguments do not have a common schema.

Schema for `x`: ENTITY()
Schema for `y`: INT32"""
        ),
    ):
      kd.comparison.equal(kd.new(), ds(1))

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r"""kd.comparison.equal: arguments do not have a common schema.

Schema for `x`: ENTITY(x=INT32)
Schema for `y`: ENTITY()"""
        ),
    ):
      kd.comparison.equal(kd.new(x=1), kd.new())

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r"""kd.comparison.equal: arguments do not have a common schema.

Schema for `x`: ENTITY(x=INT32)
Schema for `y`: OBJECT containing non-primitive values"""
        ),
    ):
      kd.comparison.equal(kd.new(x=1), kd.obj())

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            r"""kd.comparison.equal: arguments do not have a common schema.

Schema for `x`: ENTITY(x=INT32)
Schema for `y`: ITEMID"""
        ),
    ):
      kd.comparison.equal(
          kd.new(x=1), kd.obj().with_schema(schema_constants.ITEMID)
      )

  def test_repr(self):
    self.assertEqual(repr(kde.comparison.equal(I.x, I.y)), 'I.x == I.y')
    self.assertEqual(repr(kde.equal(I.x, I.y)), 'I.x == I.y')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.comparison.equal(I.x, I.y)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.comparison.equal, kde.equal))


if __name__ == '__main__':
  absltest.main()
