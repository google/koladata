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

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
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


db = data_bag.DataBag.empty()


class LogicalHasTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([1, None, 3]), ds([arolla.present(), None, arolla.present()])),
      (
          ds(['a', 1, None, 1.5]),
          ds([arolla.present(), arolla.present(), None, arolla.present()]),
      ),
      # Scalar input, scalar output.
      (1, ds(arolla.present())),
      (ds(arolla.missing()), ds(arolla.missing(), schema_constants.MASK)),
      (
          ds([db.obj(), None, db.obj()]),
          ds([arolla.present(), None, arolla.present()]),
      ),
  )
  def test_eval(self, values, expected):
    result = expr_eval.eval(kde.masking.has(values))
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.masking.has,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.masking.has(I.x)), 'kd.masking.has(I.x)')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.masking.has(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.masking.has, kde.has))


if __name__ == '__main__':
  absltest.main()
