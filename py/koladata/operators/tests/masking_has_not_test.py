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
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import eager_op_utils
from koladata.operators import kde_operators
from koladata.operators import optools
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')

db = data_bag.DataBag.empty_mutable()
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE

QTYPES = [(DATA_SLICE, DATA_SLICE)]


class LogicalHasNotTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([1, None, 3]), ds([None, arolla.present(), None])),
      (ds([1, 2, 3]), ds([None, None, None], schema_constants.MASK)),
      (ds(['a', 1, None, 1.5]), ds([None, None, arolla.present(), None])),
      # Scalar input, scalar output.
      (1, ds(arolla.missing(), schema_constants.MASK)),
      (ds(arolla.missing()), ds(arolla.present())),
      # Objects
      (ds([db.obj(), None, db.obj()]), ds([None, arolla.present(), None])),
      # OBJECT
      (
          ds([1, None, 5], schema_constants.OBJECT),
          ds([None, arolla.present(), None]),
      ),
      (
          ds([1, 2, 5], schema_constants.OBJECT),
          ds([None, None, None], schema_constants.MASK),
      ),
      # Empty and unknown inputs.
      (
          ds([None, None, None], schema_constants.OBJECT),
          ds([arolla.present(), arolla.present(), arolla.present()]),
      ),
      (
          ds([None, None, None]),
          ds([arolla.present(), arolla.present(), arolla.present()]),
      ),
      (
          ds([None, None, None], schema_constants.FLOAT32),
          ds([arolla.present(), arolla.present(), arolla.present()]),
      ),
  )
  def test_eval(self, values, expected):
    result = kd.masking.has_not(values)
    testing.assert_equal(result, expected)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.masking.has_not,
        QTYPES,
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_repr(self):
    self.assertEqual(repr(kde.masking.has_not(I.x)), '~I.x')
    self.assertEqual(repr(kde.has_not(I.x)), '~I.x')

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.masking.has_not(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.masking.has_not, kde.has_not))


if __name__ == '__main__':
  absltest.main()
