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

"""Tests for kde.core.ref."""

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
bag = data_bag.DataBag.empty
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class CoreRefTest(parameterized.TestCase):

  @parameterized.parameters(
      bag().obj(),
      bag().list([1, 2, 3]),
      bag().new(a=1),
      ds([bag().obj(a=1)]),
      ds(bag().obj(), schema_constants.ITEMID),
      ds(None).with_bag(bag()),
      bag().uu_schema(a=schema_constants.INT32),
  )
  def test_eval(self, x):
    testing.assert_equal(expr_eval.eval(kde.core.ref(x)), x.no_bag())

  def test_primitive_schema_error(self):
    with self.assertRaisesRegex(ValueError, 'unsupported schema: INT32'):
      expr_eval.eval(kde.core.ref(123))

  def test_primitive_data_error(self):
    with self.assertRaisesRegex(ValueError, 'cast INT32 to ITEMID'):
      expr_eval.eval(kde.core.ref(ds(123, schema_constants.OBJECT)))

  def test_dtype_schema_error(self):
    with self.assertRaisesRegex(ValueError, 'cannot cast DTYPE to ITEMID'):
      expr_eval.eval(kde.core.ref(schema_constants.INT32))

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.core.ref,
        [(qtypes.DATA_SLICE, qtypes.DATA_SLICE)],
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.ref(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.ref, kde.ref))


if __name__ == '__main__':
  absltest.main()
