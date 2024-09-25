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


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE


present = arolla.present()
missing = arolla.missing()

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
])


class CoreHasAttrTest(parameterized.TestCase):

  def setUp(self):
    super().setUp()
    self.db = data_bag.DataBag.empty()
    self.entity = self.db.new(a=ds([1, None]), b=ds([None, None]))
    self.object = self.db.obj(a=ds([1, None]), b=ds([None, None]))

  @parameterized.parameters(
      (kde.has_attr(I.x, 'a'), ds(present)),
      (kde.has_attr(I.x, 'b'), ds(missing)),
      (kde.has_attr(I.x, 'c'), ds(missing)),
  )
  def test_eval(self, expr, expected):
    testing.assert_equal(expr_eval.eval(expr, x=self.entity), expected)
    testing.assert_equal(expr_eval.eval(expr, x=self.object), expected)

  def test_attr_name_error(self):
    with self.assertRaisesRegex(
        ValueError,
        r'attr_name in kd.get_attr expects.*got: DataItem\(42, schema: INT32\)',
    ):
      expr_eval.eval(kde.core.has_attr(self.entity, 42))

    with self.assertRaisesRegex(
        ValueError,
        r'cannot fetch attributes without a DataBag: a; during evaluation of'
        r' operator kde.core._get_attr_with_default',
    ):
      expr_eval.eval(kde.core.has_attr(43, 'a'))

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.core.has_attr,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.core.has_attr(I.x, 'a')),
        "kde.core.has_attr(I.x, DataItem('a', schema: TEXT))",
    )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.core.has_attr(I.x, 'a')))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.core.has_attr, kde.has_attr))

if __name__ == '__main__':
  absltest.main()
