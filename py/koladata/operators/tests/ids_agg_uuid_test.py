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
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import list_item as _  # pylint: disable=unused-import
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE
UNSPECIFIED = arolla.UNSPECIFIED

QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, DATA_SLICE, DATA_SLICE),
    (DATA_SLICE, UNSPECIFIED, DATA_SLICE),
])


class IdsAggUuidTest(parameterized.TestCase):

  @parameterized.parameters(
      (ds([[1, 2], [3, None]]), 1),
      (ds([bag().obj(a=1), bag().obj(a=2)]), 1),
      (ds([bag().list([1, 2]), None, bag().list([3, 4, 5])]), 1),
      (ds([bag().dict({1: 2}), None, bag().dict({3: 4})]), 1),
      (ds([[[1, 2], [3]], [[4], [5, None, 6]]]), 1),
      (ds([[[1, 2], [3]], [[4], [5, None, 6]]]), 2),
      (ds([[[1, 2], [3]], [[4], [5, None, 6]]]), 3),
  )
  def test_eval_shapes(self, x, ndim):
    y = expr_eval.eval(kde.ids.agg_uuid(I.x, ndim=ndim), x=x)
    self.assertEqual(y.get_ndim(), x.get_ndim() - ndim)
    self.assertIsNone(y.get_bag())
    self.assertEqual(y.get_schema(), schema_constants.ITEMID)

  @parameterized.parameters(
      (ds([1, 2]), ds([1, 2]), True),
      (ds([1, 2, None]), ds([1, 2]), False),
      (ds([1, 2, None]), ds([None, 1, 2]), False),
      (ds([None, None]), ds([None, None, None]), False),
      (ds([bag().obj(a=1)]), ds([bag().obj(a=1)]), False),
      (ds([1, 2], schema=schema_constants.INT32),
       ds([1, 2], schema=schema_constants.INT64), False),
  )
  def test_eval_equal(self, x, y, expected_equal):
    is_equal = kde.ids.agg_uuid(I.x) == kde.ids.agg_uuid(I.y)
    self.assertEqual(bool(expr_eval.eval(is_equal, x=x, y=y)), expected_equal)

  def test_qtype_signatures(self):
    self.assertCountEqual(
        arolla.testing.detect_qtype_signatures(
            kde.ids.agg_uuid,
            possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
        ),
        QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.size(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.ids.agg_uuid, kde.agg_uuid))


if __name__ == '__main__':
  absltest.main()
