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

"""Tests for kd.ids.has_uuid operator."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import list_item as _  # pylint: disable=unused-import
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants


I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
bag = data_bag.DataBag.empty
DATA_SLICE = qtypes.DATA_SLICE

present = mask_constants.present
missing = mask_constants.missing


QTYPES = frozenset([
    (DATA_SLICE, DATA_SLICE),
])


class HasUuidTest(parameterized.TestCase):
  """Tests for kd.ids.has_uuid operator."""

  @parameterized.parameters(
      (ds(1), ds(missing)),
      (ds([1, 2]), ds([missing, missing])),
      (bag().obj(a=1), ds(missing)),
      (bag().new(a=1), ds(missing)),
      (bag().uu(a=1), ds(present)),
      (bag().uuobj(a=1), ds(present)),
      (
          ds([bag().uu(a=1), None, bag().uu(a=2)]),
          ds([present, missing, present]),
      ),
      (ds([bag().uuobj(a=1), None, 1]), ds([present, missing, missing])),
      (bag().uu(a=1) & None, ds(missing)),
      (ds(None, schema_constants.OBJECT), ds(missing)),
      (ds(None), ds(missing)),
      (ds([None, None]), ds([missing, missing])),
      (bag().uuobj(a=1) & None, ds(missing)),
      (bag().uu_schema(a=schema_constants.INT32), ds(present)),
      (bag().new_schema(a=schema_constants.INT32), ds(missing)),
      (
          ds([
              bag().uu_schema(a=schema_constants.INT32),
              bag().new_schema(a=schema_constants.INT32),
          ]),
          ds([present, missing]),
      ),
  )
  def test_values(self, x, expected):
    testing.assert_equal(expr_eval.eval(kde.ids.has_uuid(x)), expected)

  def test_qtype_signatures(self):
    arolla.testing.assert_qtype_signatures(
        kde.ids.has_uuid,
        QTYPES,
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.ids.has_uuid(I.x)))


if __name__ == '__main__':
  absltest.main()
