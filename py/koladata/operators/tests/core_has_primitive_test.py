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
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

present = mask_constants.present
missing = mask_constants.missing


class KodaHasPrimitiveTest(parameterized.TestCase):

  @parameterized.parameters(
      # DataItem
      (ds(None), missing),
      (ds(None, schema_constants.INT32), missing),
      (ds(1), present),
      (ds([1, None, 3]), ds([present, missing, present])),
      (ds('hello'), present),
      (ds(schema_constants.INT32), present),
      # DataSlice
      (ds(['hello', None, 'world']), ds([present, missing, present])),
      (ds(arolla.quote(kde.math.subtract(arolla.L.L1, arolla.L.L2))), present),
      # Mixed types.
      (ds(['hello', 1, 'world']), ds([present, present, present])),
      (bag().list([1, 2, 3]), missing),
      (bag().dict(ds(['hello', 'world']), ds([1, 2])), missing),
      (bag().obj(a=ds(1), b=ds(2)), missing),
      (
          ds([bag().obj(a=ds(1), b=ds(2)), 42, 'abc']),
          ds([missing, present, present]),
      ),
      # Schemas
      (
          ds([schema_constants.STRING, None, bag().new_schema()]),
          ds([present, missing, missing]),
      ),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(kd.core.has_primitive(x), expected)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.core.has_primitive(I.x)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.core.has_primitive, kde.has_primitive)
    )


if __name__ == '__main__':
  absltest.main()
