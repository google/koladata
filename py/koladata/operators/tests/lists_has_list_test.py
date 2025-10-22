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
kde = kde_operators.kde
kd = eager_op_utils.operators_container('kd')

present = mask_constants.present
missing = mask_constants.missing


class KodaHasListTest(parameterized.TestCase):

  @parameterized.parameters(
      # DataItem
      (ds(None), missing),
      (bag().list() & None, missing),
      (bag().list(), present),
      (bag().list([1, 2]), present),
      (bag().list([1, 2]).embed_schema(), present),
      (ds('hello'), missing),
      (bag().new(), missing),
      (bag().obj(), missing),
      (bag().dict(), missing),
      (bag().new_schema(), missing),
      # DataSlice
      (
          ds([bag().list([1, 2]), None, bag().list([3, 4])]),
          ds([present, missing, present]),
      ),
      (ds([None, None]), ds([missing, missing])),
      (ds([None, None], schema_constants.INT32), ds([missing, missing])),
      (ds([None, None], schema_constants.OBJECT), ds([missing, missing])),
      # Mixed types.
      (
          ds([bag().list([1, 2]).embed_schema(), None, 'world', bag().obj()]),
          ds([present, missing, missing, missing]),
      ),
  )
  def test_eval(self, x, expected):
    testing.assert_equal(kd.lists.has_list(x), expected)

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.lists.has_list(I.x)))

  def test_alias(self):
    self.assertTrue(optools.equiv_to_op(kde.lists.has_list, kde.has_list))


if __name__ == '__main__':
  absltest.main()
