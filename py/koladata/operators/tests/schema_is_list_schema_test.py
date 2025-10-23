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
from koladata.operators.tests.util import qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import schema_constants

I = input_container.InputContainer('I')

bag = data_bag.DataBag.empty_mutable()
ds = data_slice.DataSlice.from_vals
kd = eager_op_utils.operators_container('kd')
kde = kde_operators.kde

DATA_SLICE = qtypes.DATA_SLICE


class SchemaIsListSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (bag.list_schema(schema_constants.INT32), mask_constants.present),
      (
          bag.dict_schema(schema_constants.INT32, schema_constants.STRING),
          mask_constants.missing,
      ),
      (schema_constants.OBJECT, mask_constants.missing),
      (
          bag.list_schema(schema_constants.INT32).repeat(2),
          mask_constants.missing,
      ),
      (bag.list([1]), mask_constants.missing),
  )
  def test_eval(self, x, expected_result):
    testing.assert_equal(kd.schema.is_list_schema(x), expected_result)

  def test_qtype_signature(self):
    arolla.testing.assert_qtype_signatures(
        kde.schema.is_list_schema,
        [(DATA_SLICE, DATA_SLICE)],
        possible_qtypes=qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.is_list_schema(I.x)))


if __name__ == '__main__':
  absltest.main()
