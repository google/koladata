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
from koladata.operators.tests.util import qtypes as test_qtypes
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import mask_constants
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
M = arolla.M
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde
bag = data_bag.DataBag.empty()


class SchemaIsAnySchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (schema_constants.ANY, mask_constants.present),
      (schema_constants.OBJECT, mask_constants.missing),
      (
          ds([schema_constants.ANY, schema_constants.ANY]),
          mask_constants.missing,
      ),
      (bag.new_schema(x=schema_constants.ANY), mask_constants.missing),
      (ds(1, schema_constants.ANY), mask_constants.missing),
  )
  def test_eval(self, x, expected_result):
    testing.assert_equal(
        expr_eval.eval(kde.schema.is_any_schema(x)), expected_result
    )

  def test_qtype_signature(self):
    arolla.testing.assert_qtype_signatures(
        kde.schema.is_any_schema,
        ((DATA_SLICE, DATA_SLICE),),
        possible_qtypes=test_qtypes.DETECT_SIGNATURES_QTYPES,
    )

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.is_any_schema(I.x)))


if __name__ == '__main__':
  absltest.main()
