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
from koladata.expr import input_container
from koladata.expr import py_expr_eval_py_ext
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants


eval_op = py_expr_eval_py_ext.eval_op
I = input_container.InputContainer('I')
kde = kde_operators.kde
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
ENTITY_SCHEMA = data_bag.DataBag.empty().new().get_schema()
bag = data_bag.DataBag.empty
db = bag()


class SchemaGetNameTest(parameterized.TestCase):

  @parameterized.named_parameters(
      ('int32', schema_constants.INT32, ds('INT32')),
      ('float32', schema_constants.FLOAT32, ds('FLOAT32')),
      ('string', schema_constants.STRING, ds('STRING')),
      ('object', schema_constants.OBJECT, ds('OBJECT')),
      (
          'named_schema',
          db.named_schema(
              'Query',
              docs=schema_constants.OBJECT,
              query_text=schema_constants.STRING,
          ),
          ds('Query').with_bag(db),
      ),
  )
  def test_eval(self, x, expected):
    res = eval_op('kd.schema.get_repr', x)
    testing.assert_equal(res, expected)

  @parameterized.named_parameters(
      (
          'schema',
          bag().new_schema(a=schema_constants.INT32, b=schema_constants.STRING),
          r'SCHEMA\(a=INT32, b=STRING\) with id \$',
      ),
      (
          'uu_schema',
          bag().uu_schema(a=schema_constants.INT32, b=schema_constants.STRING),
          r'SCHEMA\(a=INT32, b=STRING\) with id #',
      ),
  )
  def test_eval_like(self, x, expected_regex):
    res = eval_op('kd.schema.get_repr', x).internal_as_py()
    self.assertRegex(res, expected_regex)

  def test_non_schema_error(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'kd.schema.get_repr: schema\'s schema must be SCHEMA, got: INT32',
    ):
      eval_op('kd.schema.get_repr', ds(1))

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.get_repr(I.x)))


if __name__ == '__main__':
  absltest.main()
