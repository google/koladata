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
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

I = input_container.InputContainer('I')
M = arolla.M
ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class KodaNamedSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      (
          'name',
          'name',
      ),
      (
          ds('name'),
          'name',
      ),
  )
  def test_equal(self, lhs_name, rhs_name):
    lhs = expr_eval.eval(kde.schema.named_schema(I.x), x=lhs_name)
    rhs = expr_eval.eval(kde.schema.named_schema(I.x), x=rhs_name)
    testing.assert_equal(lhs, rhs.with_bag(lhs.get_bag()))
    self.assertFalse(lhs.is_mutable())
    self.assertCountEqual(lhs.get_attr_names(intersection=True), [])

  def test_not_equal(self):
    lhs = expr_eval.eval(kde.schema.named_schema('name1'))
    rhs = expr_eval.eval(kde.schema.named_schema('name2'))
    self.assertNotEqual(
        lhs.fingerprint, rhs.with_bag(lhs.get_bag()).fingerprint
    )

  def test_name_works_as_kwarg(self):
    lhs = expr_eval.eval(kde.schema.named_schema(I.x), x='name')
    rhs = expr_eval.eval(kde.schema.named_schema(name=I.x), x='name')
    testing.assert_equal(lhs, rhs.with_bag(lhs.get_bag()))

  @parameterized.parameters(
      (
          ds(['name1', 'name2']),
          'argument `name` must be an item holding STRING',
      ),
      (
          0,
          'argument `name` must be an item holding STRING',
      ),
  )
  def test_error(self, name, err_regex):
    with self.assertRaisesRegex(
        ValueError,
        err_regex,
    ):
      _ = expr_eval.eval(kde.schema.named_schema(name))

  def test_attrs(self):
    schema = expr_eval.eval(
        kde.schema.named_schema('name', a=schema_constants.FLOAT32)
    )
    schema2 = expr_eval.eval(kde.schema.named_schema('name'))
    testing.assert_equal(
        schema.a, schema_constants.FLOAT32.with_bag(schema.get_bag())
    )
    testing.assert_equal(schema, schema2.with_bag(schema.get_bag()))

  def test_nested_attrs(self):
    schema = kde.schema.named_schema('name', a=schema_constants.FLOAT32)
    outer_schema = kde.schema.named_schema('name2', x=schema).eval()
    testing.assert_equal(
        outer_schema.x.a,
        schema_constants.FLOAT32.with_bag(outer_schema.get_bag()),
    )

  def test_wrong_attr_type(self):
    with self.assertRaisesRegex(
        ValueError, 'only schemas can be assigned as attributes of schemas'
    ):
      kde.schema.named_schema('name', a=1.0).eval()
    with self.assertRaisesRegex(
        ValueError, 'only schemas can be assigned as attributes of schemas'
    ):
      kde.schema.named_schema('name', a=ds(1.0)).eval()
    with self.assertRaisesRegex(
        ValueError,
        'trying to assign a slice with 1 dimensions to a slice with only 0'
        ' dimensions',
    ):
      kde.schema.named_schema('name', a=ds([schema_constants.INT32])).eval()

  def test_view(self):
    self.assertTrue(view.has_koda_view(kde.schema.named_schema(I.name)))

  def test_alias(self):
    self.assertTrue(
        optools.equiv_to_op(kde.schema.named_schema, kde.named_schema)
    )

  def test_repr(self):
    self.assertEqual(
        repr(kde.schema.named_schema('name')),
        "kd.schema.named_schema(DataItem('name', schema: STRING))",
    )


if __name__ == '__main__':
  absltest.main()
