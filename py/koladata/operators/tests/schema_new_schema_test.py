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

"""Tests for kde.schema.new_schema."""

from absl.testing import absltest
from absl.testing import parameterized
from arolla import arolla
from koladata.expr import expr_eval
from koladata.expr import input_container
from koladata.expr import view
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import qtypes
from koladata.types import schema_constants

I = input_container.InputContainer('I')
M = arolla.M
ds = data_slice.DataSlice.from_vals
DATA_SLICE = qtypes.DATA_SLICE
kde = kde_operators.kde


class KodaNewSchemaTest(parameterized.TestCase):

  def test_get_attr(self):
    kwargs = {
        'a': schema_constants.INT32,
        'b': schema_constants.FLOAT32,
    }
    schema = expr_eval.eval(kde.schema._new_schema(**kwargs))
    for attr_name, val in kwargs.items():
      testing.assert_equal(
          getattr(schema, attr_name), ds(val).with_db(schema.db)
      )

  def test_db_adoption(self):
    a = expr_eval.eval(kde.schema._new_schema(a=schema_constants.INT32))
    b = expr_eval.eval(kde.schema._new_schema(a=a))
    testing.assert_equal(b.a.a, ds(schema_constants.INT32).with_db(b.db))

  def test_invalid_arguments(self):
    with self.assertRaisesRegex(
        ValueError,
        'schema\'s schema must be SCHEMA, got: INT32',
    ):
      _ = expr_eval.eval(kde.schema._new_schema(
          a=schema_constants.INT32,
          b=ds(1),
      ))

  def test_non_data_slice_binding(self):
    with self.assertRaisesRegex(
        ValueError,
        'expected all arguments to be DATA_SLICE, got kwargs:'
        ' namedtuple<a=DATA_SLICE,b=UNSPECIFIED>',
    ):
      _ = expr_eval.eval(
          kde.schema._new_schema(
              a=schema_constants.INT32,
              b=arolla.unspecified(),
          )
      )

  def test_view(self):
    self.assertTrue(view.has_data_slice_view(kde.schema._new_schema()))

  # TODO: Uncomment, when this operator becomes public and we add
  # the alias back.
  # def test_alias(self):
  #   self.assertTrue(
  #       optools.equiv_to_op(kde.schema.new_schema, kde.new_schema)
  #   )


if __name__ == '__main__':
  absltest.main()
