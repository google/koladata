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

"""Tests for kd.dict_schema."""

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
bag = fns.bag


class DictSchemaTest(absltest.TestCase):

  def test_mutability(self):
    self.assertFalse(
        fns.dict_schema(
            schema_constants.STRING, schema_constants.INT32
        ).is_mutable()
    )
    self.assertTrue(
        fns.dict_schema(
            schema_constants.STRING, schema_constants.INT32, db=bag()
        ).is_mutable()
    )

  def test_simple_schema(self):
    db = bag()
    schema = fns.dict_schema(
        schema_constants.STRING, schema_constants.INT32, db
    )
    testing.assert_equal(
        schema.get_attr('__keys__'), schema_constants.STRING.with_bag(db)
    )
    testing.assert_equal(
        schema.get_attr('__values__'), schema_constants.INT32.with_bag(db)
    )

  def test_dict_schema_equivalent_to_schema_of_dict(self):
    db = bag()
    testing.assert_equal(
        fns.dict({'a': 1}, db=db).get_schema(),
        fns.dict_schema(schema_constants.STRING, schema_constants.INT32, db),
    )

  def test_no_databag(self):
    schema = fns.dict_schema(schema_constants.STRING, schema_constants.INT32)
    testing.assert_equal(
        schema.get_attr('__keys__'),
        schema_constants.STRING.with_bag(schema.get_bag()),
    )
    testing.assert_equal(
        schema.get_attr('__values__'),
        schema_constants.INT32.with_bag(schema.get_bag()),
    )

  def test_nested_schema_with_bag_adoption(self):
    db = bag()
    db2 = bag()
    schema = fns.dict_schema(
        db2.uu_schema(a=schema_constants.INT32),
        db2.uu_schema(a=schema_constants.FLOAT32),
        db,
    )
    testing.assert_equal(
        schema.get_attr('__keys__').a, schema_constants.INT32.with_bag(db)
    )
    testing.assert_equal(
        schema.get_attr('__values__').a, schema_constants.FLOAT32.with_bag(db)
    )

  def test_non_data_slice_arg(self):
    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got key_schema: UNSPECIFIED',
    ):
      _ = fns.dict_schema(key_schema=None, value_schema=None)

    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got value_schema: UNSPECIFIED',
    ):
      _ = fns.dict_schema(
          key_schema=schema_constants.FLOAT32, value_schema=None
      )

  def test_invalid_dict_key(self):
    with self.assertRaisesRegex(
        ValueError,
        'dict keys cannot be FLOAT32',
    ):
      _ = fns.dict_schema(schema_constants.FLOAT32, schema_constants.FLOAT32)

  def test_alias(self):
    self.assertIs(fns.dict_schema, fns.schema.dict_schema)


if __name__ == '__main__':
  absltest.main()
