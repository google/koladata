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

  def test_simple_schema(self):
    schema = fns.dict_schema(schema_constants.STRING, schema_constants.INT32)
    testing.assert_equal(
        schema.get_attr('__keys__').no_bag(), schema_constants.STRING
    )
    testing.assert_equal(
        schema.get_attr('__values__').no_bag(), schema_constants.INT32
    )

  def test_dict_schema_equivalent_to_schema_of_dict(self):
    testing.assert_equivalent(
        fns.dict({'a': 1}).get_schema().extract(),
        fns.dict_schema(schema_constants.STRING, schema_constants.INT32),
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
    key_schema = fns.uu_schema(a=schema_constants.INT32)
    schema = fns.dict_schema(
        key_schema,
        fns.uu_schema(a=schema_constants.FLOAT32),
    )
    self.assertNotEqual(
        key_schema.get_bag().fingerprint, schema.get_bag().fingerprint
    )
    testing.assert_equal(
        schema.get_attr('__keys__').a.no_bag(), schema_constants.INT32
    )
    testing.assert_equal(
        schema.get_attr('__values__').a.no_bag(), schema_constants.FLOAT32
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
