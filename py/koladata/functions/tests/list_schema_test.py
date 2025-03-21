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

"""Tests for kd.list_schema."""

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
bag = fns.bag


class ListSchemaTest(absltest.TestCase):

  def test_deprecated_db_arg(self):
    with self.assertRaisesRegex(
        ValueError, 'db= argument is deprecated.* db.list_schema method'
    ):
      fns.list_schema(schema_constants.INT32, db=fns.bag())

  def test_mutability(self):
    self.assertFalse(fns.list_schema(schema_constants.STRING).is_mutable())

  def test_simple_schema(self):
    schema = fns.list_schema(schema_constants.INT32)
    testing.assert_equal(
        schema.get_attr('__items__').no_bag(), schema_constants.INT32
    )

  def test_list_schema_equivalent_to_schema_of_list(self):
    testing.assert_equivalent(
        fns.list([1, 2, 3]).get_schema().extract(),
        fns.list_schema(schema_constants.INT32),
    )

  def test_no_databag(self):
    schema = fns.list_schema(schema_constants.INT32)
    testing.assert_equal(
        schema.get_attr('__items__'),
        schema_constants.INT32.with_bag(schema.get_bag()),
    )

  def test_nested_schema_with_bag_adoption(self):
    item_schema = fns.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING,
    )
    schema = fns.list_schema(item_schema)
    self.assertNotEqual(
        item_schema.get_bag().fingerprint, schema.get_bag().fingerprint
    )
    testing.assert_equal(
        schema.get_attr('__items__').a.no_bag(), schema_constants.INT32
    )

  def test_non_data_slice_arg(self):
    with self.assertRaisesRegex(
        ValueError, 'expected DATA_SLICE, got item_schema: DATA_BAG'
    ):
      _ = fns.list_schema(item_schema=bag())

  def test_alias(self):
    self.assertIs(fns.list_schema, fns.schema.list_schema)


if __name__ == '__main__':
  absltest.main()
