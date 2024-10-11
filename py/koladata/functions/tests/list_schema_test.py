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

  def test_simple_schema(self):
    db = bag()
    schema = fns.list_schema(schema_constants.INT32, db)
    testing.assert_equal(
        schema.get_attr('__items__'), schema_constants.INT32.with_db(db)
    )

  def test_list_schema_equivalent_to_schema_of_list(self):
    db = bag()
    testing.assert_equal(
        fns.list([1, 2, 3], db=db).get_schema(),
        fns.list_schema(schema_constants.INT32, db),
    )

  def test_no_databag(self):
    schema = fns.list_schema(schema_constants.INT32)
    testing.assert_equal(
        schema.get_attr('__items__'), schema_constants.INT32.with_db(schema.db)
    )

  def test_nested_schema_with_db_adoption(self):
    db = bag()
    db2 = bag()
    schema = fns.list_schema(
        fns.new_schema(
            a=schema_constants.INT32, b=schema_constants.TEXT, db=db2
        ),
        db,
    )
    testing.assert_equal(
        schema.get_attr('__items__').a, schema_constants.INT32.with_db(db)
    )

  def test_non_data_slice_arg(self):
    with self.assertRaisesRegex(
        TypeError,
        'expecting item_schema to be a DataSlice, got NoneType',
    ):
      _ = fns.list_schema(item_schema=None, db=bag())


if __name__ == '__main__':
  absltest.main()
