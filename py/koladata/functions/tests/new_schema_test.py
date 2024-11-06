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

from unittest import mock
import warnings

from absl.testing import absltest
from arolla import arolla
from koladata.functions import functions as fns
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals
bag = fns.bag


class NewSchemaTest(absltest.TestCase):

  def test_simple_schema(self):
    db = bag()
    schema = fns.schema.new_schema(
        db, a=schema_constants.INT32, b=schema_constants.STRING
    )

    testing.assert_equal(schema.a, schema_constants.INT32.with_bag(db))
    testing.assert_equal(schema.b, schema_constants.STRING.with_bag(db))

  def test_nested_schema_with_adoption(self):
    db = bag()
    db2 = bag()
    schema = fns.schema.new_schema(
        db,
        a=schema_constants.INT32,
        b=fns.schema.new_schema(db2, a=schema_constants.INT32),
    )
    testing.assert_equal(schema.a, schema_constants.INT32.with_bag(db))
    testing.assert_equal(schema.b.a, schema_constants.INT32.with_bag(db))

  def test_bag_arg(self):
    db = bag()
    schema = fns.schema.new_schema(
        a=schema_constants.INT32, b=schema_constants.STRING, db=db
    )

    testing.assert_equal(schema.a, schema_constants.INT32.with_bag(db))
    testing.assert_equal(schema.b, schema_constants.STRING.with_bag(db))

  def test_list_error(self):
    db = bag()
    with self.assertRaisesRegex(
        ValueError,
        'expected DataSlice argument, got list',
    ):
      _ = fns.schema.new_schema(
          db,
          a=schema_constants.INT32,
          b=[1, 2, 3],
      )
    testing.assert_equivalent(db, bag())

  def test_dict_error(self):
    db = bag()
    with self.assertRaisesRegex(
        ValueError,
        'expected DataSlice argument, got dict',
    ):
      _ = db.new_schema(
          a=schema_constants.INT32,
          b={'a': 1},
      )
    testing.assert_equivalent(db, bag())

  def test_non_dataslice_qvalue_error(self):
    db = bag()
    with self.assertRaisesRegex(
        ValueError,
        'expected DataSlice argument, got Text',
    ):
      _ = fns.schema.new_schema(
          db,
          a=schema_constants.INT32,
          b=arolla.text('hello'),
      )
    testing.assert_equivalent(db, bag())

  def test_non_schema_dataslice_error(self):
    db = bag()
    with self.assertRaisesRegex(
        ValueError,
        "schema's schema must be SCHEMA, got: OBJECT",
    ):
      db2 = bag()
      _ = fns.schema.new_schema(
          db,
          a=schema_constants.INT32,
          b=fns.obj(db=db2, a=schema_constants.INT32),
      )
    testing.assert_equivalent(db, bag())

  def test_deprecated_new_schema(self):
    db = bag()
    with mock.patch.object(warnings, 'warn') as mock_warn:
      _ = fns.schema.new_schema(
          db, a=schema_constants.INT32, b=schema_constants.STRING
      )
      mock_warn.assert_not_called()
      schema = fns.new_schema(
          db, a=schema_constants.INT32, b=schema_constants.STRING
      )
      mock_warn.assert_called_once()

    testing.assert_equal(schema.a, schema_constants.INT32.with_bag(db))
    testing.assert_equal(schema.b, schema_constants.STRING.with_bag(db))


if __name__ == '__main__':
  absltest.main()
