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

"""Tests for kd.set_schema."""

from absl.testing import absltest
from absl.testing import parameterized
from koladata.functions import attrs
from koladata.operators import kde_operators as _  # pylint: disable=unused-import
from koladata.testing import testing
from koladata.types import data_bag
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
db = data_bag.DataBag.empty_mutable()
db2 = data_bag.DataBag.empty_mutable()
obj1 = db.obj()
obj2 = db.obj()
entity1 = db.new()
entity2 = db.new()
s1 = entity1.get_schema()
s2 = entity2.get_schema()


class SetSchemaTest(parameterized.TestCase):

  @parameterized.parameters(
      # Scalar primitive schema
      (ds(None), schema_constants.INT32, ds(None, schema_constants.INT32)),
      (ds(1), schema_constants.OBJECT, ds(1, schema_constants.OBJECT)),
      (ds(1, schema_constants.OBJECT), schema_constants.INT32, ds(1)),
      # 1D primitive schema
      (
          ds([None, None]),
          schema_constants.INT32,
          ds([None, None], schema_constants.INT32),
      ),
      (
          ds([1, 2]),
          schema_constants.OBJECT,
          ds([1, 2], schema_constants.OBJECT),
      ),
      (ds([1, 2], schema_constants.OBJECT), schema_constants.INT32, ds([1, 2])),
  )
  def test_primitives(self, x, schema, expected):
    testing.assert_equal(attrs.set_schema(x, schema), expected)
    testing.assert_equal(
        attrs.set_schema(x.with_bag(db), schema), expected.with_bag(db)
    )

  @parameterized.parameters(
      # Entity schema -> the same Entity schema
      (entity1, s1),
      # Entity schema -> different entity schema
      (entity1, s2),
      # Entity schema -> OBJECT
      (entity1, schema_constants.OBJECT),
      # OBJECT -> Entity schema
      (ds([obj1, obj2]), s2),
  )
  def test_entities_and_objects(self, x, schema):
    res = attrs.set_schema(x, schema)
    testing.assert_equal(res.get_schema().no_bag(), schema.no_bag())

  def test_set_schema_from_different_bag(self):
    res = attrs.set_schema(entity1, db2.new_schema(x=schema_constants.INT32))
    # Check the data of schema is moved to the DataSlice's db.
    testing.assert_equal(res.get_schema().x.no_bag(), schema_constants.INT32)

  def test_errors(self):
    with self.assertRaisesRegex(
        ValueError,
        'INT64 schema can only be assigned to a DataSlice that contains only'
        ' primitives of INT64',
    ):
      attrs.set_schema(ds(1), schema_constants.INT64)

    with self.assertRaisesRegex(
        ValueError,
        'cannot set an Entity schema on a DataSlice without a DataBag.',
    ):
      attrs.set_schema(ds(1), s1)

    with self.assertRaisesRegex(
        ValueError,
        'DataSlice with an Entity schema must hold Entities or Objects',
    ):
      attrs.set_schema(ds(1).with_bag(db), s1)

    with self.assertRaisesRegex(
        ValueError,
        'INT64 schema can only be assigned to a DataSlice that contains only'
        ' primitives of INT64',
    ):
      attrs.set_schema(entity1, schema_constants.INT64)

    with self.assertRaisesRegex(
        ValueError,
        'INT64 schema can only be assigned to a DataSlice that contains only'
        ' primitives of INT64',
    ):
      attrs.set_schema(obj1, schema_constants.INT64)

    with self.assertRaisesRegex(
        ValueError,
        'INT32 schema can only be assigned to a DataSlice that contains only'
        ' primitives of INT32',
    ):
      attrs.set_schema(ds([1, '2']), schema_constants.INT32)


if __name__ == '__main__':
  absltest.main()
