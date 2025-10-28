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

from absl.testing import absltest
from koladata.functions import functions as fns
from koladata.functions import object_factories
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals


class GetAttrNamesTest(absltest.TestCase):

  def test_entity(self):
    db = object_factories.mutable_bag()
    x = db.new(a=1, b='abc')
    self.assertEqual(fns.get_attr_names(x, intersection=True), ['a', 'b'])
    self.assertEqual(fns.get_attr_names(ds([x]), intersection=True), ['a', 'b'])

  def test_object(self):
    db = object_factories.mutable_bag()
    x = db.obj(a=1, b='abc')
    self.assertEqual(fns.get_attr_names(x, intersection=True), ['a', 'b'])
    self.assertEqual(fns.get_attr_names(ds([x]), intersection=True), ['a', 'b'])
    # Returns the intersection of attributes...
    self.assertEqual(
        fns.get_attr_names(ds([x, db.obj(a='def', c=123)]), intersection=True),
        ['a'],
    )
    # ... or the union of attributes.
    self.assertEqual(
        fns.get_attr_names(ds([x, db.obj(a='def', c=123)]), intersection=False),
        ['a', 'b', 'c'],
    )

  def test_schema(self):
    db = object_factories.mutable_bag()
    self.assertEqual(
        fns.get_attr_names(
            schema_constants.INT32.with_bag(db), intersection=True
        ),
        [],
    )
    schema1 = db.new_schema(
        a=schema_constants.INT32, b=schema_constants.FLOAT32
    )
    schema2 = db.new_schema(
        a=schema_constants.INT32, c=schema_constants.FLOAT32
    )
    schemas = ds([schema1, schema2])
    # Returns either the intersection of attributes...
    self.assertEqual(fns.get_attr_names(schemas, intersection=True), ['a'])
    # ... or the union of attributes.
    self.assertEqual(
        fns.get_attr_names(schemas, intersection=False), ['a', 'b', 'c']
    )

  def test_primitives(self):
    x = ds([1, 2, 3]).with_bag(object_factories.mutable_bag())
    self.assertEqual(fns.get_attr_names(x, intersection=True), [])

  def test_no_bag_error(self):
    db = object_factories.mutable_bag()
    x = db.obj(a=1, b='abc')
    with self.assertRaisesRegex(
        ValueError, 'cannot get available attributes without a DataBag'
    ):
      fns.get_attr_names(x.no_bag(), intersection=True)


if __name__ == '__main__':
  absltest.main()
