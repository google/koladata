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
from koladata.functions import attrs
from koladata.functions import functions as fns
from koladata.functions import object_factories
from koladata.types import data_slice
from koladata.types import schema_constants

ds = data_slice.DataSlice.from_vals


class DirTest(absltest.TestCase):

  def test_entity(self):
    db = object_factories.mutable_bag()
    x = db.new(a=1, b='abc')
    self.assertEqual(attrs.dir(x), ['a', 'b'])
    self.assertEqual(attrs.dir(ds([x])), ['a', 'b'])
    self.assertEqual(attrs.dir(ds([x]), intersection=True), ['a', 'b'])
    self.assertEqual(attrs.dir(ds([x]), intersection=False), ['a', 'b'])

  def test_object(self):
    db = object_factories.mutable_bag()
    x = db.obj(a=1, b='abc')
    self.assertEqual(attrs.dir(x), ['a', 'b'])
    self.assertEqual(attrs.dir(ds([x])), ['a', 'b'])
    self.assertEqual(
        attrs.dir(ds([x, db.obj(a='def', c=123)]), intersection=True), ['a']
    )
    self.assertEqual(
        attrs.dir(ds([x, db.obj(a='def', c=123)]), intersection=False),
        ['a', 'b', 'c'],
    )
    with self.assertRaisesRegex(
        ValueError,
        r'dir\(\) cannot determine attribute names because objects'
        r' have different attributes\. Please specify intersection='
        r' explicitly\.',
    ):
      attrs.dir(ds([x, db.obj(a='def', c=123)]))

  def test_primitives(self):
    x = ds([1, 2, 3]).with_bag(object_factories.mutable_bag())
    self.assertEqual(attrs.dir(x, intersection=True), [])

  def test_schema(self):
    db = object_factories.mutable_bag()
    self.assertEqual(
        attrs.dir(schema_constants.INT32.with_bag(db), intersection=True),
        [],
    )
    schema1 = db.new_schema(
        a=schema_constants.INT32, b=schema_constants.FLOAT32
    )
    schema2 = db.new_schema(
        a=schema_constants.INT32, c=schema_constants.FLOAT32
    )
    schemas = ds([schema1, schema2])
    self.assertEqual(attrs.dir(schemas, intersection=True), ['a'])
    self.assertEqual(attrs.dir(schemas, intersection=False), ['a', 'b', 'c'])

  def test_reserved_names_intersection(self):
    db = object_factories.mutable_bag()
    x = db.new(_x=1, getdoc=2, reshape=3)
    self.assertEqual(
        attrs.dir(x, intersection=True), ['_x', 'getdoc', 'reshape']
    )

  def test_object_with_missing(self):
    db = object_factories.mutable_bag()
    x = ds([db.obj(a=1, b='abc'), None])
    self.assertEqual(attrs.dir(x), ['a', 'b'])

  def test_object_with_missing_different_attrs(self):
    db = object_factories.mutable_bag()
    x = ds([db.obj(a=1, b='abc'), None, db.obj(a='def', c=123)])
    self.assertEqual(attrs.dir(x, intersection=True), ['a'])
    self.assertEqual(attrs.dir(x, intersection=False), ['a', 'b', 'c'])

  def test_list(self):
    x = fns.list([1, 2, 3])
    self.assertEqual(attrs.dir(x), [])
    # Note: consider changing the behaviour for list schemas to return
    # __items__.
    self.assertEqual(attrs.dir(x.get_schema()), [])

  def test_list_object(self):
    x = fns.obj(fns.list([1, 2, 3]))
    self.assertEqual(attrs.dir(x), [])

  def test_dict(self):
    x = fns.dict({'a': 1, 'b': 2})
    self.assertEqual(attrs.dir(x), [])
    # Note: consider changing the behaviour for dict schemas to return
    # __keys__ and __values__.
    self.assertEqual(attrs.dir(x.get_schema()), [])

  def test_dict_object(self):
    x = fns.obj(fns.dict({'a': 1, 'b': 2}))
    self.assertEqual(attrs.dir(x), [])

  def test_errors(self):
    db = object_factories.mutable_bag()
    x = db.obj(a=1, b='abc')
    with self.assertRaisesRegex(
        ValueError, 'cannot get available attributes without a DataBag'
    ):
      attrs.dir(x.no_bag())
    with self.assertRaisesRegex(
        ValueError, 'object schema is missing for the DataItem'
    ):
      attrs.dir(
          db.new(a=1, b='abc').with_schema(schema_constants.OBJECT),
          intersection=True,
      )


if __name__ == '__main__':
  absltest.main()
