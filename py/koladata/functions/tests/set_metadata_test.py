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

import re
from absl.testing import absltest
from koladata import kd
from koladata.testing import testing


class SetMetadataTest(absltest.TestCase):

  def test_set_metadata_new(self):
    bag = kd.mutable_bag()
    schema = bag.new_schema(a=kd.INT32)

    # Metadata doesn't exist initially
    meta = kd.get_metadata(schema)
    testing.assert_equal(kd.has(meta), kd.item(None, schema=kd.MASK))

    # Set metadata
    kd.set_metadata(schema, foo='bar')

    # Verify it exists now
    meta = kd.get_metadata(schema)
    testing.assert_equal(meta.foo, kd.item('bar').with_bag(bag))

  def test_set_metadata_update(self):
    bag = kd.mutable_bag()
    schema = bag.new_schema(a=kd.INT32)

    # Set initial metadata
    kd.set_metadata(schema, foo='bar')

    # Update it
    kd.set_metadata(schema, foo='baz', abc=123)

    meta = kd.get_metadata(schema)
    testing.assert_equal(meta.foo, kd.item('baz').with_bag(bag))
    testing.assert_equal(meta.abc, kd.item(123).with_bag(bag))

  def test_no_bag_error(self):
    db = kd.mutable_bag()
    schema = db.new_schema(a=kd.INT32).no_bag()
    with self.assertRaisesRegex(
        ValueError, 'is a reference without a bag'
    ):
      kd.set_metadata(schema, foo='bar')

  def test_immutable_bag_error(self):
    schema = kd.schema.new_schema(a=kd.INT32)  # has immutable bag
    with self.assertRaisesRegex(
        ValueError, 'cannot modify/create item'
    ):
      kd.set_metadata(schema, foo='bar')

  def test_non_schema_error(self):
    bag = kd.mutable_bag()
    x = bag.new(a=1)
    # x schema is ENTITY(a=INT32), not SCHEMA
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'failed to set metadata; cannot set for a DataSlice with'
            ' ENTITY(a=INT32) schema'
        ),
    ):
      kd.set_metadata(x, foo='bar')

  def test_set_metadata_multielement_slice(self):
    bag = kd.mutable_bag()
    schema1 = bag.new_schema(a=kd.INT32)
    schema2 = bag.new_schema(b=kd.FLOAT32)
    schemas = kd.slice([schema1, schema2])

    kd.set_metadata(schemas, foo='bar')

    meta = kd.get_metadata(schemas)
    testing.assert_equal(meta.foo, kd.slice(['bar', 'bar']).with_bag(bag))


if __name__ == '__main__':
  absltest.main()
