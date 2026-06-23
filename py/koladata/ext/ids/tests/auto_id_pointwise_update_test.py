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
from absl.testing import parameterized
from koladata import kd
from koladata import kd_ext

ds = kd.slice


class AutoIdPointwiseUpdateTest(parameterized.TestCase):

  def test_basic_mapped(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),
    )
    x = kd.new(a=ds([1, 2, 3]), schema=schema)
    update_bag = kd_ext.ids.auto_id_pointwise_update(x)
    x_with_ids = x.enriched(update_bag)
    # Each item gets foo_1 independently, since each is processed as if alone.
    kd.testing.assert_equivalent(
        x_with_ids.foo_id,
        ds(['foo_1', 'foo_1', 'foo_1']),
    )

  def test_scalar_input(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),
    )
    x = kd.new(a=42, schema=schema)
    update_bag = kd_ext.ids.auto_id_pointwise_update(x)
    x_with_ids = x.enriched(update_bag)
    kd.testing.assert_equivalent(
        x_with_ids.foo_id,
        ds('foo_1'),
    )

  def test_nested_schema_mapped(self):
    child_schema = kd.schema.new_schema(val=kd.INT32)
    child_schema = kd_ext.ids.with_auto_attributes(
        child_schema,
        child_id=kd_ext.ids.auto_id('child'),
    )
    parent_schema = kd.schema.new_schema(child=child_schema)
    parent_schema = kd_ext.ids.with_auto_attributes(
        parent_schema,
        parent_id=kd_ext.ids.auto_id('parent'),
    )
    child = kd.new(val=ds([10, 20]), schema=child_schema)
    parent = kd.new(child=child, schema=parent_schema)
    update_bag = kd_ext.ids.auto_id_pointwise_update(parent)
    parent_with_ids = parent.enriched(update_bag)
    # Each parent item is processed independently:
    # parent_id: each gets 'parent_1'
    kd.testing.assert_equivalent(
        parent_with_ids.parent_id,
        ds(['parent_1', 'parent_1']),
    )
    # child_id: each gets 'child_1'
    kd.testing.assert_equivalent(
        parent_with_ids.child.child_id,
        ds(['child_1', 'child_1']),
    )

  def test_multiple_auto_id_attributes(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),
        bar_id=kd_ext.ids.auto_id('bar'),
    )
    x = kd.new(a=ds([1, 2]), schema=schema)
    update_bag = kd_ext.ids.auto_id_pointwise_update(x)
    x_with_ids = x.enriched(update_bag)
    kd.testing.assert_equivalent(
        x_with_ids.foo_id,
        ds(['foo_1', 'foo_1']),
    )
    kd.testing.assert_equivalent(
        x_with_ids.bar_id,
        ds(['bar_1', 'bar_1']),
    )

  def test_empty_input(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),
    )
    x = kd.new(a=ds([], schema=kd.INT32), schema=schema)
    update_bag = kd_ext.ids.auto_id_pointwise_update(x)
    x_with_ids = x.enriched(update_bag)
    kd.testing.assert_equivalent(
        x_with_ids.foo_id,
        ds([], schema=kd.STRING),
    )

  def test_no_auto_id_attributes(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    x = kd.new(a=ds([1, 2, 3]), schema=schema)
    update_bag = kd_ext.ids.auto_id_pointwise_update(x)
    x_with_ids = x.enriched(update_bag)
    kd.testing.assert_equivalent(x_with_ids.a, ds([1, 2, 3]))

  def test_merge_conflict(self):
    child_schema = kd.schema.new_schema(val=kd.INT32)
    child_schema = kd_ext.ids.with_auto_attributes(
        child_schema,
        child_id=kd_ext.ids.auto_id('child'),
    )
    parent_schema = kd.schema.new_schema(
        items=kd.list_schema(child_schema),
    )
    other_child = child_schema.new(val=20)
    child = child_schema.new(val=10)

    list0 = kd.list([child])
    list1 = kd.list([other_child, child])
    parent0 = parent_schema.new(items=list0)
    parent1 = parent_schema.new(items=list1)

    with self.assertRaisesRegex(
        ValueError,
        "The cause is the values of attribute 'child_id' are different",
    ):
      _ = kd_ext.ids.auto_id_pointwise_update(ds([parent0, parent1]))

if __name__ == '__main__':
  absltest.main()
