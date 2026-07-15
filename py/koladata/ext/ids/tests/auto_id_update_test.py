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

I = kd.I

bag = kd.mutable_bag
ds = kd.slice


class IdsAutoIdUpdateTest(parameterized.TestCase):

  def test_auto_id_update_attribute(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x = kd.new(a=ds([1, 2, 2, 4, 5]), schema=schema)
    x_part = x.S[2:]
    auto_id_bag = kd_ext.ids.auto_id_update(x_part)  # pyrefly: ignore[missing-attribute]
    x_part_with_ids = x_part.enriched(auto_id_bag)
    kd.testing.assert_equivalent(
        x_part_with_ids.foo_id,
        ds(['foo_1', 'foo_2', 'foo_3']),
    )

    x = x.updated(auto_id_bag)
    with self.assertRaisesRegex(
        ValueError,
        'AUTO_ID attribute foo_id with name foo already assigned to value',
    ):
      _ = kd_ext.ids.auto_id_update(x)  # pyrefly: ignore[missing-attribute]

  def test_auto_id_update_multiple_attributes(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
        bar_id=kd_ext.ids.auto_id('bar'),  # pyrefly: ignore[missing-attribute]
        also_foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x = kd.new(a=ds([1, 2, None, 2]), schema=schema)
    x_with_ids = x.enriched(kd_ext.ids.auto_id_update(x))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        x_with_ids.foo_id,
        ds(['foo_1', 'foo_2', 'foo_3', 'foo_4']),
    )
    kd.testing.assert_equivalent(
        x_with_ids.also_foo_id,
        ds(['foo_1', 'foo_2', 'foo_3', 'foo_4']),
    )
    kd.testing.assert_equivalent(
        x_with_ids.bar_id,
        ds(['bar_1', 'bar_2', 'bar_3', 'bar_4']),
    )

  def test_nested_schema(self):
    leaf_schema = kd.schema.new_schema(val=kd.INT32)
    leaf_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        leaf_schema,
        leaf_id=kd_ext.ids.auto_id('leaf'),  # pyrefly: ignore[missing-attribute]
    )
    mid_schema = kd.schema.new_schema(leaf=leaf_schema, x=kd.FLOAT32)
    mid_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        mid_schema,
        mid_id=kd_ext.ids.auto_id('mid'),  # pyrefly: ignore[missing-attribute]
    )
    root_schema = kd.schema.new_schema(mid=mid_schema)
    root_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        root_schema,
        root_id=kd_ext.ids.auto_id('root'),  # pyrefly: ignore[missing-attribute]
    )
    leaf = kd.new(val=ds([1, 2]), schema=leaf_schema)
    mid = kd.new(leaf=leaf, x=ds([1.0, 2.0]), schema=mid_schema)
    root = kd.new(mid=mid, schema=root_schema)
    root_with_ids = root.enriched(kd_ext.ids.auto_id_update(root))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        root_with_ids.root_id,
        ds(['root_1', 'root_2']),
    )
    kd.testing.assert_equivalent(
        root_with_ids.mid.mid_id,
        ds(['mid_1', 'mid_2']),
    )
    kd.testing.assert_equivalent(
        root_with_ids.mid.leaf.leaf_id,
        ds(['leaf_1', 'leaf_2']),
    )

  def test_shared_child_via_multiple_paths(self):
    child_schema = kd.schema.new_schema(val=kd.INT32)
    child_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        child_schema,
        child_id=kd_ext.ids.auto_id('child'),  # pyrefly: ignore[missing-attribute]
    )
    parent_schema = kd.schema.new_schema(
        left=child_schema,
        right=child_schema,
    )
    parent_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        parent_schema,
        parent_id=kd_ext.ids.auto_id('parent'),  # pyrefly: ignore[missing-attribute]
    )
    child = kd.new(val=ds([1, 2, 3, 4, 5]), schema=child_schema)
    # Both `left` and `right` point to the same child objects.
    parent = kd.new(
        left=(child & kd.mask([True, True, False, True, True])),
        right=(child & kd.mask([True, False, True, False, True])),
        schema=parent_schema,
    )
    parent_with_ids = parent.enriched(kd_ext.ids.auto_id_update(parent))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        parent_with_ids.parent_id,
        ds(['parent_1', 'parent_2', 'parent_3', 'parent_4', 'parent_5']),
    )
    # The child auto_id should be assigned once (same object reached twice).
    kd.testing.assert_equivalent(
        parent_with_ids.left.child_id,
        ds(['child_1', 'child_2', None, 'child_4', 'child_5']),
    )
    kd.testing.assert_equivalent(
        parent_with_ids.right.child_id,
        ds(['child_1', None, 'child_3', None, 'child_5']),
    )

  def test_shared_child_via_multiple_paths_conflict(self):
    left_schema = kd.schema.new_schema(val=kd.INT32)
    left_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        left_schema,
        child_id=kd_ext.ids.auto_id('left'),  # pyrefly: ignore[missing-attribute]
    )
    right_schema = kd.schema.new_schema(val=kd.INT32)
    right_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        right_schema,
        child_id=kd_ext.ids.auto_id('right'),  # pyrefly: ignore[missing-attribute]
    )
    parent_schema = kd.schema.new_schema(
        left=left_schema,
        right=right_schema,
    )
    parent_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        parent_schema,
        parent_id=kd_ext.ids.auto_id('parent'),  # pyrefly: ignore[missing-attribute]
    )
    child = kd.new(val=ds([1, 2, 3, 4, 5]))
    # Both `left` and `right` point to the same child objects.
    parent = kd.new(
        left=(child & kd.mask([True, True, False, True, True])).with_schema(
            left_schema
        ),
        right=(child & kd.mask([True, False, True, False, True])).with_schema(
            right_schema
        ),
        schema=parent_schema,
    )
    with self.assertRaisesRegex(
        ValueError,
        'kd_ext.ids.auto_id_update: object Entity:.* has AUTO_ID attribute'
        " child_id with name right already assigned to value 'left_1'",
    ):
      _ = parent.enriched(kd_ext.ids.auto_id_update(parent))  # pyrefly: ignore[missing-attribute]

  def test_sparse_nested_input(self):
    child_schema = kd.schema.new_schema(val=kd.INT32)
    child_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        child_schema,
        child_id=kd_ext.ids.auto_id('child'),  # pyrefly: ignore[missing-attribute]
    )
    parent_schema = kd.schema.new_schema(
        child=child_schema,
    )
    parent_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        parent_schema,
        parent_id=kd_ext.ids.auto_id('parent'),  # pyrefly: ignore[missing-attribute]
    )
    child = kd.new(val=ds([10, 20, 30]), schema=child_schema)
    # Parent has a missing child at index 1.
    parent = kd.new(
        child=ds([child.S[0], None, child.S[2]], schema=child_schema),
        schema=parent_schema,
    )
    parent_with_ids = parent.enriched(kd_ext.ids.auto_id_update(parent))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        parent_with_ids.parent_id,
        ds(['parent_1', 'parent_2', 'parent_3']),
    )
    # child_id is only assigned to present children.
    kd.testing.assert_equivalent(
        parent_with_ids.child.child_id,
        ds(['child_1', None, 'child_2']),
    )

  def test_scalar_input(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x = kd.new(a=42, schema=schema)
    x_with_ids = x.enriched(kd_ext.ids.auto_id_update(x))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        x_with_ids.foo_id,
        ds('foo_1'),
    )

  def test_no_auto_id_attributes(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    x = kd.new(a=ds([1, 2, 3]), schema=schema)
    # auto_id_update on a schema without auto attributes should be a no-op.
    auto_id_bag = kd_ext.ids.auto_id_update(x)  # pyrefly: ignore[missing-attribute]
    x_with_ids = x.enriched(auto_id_bag)
    kd.testing.assert_equivalent(x_with_ids.a, ds([1, 2, 3]))

  def test_empty_input(self):
    schema = kd.schema.new_schema(a=kd.INT32)
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x = kd.new(a=ds([], schema=kd.INT32), schema=schema)
    auto_id_bag = kd_ext.ids.auto_id_update(x)  # pyrefly: ignore[missing-attribute]
    x_with_ids = x.enriched(auto_id_bag)
    kd.testing.assert_equivalent(
        x_with_ids.foo_id,
        ds([], schema=kd.STRING),
    )

  def test_list_items_auto_id(self):
    item_schema = kd.schema.new_schema(val=kd.INT32)
    item_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        item_schema,
        item_id=kd_ext.ids.auto_id('item'),  # pyrefly: ignore[missing-attribute]
    )
    parent_schema = kd.schema.new_schema(
        items=kd.list_schema(item_schema),
    )
    items = kd.list([
        item_schema.new(val=10),
        item_schema.new(val=20),
        item_schema.new(val=30),
    ])
    parent = kd.new(items=items, schema=parent_schema)
    parent_with_ids = parent.enriched(kd_ext.ids.auto_id_update(parent))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        parent_with_ids.items[:].item_id,
        ds(['item_1', 'item_2', 'item_3']),
    )

  def test_dict_keys_auto_id(self):
    key_schema = kd.schema.new_schema(name=kd.STRING)
    key_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        key_schema,
        key_id=kd_ext.ids.auto_id('key'),  # pyrefly: ignore[missing-attribute]
    )
    parent_schema = kd.schema.new_schema(
        mapping=kd.dict_schema(key_schema, kd.INT32),
    )
    ks = key_schema.new(name=ds(['a', 'b']))
    mapping = kd.dict(
        {ks.S[0]: 1, ks.S[1]: 2},
        key_schema=key_schema,
        value_schema=kd.INT32,
    )
    parent = kd.new(mapping=mapping, schema=parent_schema)
    keys_with_ids = ks.enriched(kd_ext.ids.auto_id_update(parent))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        keys_with_ids.key_id,
        ds(['key_1', 'key_2']),
    )

  def test_dict_values_auto_id(self):
    value_schema = kd.schema.new_schema(data=kd.FLOAT32)
    value_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        value_schema,
        val_id=kd_ext.ids.auto_id('val'),  # pyrefly: ignore[missing-attribute]
    )
    parent_schema = kd.schema.new_schema(
        mapping=kd.dict_schema(kd.STRING, value_schema),
    )
    vs = value_schema.new(data=ds([1.0, 2.0, 3.0]))
    mapping = kd.dict(
        {'x': vs.S[0], 'y': vs.S[1], 'z': vs.S[2]},
        key_schema=kd.STRING,
        value_schema=value_schema,
    )
    parent = kd.new(mapping=mapping, schema=parent_schema)
    parent_with_ids = parent.enriched(kd_ext.ids.auto_id_update(parent))  # pyrefly: ignore[missing-attribute]
    assigned_ids = ds(
        [parent_with_ids.mapping[k].val_id for k in ['x', 'y', 'z']]
    )
    kd.testing.assert_equivalent(
        assigned_ids,
        ds(['val_1', 'val_2', 'val_3']),
    )

  def test_named_schema_attr_auto_id(self):
    inner_schema = kd.schema.new_schema(x=kd.INT32)
    inner_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        inner_schema,
        inner_id=kd_ext.ids.auto_id('inner'),  # pyrefly: ignore[missing-attribute]
    )
    outer_schema = kd.schema.named_schema('outer', child=inner_schema)
    outer_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        outer_schema,
        outer_id=kd_ext.ids.auto_id('outer'),  # pyrefly: ignore[missing-attribute]
    )
    children = kd.new(x=ds([10, 20, 30]), schema=inner_schema)
    x = kd.new(
        child=children,
        schema=outer_schema,
    )
    x_with_ids = x.enriched(kd_ext.ids.auto_id_update(x))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        x_with_ids.outer_id,
        ds(['outer_1', 'outer_2', 'outer_3']),
    )
    kd.testing.assert_equivalent(
        x_with_ids.child.inner_id,
        ds(['inner_1', 'inner_2', 'inner_3']),
    )


if __name__ == '__main__':
  absltest.main()
