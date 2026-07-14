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

ds = kd.slice  # pyrefly: ignore[missing-attribute]


class IdsAutoIdCleanupUpdateTest(parameterized.TestCase):

  def test_auto_id_cleanup_update(self):
    schema = kd.schema.new_schema(a=kd.INT32)  # pyrefly: ignore[missing-attribute]
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x = kd.new(a=ds([1, 2, 3]), schema=schema)  # pyrefly: ignore[missing-attribute]
    x = x.updated(kd_ext.ids.auto_id_update(x))  # pyrefly: ignore[missing-attribute]

    # Verify we have IDs
    kd.testing.assert_equivalent(x.foo_id, ds(['foo_1', 'foo_2', 'foo_3']))

    # Get cleanup update
    update_bag = kd_ext.ids.auto_id_cleanup_update(x)  # pyrefly: ignore[missing-attribute]
    x_clean = x.updated(update_bag)

    expected = kd.new(a=ds([1, 2, 3]))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(x_clean, expected, schemas_equality=False)

  def test_auto_id_cleanup_update_keep_other_metadata(self):
    schema = kd.schema.new_schema(a=kd.INT32)  # pyrefly: ignore[missing-attribute]
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    # Add other metadata manually.
    schema = kd.core.with_metadata(schema, other_meta=kd.schema.new_schema(val=kd.INT32))  # pyrefly: ignore[missing-attribute]

    x = kd.new(a=ds([1, 2, 3]), schema=schema)  # pyrefly: ignore[missing-attribute]
    x = x.updated(kd_ext.ids.auto_id_update(x))  # pyrefly: ignore[missing-attribute]

    # Get cleanup update
    update_bag = kd_ext.ids.auto_id_cleanup_update(x)  # pyrefly: ignore[missing-attribute]
    x_clean = x.updated(update_bag)

    # Verify foo_id is gone
    with self.assertRaisesRegex(
        AttributeError, "attribute 'foo_id' is missing"
    ):
      _ = x_clean.foo_id

    clean_schema = x_clean.get_schema()
    self.assertNotIn('foo_id', clean_schema.get_attr_names().to_py())

    # Verify other_meta is still in metadata
    metadata = clean_schema.maybe('__schema_metadata__')
    self.assertTrue(kd.has(metadata).to_py())  # pyrefly: ignore[missing-attribute]
    self.assertIn('other_meta', metadata.get_attr_names().to_py())
    self.assertNotIn('foo_id', metadata.get_attr_names().to_py())

  def test_cleanup_schema_only(self):
    schema = kd.schema.new_schema(a=kd.INT32)  # pyrefly: ignore[missing-attribute]
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    update_bag = kd_ext.ids.auto_id_cleanup_update(schema)  # pyrefly: ignore[missing-attribute]
    clean_schema = schema.updated(update_bag)

    # Verify foo_id is gone from schema
    self.assertNotIn('foo_id', clean_schema.get_attr_names().to_py())
    # Verify metadata is empty
    metadata = clean_schema.maybe('__schema_metadata__')
    self.assertEqual(metadata.get_attr_names().to_py(), [])

  def test_nested(self):
    leaf_schema = kd.schema.new_schema(val=kd.INT32)  # pyrefly: ignore[missing-attribute]
    leaf_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        leaf_schema,
        leaf_id=kd_ext.ids.auto_id('leaf'),  # pyrefly: ignore[missing-attribute]
    )
    root_schema = kd.schema.new_schema(leaf=leaf_schema)  # pyrefly: ignore[missing-attribute]
    root_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        root_schema,
        root_id=kd_ext.ids.auto_id('root'),  # pyrefly: ignore[missing-attribute]
    )
    leaf = kd.new(val=ds([1, 2]), schema=leaf_schema)  # pyrefly: ignore[missing-attribute]
    root = kd.new(leaf=leaf, schema=root_schema)  # pyrefly: ignore[missing-attribute]
    root = root.updated(kd_ext.ids.auto_id_update(root))  # pyrefly: ignore[missing-attribute]

    update_bag = kd_ext.ids.auto_id_cleanup_update(root)  # pyrefly: ignore[missing-attribute]
    root_clean = root.updated(update_bag)

    with self.assertRaisesRegex(
        AttributeError, "attribute 'root_id' is missing"
    ):
      _ = root_clean.root_id
    with self.assertRaisesRegex(
        AttributeError, "attribute 'leaf_id' is missing"
    ):
      _ = root_clean.leaf.leaf_id

    root_clean_schema = root_clean.get_schema()
    self.assertNotIn('root_id', root_clean_schema.get_attr_names().to_py())
    self.assertNotIn('leaf_id', root_clean_schema.leaf.get_attr_names().to_py())

  def test_nested_lists(self):
    list_item_schema = kd.schema.new_schema(val=kd.INT32)  # pyrefly: ignore[missing-attribute]
    list_item_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        list_item_schema,
        item_id=kd_ext.ids.auto_id('item'),  # pyrefly: ignore[missing-attribute]
    )
    root_schema = kd.schema.new_schema(  # pyrefly: ignore[missing-attribute]
        items=kd.list_schema(item_schema=list_item_schema)  # pyrefly: ignore[missing-attribute]
    )
    root_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        root_schema,
        root_id=kd_ext.ids.auto_id('root'),  # pyrefly: ignore[missing-attribute]
    )
    lists_exploded = kd.new(val=ds([[1, 2], [3, 4, 5]]), schema=list_item_schema)  # pyrefly: ignore[missing-attribute]
    lists = kd.implode(lists_exploded, ndim=1)  # pyrefly: ignore[missing-attribute]
    root = kd.new(items=lists, schema=root_schema)  # pyrefly: ignore[missing-attribute]
    root = root.updated(kd_ext.ids.auto_id_pointwise_update(root))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        root.items[:].item_id,
        ds([['item_1', 'item_2'], ['item_1', 'item_2', 'item_3']]),
    )
    kd.testing.assert_equivalent(root.root_id, ds(['root_1', 'root_1']))

    update_bag = kd_ext.ids.auto_id_cleanup_update(root)  # pyrefly: ignore[missing-attribute]
    root_clean = root.updated(update_bag)

    with self.assertRaisesRegex(
        AttributeError, "attribute 'root_id' is missing"
    ):
      _ = root_clean.root_id
    with self.assertRaisesRegex(
        AttributeError, "attribute 'item_id' is missing"
    ):
      _ = root_clean.items[:].item_id

    root_clean_schema = root_clean.get_schema()
    self.assertNotIn('root_id', root_clean_schema.get_attr_names().to_py())
    self.assertNotIn(
        'item_id',
        root_clean_schema.items.get_item_schema().get_attr_names().to_py()
    )

  def test_nested_dicts(self):
    value_schema = kd.schema.new_schema(val=kd.INT32)  # pyrefly: ignore[missing-attribute]
    value_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        value_schema,
        value_id=kd_ext.ids.auto_id('value'),  # pyrefly: ignore[missing-attribute]
    )
    dict_schema = kd.dict_schema(  # pyrefly: ignore[missing-attribute]
        key_schema=kd.STRING, value_schema=value_schema
    )
    root_schema = kd.schema.new_schema(values=dict_schema)  # pyrefly: ignore[missing-attribute]
    root_schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        root_schema,
        root_id=kd_ext.ids.auto_id('root'),  # pyrefly: ignore[missing-attribute]
    )
    dicts = kd.dict_shaped(  # pyrefly: ignore[missing-attribute]
        kd.shapes.new(2),  # pyrefly: ignore[missing-attribute]
        ds([['k1', 'k2'], ['k3', 'k4', 'k5']]),
        kd.new(val=ds([[1, 2], [3, 4, 5]]), schema=value_schema),  # pyrefly: ignore[missing-attribute]
        schema=dict_schema,
    )
    root = kd.new(values=dicts, schema=root_schema)  # pyrefly: ignore[missing-attribute]
    root = root.updated(kd_ext.ids.auto_id_pointwise_update(root))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        root.values[:].value_id,
        ds([['value_1', 'value_2'], ['value_1', 'value_2', 'value_3']]),
    )
    kd.testing.assert_equivalent(root.root_id, ds(['root_1', 'root_1']))

    update_bag = kd_ext.ids.auto_id_cleanup_update(root)  # pyrefly: ignore[missing-attribute]
    root_clean = root.updated(update_bag)

    with self.assertRaisesRegex(
        AttributeError, "attribute 'root_id' is missing"
    ):
      _ = root_clean.root_id
    with self.assertRaisesRegex(
        AttributeError, "attribute 'value_id' is missing"
    ):
      _ = root_clean.values[:].value_id

    root_clean_schema = root_clean.get_schema()
    self.assertNotIn('root_id', root_clean_schema.get_attr_names().to_py())
    self.assertNotIn(
        'value_id',
        root_clean_schema.values.get_value_schema().get_attr_names().to_py()
    )

  def test_auto_id_cleanup_update_set_again(self):
    schema = kd.schema.new_schema(a=kd.INT32)  # pyrefly: ignore[missing-attribute]
    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo'),  # pyrefly: ignore[missing-attribute]
    )
    x = kd.new(a=ds([1, 2, 3]), schema=schema)  # pyrefly: ignore[missing-attribute]
    x = x.enriched(kd_ext.ids.auto_id_update(x))  # pyrefly: ignore[missing-attribute]

    # Verify we have IDs
    kd.testing.assert_equivalent(x.foo_id, ds(['foo_1', 'foo_2', 'foo_3']))

    # Get cleanup update
    update_bag = kd_ext.ids.auto_id_cleanup_update(x)  # pyrefly: ignore[missing-attribute]
    x_clean = x.updated(update_bag)

    expected = kd.new(a=ds([1, 2, 3]))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(x_clean, expected, schemas_equality=False)

    schema = kd_ext.ids.with_auto_attributes(  # pyrefly: ignore[missing-attribute]
        schema,
        foo_id=kd_ext.ids.auto_id('foo')  # pyrefly: ignore[missing-attribute]
    )
    x_reset = x_clean.updated(kd.extract(schema).get_bag())  # pyrefly: ignore[missing-attribute]
    x_reset = x_reset.updated(kd_ext.ids.auto_id_update(x_reset))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        x_reset.foo_id, ds(['foo_1', 'foo_2', 'foo_3'])
    )

    # Get cleanup update again
    update_bag = kd_ext.ids.auto_id_cleanup_update(x_reset)  # pyrefly: ignore[missing-attribute]
    x_clean_again = x_reset.updated(update_bag)
    expected = kd.new(a=ds([1, 2, 3]))  # pyrefly: ignore[missing-attribute]
    kd.testing.assert_equivalent(
        x_clean_again, expected, schemas_equality=False
    )

if __name__ == '__main__':
  absltest.main()
