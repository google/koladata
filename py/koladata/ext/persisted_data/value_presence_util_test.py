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
from koladata import kd
from koladata.ext.persisted_data import value_presence_util


class ValuePresenceUtilTest(absltest.TestCase):

  def test_values_in_present_sentinels_list_are_indeed_present(self):
    for sentinel_value in value_presence_util._PRESENT_SENTINELS.values():
      self.assertTrue(kd.has(sentinel_value))

  def test_sentinels_list_converts_into_dict_without_key_collisions(self):
    self.assertEqual(
        len(value_presence_util._PRESENT_SENTINELS),
        len(value_presence_util._PRESENT_SENTINELS_LIST),
    )

  def test_get_present_sentinel_value_is_present_and_has_the_right_schema(self):
    for schema in [
        # For struct schemas:
        kd.dict_schema(kd.INT32, kd.INT64),
        kd.list_schema(kd.INT32),
        kd.named_schema('foo', bar=kd.INT32),
        kd.schema.new_schema(x=kd.INT32, y=kd.INT64, z=kd.named_schema('baz')),
        # For non-struct schemas:
        kd.INT32,
        kd.INT64,
        kd.FLOAT32,
        kd.FLOAT64,
        kd.STRING,
        kd.BYTES,
        kd.BOOLEAN,
        kd.MASK,
        kd.EXPR,
        kd.ITEMID,
        kd.SCHEMA,
    ]:
      sentinel_value = value_presence_util._get_present_sentinel_value(schema)
      self.assertEqual(sentinel_value.get_schema(), schema)
      self.assertTrue(kd.has(sentinel_value))

  def test_entity_attribute_masks(self):
    inner_schema = kd.named_schema('inner', z=kd.STRING)
    entity_schema = kd.named_schema('entity', x=kd.INT32, y=inner_schema)
    entity_ds = kd.slice([
        entity_schema.new(x=1, y=inner_schema.new(z='foo')),  # All present
        entity_schema.new(),  # Nothing present
        entity_schema.new(x=None, y=None),  # Both x and y are removed
        entity_schema.new(x=None, y=inner_schema.new(z=None)),  # Removed x,z
        None,
    ])
    kd.testing.assert_equivalent(
        value_presence_util.get_present_mask(entity_ds, 'x'),
        kd.slice([kd.present, kd.missing, kd.missing, kd.missing, kd.missing]),
    )
    kd.testing.assert_equivalent(
        value_presence_util.get_missing_mask(entity_ds, 'x'),
        kd.slice([kd.missing, kd.present, kd.missing, kd.missing, kd.missing]),
    )
    kd.testing.assert_equivalent(
        value_presence_util.get_removed_mask(entity_ds, 'x'),
        kd.slice([kd.missing, kd.missing, kd.present, kd.present, kd.missing]),
    )
    kd.testing.assert_equivalent(
        value_presence_util.get_present_mask(entity_ds, 'y'),
        kd.slice([kd.present, kd.missing, kd.missing, kd.present, kd.missing]),
    )
    kd.testing.assert_equivalent(
        value_presence_util.get_missing_mask(entity_ds, 'y'),
        kd.slice([kd.missing, kd.present, kd.missing, kd.missing, kd.missing]),
    )
    kd.testing.assert_equivalent(
        value_presence_util.get_removed_mask(entity_ds, 'y'),
        kd.slice([kd.missing, kd.missing, kd.present, kd.missing, kd.missing]),
    )

    inner_ds = entity_ds.y
    kd.testing.assert_equivalent(
        kd.has(inner_ds),
        kd.slice([kd.present, kd.missing, kd.missing, kd.present, kd.missing]),
    )
    kd.testing.assert_equivalent(
        inner_ds.z.no_bag(),
        kd.slice(['foo', None, None, None, None]),
    )
    kd.testing.assert_equivalent(
        value_presence_util.get_present_mask(inner_ds, 'z'),
        kd.slice([kd.present, kd.missing, kd.missing, kd.missing, kd.missing]),
    )
    kd.testing.assert_equivalent(
        value_presence_util.get_missing_mask(inner_ds, 'z'),
        kd.slice([kd.missing, kd.missing, kd.missing, kd.missing, kd.missing]),
    )
    kd.testing.assert_equivalent(
        value_presence_util.get_removed_mask(inner_ds, 'z'),
        kd.slice([kd.missing, kd.missing, kd.missing, kd.present, kd.missing]),
    )

  def test_entity_attribute_masks_raise_error_for_non_entity_schema(self):
    for mask_fn in [
        value_presence_util.get_present_mask,
        value_presence_util.get_missing_mask,
        value_presence_util.get_removed_mask,
    ]:
      for non_entity_slice in [
          kd.slice([1, 2, 3]),
          kd.slice([kd.list([1, 2, 3]), kd.list([4, 5, 6])]),
          kd.slice([kd.dict({1: 2}), kd.dict({3: 4})]),
      ]:
        with self.assertRaisesRegex(
            ValueError, 'does not have an entity schema'
        ):
          mask_fn(non_entity_slice, 'x')

  def test_entity_attribute_masks_raise_error_for_non_existent_attr(self):
    entity = kd.schema.new_schema(a=kd.INT32, b=kd.INT64).new
    named_entity = kd.named_schema('entity').new
    for mask_fn in [
        value_presence_util.get_present_mask,
        value_presence_util.get_missing_mask,
        value_presence_util.get_removed_mask,
    ]:
      for entity_ds in [
          kd.slice([entity(a=1, b=2), entity(a=5)]),
          kd.slice([named_entity(a=1, b=2), named_entity(a=6)]),
      ]:
        for attr_name in ['x', 'y']:
          with self.assertRaisesRegex(
              ValueError, f'does not have an attribute named "{attr_name}"'
          ):
            mask_fn(entity_ds, attr_name)

  def test_has_info_about_list_items(self):
    list_ds = kd.slice(
        [kd.list([], item_schema=kd.INT32), kd.list([5, 6]).no_bag()]
    )
    kd.testing.assert_equivalent(
        value_presence_util.has_info_about_list_items(list_ds),
        kd.slice([kd.present, kd.missing]),
    )

    list_ds = kd.slice(
        [kd.list([], item_schema=kd.NONE), kd.list([None, None]).no_bag()]
    )
    kd.testing.assert_equivalent(
        value_presence_util.has_info_about_list_items(list_ds),
        kd.slice([kd.present, kd.missing]),
    )

    entity_schema = kd.named_schema('entity', name=kd.STRING)
    list_ds = kd.slice([
        kd.list([entity_schema.new(name='foo')]),
        kd.list([entity_schema.new(name='bar')]).no_bag(),
        kd.list([None, None], item_schema=entity_schema),
    ])
    kd.testing.assert_equivalent(
        value_presence_util.has_info_about_list_items(list_ds),
        kd.slice([kd.present, kd.missing, kd.present]),
    )

  def test_has_info_about_list_items_raises_error_for_non_list_schema(self):
    for non_list_slice in [
        kd.slice([1, 2, 3]),
        kd.slice([kd.dict({1: 2}), kd.dict({3: 4})]),
        kd.slice([kd.new(x=1, y=2)]),
    ]:
      with self.assertRaisesRegex(ValueError, 'does not have a LIST schema'):
        value_presence_util.has_info_about_list_items(non_list_slice)


if __name__ == '__main__':
  absltest.main()
