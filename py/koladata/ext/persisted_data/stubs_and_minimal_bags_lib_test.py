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
from koladata.ext.persisted_data import stubs_and_minimal_bags_lib


class StubsAndMinimalBagsLibTest(absltest.TestCase):

  def test_function_schema_stub(self):
    for non_struct_schema in [
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
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.schema_stub(non_struct_schema),
          non_struct_schema.stub(),
      )

    # Struct schemas without names or metadata behave exactly like their .stub()
    # counterparts:
    for struct_schema in [
        kd.schema.new_schema(x=kd.INT32, y=kd.INT64),
        kd.schema.new_schema(x=kd.INT32, y=kd.INT64, z=kd.named_schema('foo')),
        kd.dict_schema(kd.INT32, kd.INT64),
        kd.dict_schema(kd.INT32, kd.named_schema('foo')),
        kd.list_schema(kd.INT32),
        kd.list_schema(kd.named_schema('foo')),
    ]:
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.schema_stub(struct_schema),
          struct_schema.stub(),
      )

      # If we the struct schema carries metadata, then schema_stub() will retain
      # it:
      struct_schema_with_metadata = struct_schema.enriched(
          kd.metadata(struct_schema, proto='my.proto.Message', version=123)
      )
      with self.assertRaises(AssertionError):
        kd.testing.assert_equivalent(
            stubs_and_minimal_bags_lib.schema_stub(struct_schema_with_metadata),
            struct_schema_with_metadata.stub(),
        )
      expected_schema_stub = (
          struct_schema_with_metadata.stub()
          .enriched(
              kd.attrs(
                  struct_schema_with_metadata,
                  **{
                      '__schema_metadata__': kd.core.get_metadata(
                          struct_schema_with_metadata
                      ),
                  }
              )
          )
          .extract()
      )
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.schema_stub(struct_schema_with_metadata),
          expected_schema_stub,
      )
      kd.testing.assert_equivalent(
          kd.core.get_metadata(expected_schema_stub).extract(),
          kd.core.get_metadata(struct_schema_with_metadata).extract(),
      )

    # Named schemas retain their names in schema_stub():
    struct_schema_with_name = kd.named_schema('foo', x=kd.INT32, y=kd.INT64)
    with self.assertRaises(AssertionError):
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.schema_stub(struct_schema_with_name),
          struct_schema_with_name.stub(),
      )
    kd.testing.assert_equivalent(
        stubs_and_minimal_bags_lib.schema_stub(struct_schema_with_name),
        # Notice that it retains only the name and not the attributes x, y or
        # their schemas:
        kd.named_schema('foo'),
    )
    # They also retain any associated metadata:
    struct_schema_with_name_and_metadata = struct_schema_with_name.enriched(
        kd.metadata(
            struct_schema_with_name, proto='my.proto.Message', version=123
        )
    )
    with self.assertRaises(AssertionError):
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.schema_stub(
              struct_schema_with_name_and_metadata
          ),
          struct_schema_with_name_and_metadata.stub(),
      )
    expected_schema_stub = (
        kd.named_schema('foo')
        .enriched(
            kd.attrs(
                kd.named_schema('foo'),
                **{
                    '__schema_metadata__': kd.core.get_metadata(
                        struct_schema_with_name_and_metadata
                    ),
                }
            )
        )
        .extract()
    )
    kd.testing.assert_equivalent(
        stubs_and_minimal_bags_lib.schema_stub(
            struct_schema_with_name_and_metadata
        ),
        expected_schema_stub,
    )
    kd.testing.assert_equivalent(
        kd.core.get_metadata(expected_schema_stub).extract(),
        kd.core.get_metadata(struct_schema_with_name_and_metadata).extract(),
    )

    with self.subTest('raises_for_non_primitive_metadata_attribute_value'):
      with self.assertRaisesRegex(
          ValueError,
          'schema .* has metadata attributes that are not primitives',
      ):
        stubs_and_minimal_bags_lib.schema_stub(
            struct_schema_with_name.enriched(
                kd.metadata(
                    struct_schema_with_name,
                    proto=kd.named_schema('proto', name=kd.STRING),
                )
            )
        )

  def test_minimal_bag_associating_list_with_its_items(self):
    item_schema = kd.named_schema('foo', bar=kd.INT32)

    empty_list = kd.list([], item_schema=item_schema)
    standard_list = kd.list([item_schema.new(bar=i) for i in range(3)])
    list_with_no_info_about_items = kd.list(
        [item_schema.new(bar=i + 5) for i in range(3)]
    ).no_bag()
    missing_list = None

    list_ds = kd.slice([
        empty_list,
        standard_list,
        list_with_no_info_about_items,
        missing_list,
    ])
    kd.testing.assert_equivalent(
        stubs_and_minimal_bags_lib.minimal_bag_associating_list_with_its_items(
            list_ds
        ),
        kd.bags.updated(
            empty_list[:]
            .no_bag()
            .implode(itemid=empty_list.get_itemid())
            .extract_update(),
            standard_list[:]
            .no_bag()
            .implode(itemid=standard_list.get_itemid())
            .extract_update(),
        ).merge_fallbacks(),
    )
    # At the time of writing, list_ds.stub().get_bag() would additionally
    # associate list_with_no_info_about_items with an empty list of items. That
    # is arguably a bug that should be fixed. But the next example shows that
    # even with that fixed, we still don't want to use list.stub() because its
    # bag can be far too large.

    # A nested list.
    inner_list = kd.list([1, 2, 3])
    nested_list = kd.list([inner_list])
    kd.testing.assert_equivalent(
        stubs_and_minimal_bags_lib.minimal_bag_associating_list_with_its_items(
            nested_list
        ),
        nested_list[:]
        .no_bag()
        .implode(itemid=nested_list.get_itemid())
        .extract_update(),
    )
    # Notice that nested_list.stub() here retains the full inner list:
    kd.testing.assert_equivalent(
        nested_list.stub(),
        nested_list,
    )
    self.assertEqual(nested_list.stub()[:].to_py(), [[1, 2, 3]])
    # In contrast to that, the minimal bag will not contain the items of the
    # inner list:
    self.assertEqual(
        nested_list.with_bag(
            stubs_and_minimal_bags_lib.minimal_bag_associating_list_with_its_items(
                nested_list
            )
        )[:].to_py(),
        [[]],
    )

    with self.subTest('minimal_bag_is_None'):
      self.assertIsNone(
          stubs_and_minimal_bags_lib.minimal_bag_associating_list_with_its_items(
              kd.slice(
                  [list_with_no_info_about_items, missing_list],
                  schema=kd.list_schema(item_schema),
              )
          )
      )

    with self.subTest('with_duplicate_list_itemids'):
      list_ds = kd.slice([
          standard_list,
          standard_list.no_bag(),
      ])
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.minimal_bag_associating_list_with_its_items(
              list_ds
          ),
          standard_list[:]
          .no_bag()
          .implode(itemid=standard_list.get_itemid())
          .extract_update(),
      )

    with self.subTest('with_items_that_are_schemas'):
      list_ds = kd.list([
          kd.schema.new_schema(x=kd.INT32),
          kd.INT32,
          kd.named_schema('foo', bar=kd.INT32),
      ])
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.minimal_bag_associating_list_with_its_items(
              list_ds
          ),
          list_ds.extract_update(),
      )

    with self.subTest('works_for_shared_lists_in_deep_dimensions'):
      shared_list = kd.list([1, 2])
      list_ds = kd.slice([[[shared_list], [shared_list]]])
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.minimal_bag_associating_list_with_its_items(
              list_ds
          ),
          shared_list.extract_update(),
      )

    with self.subTest('with_items_that_are_unsupported_schemas'):
      schema_item = kd.named_schema('foo', bar=kd.INT32)
      schema_item = kd.with_metadata(schema_item, my_bad=kd.new(z='gotcha!'))
      list_ds = kd.list([schema_item])
      with self.assertRaisesRegex(
          ValueError,
          'schema .* has metadata attributes that are not primitives',
      ):
        stubs_and_minimal_bags_lib.minimal_bag_associating_list_with_its_items(
            list_ds
        )

  def test_minimal_bag_associating_dict_with_its_keys_and_values(self):
    key_schema = kd.STRING
    value_schema = kd.named_schema('DictValue', bar=kd.INT32)
    dict_schema = kd.dict_schema(key_schema, value_schema)

    empty_dict = kd.dict({}, schema=dict_schema)
    value1 = value_schema.new(bar=1)
    value2 = value_schema.new(bar=2)
    standard_dict = kd.dict({'key1': value1, 'key2': value2})
    dict_with_no_info_about_keys_and_values = kd.dict(
        {'key3': value_schema.new(bar=3)}
    ).no_bag()
    missing_dict = None

    dict_ds = kd.slice([
        empty_dict,
        standard_dict,
        dict_with_no_info_about_keys_and_values,
        missing_dict,
    ])
    kd.testing.assert_equivalent(
        stubs_and_minimal_bags_lib.minimal_bag_associating_dict_with_its_keys_and_values(
            dict_ds
        ),
        kd.dicts.dict_update(
            standard_dict,
            kd.slice(['key1', 'key2']),
            kd.slice([value1.no_bag(), value2.no_bag()]),
        ),
    )

    with self.subTest('minimal_bag_is_None'):
      self.assertIsNone(
          stubs_and_minimal_bags_lib.minimal_bag_associating_dict_with_its_keys_and_values(
              kd.slice(
                  [
                      empty_dict,
                      dict_with_no_info_about_keys_and_values,
                      missing_dict,
                  ],
                  schema=dict_schema,
              )
          )
      )

    with self.subTest('with_duplicate_dict_itemids'):
      dict_ds = kd.slice([
          standard_dict,
          standard_dict.no_bag(),
      ])
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.minimal_bag_associating_dict_with_its_keys_and_values(
              dict_ds
          ),
          kd.dicts.dict_update(
              standard_dict,
              kd.slice(['key1', 'key2']),
              kd.slice([value1.no_bag(), value2.no_bag()]),
          ),
      )

    with self.subTest('works_for_shared_dicts_in_deep_dimensions'):
      value1 = kd.new(x=1, schema='foo')
      value2 = kd.new(y=2, schema='foo')
      shared_dict = kd.dict({'key1': value1, 'key2': value2})
      dict_ds = kd.slice([[[shared_dict], [shared_dict]]])
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.minimal_bag_associating_dict_with_its_keys_and_values(
              dict_ds
          ),
          kd.dicts.dict_update(
              shared_dict,
              kd.slice(['key1', 'key2']),
              kd.slice([value1.no_bag(), value2.no_bag()]),
          ),
      )

    with self.subTest('with_values_that_are_schemas'):
      dict_ds = kd.dict({
          'key1': kd.schema.new_schema(x=kd.INT32),
          'key2': kd.INT32,
          'key3': kd.named_schema('foo', bar=kd.INT32),
      })
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.minimal_bag_associating_dict_with_its_keys_and_values(
              dict_ds
          ),
          dict_ds.extract_update(),
      )

    with self.subTest('with_values_that_are_unsupported_schemas'):
      schema_item = kd.named_schema('DictValue', bar=kd.INT32)
      schema_item = kd.with_metadata(schema_item, my_bad=kd.new())
      dict_ds = kd.dict({'key1': schema_item})
      with self.assertRaisesRegex(
          ValueError,
          'schema .* has metadata attributes that are not primitives',
      ):
        stubs_and_minimal_bags_lib.minimal_bag_associating_dict_with_its_keys_and_values(
            dict_ds
        )

  def test_minimal_bag_associating_entity_with_its_attr_value(self):
    y_schema = kd.named_schema('Y', z=kd.INT32)
    entity_schema = kd.named_schema('Entity', x=kd.INT32, y=y_schema)

    empty_entity = entity_schema.new()
    y_value = y_schema.new(z=2)
    standard_entity = entity_schema.new(x=1, y=y_value)
    entity_with_no_info_about_attr_values = entity_schema.new(
        x=3, y=y_schema.new(z=4)
    ).no_bag()
    missing_entity = None

    entity_ds = kd.slice([
        empty_entity,
        standard_entity,
        entity_with_no_info_about_attr_values,
        missing_entity,
    ])
    kd.testing.assert_equivalent(
        stubs_and_minimal_bags_lib.minimal_bag_associating_entity_with_its_attr_value(
            entity_ds, 'x'
        ),
        kd.attrs(standard_entity, x=1),
    )
    kd.testing.assert_equivalent(
        stubs_and_minimal_bags_lib.minimal_bag_associating_entity_with_its_attr_value(
            entity_ds, 'y'
        ),
        kd.attrs(standard_entity, y=y_value.no_bag()),
    )

    with self.subTest('minimal_bag_is_None'):
      for attr_name in ['x', 'y']:
        self.assertIsNone(
            stubs_and_minimal_bags_lib.minimal_bag_associating_entity_with_its_attr_value(
                kd.slice([
                    empty_entity,
                    entity_with_no_info_about_attr_values,
                    missing_entity,
                ]),
                attr_name,
            )
        )

    with self.subTest('with_duplicate_entity_itemids'):
      entity_ds = kd.slice([
          standard_entity,
          standard_entity.no_bag(),
      ])
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.minimal_bag_associating_entity_with_its_attr_value(
              entity_ds, 'x'
          ),
          kd.attrs(standard_entity, x=1),
      )
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.minimal_bag_associating_entity_with_its_attr_value(
              entity_ds, 'y'
          ),
          kd.attrs(standard_entity, y=y_value.no_bag()),
      )

    with self.subTest('with_attr_values_that_are_schemas'):
      entity_schema = kd.schema.new_schema(s=kd.SCHEMA)

      recursive_schema = kd.named_schema(
          'TreeNode',
          x=kd.INT32,
      )
      recursive_schema = recursive_schema.with_attrs(
          children=kd.list_schema(recursive_schema)
      )
      recursive_schema = recursive_schema.enriched(
          kd.metadata(recursive_schema, proto='my.proto.TreeNode', version=123),
          kd.metadata(
              recursive_schema.children,
              item_proto='my.proto.TreeNodeList',
              version=456,
          ),
      )

      entity_ds = kd.slice([
          entity_schema.new(),
          entity_schema.new(s=kd.schema.new_schema(x=kd.INT32)),
          entity_schema.new(s=kd.INT32),
          entity_schema.new(s=kd.dict_schema(kd.INT32, kd.INT64)),
          entity_schema.new(s=kd.named_schema('foo', bar=kd.INT32)),
          entity_schema.new(s=kd.INT32).no_bag(),
          entity_schema.new(s=recursive_schema),
          None,
      ])
      kd.testing.assert_equivalent(
          stubs_and_minimal_bags_lib.minimal_bag_associating_entity_with_its_attr_value(
              entity_ds, 's'
          ),
          entity_ds.extract_update(),
      )

    with self.subTest('with_attr_values_that_are_unsupported_schemas'):
      schema_item = kd.named_schema('Entity', bar=kd.INT32)
      schema_item = kd.with_metadata(schema_item, my_bad=kd.new())
      entity = kd.new(bar=schema_item)
      with self.assertRaisesRegex(
          ValueError,
          'schema .* has metadata attributes that are not primitives',
      ):
        stubs_and_minimal_bags_lib.minimal_bag_associating_entity_with_its_attr_value(
            entity, 'bar'
        )

      recursive_schema = recursive_schema.enriched(
          kd.metadata(
              recursive_schema.children,
              non_primitive_metadata_attribute=kd.new(x=1),
          ),
      )
      entity_ds = kd.slice([entity_schema.new(s=recursive_schema)])
      with self.assertRaisesRegex(
          ValueError,
          'has metadata attributes that are not primitives',
      ):
        stubs_and_minimal_bags_lib.minimal_bag_associating_entity_with_its_attr_value(
            entity_ds, 's'
        )


if __name__ == '__main__':
  absltest.main()
