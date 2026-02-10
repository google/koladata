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

import os
import re

from absl.testing import absltest
from koladata import kd
from koladata.ext.persisted_data import bare_root_initial_data_manager
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import fs_implementation
from koladata.ext.persisted_data import schema_helper

BareRootInitialDataManager = (
    bare_root_initial_data_manager.BareRootInitialDataManager
)


class BareRootInitialDataManagerTest(absltest.TestCase):

  def test_get_id(self):
    self.assertEqual(
        BareRootInitialDataManager.get_id(),
        'BareRootInitialDataManager',
    )

  def test_serialization_and_deserialization_roundtrip(self):
    for root_item in [kd.new(), kd.uu(), kd.new(schema='root_schema')]:
      manager = BareRootInitialDataManager.create_new(root_item)
      kd.testing.assert_equivalent(
          manager.get_schema(), root_item.get_schema(), ids_equality=True
      )
      self.assertLen(manager.get_all_schema_node_names(), 1)
      self.assertEqual(
          manager.get_all_schema_node_names(),
          schema_helper.SchemaHelper(
              manager.get_schema()
          ).get_all_schema_node_names(),
      )
      kd.testing.assert_equivalent(
          manager.get_data_slice_for_schema_node_names(schema_node_names=set()),
          root_item,
          ids_equality=True,
      )

      persistence_dir = self.create_tempdir().full_path
      manager.serialize(
          persistence_dir, fs=fs_implementation.FileSystemInteraction()
      )

      # New managers initialized from the pre-populated persistence directory
      # also use the user-provided root item.
      new_manager = BareRootInitialDataManager.deserialize(
          persistence_dir, fs=fs_implementation.FileSystemInteraction()
      )
      kd.testing.assert_equivalent(
          new_manager.get_schema(), root_item.get_schema(), ids_equality=True
      )
      kd.testing.assert_equivalent(
          new_manager.get_data_slice_for_schema_node_names(
              schema_node_names=set()
          ),
          root_item,
          ids_equality=True,
      )

  def test_superfluous_data_is_removed_from_provided_root_item(self):
    superfluous_data = kd.new(x=1, y=2).get_bag()
    for item in [kd.new(), kd.uu(), kd.new(schema='root_schema')]:
      manager = BareRootInitialDataManager.create_new(
          item.enriched(superfluous_data)
      )
      kd.testing.assert_equivalent(
          manager.get_schema(), item.get_schema(), ids_equality=True
      )
      root = manager.get_data_slice_for_schema_node_names(
          schema_node_names=set()
      )
      kd.testing.assert_equivalent(
          root,
          item,
          ids_equality=True,
      )
      kd.testing.assert_equivalent(root.get_bag(), item.get_bag())

  def test_initialization_error_messages(self):

    with self.assertRaisesRegex(
        ValueError,
        re.escape('the root must be a scalar, i.e. a DataItem. Got: [1, 2, 3]'),
    ):
      BareRootInitialDataManager.create_new(kd.slice([1, 2, 3]))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('the root must be present. Got: None'),
    ):
      BareRootInitialDataManager.create_new(
          kd.item(None, schema=kd.schema.new_schema())
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape('the root must have an entity schema. Got: List[1, 2, 3]'),
    ):
      BareRootInitialDataManager.create_new(kd.list([1, 2, 3]))

    with self.assertRaisesRegex(
        ValueError,
        re.escape('the root must not have any attributes. Got: Entity(x=2)'),
    ):
      BareRootInitialDataManager.create_new(kd.new(x=2))

    root = kd.new()
    root_schema = kd.with_metadata(root.get_schema(), foo=kd.list([1, 2, 3]))
    root = root.with_schema(root_schema)
    with self.assertRaisesRegex(
        ValueError,
        'schema .* has metadata attributes that are not primitives',
    ):
      BareRootInitialDataManager.create_new(root)

  def test_get_data_slice_with_invalid_schema_node_names_raises_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape("schema_node_names contains invalid entries: {'hohoho!'}"),
    ):
      BareRootInitialDataManager.create_new().get_data_slice_for_schema_node_names(
          schema_node_names=['hohoho!']
      )

  def test_serialization_with_non_empty_dir_raises_error(self):
    manager = BareRootInitialDataManager.create_new()

    persistence_dir = self.create_tempdir().full_path
    with open(os.path.join(persistence_dir, 'some_file'), 'w') as f:
      f.write('some content')

    with self.assertRaisesRegex(
        ValueError,
        re.escape(f'the given persistence_dir {persistence_dir} is not empty'),
    ):
      manager.serialize(
          persistence_dir, fs=fs_implementation.FileSystemInteraction()
      )

  def test_deserialization_with_non_existing_dir_or_file_raises_error(self):
    persistence_dir = self.create_tempdir().full_path
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            f'file not found: {os.path.join(persistence_dir, "root.kd")}'
        ),
    ):
      BareRootInitialDataManager.deserialize(
          persistence_dir, fs=fs_implementation.FileSystemInteraction()
      )

    non_existing_dir = os.path.join(persistence_dir, 'non_existing_dir')
    with self.assertRaisesRegex(
        ValueError,
        re.escape(f'persistence_dir not found: {non_existing_dir}'),
    ):
      BareRootInitialDataManager.deserialize(
          non_existing_dir, fs=fs_implementation.FileSystemInteraction()
      )

  def test_root_with_itemid_minted_for_a_list(self):
    # This example is somewhat contrived. It documents the current behavior of
    # Koda, but if that should change, this test can be updated or even removed,
    # because it is not a core feature of the BareRootInitialDataManager, i.e.
    # the vast majority of users will never do something like this.

    s0 = kd.list([1, 2, 3])
    s2 = kd.dict({1: 2})
    s3 = s0.no_bag().with_bag(s2.get_bag())

    # s3's bag is not empty, but it has no data associated with s3's itemid.
    # As a result, Koda treats s3 not as a list, but as an entity with no
    # attributes.
    self.assertFalse(s3.is_list())
    self.assertTrue(s3.get_schema().is_entity_schema())
    self.assertTrue(s3.is_entity())
    self.assertEmpty(kd.dir(s3.get_schema()))
    self.assertEmpty(kd.dir(s3))

    # Every entity with no attributes can be passed as a bare root item.
    manager = BareRootInitialDataManager.create_new(s3)
    # The BareRootInitialDataManager should call s3.extract() to remove the
    # unreferenced data from the bag. The resulting bag is empty, because there
    # is no data associated with s3's itemid:
    expected_root = s3.with_bag(kd.bag())
    kd.testing.assert_equivalent(
        manager.get_data_slice_for_schema_node_names(schema_node_names=set()),
        expected_root,
        ids_equality=True,
    )

    # The root item behaves like any other entity, i.e. we can add attributes to
    # it. Koda knows that its itemid was minted for a list, but that does not
    # affect the behavior at all.
    s4 = expected_root.with_attrs(x='foo')
    kd.testing.assert_equivalent(
        s4.get_schema(),
        kd.schema.new_schema(x=kd.STRING),
    )

  def test_clear_cache_is_noop(self):
    manager = BareRootInitialDataManager.create_new()
    root = manager.get_data_slice_for_schema_node_names(schema_node_names=set())
    manager.clear_cache()
    root_after_clear_cache = manager.get_data_slice_for_schema_node_names(
        schema_node_names=set()
    )
    kd.testing.assert_equivalent(
        root, root_after_clear_cache, ids_equality=True
    )

  def test_get_description(self):
    manager = BareRootInitialDataManager.create_new()
    self.assertEqual(manager.get_description(), 'an empty root')

  def test_exists(self):
    manager = BareRootInitialDataManager.create_new()
    self.assertTrue(
        manager.exists(data_slice_path_lib.DataSlicePath.parse_from_string(''))
    )
    self.assertFalse(
        manager.exists(
            data_slice_path_lib.DataSlicePath.parse_from_string('.foo')
        )
    )

  def test_get_data_slice(self):
    manager = BareRootInitialDataManager.create_new()
    root = manager.get_data_slice(
        populate=[
            data_slice_path_lib.DataSlicePath.parse_from_string(''),
        ],
        populate_including_descendants=[
            data_slice_path_lib.DataSlicePath.parse_from_string(''),
        ],
    )
    kd.testing.assert_equivalent(root, kd.new(), schemas_equality=False)

    with self.subTest('with_invalid_path_to_populate'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              "data slice path '.abcde' passed in argument 'populate' is"
              ' invalid'
          ),
      ):
        manager.get_data_slice(
            populate=[
                data_slice_path_lib.DataSlicePath.parse_from_string('.abcde'),
            ],
            populate_including_descendants=[
                data_slice_path_lib.DataSlicePath.parse_from_string(''),
            ],
        )

    with self.subTest('with_invalid_path_to_populate_including_descendants'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              "data slice path '.abcde' passed in argument"
              " 'populate_including_descendants' is invalid"
          ),
      ):
        manager.get_data_slice(
            populate=[
                data_slice_path_lib.DataSlicePath.parse_from_string(''),
            ],
            populate_including_descendants=[
                data_slice_path_lib.DataSlicePath.parse_from_string('.abcde'),
            ],
        )

  def test_get_data_slice_at(self):
    manager = BareRootInitialDataManager.create_new()
    root = manager.get_data_slice_at(
        data_slice_path_lib.DataSlicePath.parse_from_string(''),
    )
    kd.testing.assert_equivalent(root, kd.new(), schemas_equality=False)

    root = manager.get_data_slice_at(
        data_slice_path_lib.DataSlicePath.parse_from_string(''),
        with_all_descendants=True,
    )
    kd.testing.assert_equivalent(root, kd.new(), schemas_equality=False)

    with self.subTest('with_invalid_path'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              "data slice path '.abcde' passed in argument 'populate' is"
              ' invalid'
          ),
      ):
        manager.get_data_slice_at(
            data_slice_path_lib.DataSlicePath.parse_from_string('.abcde'),
        )

    with self.subTest('with_invalid_path_with_all_descendants'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              "data slice path '.abcde' passed in argument"
              " 'populate_including_descendants' is invalid"
          ),
      ):
        manager.get_data_slice_at(
            data_slice_path_lib.DataSlicePath.parse_from_string('.abcde'),
            with_all_descendants=True,
        )

  def test_copy(self):
    manager = BareRootInitialDataManager.create_new()
    copy = manager.copy()
    # The _root_item is shared. It's immutable.
    self.assertIs(copy._root_item, manager._root_item)
    self.assertIs(copy._schema_helper, manager._schema_helper)

  def test_get_schema_at(self):
    root_schema = kd.named_schema('root_schema')
    manager = BareRootInitialDataManager.create_new(root_schema.new())
    kd.testing.assert_equivalent(
        manager.get_schema_at(
            data_slice_path_lib.DataSlicePath.parse_from_string('')
        ),
        root_schema,
        ids_equality=True,
    )

    manager = BareRootInitialDataManager.create_new()
    kd.testing.assert_equivalent(
        manager.get_schema_at(
            data_slice_path_lib.DataSlicePath.parse_from_string('')
        ),
        kd.schema.new_schema(),
    )

  def test_get_data_bag_for_schema_node_names(self):
    manager = BareRootInitialDataManager.create_new()

    kd.testing.assert_equivalent(
        manager.get_data_bag_for_schema_node_names([]),
        kd.bag(),
    )

    all_snns = manager.get_all_schema_node_names()
    self.assertLen(all_snns, 1)
    kd.testing.assert_equivalent(
        manager.get_data_bag_for_schema_node_names(all_snns),
        manager.get_data_slice_for_schema_node_names(all_snns).get_bag(),
    )

    with self.subTest('with_invalid_schema_node_names'):
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              "schema_node_names contains invalid entries: {'hohoho!'}"
          ),
      ):
        manager.get_data_bag_for_schema_node_names(['hohoho!'])


if __name__ == '__main__':
  absltest.main()
