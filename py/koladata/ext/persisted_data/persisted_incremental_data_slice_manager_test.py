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
import shutil
from typing import Any
from typing import Generator
from unittest import mock
from absl.testing import absltest
from absl.testing import parameterized
from koladata import kd
from koladata.ext.persisted_data import data_slice_manager_interface
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import fs_implementation
from koladata.ext.persisted_data import persisted_incremental_data_bag_manager
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager_metadata_pb2 as metadata_pb2
from koladata.ext.persisted_data import schema_helper as schema_helper_lib
from koladata.ext.persisted_data import simple_in_memory_data_slice_manager
from koladata.ext.persisted_data import test_only_schema_node_name_helper


PersistedIncrementalDataBagManager = (
    persisted_incremental_data_bag_manager.PersistedIncrementalDataBagManager
)

parse_dsp = data_slice_path_lib.DataSlicePath.parse_from_string
GetAttr = data_slice_path_lib.GetAttr

schema_node_name = test_only_schema_node_name_helper.schema_node_name

PersistedIncrementalDataSliceManager = (
    persisted_incremental_data_slice_manager.PersistedIncrementalDataSliceManager
)
SimpleInMemoryDataSliceManager = (
    simple_in_memory_data_slice_manager.SimpleInMemoryDataSliceManager
)
DataSliceManager = (
    PersistedIncrementalDataSliceManager | SimpleInMemoryDataSliceManager
)


def get_loaded_schema_node_names(
    manager: PersistedIncrementalDataSliceManager,
) -> set[str]:
  """Returns the loaded schema node names of the given manager."""
  loaded_bag_names = manager._data_bag_manager.get_loaded_bag_names()
  return {
      snn
      for snn, bag_names in manager._get_schema_node_name_to_data_bag_names()
      .to_py(max_depth=-1)
      .items()
      if all(bag_name in loaded_bag_names for bag_name in bag_names)
  }


def get_loaded_root_dataslice(manager: DataSliceManager) -> kd.types.DataSlice:
  """Returns the loaded root dataslice of the given manager."""
  if isinstance(manager, SimpleInMemoryDataSliceManager):
    return manager.get_data_slice()

  schema_bag = manager._schema_helper.get_schema_bag(
      get_loaded_schema_node_names(manager),
  )
  return manager._root_dataslice.updated(
      manager._data_bag_manager.get_loaded_bag()
  ).updated(schema_bag)


def get_loaded_schema(manager: DataSliceManager) -> kd.types.DataSlice:
  """Returns the schema of the loaded part of the DataSlice."""
  return get_loaded_root_dataslice(manager).get_schema()


def generate_loaded_data_slice_paths(
    manager: DataSliceManager, *, max_depth: int
) -> Generator[data_slice_path_lib.DataSlicePath, None, None]:
  """Yields all loaded data slice paths.

  The paths are induced by the loaded schema, i.e.
  self.get_loaded_schema().

  Args:
    manager: The manager whose loaded schema is used to generate the paths.
    max_depth: The maximum depth of the paths to yield. If negative, then no
      paths are yielded. If zero, then only the root path is yielded. If
      positive, then the root path and all its descendants up to the maximum
      depth are yielded. Recursive schemas typically have an infinite number of
      paths, so it is necessary to impose a limit on the depth.

  Yields:
    All loaded data slice paths.
  """
  yield from data_slice_path_lib.generate_data_slice_paths_for_arbitrary_data_slice_with_schema(
      get_loaded_schema(manager), max_depth=max_depth
  )


def is_loaded_data_slice_path(
    manager: DataSliceManager,
    path: data_slice_path_lib.DataSlicePath,
) -> bool:
  """Returns whether the given data slice path is loaded."""
  return schema_helper_lib.SchemaHelper(
      get_loaded_schema(manager)
  ).is_valid_data_slice_path(path)


class PersistedIncrementalDataSliceManagerTest(parameterized.TestCase):

  def new_manager(self, dsm_class: Any) -> DataSliceManager:
    if dsm_class == PersistedIncrementalDataSliceManager:
      persistence_dir = self.create_tempdir().full_path
      return PersistedIncrementalDataSliceManager(persistence_dir)

    assert dsm_class == SimpleInMemoryDataSliceManager
    return SimpleInMemoryDataSliceManager()

  def copy_manager(self, manager: DataSliceManager) -> DataSliceManager:
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      new_persistence_dir = os.path.join(self.create_tempdir().full_path, 'new')
      shutil.copytree(manager._persistence_dir, new_persistence_dir)  # pylint: disable=protected-access
      new_manager = PersistedIncrementalDataSliceManager(new_persistence_dir)
      return new_manager

    assert isinstance(manager, SimpleInMemoryDataSliceManager)
    new_manager = SimpleInMemoryDataSliceManager()
    new_manager._ds = manager._ds  # pylint: disable=protected-access
    return new_manager

  def assert_manager_available_data_slice_paths(
      self,
      manager: DataSliceManager,
      expected_available_data_slice_paths: set[str],
      *,
      max_depth_of_data_slice_paths: int = 5,
  ):
    actual_available_data_slice_paths = set(
        manager.generate_paths(max_depth=max_depth_of_data_slice_paths)
    )
    self.assertEqual(
        len(actual_available_data_slice_paths),
        len(expected_available_data_slice_paths),
    )
    expected_available_data_slice_paths = set(
        data_slice_path_lib.DataSlicePath.parse_from_string(s)
        for s in expected_available_data_slice_paths
    )
    self.assertEqual(
        actual_available_data_slice_paths,
        expected_available_data_slice_paths,
    )

  def assert_manager_loaded_data_slice_paths(
      self,
      manager: DataSliceManager,
      expected_loaded_data_slice_paths: set[str],
      *,
      max_depth_of_data_slice_paths: int = 5,
  ):
    actual_loaded_data_slice_paths = set(
        generate_loaded_data_slice_paths(
            manager, max_depth=max_depth_of_data_slice_paths
        )
    )
    parsed_expected_loaded_data_slice_paths = set(
        data_slice_path_lib.DataSlicePath.parse_from_string(s)
        for s in expected_loaded_data_slice_paths
    )
    self.assertEqual(
        len(parsed_expected_loaded_data_slice_paths),
        len(expected_loaded_data_slice_paths),
    )
    self.assertEqual(
        actual_loaded_data_slice_paths,
        parsed_expected_loaded_data_slice_paths,
    )

  def assert_manager_schema_node_names_to_num_bags(
      self,
      manager: DataSliceManager,
      expected_schema_node_names_to_num_bags: list[tuple[str, int]],
  ):
    if isinstance(manager, SimpleInMemoryDataSliceManager):
      return
    actual = {
        snn: len(bag_names)
        for snn, bag_names in manager._get_schema_node_name_to_data_bag_names()
        .to_py(max_depth=-1)
        .items()
    }
    expected = dict(expected_schema_node_names_to_num_bags)
    self.assertEqual(len(expected), len(expected_schema_node_names_to_num_bags))
    self.assertEqual(actual, expected)

  def assert_manager_loaded_root_dataslice_pytree(
      self,
      manager: DataSliceManager,
      expected_current_root_dataslice_pytree: Any,
  ):
    self.assertEqual(
        kd.to_pytree(get_loaded_root_dataslice(manager), max_depth=-1),
        expected_current_root_dataslice_pytree,
    )

  def assert_manager_state(
      self,
      manager: DataSliceManager,
      *,
      available_data_slice_paths: set[str],
      loaded_data_slice_paths: set[str],
      expected_loaded_root_dataslice_pytree: Any,
      max_depth_of_data_slice_paths: int = 5,
  ):
    self.assert_manager_available_data_slice_paths(
        manager,
        available_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )
    self.assert_manager_loaded_data_slice_paths(
        manager,
        loaded_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )
    self.assert_manager_loaded_root_dataslice_pytree(
        manager, expected_loaded_root_dataslice_pytree
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_basic_usage(self, dsm_class):
    manager = self.new_manager(dsm_class)
    self.assert_manager_state(
        manager,
        available_data_slice_paths={''},
        loaded_data_slice_paths={''},
        expected_loaded_root_dataslice_pytree={},
    )
    # Add some queries with only query_id populated.
    query_schema = kd.named_schema('query')
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(query_id='q1'),
            query_schema.new(query_id='q2'),
        ]),
    )

    self.assert_manager_state(
        manager,
        available_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
        },
        loaded_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
        },
        expected_loaded_root_dataslice_pytree={
            'query': [
                {'query_id': 'q1'},
                {'query_id': 'q2'},
            ],
        },
    )

    # Add some docs with only doc_id populated.
    doc_schema = kd.named_schema('doc')
    manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='doc',
        attr_value=kd.slice([
            doc_schema.new(doc_id=kd.slice([0, 1, 2, 3])).implode(),
            doc_schema.new(doc_id=kd.slice([4, 5, 6])).implode(),
        ]),
    )
    self.assert_manager_state(
        manager,
        available_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
            '.query[:].doc',
            '.query[:].doc[:]',
            '.query[:].doc[:].doc_id',
        },
        loaded_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
            '.query[:].doc',
            '.query[:].doc[:]',
            '.query[:].doc[:].doc_id',
        },
        expected_loaded_root_dataslice_pytree={
            'query': [
                {
                    'query_id': 'q1',
                    'doc': [
                        {'doc_id': 0},
                        {'doc_id': 1},
                        {'doc_id': 2},
                        {'doc_id': 3},
                    ],
                },
                {
                    'query_id': 'q2',
                    'doc': [
                        {'doc_id': 4},
                        {'doc_id': 5},
                        {'doc_id': 6},
                    ],
                },
            ],
        },
    )

    # Add the query_text feature.
    manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='query_text',
        attr_value=kd.slice(
            ['How tall is Obama', 'How high is the Eiffel tower']
        ),
    )
    self.assert_manager_state(
        manager,
        available_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
            '.query[:].doc',
            '.query[:].doc[:]',
            '.query[:].doc[:].doc_id',
            '.query[:].query_text',
        },
        loaded_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
            '.query[:].doc',
            '.query[:].doc[:]',
            '.query[:].doc[:].doc_id',
            '.query[:].query_text',
        },
        expected_loaded_root_dataslice_pytree={
            'query': [
                {
                    'query_id': 'q1',
                    'doc': [
                        {'doc_id': 0},
                        {'doc_id': 1},
                        {'doc_id': 2},
                        {'doc_id': 3},
                    ],
                    'query_text': 'How tall is Obama',
                },
                {
                    'query_id': 'q2',
                    'doc': [
                        {'doc_id': 4},
                        {'doc_id': 5},
                        {'doc_id': 6},
                    ],
                    'query_text': 'How high is the Eiffel tower',
                },
            ],
        },
    )
    # Add the doc_title feature.
    manager.update(
        at_path=parse_dsp('.query[:].doc[:]'),
        attr_name='doc_title',
        attr_value=kd.slice([
            ['title0', 'title1', 'title2', None],
            ['title4', 'title5', 'title6'],
        ]),
    )
    self.assert_manager_state(
        manager,
        available_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
            '.query[:].doc',
            '.query[:].doc[:]',
            '.query[:].doc[:].doc_id',
            '.query[:].doc[:].doc_title',
            '.query[:].query_text',
        },
        loaded_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
            '.query[:].doc',
            '.query[:].doc[:]',
            '.query[:].doc[:].doc_id',
            '.query[:].doc[:].doc_title',
            '.query[:].query_text',
        },
        expected_loaded_root_dataslice_pytree={
            'query': [
                {
                    'query_id': 'q1',
                    'doc': [
                        {'doc_id': 0, 'doc_title': 'title0'},
                        {'doc_id': 1, 'doc_title': 'title1'},
                        {'doc_id': 2, 'doc_title': 'title2'},
                        {'doc_id': 3, 'doc_title': None},
                    ],
                    'query_text': 'How tall is Obama',
                },
                {
                    'query_id': 'q2',
                    'doc': [
                        {'doc_id': 4, 'doc_title': 'title4'},
                        {'doc_id': 5, 'doc_title': 'title5'},
                        {'doc_id': 6, 'doc_title': 'title6'},
                    ],
                    'query_text': 'How high is the Eiffel tower',
                },
            ],
        },
    )
    # We can request the dataslice at a specific data path.
    self.assertEqual(
        kd.to_pytree(
            manager.get_data_slice_at(parse_dsp('.query[:].doc[:].doc_title')),
            max_depth=-1,
        ),
        [['title0', 'title1', 'title2', None], ['title4', 'title5', 'title6']],
    )

    # Some time later, in another process...
    #
    # Start a new manager from the persistence dir.
    manager = self.copy_manager(manager)
    all_available_data_slice_paths = {
        '',
        '.query',
        '.query[:]',
        '.query[:].query_id',
        '.query[:].doc',
        '.query[:].doc[:]',
        '.query[:].doc[:].doc_id',
        '.query[:].doc[:].doc_title',
        '.query[:].query_text',
    }
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      self.assert_manager_state(
          manager,
          available_data_slice_paths=all_available_data_slice_paths,
          loaded_data_slice_paths={''},
          expected_loaded_root_dataslice_pytree={},
      )
    ds = manager.get_data_slice_at(parse_dsp('.query[:].query_text'))
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      self.assert_manager_state(
          manager,
          available_data_slice_paths=all_available_data_slice_paths,
          loaded_data_slice_paths={
              '',
              '.query',
              '.query[:]',
              '.query[:].query_text',
          },
          expected_loaded_root_dataslice_pytree={
              'query': [
                  {
                      'query_text': 'How tall is Obama',
                  },
                  {
                      'query_text': 'How high is the Eiffel tower',
                  },
              ],
          },
      )
    self.assertEqual(
        kd.to_pytree(ds, max_depth=-1),
        [
            'How tall is Obama',
            'How high is the Eiffel tower',
        ],
    )
    query_id_bag = manager.get_data_slice_at(
        parse_dsp('.query[:].query_id')
    ).get_bag()
    doc_id_bag = manager.get_data_slice_at(
        parse_dsp('.query[:].doc[:].doc_id')
    ).get_bag()
    ds = manager.get_data_slice_at(parse_dsp('.query[:].doc')).updated(
        doc_id_bag << query_id_bag
    )
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      self.assert_manager_state(
          manager,
          available_data_slice_paths=all_available_data_slice_paths,
          loaded_data_slice_paths={
              '',
              '.query',
              '.query[:]',
              '.query[:].query_id',
              '.query[:].query_text',
              '.query[:].doc',
              '.query[:].doc[:]',
              '.query[:].doc[:].doc_id',
          },
          expected_loaded_root_dataslice_pytree={
              'query': [
                  {
                      'query_id': 'q1',
                      'doc': [
                          {'doc_id': 0},
                          {'doc_id': 1},
                          {'doc_id': 2},
                          {'doc_id': 3},
                      ],
                      'query_text': 'How tall is Obama',
                  },
                  {
                      'query_id': 'q2',
                      'doc': [
                          {'doc_id': 4},
                          {'doc_id': 5},
                          {'doc_id': 6},
                      ],
                      'query_text': 'How high is the Eiffel tower',
                  },
              ],
          },
      )
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      self.assertEqual(
          kd.to_pytree(ds, max_depth=-1),
          [
              [
                  {'doc_id': 0},
                  {'doc_id': 1},
                  {'doc_id': 2},
                  {'doc_id': 3},
              ],
              [
                  {'doc_id': 4},
                  {'doc_id': 5},
                  {'doc_id': 6},
              ],
          ],
      )
    else:
      # In this case, the doc_title feature is also loaded.
      self.assertEqual(
          kd.to_pytree(ds, max_depth=-1),
          [
              [
                  {'doc_id': 0, 'doc_title': 'title0'},
                  {'doc_id': 1, 'doc_title': 'title1'},
                  {'doc_id': 2, 'doc_title': 'title2'},
                  {'doc_id': 3, 'doc_title': None},
              ],
              [
                  {'doc_id': 4, 'doc_title': 'title4'},
                  {'doc_id': 5, 'doc_title': 'title5'},
                  {'doc_id': 6, 'doc_title': 'title6'},
              ],
          ],
      )
    # Similar to above, but now we request the dataslice *with all descendants*.
    ds = manager.get_data_slice_at(
        parse_dsp('.query[:].doc'), with_all_descendants=True
    )
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      self.assert_manager_state(
          manager,
          available_data_slice_paths=all_available_data_slice_paths,
          loaded_data_slice_paths={
              '',
              '.query',
              '.query[:]',
              '.query[:].query_id',
              '.query[:].query_text',
              '.query[:].doc',
              '.query[:].doc[:]',
              '.query[:].doc[:].doc_id',
              '.query[:].doc[:].doc_title',
          },
          expected_loaded_root_dataslice_pytree={
              'query': [
                  {
                      'query_id': 'q1',
                      'doc': [
                          {
                              'doc_id': 0,
                              'doc_title': 'title0',
                          },
                          {
                              'doc_id': 1,
                              'doc_title': 'title1',
                          },
                          {
                              'doc_id': 2,
                              'doc_title': 'title2',
                          },
                          {
                              'doc_id': 3,
                              'doc_title': None,
                          },
                      ],
                      'query_text': 'How tall is Obama',
                  },
                  {
                      'query_id': 'q2',
                      'doc': [
                          {
                              'doc_id': 4,
                              'doc_title': 'title4',
                          },
                          {
                              'doc_id': 5,
                              'doc_title': 'title5',
                          },
                          {
                              'doc_id': 6,
                              'doc_title': 'title6',
                          },
                      ],
                      'query_text': 'How high is the Eiffel tower',
                  },
              ],
          },
      )
    self.assertEqual(
        kd.to_pytree(ds, max_depth=-1),
        [
            [
                {
                    'doc_id': 0,
                    'doc_title': 'title0',
                },
                {
                    'doc_id': 1,
                    'doc_title': 'title1',
                },
                {
                    'doc_id': 2,
                    'doc_title': 'title2',
                },
                {
                    'doc_id': 3,
                    'doc_title': None,
                },
            ],
            [
                {
                    'doc_id': 4,
                    'doc_title': 'title4',
                },
                {
                    'doc_id': 5,
                    'doc_title': 'title5',
                },
                {
                    'doc_id': 6,
                    'doc_title': 'title6',
                },
            ],
        ],
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_update_at_path_that_is_not_loaded_yet(self, dsm_class):
    query_schema = kd.named_schema(
        'Query',
        query_id=kd.INT32,
        query_text=kd.STRING,
    )

    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([query_schema.new(), query_schema.new()]),
    )
    manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='query_id',
        attr_value=kd.slice([1, 2]),
    )
    manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='query_text',
        attr_value=kd.slice(
            ['How tall is Obama', 'How high is the Eiffel tower']
        ),
    )

    # Now we start a new manager from the persistence dir.
    # Only the root is loaded.
    manager = self.copy_manager(manager)
    available_data_slice_paths = {
        '',
        '.query',
        '.query[:]',
        '.query[:].query_id',
        '.query[:].query_text',
    }
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      self.assert_manager_state(
          manager,
          available_data_slice_paths=available_data_slice_paths,
          loaded_data_slice_paths={''},
          expected_loaded_root_dataslice_pytree={},
      )

    # Although only the root is loaded, we can still update attributes of
    # query[:], which triggers its loading.
    doc_schema = kd.named_schema('Doc', doc_id=kd.INT32, doc_title=kd.STRING)
    manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='doc',
        attr_value=kd.slice([
            kd.list([
                doc_schema.new(doc_id=0, doc_title='title0'),
                doc_schema.new(doc_id=1, doc_title='title1'),
                doc_schema.new(doc_id=2, doc_title='title2'),
                doc_schema.new(doc_id=3, doc_title=None),
            ]),
            None,
        ]),
    )
    available_data_slice_paths = {
        '',
        '.query',
        '.query[:]',
        '.query[:].query_id',
        '.query[:].query_text',
        '.query[:].doc',
        '.query[:].doc[:]',
        '.query[:].doc[:].doc_id',
        '.query[:].doc[:].doc_title',
    }
    if isinstance(manager, SimpleInMemoryDataSliceManager):
      loaded_data_slice_paths = available_data_slice_paths
    else:
      loaded_data_slice_paths = {
          '',
          '.query',
          '.query[:]',
          '.query[:].doc',
          '.query[:].doc[:]',
          '.query[:].doc[:].doc_id',
          '.query[:].doc[:].doc_title',
      }
    expected_loaded_root_dataslice_pytree = {
        'query': [
            {
                'doc': [
                    {'doc_id': 0, 'doc_title': 'title0'},
                    {'doc_id': 1, 'doc_title': 'title1'},
                    {'doc_id': 2, 'doc_title': 'title2'},
                    {'doc_id': 3, 'doc_title': None},
                ],
            },
            {
                'doc': None,
            },
        ]
    }
    if isinstance(manager, SimpleInMemoryDataSliceManager):
      # In this case query_id and query_text are also loaded.
      query = expected_loaded_root_dataslice_pytree['query']
      query[0]['query_id'] = 1
      query[0]['query_text'] = 'How tall is Obama'
      query[1]['query_id'] = 2
      query[1]['query_text'] = 'How high is the Eiffel tower'
    self.assert_manager_state(
        manager,
        available_data_slice_paths=available_data_slice_paths,
        loaded_data_slice_paths=loaded_data_slice_paths,
        expected_loaded_root_dataslice_pytree=(
            expected_loaded_root_dataslice_pytree
        ),
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_add_update_whose_schema_is_recursive_and_data_has_no_cycles(
      self, dsm_class
  ):
    tree_node_schema = kd.named_schema(
        'TreeNode',
        value=kd.STRING,
        children=kd.list_schema(kd.named_schema('TreeNode')),
    )
    tree_root = tree_node_schema.new(
        value='tree_root',
        children=kd.list([
            tree_node_schema.new(value='child1'),
            tree_node_schema.new(value='child2'),
        ]),
    )

    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''), attr_name='tree_root', attr_value=tree_root
    )

    max_depth_of_data_slice_paths = 5

    def get_expected_data_slice_paths(node_path: str, current_depth: int):
      if current_depth > max_depth_of_data_slice_paths:
        return set()
      if current_depth == max_depth_of_data_slice_paths:
        return {node_path}
      return {
          node_path,
          f'{node_path}.value',
          f'{node_path}.children',
      } | get_expected_data_slice_paths(
          f'{node_path}.children[:]', current_depth + 2
      )

    expected_data_slice_paths = {''} | get_expected_data_slice_paths(
        '.tree_root', 1
    )

    self.assert_manager_state(
        manager,
        available_data_slice_paths=expected_data_slice_paths,
        loaded_data_slice_paths=expected_data_slice_paths,
        expected_loaded_root_dataslice_pytree={
            'tree_root': {
                'children': [
                    {'children': None, 'value': 'child1'},
                    {'children': None, 'value': 'child2'},
                ],
                'value': 'tree_root',
            }
        },
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_recursive_schema_across_two_calls_to_update_with_no_cycles_in_data(
      self, dsm_class
  ):
    tree_node_schema = kd.named_schema('TreeNode')
    tree_root = tree_node_schema.new(value='tree_root')
    child1 = tree_node_schema.new(value='child1')
    child2 = tree_node_schema.new(value='child2')

    manager = self.new_manager(dsm_class)

    manager.update(
        at_path=parse_dsp(''), attr_name='tree_root', attr_value=tree_root
    )

    self.assert_manager_state(
        manager,
        available_data_slice_paths={'', '.tree_root', '.tree_root.value'},
        loaded_data_slice_paths={'', '.tree_root', '.tree_root.value'},
        expected_loaded_root_dataslice_pytree={
            'tree_root': {'value': 'tree_root'}
        },
    )
    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 0),
            (schema_node_name(manager_schema.tree_root), 1),
            (
                schema_node_name(
                    manager_schema.tree_root, action=GetAttr('value')
                ),
                1,
            ),
        ],
    )

    manager.update(
        at_path=parse_dsp('.tree_root'),
        attr_name='children',
        attr_value=kd.list([child1, child2]),
    )
    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 0),
            (schema_node_name(manager_schema.tree_root), 2),
            (schema_node_name(manager_schema.tree_root.children), 1),
            (
                schema_node_name(
                    manager_schema.tree_root, action=GetAttr('value')
                ),
                2,
            ),
        ],
    )

    max_depth_of_data_slice_paths = 5

    def get_expected_data_slice_paths(node_path: str, current_depth: int):
      if current_depth > max_depth_of_data_slice_paths:
        return set()
      if current_depth == max_depth_of_data_slice_paths:
        return {node_path}
      return {
          node_path,
          f'{node_path}.value',
          f'{node_path}.children',
      } | get_expected_data_slice_paths(
          f'{node_path}.children[:]', current_depth + 2
      )

    expected_data_slice_paths = {''} | get_expected_data_slice_paths(
        '.tree_root', 1
    )

    self.assert_manager_state(
        manager,
        available_data_slice_paths=expected_data_slice_paths,
        loaded_data_slice_paths=expected_data_slice_paths,
        expected_loaded_root_dataslice_pytree={
            'tree_root': {
                'children': [
                    {'children': None, 'value': 'child1'},
                    {'children': None, 'value': 'child2'},
                ],
                'value': 'tree_root',
            }
        },
    )
    # We have now demonstrated what we wanted to.

    # But we can continue to add grandchildren. There are currently no
    # grandchildren:
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      grandchildren_schema = kd.list_schema(
          kd.named_schema(
              'TreeNode',
              # Note that "value" is not included here, because the user
              # did not request it.
              children=kd.list_schema(kd.named_schema('TreeNode')),
          )
      )
    else:
      grandchildren_schema = kd.list_schema(
          kd.named_schema(
              'TreeNode',
              # Note that "value" is included here, because everything is always
              # loaded in the SimpleInMemoryDataSliceManager.
              value=kd.STRING,
              children=kd.list_schema(kd.named_schema('TreeNode')),
          )
      )
    kd.testing.assert_equivalent(
        manager.get_data_slice_at(
            parse_dsp('.tree_root.children[:].children')
        ).extract(),
        kd.slice(
            [None, None],
            schema=grandchildren_schema,
        ),
    )
    # But we can add an update with grandchildren:
    manager.update(
        at_path=parse_dsp('.tree_root.children[:]'),
        attr_name='children',
        attr_value=kd.slice([
            kd.list([tree_node_schema.new(value='grandchild1')]),
            kd.list([tree_node_schema.new(value='grandchild2')]),
        ]),
    )
    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 0),
            (schema_node_name(manager_schema.tree_root), 3),
            (schema_node_name(manager_schema.tree_root.children), 2),
            (
                schema_node_name(
                    manager_schema.tree_root, action=GetAttr('value')
                ),
                3,
            ),
        ],
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_add_update_whose_schema_is_recursive_and_data_has_cycles(
      self, dsm_class
  ):
    graph_node_schema = kd.named_schema(
        'GraphNode',
        label=kd.STRING,
        outgoing_edges=kd.list_schema(kd.named_schema('GraphNode')),
    )
    node1 = graph_node_schema.new(label='node1')
    node2 = graph_node_schema.new(
        label='node2', outgoing_edges=kd.list([node1])
    )
    node1 = node1.updated(kd.attrs(node1, outgoing_edges=kd.list([node2])))

    # We have successfully created a cycle in the data:
    self.assertEqual(
        node1.outgoing_edges[:].outgoing_edges[:].label.to_py(), [['node1']]
    )
    kd.testing.assert_equivalent(
        node1.outgoing_edges[:].outgoing_edges[:].flatten(),
        kd.slice([node1]),
    )

    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='graph_nodes',
        attr_value=kd.list([node1, node2]),
    )

    max_depth_of_data_slice_paths = 5

    def get_expected_data_slice_paths(node_path: str, current_depth: int):
      if current_depth > max_depth_of_data_slice_paths:
        return set()
      if current_depth == max_depth_of_data_slice_paths:
        return {node_path}
      return {
          node_path,
          f'{node_path}.label',
          f'{node_path}.outgoing_edges',
      } | get_expected_data_slice_paths(
          f'{node_path}.outgoing_edges[:]', current_depth + 2
      )

    expected_data_slice_paths = {
        '',
        '.graph_nodes',
    } | get_expected_data_slice_paths('.graph_nodes[:]', 2)

    self.assert_manager_available_data_slice_paths(
        manager,
        expected_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )
    self.assert_manager_loaded_data_slice_paths(
        manager,
        expected_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )
    self.assertEqual(
        get_loaded_root_dataslice(manager)
        .graph_nodes[:]
        .L[0]  # node1
        .outgoing_edges[:]
        .outgoing_edges[:]
        .label.to_py(),
        [['node1']],
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_recursive_schema_across_two_calls_to_update_with_cycles_in_data(
      self, dsm_class
  ):
    manager = self.new_manager(dsm_class)

    graph_node_schema = kd.named_schema(
        'GraphNode',
        label=kd.STRING,
    )
    node1 = graph_node_schema.new(label='node1')
    manager.update(at_path=parse_dsp(''), attr_name='node', attr_value=node1)

    node2 = graph_node_schema.new(
        label='node2', outgoing_edges=kd.list([node1])
    )
    manager.update(
        at_path=parse_dsp('.node'),
        attr_name='outgoing_edges',
        attr_value=kd.list([node2]),
    )

    # We have successfully created a cycle in the data:
    loaded_root_dataslice = get_loaded_root_dataslice(manager)
    kd.testing.assert_equivalent(
        loaded_root_dataslice.node,
        loaded_root_dataslice.node.outgoing_edges[:]
        .outgoing_edges[:]
        .flatten()
        .S[0],
    )
    # Phrased a little differently:
    kd.testing.assert_equivalent(
        loaded_root_dataslice.node.no_bag(),
        manager.get_data_slice_at(
            parse_dsp('.node.outgoing_edges[:].outgoing_edges[:]')
        )
        .flatten()
        .S[0]
        .no_bag(),
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 0),
            (schema_node_name(manager_schema.node), 2),
            (
                schema_node_name(manager_schema.node, action=GetAttr('label')),
                2,
            ),
            (schema_node_name(manager_schema.node.outgoing_edges), 1),
        ],
    )

    max_depth_of_data_slice_paths = 5

    def get_expected_data_slice_paths(node_path: str, current_depth: int):
      if current_depth > max_depth_of_data_slice_paths:
        return set()
      if current_depth == max_depth_of_data_slice_paths:
        return {node_path}
      return {
          node_path,
          f'{node_path}.label',
          f'{node_path}.outgoing_edges',
      } | get_expected_data_slice_paths(
          f'{node_path}.outgoing_edges[:]', current_depth + 2
      )

    expected_data_slice_paths = {''} | get_expected_data_slice_paths('.node', 1)

    self.assert_manager_available_data_slice_paths(
        manager,
        expected_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )
    self.assert_manager_loaded_data_slice_paths(
        manager,
        expected_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )

    # We can add 'incoming_edges' to the nodes.
    manager.update(
        at_path=parse_dsp('.node'),
        attr_name='incoming_edges',
        attr_value=kd.list(
            [node2.stub().with_attrs(incoming_edges=kd.list([node1.stub()]))]
        ),
    )
    # We have successfully created two cycles in the data:
    loaded_root_dataslice = get_loaded_root_dataslice(manager)
    kd.testing.assert_equivalent(
        loaded_root_dataslice.node,
        loaded_root_dataslice.node.outgoing_edges[:]
        .outgoing_edges[:]
        .flatten()
        .S[0],
    )
    kd.testing.assert_equivalent(
        loaded_root_dataslice.node,
        loaded_root_dataslice.node.incoming_edges[:]
        .incoming_edges[:]
        .flatten()
        .S[0],
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 0),
            (schema_node_name(manager_schema.node), 3),
            (schema_node_name(manager_schema.node.incoming_edges), 2),
            (
                schema_node_name(manager_schema.node, action=GetAttr('label')),
                2,
            ),
        ],
    )

    def new_expected_data_slice_paths(node_path: str, current_depth: int):
      if current_depth == max_depth_of_data_slice_paths:
        return {node_path}
      if current_depth > max_depth_of_data_slice_paths:
        return set()
      return (
          {
              node_path,
              f'{node_path}.label',
              f'{node_path}.outgoing_edges',
              f'{node_path}.incoming_edges',
          }
          | new_expected_data_slice_paths(
              f'{node_path}.outgoing_edges[:]', current_depth + 2
          )
          | new_expected_data_slice_paths(
              f'{node_path}.incoming_edges[:]', current_depth + 2
          )
      )

    expected_data_slice_paths = {''} | new_expected_data_slice_paths('.node', 1)

    self.assert_manager_available_data_slice_paths(
        manager,
        expected_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )
    self.assert_manager_loaded_data_slice_paths(
        manager,
        expected_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )

    # Add another feature that is only present on one of the nodes.
    manager.update(
        at_path=parse_dsp('.node'),
        attr_name='another_label',
        attr_value=kd.item('foo'),
    )
    self.assertEqual(
        manager.get_data_slice_at(parse_dsp('.node.another_label')).to_py(),
        'foo',
    )
    self.assertEqual(
        manager.get_data_slice_at(
            parse_dsp('.node.outgoing_edges[:].another_label')
        ).to_py(),
        [None],
    )
    self.assertEqual(
        manager.get_data_slice_at(
            parse_dsp('.node.outgoing_edges[:].incoming_edges[:].another_label')
        ).to_py(),
        [['foo']],
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 0),
            (schema_node_name(manager_schema.node), 3),
            (
                schema_node_name(
                    manager_schema.node, action=GetAttr('another_label')
                ),
                1,
            ),
            (schema_node_name(manager_schema.node.incoming_edges), 2),
            (
                schema_node_name(manager_schema.node, action=GetAttr('label')),
                2,
            ),
        ],
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_another_recursive_tree_schema_add_parent_node_pointers(
      self, dsm_class
  ):
    tree_node_schema = kd.named_schema(
        'TreeNode',
        value=kd.STRING,
        children=kd.list_schema(kd.named_schema('TreeNode')),
    )
    tree_root = tree_node_schema.new(
        value='tree_root',
        children=kd.list([
            tree_node_schema.new(value='child1'),
            tree_node_schema.new(
                value='child2',
                children=kd.list([tree_node_schema.new(value='grandchild1')]),
            ),
        ]),
    )

    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''), attr_name='tree_root', attr_value=tree_root
    )

    self.assert_manager_loaded_root_dataslice_pytree(
        manager,
        {
            'tree_root': kd.to_pytree(tree_root, max_depth=-1),
        },
    )

    num_parent_updates = 0
    while True:
      parent_path = parse_dsp(
          '.tree_root' + '.children[:]' * num_parent_updates
      )
      child_path = parent_path.concat(parse_dsp('.children[:]'))
      parents = manager.get_data_slice_at(parent_path).stub()
      children = manager.get_data_slice_at(child_path)
      if children.is_empty():
        break
      num_parent_updates += 1
      manager.update(at_path=child_path, attr_name='parent', attr_value=parents)
    self.assertEqual(num_parent_updates, 2)

    ds = get_loaded_root_dataslice(manager)
    self.assertEqual(
        ds.tree_root.children[:].parent.value.to_py(),
        ['tree_root', 'tree_root'],
    )
    self.assertEqual(
        ds.tree_root.children[:].children[:].parent.value.to_py(),
        [[], ['child2']],
    )
    self.assertEqual(
        ds.tree_root.children[:].children[:].children[:].parent.value.to_py(),
        [[], [[]]],
    )

    max_depth_of_data_slice_paths = 5

    def get_expected_data_slice_paths(node_path: str, current_depth: int):
      if current_depth > max_depth_of_data_slice_paths:
        return set()
      if current_depth == max_depth_of_data_slice_paths:
        return {node_path}
      return (
          # Expand zero steps:
          {node_path}
          |
          # Expand one step:
          {
              f'{node_path}.value',
              f'{node_path}.children',
              f'{node_path}.parent',
          }
          |
          # Expand recursively:
          get_expected_data_slice_paths(
              f'{node_path}.children[:]', current_depth + 2
          )
          | get_expected_data_slice_paths(
              f'{node_path}.parent', current_depth + 1
          )
      )

    expected_data_slice_paths = {''} | get_expected_data_slice_paths(
        '.tree_root', 1
    )

    self.assert_manager_available_data_slice_paths(
        manager,
        expected_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )
    self.assert_manager_loaded_data_slice_paths(
        manager,
        expected_data_slice_paths,
        max_depth_of_data_slice_paths=max_depth_of_data_slice_paths,
    )
    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 0),
            (
                schema_node_name(manager_schema.tree_root),
                1 + num_parent_updates,
            ),
            (schema_node_name(manager_schema.tree_root.children), 1),
            (
                schema_node_name(
                    manager_schema.tree_root, action=GetAttr('value')
                ),
                1,
            ),
        ],
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_aliasing_interaction_with_all_descendants(self, dsm_class):
    o = kd.new(x=1)
    manager = self.new_manager(dsm_class)
    manager.update(at_path=parse_dsp(''), attr_name='foo', attr_value=o)

    # Next, we make ".bar" an alias for ".foo".
    manager.update(at_path=parse_dsp(''), attr_name='bar', attr_value=o.stub())

    foo_path = parse_dsp('.foo')
    bar_path = parse_dsp('.bar')

    # Next, we add a new feature to ".foo". It is implicitly added to  ".bar"
    # as well because of the aliasing.
    manager.update(at_path=foo_path, attr_name='y', attr_value=kd.item(2))
    # Both ".foo" and ".bar" now have the same value for "y", as expected.
    self.assertEqual(
        manager.get_data_slice_at(
            foo_path, with_all_descendants=True
        ).y.to_py(),
        2,
    )
    self.assertEqual(
        manager.get_data_slice_at(
            bar_path, with_all_descendants=True
        ).y.to_py(),
        2,
    )

    # Starting from a state where nothing is loaded, the manager should know
    # that "y" is a descendant of ".foo" and ".bar".
    new_manager = self.copy_manager(manager)
    self.assertEqual(
        new_manager.get_data_slice_at(
            foo_path, with_all_descendants=True
        ).y.to_py(),
        2,
    )
    new_manager = self.copy_manager(manager)
    self.assertEqual(
        new_manager.get_data_slice_at(
            bar_path, with_all_descendants=True
        ).y.to_py(),
        2,
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 0),
            (schema_node_name(manager_schema.bar), 2),
            (
                schema_node_name(manager_schema.bar, action=GetAttr('x')),
                1,
            ),
            (
                schema_node_name(manager_schema.bar, action=GetAttr('y')),
                1,
            ),
        ],
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_deeper_aliasing(self, dsm_class):
    o = kd.new(x=kd.new(z=1))
    manager = self.new_manager(dsm_class)
    manager.update(at_path=parse_dsp(''), attr_name='foo', attr_value=o)

    # Next, we make ".bar" an alias for ".foo". However, we don't use o.stub()
    # here, so the aliasing is much deeper.
    manager.update(at_path=parse_dsp(''), attr_name='bar', attr_value=o)

    foo_path = parse_dsp('.foo')
    bar_path = parse_dsp('.bar')

    # Next, we add a new feature to ".foo". It is implicitly added to  ".bar"
    # as well because of the aliasing.
    manager.update(at_path=foo_path, attr_name='y', attr_value=kd.item(2))
    # Both ".foo" and ".bar" now have the same value for "y", as expected.
    self.assertEqual(
        manager.get_data_slice_at(
            foo_path, with_all_descendants=True
        ).y.to_py(),
        2,
    )
    self.assertEqual(
        manager.get_data_slice_at(
            bar_path, with_all_descendants=True
        ).y.to_py(),
        2,
    )

    # Starting from a state where nothing is loaded, the manager should know
    # that "y" is a descendant of ".foo" and ".bar".
    new_manager = self.copy_manager(manager)
    self.assertEqual(
        new_manager.get_data_slice_at(
            foo_path, with_all_descendants=True
        ).y.to_py(),
        2,
    )
    new_manager = self.copy_manager(manager)
    self.assertEqual(
        new_manager.get_data_slice_at(
            bar_path, with_all_descendants=True
        ).y.to_py(),
        2,
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 0),
            (schema_node_name(manager_schema.bar), 2),
            (schema_node_name(manager_schema.bar.x), 2),
            (
                schema_node_name(manager_schema.bar.x, action=GetAttr('z')),
                2,
            ),
            (
                schema_node_name(manager_schema.bar, action=GetAttr('y')),
                1,
            ),
        ],
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_schema_overriding(self, dsm_class):
    x = kd.item(1)
    manager = self.new_manager(dsm_class)
    manager.update(at_path=parse_dsp(''), attr_name='x', attr_value=x)

    new_x = kd.item('foo')
    manager.update(
        at_path=parse_dsp(''),
        attr_name='x',
        attr_value=new_x,
    )
    x_path = parse_dsp('.x')

    self.assertEqual(manager.get_data_slice_at(x_path).to_py(), 'foo')

    manager = self.copy_manager(manager)
    self.assertEqual(manager.get_data_slice_at(x_path).to_py(), 'foo')
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      # Note that the original bag for x is not loaded here. The only 2 bags
      # that are loaded are the ones for the root and for the new 'x'.
      self.assertLen(manager._data_bag_manager.get_loaded_bag_names(), 2)

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_deeper_schema_overriding(self, dsm_class):
    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''), attr_name='x', attr_value=kd.list([1, 2])
    )
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      original_non_root_bag_names = (
          manager._data_bag_manager.get_loaded_bag_names() - {''}
      )
    manager.update(
        at_path=parse_dsp(''),
        attr_name='x',
        attr_value=kd.list(['a', 'b', 'c']),
    )

    self.assertEqual(
        manager.get_data_slice_at(parse_dsp('.x[:]')).to_py(), ['a', 'b', 'c']
    )

    manager = self.copy_manager(manager)
    self.assertEqual(
        manager.get_data_slice_at(parse_dsp('.x[:]')).to_py(), ['a', 'b', 'c']
    )
    if isinstance(manager, PersistedIncrementalDataSliceManager):
      # The original bags for x and its items are not loaded here:
      self.assertEmpty(
          manager._data_bag_manager.get_loaded_bag_names()
          & original_non_root_bag_names  # pylint: disable=undefined-variable
      )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_values_of_primitive_schemas_are_not_aliased(self, dsm_class):
    # In contrast to OBJECT and SCHEMA above, values of primitive schemas are
    # not aliased even if we create stubs. In this test we use a value with
    # schema kd.INT32 to illustrate the point:
    value = kd.item(1)
    manager = self.new_manager(dsm_class)
    manager.update(at_path=parse_dsp(''), attr_name='foo', attr_value=value)

    # Next, we set ".bar" to a stub of ".foo". However, no aliasing happens -
    # it copies the value instead.
    manager.update(
        at_path=parse_dsp(''), attr_name='bar', attr_value=value.stub()
    )

    # We can access the values of both:
    self.assertEqual(manager.get_data_slice_at(parse_dsp('.foo')).to_py(), 1)
    self.assertEqual(manager.get_data_slice_at(parse_dsp('.bar')).to_py(), 1)

    # Starting from a state where nothing is loaded, we should still be able to
    # access the value via ".bar".
    manager = self.copy_manager(manager)
    self.assertEqual(manager.get_data_slice_at(parse_dsp('.bar')).to_py(), 1)

    if isinstance(manager, PersistedIncrementalDataSliceManager):
      manager_schema = manager.get_schema()
      # It knows that ".foo" and ".bar" are not true aliases:
      self.assert_manager_schema_node_names_to_num_bags(
          manager,
          [
              (schema_node_name(manager_schema), 0),
              (schema_node_name(manager_schema, action=GetAttr('foo')), 1),
              (schema_node_name(manager_schema, action=GetAttr('bar')), 1),
          ],
      )
      # It loaded one bag for the root and one for ".bar".
      self.assertLen(manager._data_bag_manager.get_loaded_bag_names(), 2)

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_values_with_schema_itemid_are_not_aliased(self, dsm_class):
    # Similar to values of primitive schemas, ITEMID values are not aliased when
    # we create stubs.
    value = kd.new_itemid()
    manager = self.new_manager(dsm_class)
    manager.update(at_path=parse_dsp(''), attr_name='foo', attr_value=value)

    # Next, we set ".bar" to a stub of ".foo". However, no aliasing happens -
    # it copies the value instead.
    manager.update(
        at_path=parse_dsp(''), attr_name='bar', attr_value=value.stub()
    )

    # We can access the values of both:
    self.assertEqual(manager.get_data_slice_at(parse_dsp('.foo')), value)
    self.assertEqual(manager.get_data_slice_at(parse_dsp('.bar')), value)

    # Starting from a state where nothing is loaded, we should still be able to
    # access the value via ".bar".
    manager = self.copy_manager(manager)
    self.assertEqual(manager.get_data_slice_at(parse_dsp('.bar')), value)

    if isinstance(manager, PersistedIncrementalDataSliceManager):
      manager_schema = manager.get_schema()
      # It knows that ".foo" and ".bar" are not true aliases:
      self.assert_manager_schema_node_names_to_num_bags(
          manager,
          [
              (schema_node_name(manager_schema), 0),
              (schema_node_name(manager_schema, action=GetAttr('foo')), 1),
              (schema_node_name(manager_schema, action=GetAttr('bar')), 1),
          ],
      )
      # It loaded one bag for the root and one for ".bar".
      self.assertLen(manager._data_bag_manager.get_loaded_bag_names(), 2)

  def test_persistence_dir_is_hermetic(self):
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = PersistedIncrementalDataSliceManager(persistence_dir)
    query_schema = kd.named_schema('query')
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(query_id='q1'),
            query_schema.new(query_id='q2'),
        ]),
    )
    doc_schema = kd.named_schema('doc')
    manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='doc',
        attr_value=kd.slice([
            doc_schema.new(doc_id=kd.slice([0, 1, 2, 3])).implode(),
            doc_schema.new(doc_id=kd.slice([4, 5, 6])).implode(),
        ]),
    )

    # Move the whole persistence_dir to a new location.
    new_persistence_dir = os.path.join(self.create_tempdir().full_path, 'copy')
    shutil.move(persistence_dir, new_persistence_dir)
    persistence_dir = new_persistence_dir

    # Initialize a new manager with the new persistence directory.
    manager = PersistedIncrementalDataSliceManager(persistence_dir)
    ds = manager.get_data_slice(populate_including_descendants={parse_dsp('')})
    self.assertEqual(
        ds.query[:].doc[:].doc_id.to_py(), [[0, 1, 2, 3], [4, 5, 6]]
    )

    # We can add further updates to a manager that was initialized from a
    # persistence_dir that was already populated.
    manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='query_text',
        attr_value=kd.slice(
            ['How tall is Obama', 'How high is the Eiffel tower']
        ),
    )

    # These additional updates are also persisted, and can be picked up by new
    # manager instances.
    manager = PersistedIncrementalDataSliceManager(persistence_dir)
    ds = manager.get_data_slice(populate_including_descendants={parse_dsp('')})
    self.assertEqual(
        ds.query[:].doc[:].doc_id.to_py(), [[0, 1, 2, 3], [4, 5, 6]]
    )
    self.assertEqual(
        ds.query[:].query_text.to_py(),
        ['How tall is Obama', 'How high is the Eiffel tower'],
    )

  def test_metadata_version(self):
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = PersistedIncrementalDataSliceManager(persistence_dir)
    # Starting from an empty persistence directory, the version should be 1.0.0.
    self.assertEqual(manager._metadata.version, '1.0.0')
    manager.update(at_path=parse_dsp(''), attr_name='x', attr_value=kd.item(1))
    # The version should still be 1.0.0 after adding an update.
    self.assertEqual(manager._metadata.version, '1.0.0')

    manager = PersistedIncrementalDataSliceManager(persistence_dir)
    # The version is 1.0.0 after starting from a populated persistence directory
    self.assertEqual(manager._metadata.version, '1.0.0')

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_is_valid_available_and_loaded_data_slice_path(self, dsm_class):
    manager = self.new_manager(dsm_class)
    self.assertTrue(manager.exists(parse_dsp('')))
    self.assertTrue(is_loaded_data_slice_path(manager, parse_dsp('')))
    self.assertFalse(manager.exists(parse_dsp('.foo')))
    self.assertFalse(is_loaded_data_slice_path(manager, parse_dsp('.bar')))

    manager.update(
        at_path=parse_dsp(''),
        attr_name='foo',
        attr_value=kd.new(x=1),
    )
    for dsp_string in ['', '.foo', '.foo.x']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      self.assertTrue(is_loaded_data_slice_path(manager, dsp))
    for dsp_string in ['.foo.y', '.foo.y.z']:
      dsp = parse_dsp(dsp_string)
      self.assertFalse(manager.exists(dsp))
      self.assertFalse(is_loaded_data_slice_path(manager, dsp))
    manager.update(
        at_path=parse_dsp('.foo'),
        attr_name='y',
        attr_value=kd.new(z=2),
    )
    for dsp_string in ['', '.foo', '.foo.x', '.foo.y', '.foo.y.z']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      self.assertTrue(is_loaded_data_slice_path(manager, dsp))

    # Start again from a state where nothing is loaded.
    manager = self.copy_manager(manager)
    self.assertTrue(manager.exists(parse_dsp('')))
    self.assertTrue(is_loaded_data_slice_path(manager, parse_dsp('')))
    for dsp_string in ['.foo', '.foo.x', '.foo.y', '.foo.y.z']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      if dsm_class == PersistedIncrementalDataSliceManager:
        self.assertFalse(is_loaded_data_slice_path(manager, dsp))

    manager.get_data_slice_at(parse_dsp('.foo'))
    for dsp_string in ['.foo']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      self.assertTrue(is_loaded_data_slice_path(manager, dsp))
    for dsp_string in ['.foo.x', '.foo.y', '.foo.y.z']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      if dsm_class == PersistedIncrementalDataSliceManager:
        self.assertFalse(is_loaded_data_slice_path(manager, dsp))

    manager.get_data_slice_at(parse_dsp('.foo.y.z'))
    for dsp_string in ['.foo', '.foo.y', '.foo.y.z']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      self.assertTrue(is_loaded_data_slice_path(manager, dsp))
    for dsp_string in ['.foo.x']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      if dsm_class == PersistedIncrementalDataSliceManager:
        self.assertFalse(is_loaded_data_slice_path(manager, dsp))

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_update_at_bad_path_raises(self, dsm_class):
    manager = self.new_manager(dsm_class)

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid data slice path: '.foo.x'")
    ):
      manager.update(
          at_path=parse_dsp('.foo.x'),
          attr_name='y',
          attr_value=kd.new(z=2),
      )

    manager.update(
        at_path=parse_dsp(''),
        attr_name='foo',
        attr_value=kd.list([1, 2, 3]),
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "the schema at data slice path '.foo' is LIST[INT32], which does"
            ' not support updates. Please pass a data slice path that is'
            ' associated with an entity schema'
        ),
    ):
      manager.update(
          at_path=parse_dsp('.foo'),
          attr_name='x',
          attr_value=kd.new(z=2),
      )

  def test_koda_behavior_of_object_schema(self):
    # This test is to document the behavior of vanilla Koda.
    # It demonstrates the behaviors that make it difficult to accept the use
    # of OBJECT in the schema of an incremental data slice.

    query_schema = kd.named_schema('query')
    new_query = query_schema.new
    doc_schema = kd.named_schema('doc')
    new_doc = doc_schema.new

    root_v0 = kd.new(
        query=kd.slice([
            new_query(
                query_id='q1',
                doc=new_doc(doc_id=kd.slice([0, 1, 2, 3])).implode(),
            ),
            new_query(
                query_id='q2', doc=new_doc(doc_id=kd.slice([4, 5, 6])).implode()
            ),
        ]).implode()
    )

    # We add an alias to the doc sub-slice. The alias has an OBJECT schema:
    root_v1 = root_v0.updated(
        kd.attrs(root_v0.query[:], doc_obj=kd.obj(root_v0.query[:].doc))
    )
    # Check that it's indeed an alias, i.e. that the itemids agree:
    kd.testing.assert_equivalent(
        root_v1.query[:].doc_obj.get_itemid(),
        root_v1.query[:].doc.get_itemid(),
    )

    # Next, we add a new attribute to the doc sub-slice.
    root_v2 = root_v1.updated(
        kd.attrs(root_v1.query[:].doc[:], new_doc_feature='foo')
    )
    # The new attribute is automatically visible in the doc_obj alias:
    kd.testing.assert_equivalent(
        root_v2.query[:].doc_obj[:].new_doc_feature,
        root_v2.query[:].doc[:].new_doc_feature,
    )
    # The schema of the doc_obj alias is OBJECT:
    self.assertEqual(
        root_v2.query[:].doc_obj.get_schema(),
        kd.OBJECT,
    )
    # But calling get_obj_schema() shows that its embedded schema is updated:
    kd.testing.assert_equivalent(
        root_v2.query[:].doc_obj.get_obj_schema(),
        root_v2.query[:].doc.get_schema().repeat(2),
    )

    # Next, we create an new alias to the doc_obj alias. The new alias is
    # called another_doc_obj. Adding a new attribute to the new alias and
    # updating the slice has the effect of also adding the new attribute to the
    # doc sub-slice and the doc_obj sub-slice. So updating an OBJECT can
    # potentially affect other sub-slices that do not have OBJECT schemas, and
    # it can also affect OBJECTs at other locations in the overall schema.
    doc_obj = root_v2.query[:].doc_obj
    another_doc_obj = doc_obj.updated(
        kd.attrs(doc_obj[:], another_doc_feature='bar')
    )
    root_v3 = root_v2.updated(
        kd.attrs(root_v2.query[:], another_doc_obj=another_doc_obj)
    )
    self.assertEqual(
        root_v3.query[:].another_doc_obj.get_schema(),
        kd.OBJECT,
    )
    kd.testing.assert_equivalent(
        root_v3.query[:].another_doc_obj[:].another_doc_feature,
        root_v3.query[:].doc[:].another_doc_feature,
    )
    kd.testing.assert_equivalent(
        root_v3.query[:].another_doc_obj[:].another_doc_feature,
        root_v3.query[:].doc_obj[:].another_doc_feature,
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_object_schema_is_not_supported(self, dsm_class):
    # This test shows that the functionality shown in
    # test_koda_behavior_of_object_schema is not supported by
    # PersistedIncrementalDataSliceManager. In particular, the manager does not
    # support OBJECT schemas in updates.

    manager = self.new_manager(dsm_class)

    query_schema = kd.named_schema('query')
    new_query = query_schema.new
    doc_schema = kd.named_schema('doc')
    new_doc = doc_schema.new

    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.slice([
            new_query(
                query_id='q1',
                doc=new_doc(doc_id=kd.slice([0, 1, 2, 3])).implode(),
            ),
            new_query(
                query_id='q2', doc=new_doc(doc_id=kd.slice([4, 5, 6])).implode()
            ),
        ]).implode(),
    )

    # Try to add an alias to the doc sub-slice. The alias has an OBJECT schema,
    # so the update is not supported.
    with self.assertRaisesRegex(
        ValueError, re.escape('OBJECT schemas are not supported')
    ):
      manager.update(
          at_path=parse_dsp('.query[:]'),
          attr_name='doc_obj',
          attr_value=kd.obj(
              manager.get_data_slice_at(parse_dsp('.query[:].doc'))
          ),
      )

    # All updates with OBJECT schemas are rejected.
    with self.assertRaisesRegex(
        ValueError, re.escape('OBJECT schemas are not supported')
    ):
      manager.update(
          at_path=parse_dsp('.query[:].doc[:]'),
          attr_name='one',
          attr_value=kd.obj(1),
      )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_behavior_of_itemid_with_multiple_schemas(self, dsm_class):
    # Demonstrates that the behavior of PersistedIncrementalDataSliceManager is
    # not consistent with the behavior of vanilla Koda. See below for the
    # assertion that is different. For that reason, we state in the docstring
    # of update() that the behavior is undefined if an itemid is associated with
    # two or more structured schemas.

    manager = self.new_manager(dsm_class)

    e_foo = kd.new(a=1, schema='foo')
    e_bar = e_foo.with_schema(kd.named_schema('bar', a=kd.INT32))
    foo_wrapper = kd.new(schema='foo_wrapper')
    manager.update(
        at_path=parse_dsp(''), attr_name='foo', attr_value=foo_wrapper
    )

    manager.update(at_path=parse_dsp('.foo'), attr_name='x', attr_value=e_foo)
    manager.update(at_path=parse_dsp(''), attr_name='y', attr_value=e_bar)

    manager.update(
        at_path=parse_dsp('.foo.x'), attr_name='b', attr_value=kd.item(2)
    )
    manager.update(
        at_path=parse_dsp('.y'), attr_name='c', attr_value=kd.item(3)
    )

    manager.update(
        at_path=parse_dsp('.foo.x'), attr_name='a', attr_value=kd.item(4)
    )

    new_manager = self.copy_manager(manager)
    self.assertEqual(
        new_manager.get_data_slice_at(
            parse_dsp('.foo.x'), with_all_descendants=True
        ).to_pytree(max_depth=-1),
        {'a': 4, 'b': 2},
    )

    new_manager = self.copy_manager(manager)
    actual_y = new_manager.get_data_slice_at(
        parse_dsp('.y'), with_all_descendants=True
    ).to_pytree(max_depth=-1)
    # The behavior of PersistedIncrementalDataSliceManager is different from
    # that of vanilla Koda. The reason is that indexing on the schema alone is
    # not sufficient to understand that the
    # manager.update(
    #     at_path=parse_dsp('.x'), attr_name='a', attr_value=kd.item(4)
    # )
    # update also affected the dataslice at .y, since .y seemed to have a
    # different schema than .x
    # For that reason, we state in the docstring of update() that the behavior
    # is undefined if an itemid is associated with two or more structured
    # schemas.
    if dsm_class == PersistedIncrementalDataSliceManager:
      expected_y = {'a': 1, 'c': 3}
    else:
      expected_y = {'a': 4, 'c': 3}
    self.assertEqual(actual_y, expected_y)

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_behavior_of_schema_metadata_itemid_with_multiple_schemas(
      self, dsm_class
  ):
    # Demonstrates that the behavior of PersistedIncrementalDataSliceManager is
    # not consistent with the behavior of vanilla Koda. See below for the
    # assertion that is different. For that reason, we state in the docstring
    # of update() that the behavior is undefined if an itemid of a schema
    # metadata object is also associated with a structured schema in the main
    # dataslice.

    foo_schema = kd.named_schema('foo', a=kd.INT32)
    foo_schema = kd.with_metadata(foo_schema, proto_name='my.proto.Message')
    schema_metadata_object = kd.get_metadata(foo_schema)
    explicit_metadata_schema = kd.named_schema(
        'my_metadata', proto_name=kd.STRING
    )
    schema_metadata_entity = schema_metadata_object.with_schema(
        explicit_metadata_schema
    )

    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='my_data',
        attr_value=kd.new(
            # This line associates the itemid of schema_metadata_object with
            # the schema kd.OBJECT:
            foo=foo_schema.new(a=1),
            # This line associates the itemid of schema_metadata_object with
            # explicit_metadata_schema:
            metadata=schema_metadata_entity,
        ),
    )

    my_data = manager.get_data_slice_at(
        parse_dsp('.my_data'), with_all_descendants=True
    )
    # The same itemid can be observed in two ways:
    self.assertEqual(
        kd.get_metadata(my_data.foo.get_schema()).get_itemid(),
        my_data.metadata.get_itemid(),
    )
    # But they report different schemas:
    self.assertEqual(
        kd.get_metadata(my_data.foo.get_schema()).get_schema(), kd.OBJECT
    )
    self.assertTrue(my_data.metadata.get_schema().is_struct_schema())

    # We augment the schema metadata object with a new attribute.
    augmented_foo_schema = kd.with_metadata(foo_schema, version=123)
    manager.update(
        at_path=parse_dsp('.my_data'),
        attr_name='bar',
        attr_value=augmented_foo_schema.new(a=2),
    )

    # The new metadata attribute is also visible on the foo sub-slice:
    self.assertEqual(
        kd.get_metadata(
            manager.get_data_slice_at(parse_dsp('.my_data.foo')).get_schema()
        ).version,
        123,
    )

    # The new attribute is present on the explicit metadata sub-slice in
    # vanilla Koda, but not in PersistedIncrementalDataSliceManager. This is why
    # the docstring says that the behavior is undefined.
    explicit_metadata_version = (
        manager.get_data_slice_at(parse_dsp('.my_data.metadata'))
        .with_schema(explicit_metadata_schema.with_attrs(version=kd.INT32))
        .version
    )
    if dsm_class == SimpleInMemoryDataSliceManager:
      expected_version = kd.item(123)
    else:
      expected_version = kd.item(None, schema=kd.INT32)
    kd.testing.assert_deep_equivalent(
        explicit_metadata_version,
        expected_version,
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_updates_preserve_schema_metadata(self, dsm_class):
    query_schema = kd.named_schema('query', id=kd.INT32, text=kd.STRING)
    query_schema = kd.with_metadata(
        query_schema, proto='my.proto.Message', version=123
    )

    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(query_id=0, text='How tall is Obama'),
            query_schema.new(query_id=1, text='How high is the Eiffel tower'),
        ]),
    )

    # Start anew from the persisted state of pidsm:
    manager = self.copy_manager(manager)
    actual_schema = manager.get_data_slice_at(
        parse_dsp('.query[:]')
    ).get_schema()
    kd.testing.assert_deep_equivalent(
        kd.get_metadata(actual_schema),
        kd.get_metadata(query_schema),
    )

  def test_issue_with_non_primitive_schema_metadata_attributes(self):
    # This test demonstrates the behavior of vanilla Koda and why it is
    # problematic for the PersistedIncrementalDataSliceManager.

    query_schema = kd.named_schema('query', id=kd.INT32, text=kd.STRING)

    query_data = kd.list([
        query_schema.new(query_id=0, text='How tall is Obama'),
        query_schema.new(query_id=1, text='How high is the Eiffel tower'),
    ])
    root = kd.new(query=query_data)

    # On the surface, this schema is totally unrelated to the query schema.
    unrelated_schema = kd.named_schema('unrelated', x=kd.INT32)
    # However, if the schema metadata objects can have non-primitive attributes,
    # then there is a covert channel through which we can update pretty much
    # anything in the main dataslice.
    unrelated_schema = kd.with_metadata(
        unrelated_schema,
        covert_query_data_update=query_data[:]
        .with_attrs(query_id=kd.slice([100, 200]))
        .implode(),
    )

    # The manager would receive this update to the main dataslice, and inspect
    # its schema to determine the effects of the update. The schema metadata
    # object has the opaque schema kd.OBJECT, so even if the manager explicitly
    # gets all the schema metadata objects (also of sub-schemas), it would not
    # be able to tell that the update affects the query data.
    root = root.with_attrs(unrelated=unrelated_schema.new(x=42))
    # Yet the covert update takes place in vanilla Koda.
    kd.testing.assert_deep_equivalent(
        root.query[:].query_id, kd.slice([100, 200])
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_update_raises_for_non_primitive_schema_metadata_attributes(
      self, dsm_class
  ):
    # The reasons for why it raises are discussed in
    # test_issue_with_non_primitive_schema_metadata_attributes.

    query_schema = kd.named_schema('query', id=kd.INT32, text=kd.STRING)
    metadata_update = kd.metadata(query_schema, foo=1, bar=kd.new(zoo='gotcha'))
    query_schema = query_schema.updated(metadata_update)

    manager = self.new_manager(dsm_class)
    with self.assertRaisesRegex(
        ValueError, 'has metadata attributes that are not primitives'
    ):
      manager.update(
          at_path=parse_dsp(''),
          attr_name='query',
          attr_value=kd.list([
              query_schema.new(query_id=0, text='How tall is Obama'),
              query_schema.new(query_id=1, text='How high is the Eiffel tower'),
          ]),
      )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_latest_schema_metadata_is_used(self, dsm_class):
    query_schema = kd.named_schema('query', id=kd.INT32, text=kd.STRING)
    doc_schema = kd.named_schema('doc', doc_id=kd.INT32, title=kd.STRING)

    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(
                query_id=0,
                text='How tall is Obama',
                doc=kd.list([
                    doc_schema.new(doc_id=0, title='Barack Obama'),
                    doc_schema.new(doc_id=1, title='Michelle Obama'),
                ]),
            ),
            query_schema.new(query_id=1, text='How high is the Eiffel tower'),
        ]),
    )

    # Update the doc schema with metadata.
    updated_doc_schema = kd.with_metadata(
        doc_schema, proto='my.proto.Message', version=123
    )
    manager.update(
        at_path=parse_dsp(''),
        attr_name='new_doc',
        attr_value=updated_doc_schema.new(doc_id=2, title='New doc'),
    )

    # At the time we populated '.query[:].doc[:]' there was no metadata. But
    # now we ought to have it. That might seem a bit strange, but it is
    # consistent with the behavior of vanilla Koda.
    manager = self.copy_manager(manager)
    actual_schema = manager.get_data_slice_at(
        parse_dsp('.query[:].doc[:]')
    ).get_schema()
    kd.testing.assert_deep_equivalent(
        kd.get_metadata(actual_schema),
        kd.get_metadata(updated_doc_schema),
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_complex_dict_keys(self, dsm_class):
    query_schema = kd.named_schema(
        'query',
        query_id=kd.INT32,
        text=kd.STRING,
    )
    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.dict({
            query_schema.new(
                query_id=0,
                text='How tall is Obama',
            ): 1,
            query_schema.new(
                query_id=1,
                text='How high is the Eiffel tower',
            ): 2,
        }),
    )

    manager = self.copy_manager(manager)
    self.assertEqual(
        # We need to use set() to ignore the order of the keys.
        set(
            manager.get_data_slice_at(
                parse_dsp('.query.get_keys().text')
            ).to_pytree(max_depth=-1)
        ),
        {'How tall is Obama', 'How high is the Eiffel tower'},
    )

  def test_updates_add_bags_deterministically(self):
    # This is not a core feature; it is simply nice to have for debugging.

    persistence_dir = self.create_tempdir().full_path
    original_manager = PersistedIncrementalDataSliceManager(persistence_dir)

    doc_schema = kd.named_schema(
        'doc',
        doc_id=kd.INT32,
        title=kd.STRING,
    )
    query_schema = kd.named_schema(
        'query',
        query_id=kd.INT32,
        text=kd.STRING,
        doc=kd.list_schema(doc_schema),
    )

    query_data = kd.list([
        query_schema.new(
            query_id=0,
            text='How tall is Obama',
        ),
        query_schema.new(
            query_id=1,
            text='How high is the Eiffel tower',
        ),
    ])
    doc_data = kd.slice([
        kd.list([
            doc_schema.new(doc_id=0, title='Barack Obama'),
            doc_schema.new(doc_id=1, title='Michelle Obama'),
        ]),
        None,
    ])

    managers = [self.copy_manager(original_manager) for _ in range(5)]
    for manager in managers:
      manager.update(
          at_path=parse_dsp(''),
          attr_name='query',
          attr_value=query_data,
      )
      manager.update(
          at_path=parse_dsp('.query[:]'),
          attr_name='doc',
          attr_value=doc_data,
      )

    def get_bags_in_the_bag_manager_total_order(manager):
      assert isinstance(manager, PersistedIncrementalDataSliceManager)
      bag_manager = manager._data_bag_manager
      bag_names = bag_manager.get_available_bag_names()
      bag_names = bag_manager._canonical_topological_sorting(bag_names)
      # The canonical topological sorting always returns the bags in the same
      # order, namely the lexicographic order:
      self.assertEqual(bag_names, sorted(bag_names))
      return [bag_manager.get_minimal_bag({b}) for b in bag_names]

    for bags in zip(
        *[get_bags_in_the_bag_manager_total_order(m) for m in managers]
    ):
      # The bags at the same position of the total order are equivalent.
      for i in range(1, len(bags)):
        kd.testing.assert_equivalent(bags[0], bags[i])

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_subslice_with_list_schema_where_multiple_lists_are_missing(
      self, dsm_class
  ):
    query_token = kd.named_schema('query_token', text=kd.STRING)
    query_schema = kd.named_schema(
        'query',
        query_id=kd.INT32,
        text=kd.STRING,
        noun_tokens=kd.list_schema(query_token),
    )

    manager = self.new_manager(dsm_class)

    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(
                query_id=0,
                text='How tall is Obama',
                noun_tokens=kd.list(
                    [None, None, None, query_token.new(text='Obama')]
                ),
            ),
            None,
            query_schema.new(
                query_id=1,
                text='How high is the Eiffel tower',
                noun_tokens=kd.list([
                    None,
                    None,
                    None,
                    None,
                    query_token.new(text='Eiffel tower'),
                ]),
            ),
            None,
        ]),
    )

    if dsm_class == PersistedIncrementalDataSliceManager:
      manager = self.copy_manager(manager)
      self.assertEqual(
          manager.get_data_slice_at(parse_dsp('.query[:]')).to_pytree(
              max_depth=-1
          ),
          [{}, None, {}, None],
      )

    manager = self.copy_manager(manager)
    self.assertEqual(
        manager.get_data_slice_at(
            parse_dsp('.query[:].noun_tokens[:]'), with_all_descendants=True
        ).to_pytree(max_depth=-1),
        [
            [None, None, None, {'text': 'Obama'}],
            [],
            [None, None, None, None, {'text': 'Eiffel tower'}],
            [],
        ],
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_subslice_with_dict_schema_where_multiple_dicts_are_missing(
      self, dsm_class
  ):
    token_info_schema = kd.named_schema('token_info', is_noun=kd.BOOLEAN)
    query_schema = kd.named_schema(
        'query',
        query_id=kd.INT32,
        text=kd.STRING,
        token_info=kd.dict_schema(kd.STRING, token_info_schema),
    )

    query_data = kd.list([
        kd.dict({
            0: query_schema.new(
                query_id=0,
                text='How tall is Obama',
                token_info=kd.dict({
                    'How': token_info_schema.new(is_noun=False),
                    'Obama': token_info_schema.new(is_noun=True),
                }),
            ),
            1: query_schema.new(query_id=1, text='How high is Table Mountain'),
        }),
        None,
        kd.dict({
            2: query_schema.new(
                query_id=2,
                text='How high is the Eiffel tower',
                token_info=kd.dict({
                    'How': token_info_schema.new(is_noun=False),
                    'high': token_info_schema.new(is_noun=False),
                    'Eiffel tower': token_info_schema.new(is_noun=True),
                }),
            )
        }),
        None,
        kd.dict({
            3: kd.item(None, schema=query_schema),
            4: None,
        }),
    ])

    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=query_data,
    )

    if dsm_class == PersistedIncrementalDataSliceManager:
      # Start from a state where nothing is loaded.
      manager = self.copy_manager(manager)
      self.assertEqual(
          manager.get_data_slice_at(parse_dsp('.query[:]')).to_pytree(
              max_depth=-1
          ),
          [{}, None, {}, None, {}],
      )

    # Start from a state where nothing is loaded in the pidsm.
    manager = self.copy_manager(manager)
    kd.testing.assert_deep_equivalent(
        manager.get_data_slice_at(
            parse_dsp('.query[:]'), with_all_descendants=True
        ),
        query_data[:],
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_removal_and_updates_of_selected_items(self, dsm_class):
    token_info_schema = kd.named_schema('token_info', is_noun=kd.BOOLEAN)
    doc_schema = kd.named_schema(
        'doc',
        doc_id=kd.INT32,
        title=kd.STRING,
        tokens=kd.dict_schema(kd.STRING, token_info_schema),
    )
    query_metadata_schema = kd.named_schema(
        'query_metadata',
        locale=kd.STRING,
    )
    query_schema = kd.named_schema(
        'query',
        query_id=kd.INT32,
        text=kd.STRING,
        doc=kd.list_schema(doc_schema),
        metadata=query_metadata_schema,
    )

    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(
                query_id=0,
                text='How tall is Obama',
                doc=kd.list([
                    doc_schema.new(doc_id=0, title='Barack Obama'),
                    doc_schema.new(doc_id=1, title='Michelle Obama'),
                    doc_schema.new(doc_id=2, title='George W. Bush'),
                ]),
            ),
            query_schema.new(
                query_id=1,
                text='How high is the Eiffel tower',
                doc=kd.list([
                    doc_schema.new(
                        doc_id=3,
                        title='Eiffel tower',
                        tokens=kd.dict({
                            'Eiffel tower': token_info_schema.new(is_noun=True)
                        }),
                    ),
                    doc_schema.new(doc_id=4, title='Tower of London'),
                ]),
                metadata=query_metadata_schema.new(locale='en-GB'),
            ),
        ]),
    )

    with self.subTest(name='update_and_remove_primitives'):
      # This is how we can update the first query text and remove the second
      # query text. Note that None is interpreted as a removed value.
      new_manager = self.copy_manager(manager)
      new_manager.update(
          at_path=parse_dsp('.query[:]'),
          attr_name='text',
          attr_value=kd.slice(['How tall is Barack Obama', None]),
      )
      kd.testing.assert_equivalent(
          new_manager.get_data_slice_at(parse_dsp('.query[:].text')).no_bag(),
          kd.slice([
              'How tall is Barack Obama',  # It's now updated!
              None,  # It's now removed!
          ]),
      )
      del new_manager

    with self.subTest(name='update_selected_primitives'):
      # If we wanted to update the query text of the first query without
      # removing the second query's text, then we can do the following:
      manager.update(
          at_path=parse_dsp('.query[:]'),
          attr_name='text',
          attr_value=(
              kd.slice(['How tall is Barack Obama', None])
              | manager.get_data_slice_at(parse_dsp('.query[:].text'))
          ),
      )
      kd.testing.assert_equivalent(
          manager.get_data_slice_at(parse_dsp('.query[:].text')).no_bag(),
          kd.slice([
              'How tall is Barack Obama',
              'How high is the Eiffel tower',
          ]),
      )

    with self.subTest(name='update_and_remove_dicts'):
      # The same is true for dicts. We can update and remove values:
      new_manager = self.copy_manager(manager)
      new_manager.update(
          at_path=parse_dsp('.query[:].doc[:]'),
          attr_name='tokens',
          attr_value=kd.slice([
              [
                  kd.dict({
                      'Barack Obama': token_info_schema.new(is_noun=True),
                  }),
                  None,
                  None,
              ],
              [
                  None,
                  None,
              ],
          ]),
      )
      self.assertEqual(
          new_manager.get_data_slice_at(
              parse_dsp('.query[:].doc[:]'), with_all_descendants=True
          ).to_pytree(max_depth=-1),
          [
              [
                  {
                      'doc_id': 0,
                      'title': 'Barack Obama',
                      'tokens': {'Barack Obama': {'is_noun': True}},
                  },
                  {'doc_id': 1, 'title': 'Michelle Obama', 'tokens': None},
                  {'doc_id': 2, 'title': 'George W. Bush', 'tokens': None},
              ],
              [
                  # We removed the tokens from this doc:
                  {'doc_id': 3, 'title': 'Eiffel tower', 'tokens': None},
                  {'doc_id': 4, 'title': 'Tower of London', 'tokens': None},
              ],
          ],
      )
      del new_manager

    with self.subTest(name='update_selected_dicts'):
      # Alternatively, we can update selected values without removing others:
      manager.update(
          at_path=parse_dsp('.query[:].doc[:]'),
          attr_name='tokens',
          attr_value=(
              kd.slice([
                  [
                      kd.dict({
                          'Barack Obama': token_info_schema.new(is_noun=True),
                      }),
                      None,
                      None,
                  ],
                  [
                      None,
                      None,
                  ],
              ])
              | manager.get_data_slice_at(
                  parse_dsp('.query[:].doc[:].tokens')
              ).stub()  # The .stub() is not necessary when using pidsm.
          ),
      )
      self.assertEqual(
          manager.get_data_slice_at(
              parse_dsp('.query[:].doc[:]'), with_all_descendants=True
          ).to_pytree(max_depth=-1),
          [
              [
                  {
                      'doc_id': 0,
                      'title': 'Barack Obama',
                      'tokens': {'Barack Obama': {'is_noun': True}},
                  },
                  {'doc_id': 1, 'title': 'Michelle Obama', 'tokens': None},
                  {'doc_id': 2, 'title': 'George W. Bush', 'tokens': None},
              ],
              [
                  {
                      'doc_id': 3,
                      'title': 'Eiffel tower',
                      # We now retain the tokens:
                      'tokens': {'Eiffel tower': {'is_noun': True}},
                  },
                  {'doc_id': 4, 'title': 'Tower of London', 'tokens': None},
              ],
          ],
      )

    with self.subTest(name='update_and_remove_entities'):
      # The same is true for entities. We can update and remove entity values:
      new_manager = self.copy_manager(manager)
      new_manager.update(
          at_path=parse_dsp('.query[:]'),
          attr_name='metadata',
          attr_value=kd.slice([
              query_metadata_schema.new(locale='en-US'),
              None,
          ]),
      )
      self.assertEqual(
          new_manager.get_data_slice_at(
              parse_dsp('.query[:].metadata'), with_all_descendants=True
          ).to_pytree(max_depth=-1),
          [
              {'locale': 'en-US'},
              # We removed the metadata from the second query:
              None,
          ],
      )
      del new_manager

    with self.subTest(name='update_selected_entities'):
      # Alternatively, we can update selected entity values without removing
      # others:
      manager.update(
          at_path=parse_dsp('.query[:]'),
          attr_name='metadata',
          attr_value=(
              kd.slice([
                  query_metadata_schema.new(locale='en-US'),
                  None,
              ])
              | manager.get_data_slice_at(
                  parse_dsp('.query[:].metadata')
              ).stub()  # The .stub() is not necessary when using pidsm.
          ),
      )
      self.assertEqual(
          manager.get_data_slice_at(
              parse_dsp('.query[:].metadata'), with_all_descendants=True
          ).to_pytree(max_depth=-1),
          [
              {'locale': 'en-US'},
              # We retain the metadata of the second query:
              {'locale': 'en-GB'},
          ],
      )

    with self.subTest(name='update_and_remove_lists'):
      # The same is true for lists. We can update and remove values:
      new_manager = self.copy_manager(manager)
      new_manager.update(
          at_path=parse_dsp('.query[:]'),
          attr_name='doc',
          attr_value=kd.slice([
              kd.list([
                  doc_schema.new(doc_id=5, title='Barack Obama'),
              ]),
              None,
          ]),
      )
      self.assertEqual(
          new_manager.get_data_slice_at(
              parse_dsp('.query[:].doc[:].doc_id'), with_all_descendants=True
          ).to_pytree(max_depth=-1),
          [
              [5],
              # We removed all docs from the second query:
              [],
          ],
      )
      del new_manager

    with self.subTest(name='update_selected_lists'):
      # Alternatively, we can update selected values without removing others:
      manager.update(
          at_path=parse_dsp('.query[:]'),
          attr_name='doc',
          attr_value=(
              kd.slice([
                  kd.list([
                      doc_schema.new(doc_id=5, title='Barack Obama'),
                  ]),
                  None,
              ])
              | manager.get_data_slice_at(parse_dsp('.query[:].doc')).stub()
          ),
      )
      self.assertEqual(
          manager.get_data_slice_at(
              parse_dsp('.query[:].doc[:].doc_id'), with_all_descendants=True
          ).to_pytree(max_depth=-1),
          [
              [5],
              # We retain the docs of the second query:
              [3, 4],
          ],
      )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_slices_where_all_values_are_missing(self, dsm_class):
    doc_schema = kd.named_schema(
        'doc',
        doc_id=kd.INT32,
        title=kd.STRING,
    )
    query_schema = kd.named_schema(
        'query',
        query_id=kd.INT32,
        text=kd.STRING,
        doc=kd.list_schema(doc_schema),
    )
    manager = self.new_manager(dsm_class)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(
                query_id=0,
                text='How tall is Obama',
            ),
            query_schema.new(
                query_id=1,
                text='How high is the Eiffel tower',
            ),
        ]),
    )
    kd.testing.assert_equivalent(
        manager.get_data_slice_at(
            parse_dsp('.query[:].doc[:].title')
        ).extract(),
        kd.slice([[], []], schema=kd.STRING).with_bag(kd.bag()),
    )

  def test_issues_with_allowing_kd_schema_as_a_subschema(self):
    # This test shows that, in vanilla Koda:
    # 1. Updates to a subslice with schema kd.SCHEMA can cause the schema of the
    #    main dataslice (i.e. the outer or containing dataslice) to be updated.
    # 2. Updating the schema of the main dataslice can cause the data of a
    #    subslice with schema kd.SCHEMA to be updated.
    # These behaviors are problematic for the
    # PersistedIncrementalDataSliceManager, which indexes an update by
    # considering only its schema. In particular, in the case of
    # 1. The update has the schema kd.SCHEMA, so on the basis of only that, the
    #    manager does not know which parts of the schema of the main dataslice
    #    are affected and how.
    # 2. The manager does not know which subslices with schema kd.SCHEMA are
    #    affected by an update to the main dataslice's schema.
    # Since users can perform multiple updates of kinds 1 and 2 in any
    # interleaved order, it seems there is no simple way to make the manager
    # correctly reason about them without analyzing/indexing each update on the
    # basis of the itemids contained therein.
    # Since that would be a significant departure from the current manager
    # design, it seems more attractive for the moment to ban the use of
    # kd.SCHEMA as a subschema. As a result, managed dataslices enforce a
    # complete separation between data and schema:
    # 1. Data cannot contain subslices that are schemas: kd.SCHEMA as well as
    #    kd.OBJECT are banned.
    # 2. Schema cannot contain data apart from metadata attributes, but they
    #    must be primitives. Hence no aliasing is possible - an update to
    #    the schema cannot update data in the main dataslice.

    doc_schema = kd.named_schema(
        'doc',
        doc_id=kd.INT32,
        title=kd.STRING,
        some_schema=kd.SCHEMA,
    )
    query_schema = kd.named_schema(
        'query',
        query_id=kd.INT32,
        text=kd.STRING,
        doc=kd.list_schema(doc_schema),
    )

    root = kd.new()
    root = root.updated(
        kd.attrs(
            root,
            query=kd.list([
                query_schema.new(
                    query_id=0,
                    text='How tall is Obama',
                    doc=kd.list([
                        doc_schema.new(doc_id=5, title='Barack Obama'),
                    ]),
                ),
                query_schema.new(
                    query_id=1,
                    text='How high is the Eiffel tower',
                ),
            ]),
        )
    )
    # Store the query schema as a scalar in the root.
    root = root.updated(
        kd.attrs(
            root,
            stored_query_schema=query_schema,
        )
    )

    # Issue #1: Updates to a subslice with schema kd.SCHEMA can cause the main
    # schema to be updated.
    #
    # Add a new attribute to the query schema.
    token_schema = kd.named_schema('token', text=kd.STRING)
    new_query_schema = query_schema.with_attr(
        'tokens',
        kd.list_schema(token_schema),
    )
    # Update the query schema by passing the new schema as a dataslice with
    # schema kd.SCHEMA. This is sneaky but it's allowed by vanilla Koda.
    root = root.updated(
        kd.attrs(
            root.query[:].doc[:],
            some_schema=kd.slice([[new_query_schema], []]),
        )
    )
    # The schema of the main dataslice managed by the manager is updated.
    kd.testing.assert_deep_equivalent(
        root.get_schema().query.get_item_schema(),
        new_query_schema,
    )
    # There is a new valid DataSlicePath for the new tokens attribute.
    kd.testing.assert_deep_equivalent(
        root.query[:].tokens,
        kd.slice([None, None], schema=token_schema),
    )
    # The stored query schema is also updated.
    kd.testing.assert_deep_equivalent(
        root.stored_query_schema,
        new_query_schema,
    )

    # Issue #2: Updating the schema of the main dataslice managed by the manager
    # can cause the data of a subslice with schema kd.SCHEMA to be updated.
    #
    # Add another attribute to the query schema via a data update.
    root = root.updated(
        kd.attrs(
            root.query[:],
            locale=kd.item('en-US'),
        )
    )
    expected_query_schema = new_query_schema.with_attr('locale', kd.STRING)
    kd.testing.assert_deep_equivalent(
        root.get_schema().query.get_item_schema(),
        expected_query_schema,
    )
    # This new attribute is also visible in the stored query schema, which has
    # schema kd.SCHEMA.
    kd.testing.assert_deep_equivalent(
        root.stored_query_schema,
        expected_query_schema,
    )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_schema_schema_is_not_supported(self, dsm_class):
    # Because of the issues shown in
    # test_issues_with_allowing_kd_schema_as_a_subschema, the
    # PersistedIncrementalDataSliceManager does not support the use of SCHEMA
    # schemas in updates. For consistency, the SimpleInMemoryDataSliceManager
    # does not support it either.

    manager = self.new_manager(dsm_class)

    query_schema = kd.named_schema('query')
    new_query = query_schema.new
    doc_schema = kd.named_schema('doc')
    new_doc = doc_schema.new

    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.slice([
            new_query(
                query_id='q1',
                doc=new_doc(doc_id=kd.slice([0, 1, 2, 3])).implode(),
            ),
            new_query(
                query_id='q2', doc=new_doc(doc_id=kd.slice([4, 5, 6])).implode()
            ),
        ]).implode(),
    )

    # Try to add an alias to the query schema. The alias has schema SCHEMA,
    # so the update is not supported.
    with self.assertRaisesRegex(
        ValueError, re.escape('SCHEMA schemas are not supported')
    ):
      manager.update(
          at_path=parse_dsp(''),
          attr_name='query_schema',
          attr_value=manager.get_schema().query,
      )

    # All updates with SCHEMA schemas are rejected.
    with self.assertRaisesRegex(
        ValueError, re.escape('SCHEMA schemas are not supported')
    ):
      manager.update(
          at_path=parse_dsp('.query[:].doc[:]'),
          attr_name='doc_id_schema',
          attr_value=kd.INT32,
      )
    # This is the case even when SCHEMA is used as a subschema.
    with self.assertRaisesRegex(
        ValueError, re.escape('SCHEMA schemas are not supported')
    ):
      manager.update(
          at_path=parse_dsp('.query[:]'),
          attr_name='doc_schema',
          attr_value=kd.new(title=kd.STRING),
      )

  @parameterized.named_parameters(
      ('pidsm', PersistedIncrementalDataSliceManager),
      ('simdsm', SimpleInMemoryDataSliceManager),
  )
  def test_generate_paths_with_various_max_depth_values(self, dsm_class):
    manager = self.new_manager(dsm_class)

    query_schema = kd.named_schema('query')
    new_query = query_schema.new
    doc_schema = kd.named_schema('doc')
    new_doc = doc_schema.new

    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.slice([
            new_query(
                query_id='q1',
                doc=new_doc(doc_id=kd.slice([0, 1, 2, 3])).implode(),
            ),
            new_query(
                query_id='q2', doc=new_doc(doc_id=kd.slice([4, 5, 6])).implode()
            ),
        ]).implode(),
    )

    # Passing max_depth=-1 should yield all paths.
    self.assertEqual(
        set(manager.generate_paths(max_depth=-1)),
        set([
            parse_dsp(''),
            parse_dsp('.query'),
            parse_dsp('.query[:]'),
            parse_dsp('.query[:].query_id'),
            parse_dsp('.query[:].doc'),
            parse_dsp('.query[:].doc[:]'),
            parse_dsp('.query[:].doc[:].doc_id'),
        ]),
    )
    # Passing any other negative value should yield no paths.
    for max_depth in [-100, -3, -2]:
      self.assertEmpty(set(manager.generate_paths(max_depth=max_depth)))
    self.assertEqual(
        set(manager.generate_paths(max_depth=0)),
        set([parse_dsp('')]),
    )
    self.assertEqual(
        set(manager.generate_paths(max_depth=1)),
        set([
            parse_dsp(''),
            parse_dsp('.query'),
        ]),
    )
    self.assertEqual(
        set(manager.generate_paths(max_depth=2)),
        set([
            parse_dsp(''),
            parse_dsp('.query'),
            parse_dsp('.query[:]'),
        ]),
    )
    self.assertEqual(
        set(manager.generate_paths(max_depth=3)),
        set([
            parse_dsp(''),
            parse_dsp('.query'),
            parse_dsp('.query[:]'),
            parse_dsp('.query[:].query_id'),
            parse_dsp('.query[:].doc'),
        ]),
    )
    self.assertEqual(
        set(manager.generate_paths(max_depth=4)),
        set([
            parse_dsp(''),
            parse_dsp('.query'),
            parse_dsp('.query[:]'),
            parse_dsp('.query[:].query_id'),
            parse_dsp('.query[:].doc'),
            parse_dsp('.query[:].doc[:]'),
        ]),
    )
    for max_depth in [5, 10, 100]:
      self.assertEqual(
          set(manager.generate_paths(max_depth=max_depth)),
          set(manager.generate_paths(max_depth=-1)),  # All paths
      )

  def test_get_data_slice(self):
    # Most of the tests above call get_data_slice() by passing a singleton set
    # to either populate or populate_including_descendants. The same happens
    # when calling get_subslice(). This test exercises more complex calls where
    # when both populate and populate_including_descendants are passed, or when
    # their values are not singleton sets.

    doc_schema = kd.named_schema('doc')
    token_info_schema = kd.named_schema('token_info')
    query_schema = kd.named_schema('query')
    new_query = query_schema.new
    new_doc = doc_schema.new
    new_token_info = token_info_schema.new

    query_data = kd.list([
        new_query(
            query_id=1,
            text='How tall is Obama',
            tokens=kd.list([
                new_token_info(
                    token_text='How',
                    part_of_speech='PRON',
                ),
                new_token_info(
                    token_text='tall',
                    part_of_speech='ADJ',
                ),
                new_token_info(
                    token_text='is',
                    part_of_speech='VERB',
                ),
                new_token_info(
                    token_text='Obama',
                    part_of_speech='NOUN',
                ),
            ]),
            doc=kd.list([
                new_doc(doc_id=1, title='Barack Obama'),
                new_doc(doc_id=2, title='Michelle Obama'),
            ]),
        ),
        new_query(
            query_id=2,
            text='How high is the Eiffel tower',
            tokens=kd.list([
                new_token_info(
                    token_text='How',
                    part_of_speech='PRON',
                ),
                new_token_info(
                    token_text='high',
                    part_of_speech='ADJ',
                ),
                new_token_info(
                    token_text='is',
                    part_of_speech='VERB',
                ),
                new_token_info(
                    token_text='the',
                    part_of_speech='DET',
                ),
                new_token_info(
                    token_text='Eiffel',
                    part_of_speech='NOUN',
                ),
                new_token_info(
                    token_text='tower',
                    part_of_speech='NOUN',
                ),
            ]),
            doc=kd.list([
                new_doc(doc_id=3, title='Eiffel tower'),
                new_doc(doc_id=4, title='Louvre Museum'),
                None,
            ]),
        ),
    ])

    # At present, Koda does not support deleting attributes from a schema.
    # So below, we will re-construct the schema but omit certain attributes. It
    # is good to know the full query schema for copy-pasting and deleting
    # attributes from it.
    kd.testing.assert_deep_equivalent(
        query_data.get_schema(),
        kd.list_schema(
            kd.named_schema(
                'query',
                query_id=kd.INT32,
                text=kd.STRING,
                tokens=kd.list_schema(
                    kd.named_schema(
                        'token_info',
                        token_text=kd.STRING,
                        part_of_speech=kd.STRING,
                    )
                ),
                doc=kd.list_schema(
                    kd.named_schema(
                        'doc',
                        doc_id=kd.INT32,
                        title=kd.STRING,
                    )
                ),
            )
        ),
    )

    persisted_data_dir = self.create_tempdir().full_path
    manager = PersistedIncrementalDataSliceManager(persisted_data_dir)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=query_data,
    )

    def get_new_root_data(new_query_schema):
      new_root_schema = (
          manager.get_schema().stub().with_attr('query', new_query_schema)
      )
      return (
          manager.get_data_slice()  # Populate nothing.
          .stub()  # Not really needed, but added for clarity.
          .with_attr('query', query_data)  # The original unchanged data.
          .extract(schema=new_root_schema)
      )

    # No query tokens.
    kd.testing.assert_deep_equivalent(
        manager.get_data_slice(
            populate={
                parse_dsp('.query[:].text'),
                parse_dsp('.query[:].query_id'),
            },
            populate_including_descendants={
                parse_dsp('.query[:].doc[:]'),
            },
        ),
        get_new_root_data(
            kd.list_schema(
                kd.named_schema(
                    'query',
                    query_id=kd.INT32,
                    text=kd.STRING,
                    doc=kd.list_schema(
                        kd.named_schema(
                            'doc',
                            doc_id=kd.INT32,
                            title=kd.STRING,
                        )
                    ),
                )
            )
        ),
    )

    # No query ids.
    self.assertEqual(
        manager.get_data_slice(
            populate={
                parse_dsp('.query[:].text'),
            },
            populate_including_descendants={
                parse_dsp('.query[:].doc'),
                parse_dsp('.query[:].tokens'),
            },
        ),
        get_new_root_data(
            kd.list_schema(
                kd.named_schema(
                    'query',
                    text=kd.STRING,
                    tokens=kd.list_schema(
                        kd.named_schema(
                            'token_info',
                            token_text=kd.STRING,
                            part_of_speech=kd.STRING,
                        )
                    ),
                    doc=kd.list_schema(
                        kd.named_schema(
                            'doc',
                            doc_id=kd.INT32,
                            title=kd.STRING,
                        )
                    ),
                )
            )
        ),
    )

    # See how the query text is tokenized, ignoring the part of speech. Doc info
    # is completely absent.
    self.assertEqual(
        manager.get_data_slice(
            populate={
                parse_dsp('.query[:].text'),
                parse_dsp('.query[:].tokens[:].token_text'),
            },
        ),
        get_new_root_data(
            kd.list_schema(
                kd.named_schema(
                    'query',
                    text=kd.STRING,
                    tokens=kd.list_schema(
                        kd.named_schema(
                            'token_info',
                            token_text=kd.STRING,
                        )
                    ),
                )
            )
        ),
    )

  def test_pidsm_docstrings_agree_with_the_interface_class(self):
    for method_name in dir(
        data_slice_manager_interface.DataSliceManagerInterface
    ):
      if method_name.startswith('_'):
        continue
      self.assertEqual(
          getattr(
              data_slice_manager_interface.DataSliceManagerInterface,
              method_name,
          ).__doc__,
          getattr(PersistedIncrementalDataSliceManager, method_name).__doc__,
      )

  def test_clear_cache(self):
    query_data = kd.list([
        kd.named_schema('query').new(
            query_id=0,
            text='How tall is Obama',
            doc=kd.list([
                kd.named_schema('doc').new(doc_id=1, title='Barack Obama'),
            ]),
        ),
    ])

    persisted_data_dir = self.create_tempdir().full_path
    manager = PersistedIncrementalDataSliceManager(persisted_data_dir)
    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=query_data,
    )
    kd.testing.assert_deep_equivalent(
        get_loaded_root_dataslice(manager), kd.new(query=query_data)
    )

    manager.clear_cache()

    # The data is not loaded anymore.
    kd.testing.assert_deep_equivalent(
        get_loaded_root_dataslice(manager), kd.new()
    )
    # The schema is still loaded.
    kd.testing.assert_deep_equivalent(
        manager.get_schema(),
        kd.schema.new_schema(query=query_data.get_schema()),
    )

    # The data can be loaded again.
    kd.testing.assert_deep_equivalent(
        manager.get_data_slice(
            populate_including_descendants={parse_dsp('.query')}
        ),
        kd.new(query=query_data),
    )
    kd.testing.assert_deep_equivalent(
        get_loaded_root_dataslice(manager), kd.new(query=query_data)
    )

    manager.clear_cache()

    def assert_bag_managers_have_equivalent_state(
        bm1: PersistedIncrementalDataBagManager,
        bm2: PersistedIncrementalDataBagManager,
    ):
      for attr_name in bm1.__dict__:
        bm1_attr = getattr(bm1, attr_name)
        bm2_attr = getattr(bm2, attr_name)
        if attr_name == '_loaded_bags_cache':
          self.assertEqual(bm1_attr.keys(), bm2_attr.keys())
          for key in bm1_attr.keys():
            kd.testing.assert_equivalent(bm1_attr[key], bm2_attr[key])
          continue
        if isinstance(bm1_attr, kd.types.DataSlice):
          kd.testing.assert_equivalent(bm1_attr, bm2_attr)
          continue
        self.assertEqual(bm1_attr, bm2_attr)

    # After clearing the cache, the state of the manager is the same as that of
    # a new manager instance with the same persistence_dir.
    new_manager = PersistedIncrementalDataSliceManager(
        persisted_data_dir, fs=manager._fs
    )
    for attr_name in manager.__dict__:
      manager_attr = getattr(manager, attr_name)
      new_manager_attr = getattr(new_manager, attr_name)
      if isinstance(manager_attr, kd.types.DataSlice):
        kd.testing.assert_deep_equivalent(manager_attr, new_manager_attr)
        continue
      if isinstance(manager_attr, PersistedIncrementalDataBagManager):
        assert_bag_managers_have_equivalent_state(
            manager_attr, new_manager_attr
        )
        continue
      if attr_name == '_schema_helper':
        kd.testing.assert_deep_equivalent(
            manager_attr._schema, new_manager_attr._schema
        )
        continue
      self.assertEqual(manager_attr, new_manager_attr)

  def test_branch(self):
    trunk_dir = self.create_tempdir().full_path
    trunk_manager = PersistedIncrementalDataSliceManager(trunk_dir)

    query_schema = kd.named_schema('query')
    trunk_manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(query_id='q1'),
            query_schema.new(query_id='q2'),
        ]),
    )

    doc_schema = kd.named_schema('doc')
    trunk_manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='doc',
        attr_value=kd.slice([
            doc_schema.new(doc_id=kd.slice([0, 1, 2, 3])).implode(),
            doc_schema.new(doc_id=kd.slice([4, 5, 6])).implode(),
        ]),
    )

    branch_dir = self.create_tempdir().full_path
    branch_manager = trunk_manager.branch(branch_dir)

    # Right after branching, the branch and the trunk have the same state.
    kd.testing.assert_deep_equivalent(
        branch_manager.get_schema(), trunk_manager.get_schema()
    )
    kd.testing.assert_deep_equivalent(
        branch_manager.get_data_slice(
            populate_including_descendants={parse_dsp('')}
        ),
        trunk_manager.get_data_slice(
            populate_including_descendants={parse_dsp('')}
        ),
    )

    # Creating the branch did not copy any of the bags. It created metadata
    # files to point to the trunk's bags.
    for subdir in [
        'data_bags',
        'schema_bags',
        'snn_to_data_bags_updates',
    ]:
      self.assertEqual(
          os.listdir(os.path.join(branch_dir, subdir)), ['metadata.pb']
      )

    # This update is for the trunk only.
    trunk_manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='query_text',
        attr_value=kd.slice(
            ['How tall is Obama', 'How high is the Eiffel tower']
        ),
    )
    kd.testing.assert_deep_equivalent(
        trunk_manager.get_data_slice_at(parse_dsp('.query[:].query_text')),
        kd.slice(['How tall is Obama', 'How high is the Eiffel tower']),
    )
    # The branch does not see the update to the trunk.
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "data slice path '.query[:].query_text' passed in argument"
            " 'populate' is invalid"
        ),
    ):
      branch_manager.get_data_slice_at(parse_dsp('.query[:].query_text'))
    # New instances for branch_dir also don't see the update.
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "data slice path '.query[:].query_text' passed in argument"
            " 'populate' is invalid"
        ),
    ):
      PersistedIncrementalDataSliceManager(branch_dir).get_data_slice_at(
          parse_dsp('.query[:].query_text')
      )

    # This update is for the branch only.
    branch_manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='query_text',
        attr_value=kd.slice(
            ['How high is the statue of Liberty', 'How low is the dead sea']
        ),
    )
    kd.testing.assert_deep_equivalent(
        branch_manager.get_data_slice_at(parse_dsp('.query[:].query_text')),
        kd.slice(
            ['How high is the statue of Liberty', 'How low is the dead sea']
        ),
    )
    # The trunk does not see the update.
    kd.testing.assert_deep_equivalent(
        trunk_manager.get_data_slice_at(parse_dsp('.query[:].query_text')),
        kd.slice(['How tall is Obama', 'How high is the Eiffel tower']),
    )
    # New instances for trunk_dir also don't see the update.
    kd.testing.assert_deep_equivalent(
        PersistedIncrementalDataSliceManager(trunk_dir).get_data_slice_at(
            parse_dsp('.query[:].query_text')
        ),
        kd.slice(['How tall is Obama', 'How high is the Eiffel tower']),
    )

    # Branches can be re-branched.
    twig_dir = self.create_tempdir().full_path
    twig_manager = branch_manager.branch(twig_dir)

    # The twig is based on the current state of the branch. It has query_text.
    kd.testing.assert_deep_equivalent(
        twig_manager.get_data_slice_at(parse_dsp('.query[:].query_text')),
        kd.slice(
            ['How high is the statue of Liberty', 'How low is the dead sea']
        ),
    )

    # An update to the twig does not affect the branch or the trunk.
    twig_manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='query_text',
        attr_value=kd.slice(
            ['How high is a rainforest giant', 'How high can humans jump']
        ),
    )
    kd.testing.assert_deep_equivalent(
        twig_manager.get_data_slice_at(parse_dsp('.query[:].query_text')),
        kd.slice(
            ['How high is a rainforest giant', 'How high can humans jump']
        ),
    )
    kd.testing.assert_deep_equivalent(
        branch_manager.get_data_slice_at(parse_dsp('.query[:].query_text')),
        kd.slice(
            ['How high is the statue of Liberty', 'How low is the dead sea']
        ),
    )
    kd.testing.assert_deep_equivalent(
        trunk_manager.get_data_slice_at(parse_dsp('.query[:].query_text')),
        kd.slice(['How tall is Obama', 'How high is the Eiffel tower']),
    )

  def test_branch_with_various_states_of_output_dir(self):
    trunk_dir = self.create_tempdir().full_path
    trunk_manager = PersistedIncrementalDataSliceManager(trunk_dir)
    query_schema = kd.named_schema('query')
    trunk_manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(query_id='q1'),
            query_schema.new(query_id='q2'),
        ]),
    )

    # The code guards against overwriting the trunk artifacts with a branch:
    with self.assertRaisesRegex(ValueError, 'the output_dir .* is not empty'):
      trunk_manager.branch(trunk_dir)

    # Branching into a non-existing directory works.
    branch_dir = os.path.join(self.create_tempdir().full_path, 'non_existing')
    self.assertFalse(os.path.exists(branch_dir))
    trunk_manager.branch(branch_dir)

    # Branching into a separately created empty directory works.
    branch_dir = self.create_tempdir().full_path
    self.assertTrue(os.path.exists(branch_dir))
    trunk_manager.branch(branch_dir)

    # Unexpected subdirectories are not allowed.
    branch_dir = self.create_tempdir().full_path
    os.makedirs(os.path.join(branch_dir, 'some_unexpected_subdir'))
    with self.assertRaisesRegex(ValueError, 'the output_dir .* is not empty'):
      trunk_manager.branch(branch_dir)

    # Unexpected files are not allowed.
    branch_dir = self.create_tempdir().full_path
    with open(os.path.join(branch_dir, 'metadata.pb'), 'wb') as f:
      f.write(b'some content')
    with self.assertRaisesRegex(ValueError, 'the output_dir .* is not empty'):
      trunk_manager.branch(branch_dir)

    # Cannot branch into a file.
    branch_dir = os.path.join(self.create_tempdir().full_path, 'output_dir')
    with open(branch_dir, 'w') as f:
      f.write('some content')
    with self.assertRaisesRegex(
        ValueError, 'the output_dir .* exists but it is not a directory'
    ):
      trunk_manager.branch(branch_dir)

  @parameterized.parameters(True, False)
  def test_branch_with_and_without_fs(self, use_new_fs: bool):
    trunk_dir = self.create_tempdir().full_path
    trunk_fs = mock.Mock(wraps=fs_implementation.FileSystemInteraction())
    trunk_manager = PersistedIncrementalDataSliceManager(trunk_dir, fs=trunk_fs)
    query_schema = kd.named_schema('query')
    trunk_manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            query_schema.new(query_id='q1'),
            query_schema.new(query_id='q2'),
        ]),
    )
    trunk_fs.reset_mock()

    branch_dir = self.create_tempdir().full_path
    if use_new_fs:
      branch_fs = mock.Mock(wraps=fs_implementation.FileSystemInteraction())
      branch_manager = trunk_manager.branch(branch_dir, fs=branch_fs)
      used_fs = branch_fs
    else:
      branch_manager = trunk_manager.branch(branch_dir)
      used_fs = trunk_fs

    self.assertEqual(trunk_manager._fs, trunk_fs)
    self.assertEqual(branch_manager._fs, used_fs)

    if use_new_fs:
      # All writes to branch_dir happened via branch_fs.
      # All reads from branch_dir and trunk_dir happened via branch_fs. There
      # are reads because the branching never copied the bags, and some bags are
      # read when initializing/using the branch.
      # The trunk did not do any reads or writes via trunk_fs: the needed data
      # was already resident in memory, and no writes to trunk_dir happened.
      self.assertEmpty(trunk_fs.method_calls)
    else:
      self.assertNotEmpty(trunk_fs.method_calls)

  def test_action_history_is_tracked_in_metadata(self):

    def without_timestamp(
        action_metadata: metadata_pb2.ActionMetadata,
    ) -> metadata_pb2.ActionMetadata:
      self.assertTrue(action_metadata.HasField('timestamp'))
      result = metadata_pb2.ActionMetadata()
      result.CopyFrom(action_metadata)
      result.ClearField('timestamp')
      return result

    persistence_dir = self.create_tempdir().full_path
    manager = PersistedIncrementalDataSliceManager(persistence_dir)

    self.assertLen(manager._metadata.action_history, 1)
    action_0 = manager._metadata.action_history[0]
    self.assertEqual(
        without_timestamp(action_0),
        metadata_pb2.ActionMetadata(
            description='Initial state with an empty root DataSlice',
            added_data_bag_name=[''],
            added_schema_bag_name=[''],
            added_snn_to_data_bags_update_bag_name=[''],
            creation=metadata_pb2.CreationAction(),
        ),
    )
    # Metadata is written to disk:
    self.assertEqual(
        persisted_incremental_data_slice_manager._read_metadata(
            fs_implementation.FileSystemInteraction(), persistence_dir
        ),
        manager._metadata,
    )

    manager.update(
        at_path=parse_dsp(''),
        attr_name='query',
        attr_value=kd.list([
            kd.named_schema('query').new(query_id='q1'),
            kd.named_schema('query').new(query_id='q2'),
        ]),
    )

    self.assertLen(manager._metadata.action_history, 2)
    self.assertEqual(manager._metadata.action_history[0], action_0)
    action_1 = manager._metadata.action_history[1]
    self.assertEqual(
        without_timestamp(action_1),
        metadata_pb2.ActionMetadata(
            added_data_bag_name=sorted(
                manager._data_bag_manager.get_available_bag_names() - {''}
            ),
            added_schema_bag_name=sorted(
                manager._schema_bag_manager.get_available_bag_names() - {''}
            ),
            added_snn_to_data_bags_update_bag_name=sorted(
                manager._schema_node_name_to_data_bags_updates_manager.get_available_bag_names()
                - {''}
            ),
            attribute_update=metadata_pb2.AttributeUpdateAction(
                at_path='',
                attr_name='query',
            ),
        ),
    )
    # Metadata is written to disk:
    self.assertEqual(
        persisted_incremental_data_slice_manager._read_metadata(
            fs_implementation.FileSystemInteraction(), persistence_dir
        ),
        manager._metadata,
    )

    branch_dir = self.create_tempdir().full_path
    branch_manager = manager.branch(branch_dir)
    self.assertLen(branch_manager._metadata.action_history, 1)
    branch_action_0 = branch_manager._metadata.action_history[0]
    self.assertEqual(
        without_timestamp(branch_action_0),
        metadata_pb2.ActionMetadata(
            added_data_bag_name=sorted(
                manager._data_bag_manager.get_available_bag_names()
            ),
            added_schema_bag_name=sorted(
                manager._schema_bag_manager.get_available_bag_names()
            ),
            added_snn_to_data_bags_update_bag_name=sorted(
                manager._schema_node_name_to_data_bags_updates_manager.get_available_bag_names()
            ),
            branch=metadata_pb2.BranchAction(
                parent_persistence_directory=persistence_dir,
                parent_action_history_index=1,
            ),
        ),
    )
    # Metadata is written to disk:
    self.assertEqual(
        persisted_incremental_data_slice_manager._read_metadata(
            fs_implementation.FileSystemInteraction(), branch_dir
        ),
        branch_manager._metadata,
    )

    branch_manager.update(
        at_path=parse_dsp('.query[:]'),
        attr_name='query_text',
        attr_value=kd.slice(
            ['How tall is Obama', 'How high is the Eiffel tower']
        ),
    )
    self.assertLen(branch_manager._metadata.action_history, 2)
    self.assertEqual(
        branch_manager._metadata.action_history[0], branch_action_0
    )
    branch_action_1 = branch_manager._metadata.action_history[1]
    self.assertEqual(
        without_timestamp(branch_action_1),
        metadata_pb2.ActionMetadata(
            added_data_bag_name=sorted(
                branch_manager._data_bag_manager.get_available_bag_names()
                - manager._data_bag_manager.get_available_bag_names()
            ),
            added_schema_bag_name=sorted(
                branch_manager._schema_bag_manager.get_available_bag_names()
                - manager._schema_bag_manager.get_available_bag_names()
            ),
            added_snn_to_data_bags_update_bag_name=sorted(
                branch_manager._schema_node_name_to_data_bags_updates_manager.get_available_bag_names()
                - manager._schema_node_name_to_data_bags_updates_manager.get_available_bag_names()
            ),
            attribute_update=metadata_pb2.AttributeUpdateAction(
                at_path='.query[:]',
                attr_name='query_text',
            ),
        ),
    )
    # Metadata is written to disk:
    self.assertEqual(
        persisted_incremental_data_slice_manager._read_metadata(
            fs_implementation.FileSystemInteraction(), branch_dir
        ),
        branch_manager._metadata,
    )
    # The trunk is not affected by the updates to the branch.
    self.assertEqual(manager._metadata.action_history[0], action_0)
    self.assertEqual(manager._metadata.action_history[1], action_1)
    self.assertEqual(
        persisted_incremental_data_slice_manager._read_metadata(
            fs_implementation.FileSystemInteraction(), persistence_dir
        ),
        manager._metadata,
    )


if __name__ == '__main__':
  absltest.main()
