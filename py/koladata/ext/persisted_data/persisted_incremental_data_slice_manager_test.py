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
from absl.testing import absltest
from koladata import kd
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager as pidsm
from koladata.ext.persisted_data import schema_helper as schema_helper_lib
from koladata.ext.persisted_data import test_only_schema_node_name_helper


parse_dsp = data_slice_path_lib.DataSlicePath.parse_from_string
GetAttr = data_slice_path_lib.GetAttr

schema_node_name = test_only_schema_node_name_helper.schema_node_name


def get_loaded_root_dataslice(
    manager: pidsm.PersistedIncrementalDataSliceManager,
) -> kd.types.DataSlice:
  """Returns the loaded root dataslice of the given manager."""
  return manager._root_dataslice.updated(manager._bag_manager.get_loaded_bag())


def get_loaded_schema(
    manager: pidsm.PersistedIncrementalDataSliceManager,
) -> kd.types.DataSlice:
  """Returns the schema of the loaded part of the DataSlice."""
  return get_loaded_root_dataslice(manager).get_schema()


def generate_loaded_data_slice_paths(
    manager: pidsm.PersistedIncrementalDataSliceManager, *, max_depth: int
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
    manager: pidsm.PersistedIncrementalDataSliceManager,
    path: data_slice_path_lib.DataSlicePath,
) -> bool:
  """Returns whether the given data slice path is loaded."""
  return schema_helper_lib.SchemaHelper(
      get_loaded_schema(manager)
  ).is_valid_data_slice_path(path)


class PersistedIncrementalDataSliceManagerTest(absltest.TestCase):

  def assert_manager_available_data_slice_paths(
      self,
      manager: pidsm.PersistedIncrementalDataSliceManager,
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
      manager: pidsm.PersistedIncrementalDataSliceManager,
      expected_loaded_data_slice_paths: set[str],
      *,
      max_depth_of_data_slice_paths: int = 5,
  ):
    actual_loaded_data_slice_paths = set(
        generate_loaded_data_slice_paths(
            manager, max_depth=max_depth_of_data_slice_paths
        )
    )
    self.assertEqual(
        len(actual_loaded_data_slice_paths),
        len(expected_loaded_data_slice_paths),
    )
    expected_loaded_data_slice_paths = set(
        data_slice_path_lib.DataSlicePath.parse_from_string(s)
        for s in expected_loaded_data_slice_paths
    )
    self.assertEqual(
        actual_loaded_data_slice_paths,
        expected_loaded_data_slice_paths,
    )

  def assert_manager_schema_node_names_to_num_bags(
      self,
      manager: pidsm.PersistedIncrementalDataSliceManager,
      expected_schema_node_names_to_num_bags: list[tuple[str, int]],
  ):
    actual = {
        snn: len(bag_names)
        for snn, bag_names in manager._schema_node_name_to_bag_names.items()
    }
    expected = dict(expected_schema_node_names_to_num_bags)
    self.assertEqual(len(expected), len(expected_schema_node_names_to_num_bags))
    self.assertEqual(actual, expected)

  def assert_manager_loaded_root_dataslice_pytree(
      self,
      manager: pidsm.PersistedIncrementalDataSliceManager,
      expected_current_root_dataslice_pytree: Any,
  ):
    self.assertEqual(
        kd.to_pytree(get_loaded_root_dataslice(manager), max_depth=-1),
        expected_current_root_dataslice_pytree,
    )

  def assert_manager_state(
      self,
      manager: pidsm.PersistedIncrementalDataSliceManager,
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

  def test_basic_usage(self):
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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
            manager.get_data_slice(parse_dsp('.query[:].doc[:].doc_title')),
            max_depth=-1,
        ),
        [['title0', 'title1', 'title2', None], ['title4', 'title5', 'title6']],
    )

    # Some time later, in another process...
    #
    # Start a new manager from the persistence dir.
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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
    self.assert_manager_state(
        manager,
        available_data_slice_paths=all_available_data_slice_paths,
        loaded_data_slice_paths={''},
        expected_loaded_root_dataslice_pytree={},
    )
    ds = manager.get_data_slice(parse_dsp('.query[:].query_text'))
    self.assert_manager_state(
        manager,
        available_data_slice_paths=all_available_data_slice_paths,
        loaded_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
            '.query[:].query_text',
        },
        expected_loaded_root_dataslice_pytree={
            'query': [
                {
                    'query_id': 'q1',
                    'query_text': 'How tall is Obama',
                },
                {
                    'query_id': 'q2',
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
    ds = manager.get_data_slice(parse_dsp('.query[:].doc'))
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
    # Similar to above, but now we request the dataslice *with all descendants*.
    ds = manager.get_data_slice(
        parse_dsp('.query[:].doc'), with_all_descendants=True
    )
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

  def test_update_at_path_that_is_not_loaded_yet(self):
    query_schema = kd.named_schema(
        'Query',
        query_id=kd.INT32,
        query_text=kd.STRING,
    )

    persistence_dir = self.create_tempdir().full_path
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assert_manager_state(
        manager,
        available_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            '.query[:].query_id',
            '.query[:].query_text',
        },
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
    self.assert_manager_state(
        manager,
        available_data_slice_paths={
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
        loaded_data_slice_paths={
            '',
            '.query',
            '.query[:]',
            # TODO: '.query[:].query_id' and '.query[:].query_text'
            # appear here because the update that added '.query' used a schema
            # that already mentioned '.query_id' and '.query_text'. This is
            # probably not the desired behavior, and should be fixed later on
            # when an update is chopped up into fine-grained DataBags. It's
            # currently not easy to fix it when we have one DataBag for the
            # entire update, because that DataBag will always contain the given
            # schema in full. Fixing this will also remove query_id and
            # query_text from the pytree below.
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
                    'doc': [
                        {'doc_id': 0, 'doc_title': 'title0'},
                        {'doc_id': 1, 'doc_title': 'title1'},
                        {'doc_id': 2, 'doc_title': 'title2'},
                        {'doc_id': 3, 'doc_title': None},
                    ],
                    'query_id': None,
                    'query_text': None,
                },
                {'doc': None, 'query_id': None, 'query_text': None},
            ]
        },
    )

  def test_add_update_whose_schema_is_recursive_and_data_has_no_cycles(self):
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

    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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

  def test_recursive_schema_across_two_calls_to_update_with_no_cycles_in_data(
      self,
  ):
    tree_node_schema = kd.named_schema('TreeNode')
    tree_root = tree_node_schema.new(value='tree_root')
    child1 = tree_node_schema.new(value='child1')
    child2 = tree_node_schema.new(value='child2')

    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)

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
            (schema_node_name(manager_schema), 1),
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
            (schema_node_name(manager_schema), 1),
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
    kd.testing.assert_equivalent(
        manager.get_data_slice(
            parse_dsp('.tree_root.children[:].children')
        ).extract(),
        kd.slice(
            [None, None],
            schema=kd.list_schema(
                kd.named_schema(
                    'TreeNode',
                    value=kd.STRING,
                    children=kd.list_schema(kd.named_schema('TreeNode')),
                )
            ),
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
            (schema_node_name(manager_schema), 1),
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

  def test_add_update_whose_schema_is_recursive_and_data_has_cycles(self):
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

    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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

  def test_recursive_schema_across_two_calls_to_update_with_cycles_in_data(
      self,
  ):
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)

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
        loaded_root_dataslice.node,
        manager.get_data_slice(
            parse_dsp('.node.outgoing_edges[:].outgoing_edges[:]')
        )
        .flatten()
        .S[0],
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 1),
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
            (schema_node_name(manager_schema), 1),
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
        manager.get_data_slice(parse_dsp('.node.another_label')).to_py(),
        'foo',
    )
    self.assertEqual(
        manager.get_data_slice(
            parse_dsp('.node.outgoing_edges[:].another_label')
        ).to_py(),
        [None],
    )
    self.assertEqual(
        manager.get_data_slice(
            parse_dsp('.node.outgoing_edges[:].incoming_edges[:].another_label')
        ).to_py(),
        [['foo']],
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 1),
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

  def test_another_recursive_tree_schema_add_parent_node_pointers(
      self,
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

    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    manager.update(
        at_path=parse_dsp(''), attr_name='tree_root', attr_value=tree_root
    )

    num_parent_updates = 0
    while True:
      parent_path = parse_dsp(
          '.tree_root' + '.children[:]' * num_parent_updates
      )
      child_path = parent_path.concat(parse_dsp('.children[:]'))
      parents = manager.get_data_slice(parent_path).stub()
      children = manager.get_data_slice(child_path).stub()
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
            (schema_node_name(manager_schema), 1),
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

  def test_aliasing_interaction_with_all_descendants(self):
    o = kd.new(x=1)
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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
        manager.get_data_slice(foo_path, with_all_descendants=True).y.to_py(), 2
    )
    self.assertEqual(
        manager.get_data_slice(bar_path, with_all_descendants=True).y.to_py(), 2
    )

    # Starting from a state where nothing is loaded, the manager should know
    # that "y" is a descendant of ".foo" and ".bar".
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(
        manager.get_data_slice(foo_path, with_all_descendants=True).y.to_py(), 2
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(
        manager.get_data_slice(bar_path, with_all_descendants=True).y.to_py(), 2
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 1),
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

  def test_deeper_aliasing(self):
    o = kd.new(x=kd.new(z=1))
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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
        manager.get_data_slice(foo_path, with_all_descendants=True).y.to_py(), 2
    )
    self.assertEqual(
        manager.get_data_slice(bar_path, with_all_descendants=True).y.to_py(), 2
    )

    # Starting from a state where nothing is loaded, the manager should know
    # that "y" is a descendant of ".foo" and ".bar".
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(
        manager.get_data_slice(foo_path, with_all_descendants=True).y.to_py(), 2
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(
        manager.get_data_slice(bar_path, with_all_descendants=True).y.to_py(), 2
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 1),
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

  def test_schema_overriding(self):
    x = kd.item(1)
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    manager.update(at_path=parse_dsp(''), attr_name='x', attr_value=x)

    new_x = kd.item('foo')
    manager.update(
        at_path=parse_dsp(''),
        attr_name='x',
        attr_value=new_x,
        overwrite_schema=True,
    )
    x_path = parse_dsp('.x')

    self.assertEqual(manager.get_data_slice(x_path).to_py(), 'foo')

    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(manager.get_data_slice(x_path).to_py(), 'foo')
    # Note that the original bag for x is not loaded here. The only 2 bags that
    # are loaded are the ones for the root and for the new 'x'.
    self.assertLen(manager._bag_manager.get_loaded_bag_names(), 2)

  def test_deeper_schema_overriding(self):
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    manager.update(
        at_path=parse_dsp(''), attr_name='x', attr_value=kd.list([1, 2])
    )
    manager.update(
        at_path=parse_dsp(''),
        attr_name='x',
        attr_value=kd.list(['a', 'b', 'c']),
        overwrite_schema=True,
    )

    self.assertEqual(
        manager.get_data_slice(parse_dsp('.x[:]')).to_py(), ['a', 'b', 'c']
    )

    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(
        manager.get_data_slice(parse_dsp('.x[:]')).to_py(), ['a', 'b', 'c']
    )
    # Note that the original bag for x is not loaded here. The only 2 bags that
    # are loaded are the ones for the root and for the new 'x'.
    self.assertLen(manager._bag_manager.get_loaded_bag_names(), 2)

  def test_schema_aliasing(self):
    s = kd.list_schema(item_schema=kd.INT32)
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    manager.update(at_path=parse_dsp(''), attr_name='foo', attr_value=s)

    # Next, we make ".bar" an alias for ".foo".
    manager.update(at_path=parse_dsp(''), attr_name='bar', attr_value=s.stub())

    # We can access ".get_item_schema()" via ".bar" (the subschema at ".bar" is
    # kd.SCHEMA):
    self.assertEqual(
        manager.get_data_slice(parse_dsp('.bar')).get_item_schema(), kd.INT32
    )
    self.assertEqual(
        manager._schema_helper.get_subschema_at(
            manager._schema_helper.get_schema_node_name_for_data_slice_path(
                parse_dsp('.bar')
            )
        ),
        kd.SCHEMA,
    )

    # Starting from a state where nothing is loaded, we should still be able to
    # access ".get_item_schema()" via ".bar". So internally, the manager must
    # understand that SCHEMAs can be aliased.
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(
        manager.get_data_slice(parse_dsp('.bar')).get_item_schema(), kd.INT32
    )

    manager_schema = manager.get_schema()
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 1),
            (schema_node_name(manager_schema, action=GetAttr('bar')), 2),
        ],
    )
    # It loaded one bag for the root and two for the schema node name
    # shared:SCHEMA.
    self.assertLen(manager._bag_manager.get_loaded_bag_names(), 3)

  def test_values_of_primitive_schemas_are_not_aliased(self):
    # In contrast to OBJECT and SCHEMA above, values of primitive schemas are
    # not aliased even if we create stubs. In this test we use a value with
    # schema kd.INT32 to illustrate the point:
    value = kd.item(1)
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    manager.update(at_path=parse_dsp(''), attr_name='foo', attr_value=value)

    # Next, we set ".bar" to a stub of ".foo". However, no aliasing happens -
    # it copies the value instead.
    manager.update(
        at_path=parse_dsp(''), attr_name='bar', attr_value=value.stub()
    )

    # We can access the values of both:
    self.assertEqual(manager.get_data_slice(parse_dsp('.foo')).to_py(), 1)
    self.assertEqual(manager.get_data_slice(parse_dsp('.bar')).to_py(), 1)

    # Starting from a state where nothing is loaded, we should still be able to
    # access the value via ".bar".
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(manager.get_data_slice(parse_dsp('.bar')).to_py(), 1)

    manager_schema = manager.get_schema()
    # It knows that ".foo" and ".bar" are not true aliases:
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 1),
            (schema_node_name(manager_schema, action=GetAttr('foo')), 1),
            (schema_node_name(manager_schema, action=GetAttr('bar')), 1),
        ],
    )
    # It loaded one bag for the root and one for ".bar".
    self.assertLen(manager._bag_manager.get_loaded_bag_names(), 2)

  def test_values_of_with_schema_itemid_are_not_aliased(self):
    # Similar to values of primitive schemas, ITEMID values are not aliased when
    # we create stubs.
    value = kd.new_itemid()
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    manager.update(at_path=parse_dsp(''), attr_name='foo', attr_value=value)

    # Next, we set ".bar" to a stub of ".foo". However, no aliasing happens -
    # it copies the value instead.
    manager.update(
        at_path=parse_dsp(''), attr_name='bar', attr_value=value.stub()
    )

    # We can access the values of both:
    self.assertEqual(manager.get_data_slice(parse_dsp('.foo')), value)
    self.assertEqual(manager.get_data_slice(parse_dsp('.bar')), value)

    # Starting from a state where nothing is loaded, we should still be able to
    # access the value via ".bar".
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(manager.get_data_slice(parse_dsp('.bar')), value)

    manager_schema = manager.get_schema()
    # It knows that ".foo" and ".bar" are not true aliases:
    self.assert_manager_schema_node_names_to_num_bags(
        manager,
        [
            (schema_node_name(manager_schema), 1),
            (schema_node_name(manager_schema, action=GetAttr('foo')), 1),
            (schema_node_name(manager_schema, action=GetAttr('bar')), 1),
        ],
    )
    # It loaded one bag for the root and one for ".bar".
    self.assertLen(manager._bag_manager.get_loaded_bag_names(), 2)

  def test_persistence_dir_is_hermetic(self):
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    ds = manager.get_data_slice(with_all_descendants=True)
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
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    ds = manager.get_data_slice(with_all_descendants=True)
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
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    # Starting from an empty persistence directory, the version should be 1.0.0.
    self.assertEqual(manager._metadata.version, '1.0.0')
    manager.update(at_path=parse_dsp(''), attr_name='x', attr_value=kd.item(1))
    # The version should still be 1.0.0 after adding an update.
    self.assertEqual(manager._metadata.version, '1.0.0')

    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    # The version is 1.0.0 after starting from a populated persistence directory
    self.assertEqual(manager._metadata.version, '1.0.0')

  def test_is_valid_available_and_loaded_data_slice_path(self):
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertTrue(manager.exists(parse_dsp('')))
    self.assertTrue(is_loaded_data_slice_path(manager, parse_dsp('')))
    for dsp_string in ['.foo', '.foo.x', '.foo.y', '.foo.y.z']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      self.assertFalse(is_loaded_data_slice_path(manager, dsp))

    manager.get_data_slice(parse_dsp('.foo'))
    for dsp_string in ['.foo', '.foo.x']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      self.assertTrue(is_loaded_data_slice_path(manager, dsp))
    for dsp_string in ['.foo.y', '.foo.y.z']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      self.assertFalse(is_loaded_data_slice_path(manager, dsp))

    manager.get_data_slice(parse_dsp('.foo.y.z'))
    for dsp_string in ['.foo', '.foo.x', '.foo.y', '.foo.y.z']:
      dsp = parse_dsp(dsp_string)
      self.assertTrue(manager.exists(dsp))
      self.assertTrue(is_loaded_data_slice_path(manager, dsp))

  def test_update_at_bad_path_raises(self):
    persistence_dir = os.path.join(
        self.create_tempdir().full_path, 'persisted_dataslice'
    )
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)

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

  def test_object_schema_is_not_supported(self):
    # This test shows that the functionality shown in
    # test_koda_behavior_of_object_schema is not supported by
    # PersistedIncrementalDataSliceManager. In particular, the manager does not
    # support OBJECT schemas in updates.

    persistence_dir = self.create_tempdir().full_path
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)

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
          attr_value=kd.obj(manager.get_data_slice(parse_dsp('.query[:].doc'))),
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

  def test_koda_behavior_of_itemid_with_multiple_schemas(self):
    e_foo = kd.new(a=1, schema='foo')
    e_bar = e_foo.with_schema(kd.named_schema('bar', a=kd.INT32))

    root = kd.new()

    root = root.with_attrs(x=kd.new(z=e_foo))
    root = root.with_attrs(y=e_bar)

    root = root.updated(kd.attrs(root.x.z, b=kd.item(2)))
    root = root.updated(kd.attrs(root.y, c=kd.item(3)))

    root = root.updated(kd.attrs(root.x.z, a=kd.item(4)))

    with self.assertRaisesRegex(
        ValueError, re.escape('reached with different schemas')
    ):
      _ = root.to_pytree(max_depth=-1)

    with self.assertRaisesRegex(
        ValueError, re.escape('reached with different schemas')
    ):
      _ = root.extract().to_pytree(max_depth=-1)

    self.assertEqual(
        root.x.z.extract().to_pytree(max_depth=-1),
        {'a': 4, 'b': 2},
    )
    self.assertEqual(
        root.y.extract().to_pytree(max_depth=-1),
        {'a': 4, 'c': 3},
    )

  def test_behavior_of_itemid_with_multiple_schemas(self):
    # Demonstrates that the behavior of PersistedIncrementalDataSliceManager is
    # consistent with the behavior of vanilla Koda. See below for the
    # assertion that is different. For that reason, we state in the docstring
    # of update() that the behavior is undefined if an itemid is associated with
    # two or more structured schemas.

    persistence_dir = self.create_tempdir().full_path
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)

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

    new_manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    with self.assertRaisesRegex(
        ValueError, re.escape('reached with different schemas')
    ):
      _ = new_manager.get_data_slice(with_all_descendants=True).to_pytree(
          max_depth=-1
      )

    with self.assertRaisesRegex(
        ValueError, re.escape('reached with different schemas')
    ):
      _ = (
          new_manager.get_data_slice(with_all_descendants=True)
          .extract()
          .to_pytree(max_depth=-1)
      )

    new_manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(
        new_manager.get_data_slice(
            parse_dsp('.foo.x'), with_all_descendants=True
        ).to_pytree(max_depth=-1),
        {'a': 4, 'b': 2},
    )

    new_manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    self.assertEqual(
        new_manager.get_data_slice(
            parse_dsp('.y'), with_all_descendants=True
        ).to_pytree(max_depth=-1),
        # This is different from the vanilla Koda behavior, which is
        # {'a': 4, 'c': 3}. The reason is that indexing on the schema alone is
        # not sufficient to understand that the
        # manager.update(
        #     at_path=parse_dsp('.x'), attr_name='a', attr_value=kd.item(4)
        # )
        # update also affected the dataslice at .y, since .y seemed to have a
        # different schema than .x
        {'a': 1, 'c': 3},
    )


if __name__ == '__main__':
  absltest.main()
