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
from typing import Callable

from absl.testing import absltest
from koladata import kd
from koladata.ext.persisted_data import bare_root_initial_data_manager
from koladata.ext.persisted_data import data_slice_manager_view as data_slice_manager_view_lib
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager as pidsm


DataSlicePath = data_slice_path_lib.DataSlicePath
DataSliceManagerView = data_slice_manager_view_lib.DataSliceManagerView


class DataSliceManagerViewTest(absltest.TestCase):

  def test_typical_usage(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        persistence_dir,
        description=(
            'Initial state of manager for playing with queries and docs'
        ),
    )
    root = DataSliceManagerView(manager)

    root.query = (
        kd.list([
            kd.named_schema('query').new(query_id=0, text='How tall is Obama'),
            kd.named_schema('query').new(
                query_id=1, text='How high is the Eiffel tower'
            ),
        ]),
        'Added queries with query_id and text',
    )

    queries = root.query[:]

    expected_query_schema = kd.named_schema(
        'query', query_id=kd.INT32, text=kd.STRING
    )
    kd.testing.assert_equivalent(queries.get_schema(), expected_query_schema)

    kd.testing.assert_equivalent(
        queries.text.get(),
        kd.slice(['How tall is Obama', 'How high is the Eiffel tower']),
    )
    kd.testing.assert_equivalent(
        queries.get(populate_including_descendants=[queries]),
        kd.slice([
            expected_query_schema.new(query_id=0, text='How tall is Obama'),
            expected_query_schema.new(
                query_id=1, text='How high is the Eiffel tower'
            ),
        ]),
    )
    restricted_query_schema = kd.named_schema('query', query_id=kd.INT32)
    kd.testing.assert_equivalent(
        root.get(populate=[queries.query_id]),
        kd.new(
            query=kd.list([
                restricted_query_schema.new(query_id=0),
                restricted_query_schema.new(query_id=1),
            ])
        ),
        schemas_equality=False,
    )

    queries.doc = (
        kd.slice([
            kd.list([
                kd.named_schema('doc').new(doc_id=1, title='Barack Obama'),
                kd.named_schema('doc').new(doc_id=2, title='Michelle Obama'),
            ]),
            kd.list(
                [kd.named_schema('doc').new(doc_id=2, title='Tower of London')]
            ),
        ]),
        'Added docs to queries. Populated doc_id and title',
    )

    docs = queries.doc[:]

    self.assertEqual(
        docs.title.get_schema(),
        kd.STRING,
    )
    kd.testing.assert_equivalent(
        docs.title.get(),
        kd.slice([['Barack Obama', 'Michelle Obama'], ['Tower of London']]),
    )

    with self.subTest('get_path_from_root'):
      self.assertEqual(
          root.get_path_from_root(),
          DataSlicePath.parse_from_string(''),
      )
      self.assertEqual(
          root.query.get_path_from_root(),
          DataSlicePath.parse_from_string('.query'),
      )
      self.assertEqual(
          queries.get_path_from_root(),
          DataSlicePath.parse_from_string('.query[:]'),
      )
      self.assertEqual(
          docs.get_path_from_root(),
          DataSlicePath.parse_from_string('.query[:].doc[:]'),
      )
      self.assertEqual(
          docs.title.get_path_from_root(),
          DataSlicePath.parse_from_string('.query[:].doc[:].title'),
      )

    with self.subTest('get_manager'):
      self.assertEqual(
          docs.title.get_manager(),
          manager,
      )
      self.assertEqual(
          queries.get_manager(),
          manager,
      )

    with self.subTest('get_root'):
      self.assertEqual(
          docs.title.get_root(),
          root,
      )
      self.assertEqual(
          queries.get_root(),
          root,
      )
      self.assertEqual(
          docs.get_root(),
          root,
      )

    with self.subTest('get_parent'):
      self.assertEqual(
          docs.title.get_parent(),
          docs,
      )
      self.assertEqual(
          queries.get_parent(),
          root.query,
      )
      with self.assertRaisesRegex(
          ValueError,
          re.escape("the path '' has no parent"),
      ):
        root.get_parent()

    with self.subTest('get_grandparent'):
      self.assertEqual(
          docs.title.get_grandparent(),
          docs.get_parent(),
      )
      self.assertEqual(
          docs.get_grandparent(),
          queries,
      )
      self.assertEqual(
          queries.get_grandparent(),
          root,
      )
      with self.assertRaisesRegex(
          ValueError,
          re.escape("the path '' has no grandparent"),
      ):
        root.get_grandparent()
      with self.assertRaisesRegex(
          ValueError,
          re.escape("the path '.query' has no grandparent"),
      ):
        root.query.get_grandparent()

    with self.subTest('get_ancestor'):
      self.assertEqual(
          docs.title.get_ancestor(num_levels_up=0),
          docs.title,
      )
      self.assertEqual(
          docs.title.get_ancestor(num_levels_up=1),
          docs,
      )
      self.assertEqual(
          docs.title.get_ancestor(num_levels_up=2),
          docs.get_parent(),
      )
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              "the path '' does not support num_levels_up=1. The maximum"
              ' valid value is 0'
          ),
      ):
        root.get_ancestor(num_levels_up=1)
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              "the path '.query[:]' does not support num_levels_up=5. The"
              ' maximum valid value is 2'
          ),
      ):
        queries.get_ancestor(num_levels_up=5)
      with self.assertRaisesRegex(
          ValueError,
          re.escape('num_levels_up must be >= 0, but got -1'),
      ):
        docs.title.get_ancestor(num_levels_up=-1)

    with self.subTest('get_children'):
      self.assertEqual(
          root.get_children(),
          [root.query],
      )
      self.assertEqual(
          root.query.get_children(),
          [queries],
      )
      self.assertEqual(
          queries.get_children(),
          [
              DataSliceManagerView(
                  manager, DataSlicePath.parse_from_string(path)
              )
              # Note the fixed order of the paths.
              for path in [
                  '.query[:].doc',
                  '.query[:].query_id',
                  '.query[:].text',
              ]
          ],
      )
      self.assertEqual(
          docs.get_children(),
          [
              DataSliceManagerView(
                  manager, DataSlicePath.parse_from_string(path)
              )
              for path in [
                  '.query[:].doc[:].doc_id',
                  '.query[:].doc[:].title',
              ]
          ],
      )
      self.assertEmpty(docs.title.get_children())

    with self.subTest('get_list_items'):
      self.assertEqual(
          root.query.get_list_items(),
          queries,
      )
      self.assertEqual(
          root.query.get_list_items().doc.get_list_items(),
          docs,
      )

    with self.subTest('get_attr'):
      self.assertEqual(
          root.get_attr('query'),
          root.query,
      )
      self.assertEqual(
          queries.get_attr('query_id'),
          queries.query_id,
      )
      self.assertEqual(
          docs.get_attr('title'),
          docs.title,
      )

    token_info_schema = kd.named_schema('token_info', part_of_speech=kd.STRING)
    with self.subTest('dict_operations'):
      queries.token = (
          kd.slice([
              kd.dict(
                  {
                      'How': token_info_schema.new(part_of_speech='DET'),
                      'tall': token_info_schema.new(part_of_speech='ADJ'),
                      'is': token_info_schema.new(part_of_speech='VERB'),
                      'Obama': token_info_schema.new(part_of_speech='NOUN'),
                  },
              ),
              kd.dict({
                  'How': token_info_schema.new(part_of_speech='DET'),
                  'high': token_info_schema.new(part_of_speech='ADJ'),
                  'is': token_info_schema.new(part_of_speech='VERB'),
                  'the': token_info_schema.new(part_of_speech='DET'),
                  'Eiffel': token_info_schema.new(part_of_speech='NOUN'),
                  'tower': token_info_schema.new(part_of_speech='NOUN'),
              }),
          ]),
          'Added tokens of the query text',
      )

      tokens = root.query[:].token

      kd.testing.assert_equivalent(
          tokens.get_schema(),
          kd.dict_schema(kd.STRING, token_info_schema),
      )
      self.assertEqual(
          tokens.get_children(),
          [
              # Note the fixed order: keys before values.
              tokens.get_dict_keys(),
              tokens.get_dict_values(),
          ],
      )
      self.assertEqual(
          # The keys are not ordered, so we use sets of keys.
          [set(t) for t in tokens.get_dict_keys().get().to_py()],
          [
              {'How', 'tall', 'is', 'Obama'},
              {'How', 'high', 'is', 'the', 'Eiffel', 'tower'},
          ],
      )
      kd.testing.assert_equivalent(
          tokens.get_dict_values().get_schema(),
          token_info_schema,
      )
      self.assertEqual(
          [
              dict(zip(keys, values))
              for keys, values in zip(
                  tokens.get_dict_keys().get().to_py(),
                  tokens.get_dict_values().part_of_speech.get().to_py(),
              )
          ],
          [
              {'How': 'DET', 'tall': 'ADJ', 'is': 'VERB', 'Obama': 'NOUN'},
              {
                  'How': 'DET',
                  'high': 'ADJ',
                  'is': 'VERB',
                  'the': 'DET',
                  'Eiffel': 'NOUN',
                  'tower': 'NOUN',
              },
          ],
      )

  def test_wrong_and_acceptable_initialization(self):
    manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path
    )
    with self.assertRaisesRegex(
        ValueError, re.escape("invalid data slice path: '.query[:]'")
    ):
      DataSliceManagerView(
          manager, DataSlicePath.parse_from_string('.query[:]')
      )
    with self.assertRaisesRegex(
        ValueError, re.escape("invalid data slice path: '.query'")
    ):
      DataSliceManagerView(manager, DataSlicePath.parse_from_string('.query'))

    manager.update(
        at_path=DataSlicePath.parse_from_string(''),
        attr_name='query',
        attr_value=kd.list([
            kd.named_schema('query').new(query_id=0, text='How tall is Obama'),
            kd.named_schema('query').new(
                query_id=1, text='How high is the Eiffel tower'
            ),
        ]),
    )
    some_view = DataSliceManagerView(
        manager, DataSlicePath.parse_from_string('.query')
    )
    self.assertEqual(
        some_view.get_schema(),
        kd.list_schema(
            kd.named_schema('query', query_id=kd.INT32, text=kd.STRING)
        ),
    )
    some_view = DataSliceManagerView(
        manager, DataSlicePath.parse_from_string('.query[:]')
    )
    self.assertEqual(
        some_view.get_schema(),
        kd.named_schema('query', query_id=kd.INT32, text=kd.STRING),
    )
    with self.assertRaisesRegex(
        ValueError, re.escape("invalid data slice path: '.query[:].doc'")
    ):
      DataSliceManagerView(
          manager, DataSlicePath.parse_from_string('.query[:].doc')
      )

  def test_views_that_become_invalid_after_update(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        persistence_dir
    )

    root = DataSliceManagerView(manager)
    root.query = kd.list([
        kd.named_schema('query').new(query_id=0, text='How tall is Obama'),
        kd.named_schema('query').new(
            query_id=1, text='How high is the Eiffel tower'
        ),
    ])
    query = root.query[:]

    query.doc = kd.slice([
        kd.list([
            kd.named_schema('doc').new(doc_id=1, title='Barack Obama'),
            kd.named_schema('doc').new(doc_id=2, title='Michelle Obama'),
        ]),
        kd.list(
            [kd.named_schema('doc').new(doc_id=2, title='Tower of London')]
        ),
    ])
    doc_list = query.doc
    doc = doc_list[:]
    doc_title = doc.title

    token_info_schema = kd.named_schema('token_info', part_of_speech=kd.STRING)
    query.tokens = kd.slice([
        kd.dict(
            {
                'How': token_info_schema.new(part_of_speech='DET'),
                'tall': token_info_schema.new(part_of_speech='ADJ'),
                'is': token_info_schema.new(part_of_speech='VERB'),
                'Obama': token_info_schema.new(part_of_speech='NOUN'),
            },
        ),
        kd.dict({
            'How': token_info_schema.new(part_of_speech='DET'),
            'high': token_info_schema.new(part_of_speech='ADJ'),
            'is': token_info_schema.new(part_of_speech='VERB'),
            'the': token_info_schema.new(part_of_speech='DET'),
            'Eiffel': token_info_schema.new(part_of_speech='NOUN'),
            'tower': token_info_schema.new(part_of_speech='NOUN'),
        }),
    ])
    tokens = query.tokens
    tokens_keys = tokens.get_dict_keys()
    tokens_values = tokens.get_dict_values()

    # We now give an update that will make the non-root views above invalid.
    root.query = kd.dict({'hello': 1, 'world': 2})
    # Add a new list to have a valid list in the root.
    root.some_list = kd.list([1, 2, 3])

    # Accessing/updating the underlying DataSlice and its schema should complain
    # when the views are invalid.
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:]'"),
    ):
      query.get_schema()
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].doc[:].title'"),
    ):
      doc_title.get_data_slice()
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].doc[:].title'"),
    ):
      doc_title.get()
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].doc[:].title'"),
    ):
      doc_title.filter(kd.missing)  # Try to filter out all the docs.
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].doc[:]'"),
    ):
      doc.word_count = kd.item(12345)

    # The state of invalid views can still be accessed:
    self.assertEqual(
        doc.get_path_from_root(),
        DataSlicePath.parse_from_string('.query[:].doc[:]'),
    )
    self.assertEqual(tokens_values.get_manager(), manager)
    self.assertFalse(tokens_keys.is_view_valid())

    # Generic methods for navigation can sometimes succeed even when the view is
    # invalid.
    self.assertEqual(tokens_values.get_root(), root)
    self.assertEqual(query.get_parent(), root.query)
    self.assertEqual(tokens.get_grandparent(), root.query)
    self.assertEqual(tokens_values.get_ancestor(num_levels_up=3), root.query)
    # However, the latter 3 methods will fail if the resulting view is invalid.
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].tokens'"),
    ):
      tokens_values.get_parent()
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:]'"),
    ):
      tokens_keys.get_grandparent()
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:]'"),
    ):
      doc_title.get_ancestor(num_levels_up=3)
    # Getting children of an invalid view always fails.
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].doc[:].title'"),
    ):
      doc_title.get_children()

    # Specific methods for navigation require valid views from which to start
    # the navigation.
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].doc'"),
    ):
      doc_list.get_list_items()
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].doc'"),
    ):
      doc_list[:]  # pylint: disable=pointless-statement
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].tokens'"),
    ):
      tokens.get_dict_keys()
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].tokens'"),
    ):
      tokens.get_dict_values()
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].tokens'"),
    ):
      tokens.get_attr('part_of_speech')
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].tokens.get_values()'"),
    ):
      tokens_values.part_of_speech.get_data_slice()

    with self.subTest('ipython_asks_for__all__'):
      # view.__all__ should always succeed, for valid and invalid views.
      # However, its result depends on whether the view is valid and on the
      # schema of the underlying data.

      self.assertTrue(hasattr(root, '__all__'))

      # A valid entity view:
      self.assertEqual(
          root.__all__,
          # Note no get_parent() or get_grandparent() or get_list_items() here:
          [
              'find_descendants',
              'get',
              'get_ancestor',
              'get_attr',
              'get_children',
              'get_data_slice',
              'get_manager',
              'get_path_from_root',
              'get_root',
              'get_schema',
              'grep_descendants',
              'is_view_valid',
              'query',
              'some_list',
              'update',
          ],
      )
      # A valid list view:
      self.assertEqual(
          root.some_list.__all__,
          # Note no get_grandparent() here:
          [
              'filter',
              'find_descendants',
              'get',
              'get_ancestor',
              'get_children',
              'get_data_slice',
              'get_list_items',
              'get_manager',
              'get_parent',
              'get_path_from_root',
              'get_root',
              'get_schema',
              'grep_descendants',
              'is_view_valid',
          ],
      )
      # A valid dict view:
      self.assertEqual(
          root.query.__all__,
          [
              'filter',
              'find_descendants',
              'get',
              'get_ancestor',
              'get_children',
              'get_data_slice',
              'get_dict_keys',
              'get_dict_values',
              'get_manager',
              'get_parent',
              'get_path_from_root',
              'get_root',
              'get_schema',
              'grep_descendants',
              'is_view_valid',
          ],
      )
      # A view without children but with a valid grandparent:
      self.assertEqual(
          root.query.get_dict_values().__all__,
          [
              'filter',
              'get',
              'get_ancestor',
              'get_data_slice',
              'get_grandparent',
              'get_manager',
              'get_parent',
              'get_path_from_root',
              'get_root',
              'get_schema',
              'is_view_valid',
          ],
      )
      # An invalid view whose parent is still invalid but whose grandparent is
      # valid:
      self.assertEqual(
          tokens.__all__,
          [
              'get_ancestor',
              'get_grandparent',
              'get_manager',
              'get_path_from_root',
              'get_root',
              'is_view_valid',
          ],
      )
      # An invalid view with no valid parent or grandparent:
      self.assertEqual(
          tokens_values.__all__,
          [
              'get_ancestor',
              'get_manager',
              'get_path_from_root',
              'get_root',
              'is_view_valid',
          ],
      )
      # The root view:
      self.assertEqual(
          root.__all__,
          [
              # Note: no filter here because the root cannot be filtered out.
              # Also, no get_parent or get_grandparent here:
              'find_descendants',
              'get',
              'get_ancestor',
              'get_attr',
              'get_children',
              'get_data_slice',
              'get_manager',
              'get_path_from_root',
              'get_root',
              'get_schema',
              'grep_descendants',
              'is_view_valid',
              'query',
              'some_list',
              'update',
          ],
      )

  def test_branch_and_filter_recipe(self):

    def create_initial_manager() -> pidsm.PersistedIncrementalDataSliceManager:
      persistence_dir = self.create_tempdir().full_path
      manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
          persistence_dir
      )
      root = DataSliceManagerView(manager)

      root.query = (
          kd.list([
              kd.named_schema('query').new(
                  query_id=0, text='How tall is Obama'
              ),
              kd.named_schema('query').new(
                  query_id=1, text='How high is the Eiffel tower'
              ),
          ]),
          'Added query with query_id and text populated',
      )

      queries = root.query[:]

      queries.doc = (
          kd.slice([
              kd.list([
                  kd.named_schema('doc').new(doc_id=1, title='Barack Obama'),
                  kd.named_schema('doc').new(doc_id=2, title='Michelle Obama'),
              ]),
              kd.list([
                  kd.named_schema('doc').new(doc_id=3, title='Tower of London')
              ]),
          ]),
          'Added doc with doc_id and title populated',
      )

      return manager

    def branch_and_filter(
        manager: pidsm.PersistedIncrementalDataSliceManager,
    ) -> pidsm.PersistedIncrementalDataSliceManager:
      branch_dir = self.create_tempdir().full_path
      manager = manager.branch(
          branch_dir, description='Branch to showcase filtering'
      )

      root = DataSliceManagerView(manager)
      queries = root.query[:]
      docs = queries.doc[:]

      # Filter the docs to only keep the ones with "Barack" in the title, and
      # filter the queries to only keep the ones with at least one such doc.
      new_docs = docs.get_data_slice().select(
          kd.strings.contains(docs.title.get_data_slice(), 'Barack')
      )
      queries.doc = (
          new_docs.implode(),
          'Filtered docs to keep only those with "Barack" in the title',
      )
      new_queries = queries.get_data_slice().select(
          kd.agg_any(kd.has(new_docs))
      )
      root.query = (
          new_queries.implode(),
          (
              'Filtered queries to keep only those with at least one doc with'
              ' "Barack" in the title'
          ),
      )

      return manager

    manager = create_initial_manager()
    filtered_manager = branch_and_filter(manager)
    root = DataSliceManagerView(manager)
    filtered_root = DataSliceManagerView(filtered_manager)
    kd.testing.assert_equivalent(
        root.get_data_slice(populate_including_descendants=[root]),
        kd.new(
            query=kd.list([
                kd.named_schema('query').new(
                    query_id=0,
                    text='How tall is Obama',
                    doc=kd.list([
                        kd.named_schema('doc').new(
                            doc_id=1, title='Barack Obama'
                        ),
                        kd.named_schema('doc').new(
                            doc_id=2, title='Michelle Obama'
                        ),
                    ]),
                ),
                kd.named_schema('query').new(
                    query_id=1,
                    text='How high is the Eiffel tower',
                    doc=kd.list([
                        kd.named_schema('doc').new(
                            doc_id=3, title='Tower of London'
                        )
                    ]),
                ),
            ]),
        ),
        schemas_equality=False,
    )
    # The filtering recipe above results in one query with one doc:
    kd.testing.assert_equivalent(
        filtered_root.get_data_slice(
            populate_including_descendants=[filtered_root]
        ),
        kd.new(
            query=kd.list([
                kd.named_schema('query').new(
                    query_id=0,
                    text='How tall is Obama',
                    doc=kd.list([
                        kd.named_schema('doc').new(
                            doc_id=1, title='Barack Obama'
                        )
                    ]),
                ),
            ]),
        ),
        schemas_equality=False,
    )

    manager_revision_descriptions = [
        revision_metadata.description
        for revision_metadata in manager._metadata.revision_history
    ]
    self.assertEqual(
        manager_revision_descriptions,
        [
            'Initial state with an empty root',
            'Added query with query_id and text populated',
            'Added doc with doc_id and title populated',
        ],
    )
    filtered_manager_revision_descriptions = [
        revision_metadata.description
        for revision_metadata in filtered_manager._metadata.revision_history
    ]
    self.assertEqual(
        filtered_manager_revision_descriptions,
        [
            'Branch to showcase filtering',
            'Filtered docs to keep only those with "Barack" in the title',
            (
                'Filtered queries to keep only those with at least one doc with'
                ' "Barack" in the title'
            ),
        ],
    )

  def test_filter(self):
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
    query_list = kd.list([
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
                        'How': token_info_schema.new(is_noun=False),
                        'Eiffel tower': token_info_schema.new(is_noun=True),
                    }),
                ),
                doc_schema.new(doc_id=4, title='Tower of London'),
            ]),
            metadata=query_metadata_schema.new(locale='en-GB'),
        ),
        query_schema.new(
            query_id=2,
            text='How old is Bush?',
            doc=kd.list([
                doc_schema.new(
                    doc_id=5,
                    title='George W. Bush hands over the baton',
                    tokens=kd.dict({
                        'George W. Bush': token_info_schema.new(is_noun=True),
                        'hands': token_info_schema.new(is_noun=False),
                    }),
                ),
                doc_schema.new(doc_id=6, title='The oldest bushes'),
                doc_schema.new(doc_id=7, title='Ancient bushes'),
            ]),
            metadata=query_metadata_schema.new(locale='en-US'),
        ),
    ])

    persistence_dir = self.create_tempdir().full_path
    trunk_initial_data_manager = (
        bare_root_initial_data_manager.BareRootInitialDataManager.create_new()
    )
    trunk_manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        persistence_dir, initial_data_manager=trunk_initial_data_manager
    )
    trunk_root = DataSliceManagerView(trunk_manager)
    trunk_root.query = query_list, 'Added queries populated with data'

    with self.subTest('filter_root'):
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path, description='Branch to filter root'
      )
      root = DataSliceManagerView(branch_manager)
      with self.assertRaisesRegex(
          ValueError,
          re.escape(
              'the root cannot be filtered. Please filter a non-root path'
          ),
      ):
        root.filter(kd.present)

    def _test_filter_docs_from_view(
        view_creator_fn: Callable[[DataSliceManagerView], DataSliceManagerView],
    ):
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path, description='Branch to filter docs'
      )
      root = DataSliceManagerView(branch_manager)
      view = view_creator_fn(root)
      # Filter the docs to only keep the ones with "Barack" in the title, and
      # filter the queries to only keep the ones with at least one such doc.
      view.filter(
          kd.strings.contains(
              root.query[:].doc[:].title.get_data_slice(), 'Barack'
          ),
          description=(
              'Filtered docs to keep only those with "Barack" in the title'
          ),
      )
      kd.testing.assert_equivalent(
          root.get_data_slice(populate_including_descendants=[root]),
          trunk_initial_data_manager.get_schema().new(
              query=kd.list([
                  query_schema.new(
                      query_id=0,
                      text='How tall is Obama',
                      doc=kd.list([
                          doc_schema.new(doc_id=0, title='Barack Obama'),
                      ]),
                  ),
              ])
          ),
      )
      # The itemids of the entities in the filtered DataSlice are the same as
      # in the unfiltered DataSlice:
      kd.testing.assert_equivalent(
          root.get_data_slice().get_itemid(),
          trunk_root.get_data_slice().get_itemid(),
      )
      kd.testing.assert_equivalent(
          root.query[:].get_data_slice().S[0].get_itemid(),
          trunk_root.query[:].get_data_slice().S[0].get_itemid(),
      )
      kd.testing.assert_equivalent(
          root.query[:].doc[:].get_data_slice().S[0, 0].get_itemid(),
          trunk_root.query[:].doc[:].get_data_slice().S[0, 0].get_itemid(),
      )
      branch_manager_revision_descriptions = [
          revision_metadata.description
          for revision_metadata in branch_manager._metadata.revision_history
      ]
      self.assertEqual(
          branch_manager_revision_descriptions,
          [
              'Branch to filter docs',
              'Filtered docs to keep only those with "Barack" in the title',
          ],
      )

    with self.subTest('filter_docs_from_doc_entity_view'):
      _test_filter_docs_from_view(lambda root: root.query[:].doc[:])

    with self.subTest('filter_docs_from_doc_feature_view'):
      _test_filter_docs_from_view(lambda root: root.query[:].doc[:].title)

    with self.subTest('filter_docs_with_all_present_mask_is_a_noop'):
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path, description='Branch to filter docs'
      )
      root = DataSliceManagerView(branch_manager)
      doc = root.query[:].doc[:]
      for selection_mask in [kd.present, kd.val_like(doc.get(), kd.present)]:
        doc.filter(
            selection_mask,
            description='Filtered docs by selecting all of them',
        )
        kd.testing.assert_equivalent(
            root.get_data_slice(populate_including_descendants=[root]),
            trunk_root.get_data_slice(
                populate_including_descendants=[trunk_root]
            ),
            ids_equality=True,
        )
        branch_manager_revision_descriptions = [
            revision_metadata.description
            for revision_metadata in branch_manager._metadata.revision_history
        ]
        self.assertEqual(
            branch_manager_revision_descriptions,
            [
                'Branch to filter docs',
                # Note that the filtering above is a no-op, so no revision is
                # added to the branch manager.
            ],
        )

    with self.subTest('filter_docs_with_all_missing_mask'):
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path, description='Branch to filter docs'
      )
      root = DataSliceManagerView(branch_manager)
      doc = root.query[:].doc[:]
      doc.filter(
          kd.missing,
          description='Filtered all docs',
      )
      kd.testing.assert_equivalent(
          root.get_data_slice(populate_including_descendants=[root]),
          trunk_root.get_data_slice()
          .with_attr('query', None)
          .with_schema(trunk_root.get_schema()),
          ids_equality=True,
      )
      branch_manager_revision_descriptions = [
          revision_metadata.description
          for revision_metadata in branch_manager._metadata.revision_history
      ]
      self.assertEqual(
          branch_manager_revision_descriptions,
          [
              'Branch to filter docs',
              'Filtered all docs',
          ],
      )

    with self.subTest('filter_queries_from_a_doc_list_view'):
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path,
          description='Branch to filter queries',
      )
      root = DataSliceManagerView(branch_manager)
      doc_list = root.query[:].doc
      doc_list.filter(
          # This business with explode and implode is needed because the size
          # operator does not work for the minimal DataSlices of list views.
          # Such minimal DataSlices only have the list itemids and the list
          # schemaid and no information about the number of items.
          # That might seems strange, but it is consistent with the behavior of
          # DICT stubs, which also do not know the number of items in the dict,
          # and entity stubs, which do not know whether an attribute is present
          # or not.
          kd.lists.size(doc_list[:].get_data_slice().implode()) <= 2,
          description=(
              'Filtered queries to keep only those with a doc list of size at'
              ' most 2'
          ),
      )
      kd.testing.assert_equivalent(
          root.get_data_slice(populate_including_descendants=[root]),
          trunk_initial_data_manager.get_schema().new(
              query=kd.list([
                  query_schema.new(
                      query_id=1,
                      text='How high is the Eiffel tower',
                      doc=kd.list([
                          doc_schema.new(
                              doc_id=3,
                              title='Eiffel tower',
                              tokens=kd.dict({
                                  'How': token_info_schema.new(is_noun=False),
                                  'Eiffel tower': token_info_schema.new(
                                      is_noun=True
                                  ),
                              }),
                          ),
                          doc_schema.new(doc_id=4, title='Tower of London'),
                      ]),
                      metadata=query_metadata_schema.new(locale='en-GB'),
                  ),
              ])
          ),
      )
      # The itemids of the entities in the filtered DataSlice are the same as
      # in the unfiltered DataSlice:
      kd.testing.assert_equivalent(
          root.get_data_slice().get_itemid(),
          trunk_root.get_data_slice().get_itemid(),
      )
      kd.testing.assert_equivalent(
          root.query[:].get_data_slice().S[0].get_itemid(),
          trunk_root.query[:].get_data_slice().S[1].get_itemid(),
      )
      kd.testing.assert_equivalent(
          root.query[:]
          .doc[:]
          .get_data_slice(populate_including_descendants=[root.query[:].doc[:]])
          .L[0],
          trunk_root.query[:]
          .doc[:]
          .get_data_slice(
              populate_including_descendants=[trunk_root.query[:].doc[:]]
          )
          .L[1],
          ids_equality=True,
      )
      branch_manager_revision_descriptions = [
          revision_metadata.description
          for revision_metadata in branch_manager._metadata.revision_history
      ]
      self.assertEqual(
          branch_manager_revision_descriptions,
          [
              'Branch to filter queries',
              (
                  'Filtered queries to keep only those with a doc list of size'
                  ' at most 2'
              ),
          ],
      )

    with self.subTest('filter_from_dict_keys'):
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path, description='Branch to filter tokens'
      )
      root = DataSliceManagerView(branch_manager)
      token_key = root.query[:].doc[:].tokens.get_dict_keys()
      token_key.filter(
          token_key.get_data_slice() == 'hands',
          description='Filtered token keys to keep only those that are "hands"',
      )
      kd.testing.assert_equivalent(
          root.get_data_slice(populate_including_descendants=[root]),
          trunk_initial_data_manager.get_schema().new(
              query=kd.list([
                  query_schema.new(
                      query_id=2,
                      text='How old is Bush?',
                      doc=kd.list([
                          doc_schema.new(
                              doc_id=5,
                              title='George W. Bush hands over the baton',
                              tokens=kd.dict({
                                  'hands': token_info_schema.new(is_noun=False),
                                  # Note that the other tokens are filtered out.
                              }),
                          ),
                          # Note that the other docs are filtered out.
                      ]),
                      metadata=query_metadata_schema.new(locale='en-US'),
                  ),
              ])
          ),
      )

    with self.subTest('filter_from_dict_values'):
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path,
          description=(
              'Branch to filter tokens to keep only those that are nouns'
          ),
      )
      root = DataSliceManagerView(branch_manager)
      is_noun = root.query[:].doc[:].tokens.get_dict_values().is_noun
      is_noun.filter(
          is_noun.get_data_slice() == kd.item(True),
          description='Filtered token values to keep only those that are nouns',
      )

      kd.testing.assert_equivalent(
          root.get_data_slice(populate_including_descendants=[root]),
          trunk_initial_data_manager.get_schema().new(
              query=kd.list([
                  query_schema.new(
                      query_id=1,
                      text='How high is the Eiffel tower',
                      doc=kd.list([
                          doc_schema.new(
                              doc_id=3,
                              title='Eiffel tower',
                              tokens=kd.dict({
                                  'Eiffel tower': token_info_schema.new(
                                      is_noun=True
                                  ),
                              }),
                          ),
                      ]),
                      metadata=query_metadata_schema.new(locale='en-GB'),
                  ),
                  query_schema.new(
                      query_id=2,
                      text='How old is Bush?',
                      doc=kd.list([
                          doc_schema.new(
                              doc_id=5,
                              title='George W. Bush hands over the baton',
                              tokens=kd.dict({
                                  'George W. Bush': token_info_schema.new(
                                      is_noun=True
                                  ),
                              }),
                          ),
                      ]),
                      metadata=query_metadata_schema.new(locale='en-US'),
                  ),
              ])
          ),
      )

    with self.subTest('filter_from_dict_view'):
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path,
          description='Branch to filter docs to keep only those with tokens',
      )
      root = DataSliceManagerView(branch_manager)
      tokens = root.query[:].doc[:].tokens
      tokens.filter(
          kd.has(tokens.get_data_slice()),
          description='Filtered docs to keep only those with tokens',
      )

      kd.testing.assert_equivalent(
          root.get_data_slice(populate_including_descendants=[root]),
          trunk_initial_data_manager.get_schema().new(
              query=kd.list([
                  query_schema.new(
                      query_id=1,
                      text='How high is the Eiffel tower',
                      doc=kd.list([
                          doc_schema.new(
                              doc_id=3,
                              title='Eiffel tower',
                              tokens=kd.dict({
                                  'How': token_info_schema.new(is_noun=False),
                                  'Eiffel tower': token_info_schema.new(
                                      is_noun=True
                                  ),
                              }),
                          ),
                      ]),
                      metadata=query_metadata_schema.new(locale='en-GB'),
                  ),
                  query_schema.new(
                      query_id=2,
                      text='How old is Bush?',
                      doc=kd.list([
                          doc_schema.new(
                              doc_id=5,
                              title='George W. Bush hands over the baton',
                              tokens=kd.dict({
                                  'George W. Bush': token_info_schema.new(
                                      is_noun=True
                                  ),
                                  'hands': token_info_schema.new(is_noun=False),
                              }),
                          ),
                      ]),
                      metadata=query_metadata_schema.new(locale='en-US'),
                  ),
              ])
          ),
      )

    with self.subTest('filter_description_in_revision_history'):
      # When an explicit description is provided, it is stored in the revision
      # history:
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path, description='Branch to filter docs'
      )
      root = DataSliceManagerView(branch_manager)
      doc = root.query[:].doc[:]
      doc.filter(
          kd.missing,
          description='Filtered all docs',
      )
      self.assertEqual(
          branch_manager._metadata.revision_history[-1].description,
          'Filtered all docs',
      )

      # When no explicit description is provided, the default description is
      # used. It mentions that a filtering operation happened and at which path,
      # so it is reasonably informative.
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path, description='Branch to filter docs'
      )
      root = DataSliceManagerView(branch_manager)
      doc = root.query[:].doc[:]
      doc.filter(
          kd.missing,
          # No description is provided here.
      )
      self.assertEqual(
          branch_manager._metadata.revision_history[-1].description,
          'Filtered items at path ".query[:].doc[:]"',
      )

    with self.subTest('filter_docs_and_then_filter_queries'):
      branch_manager = trunk_manager.branch(
          self.create_tempdir().full_path,
          description='Branch to filter docs and then to filter queries',
      )
      root = DataSliceManagerView(branch_manager)
      query = root.query[:]
      tokens = query.doc[:].tokens
      tokens.filter(
          kd.has(tokens.get()),
          description='Filtered docs to keep only those with tokens',
      )
      kd.testing.assert_equivalent(
          root.get_data_slice(populate_including_descendants=[root]),
          trunk_initial_data_manager.get_schema().new(
              query=kd.list([
                  query_schema.new(
                      query_id=1,
                      text='How high is the Eiffel tower',
                      doc=kd.list([
                          doc_schema.new(
                              doc_id=3,
                              title='Eiffel tower',
                              tokens=kd.dict({
                                  'How': token_info_schema.new(is_noun=False),
                                  'Eiffel tower': token_info_schema.new(
                                      is_noun=True
                                  ),
                              }),
                          ),
                      ]),
                      metadata=query_metadata_schema.new(locale='en-GB'),
                  ),
                  query_schema.new(
                      query_id=2,
                      text='How old is Bush?',
                      doc=kd.list([
                          doc_schema.new(
                              doc_id=5,
                              title='George W. Bush hands over the baton',
                              tokens=kd.dict({
                                  'George W. Bush': token_info_schema.new(
                                      is_noun=True
                                  ),
                                  'hands': token_info_schema.new(is_noun=False),
                              }),
                          ),
                      ]),
                      metadata=query_metadata_schema.new(locale='en-US'),
                  ),
              ])
          ),
      )

      query.filter(
          kd.strings.contains(query.text.get(), 'Bush'),
          description=(
              'Filtered queries to keep only those with "Bush" in the title'
          ),
      )
      kd.testing.assert_equivalent(
          root.get(populate_including_descendants=[root]),
          trunk_initial_data_manager.get_schema().new(
              query=kd.list([
                  query_schema.new(
                      query_id=2,
                      text='How old is Bush?',
                      doc=kd.list([
                          doc_schema.new(
                              doc_id=5,
                              title='George W. Bush hands over the baton',
                              tokens=kd.dict({
                                  'George W. Bush': token_info_schema.new(
                                      is_noun=True
                                  ),
                                  'hands': token_info_schema.new(is_noun=False),
                              }),
                          ),
                      ]),
                      metadata=query_metadata_schema.new(locale='en-US'),
                  ),
              ])
          ),
      )

      # The two filter operations have separate entries in the revision history:
      self.assertEqual(
          [m.description for m in branch_manager._metadata.revision_history],
          [
              'Branch to filter docs and then to filter queries',
              'Filtered docs to keep only those with tokens',
              'Filtered queries to keep only those with "Bush" in the title',
          ],
      )

  def test_error_messages_when_sugar_does_not_apply_to_attribute_name(self):
    manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path
    )
    root = DataSliceManagerView(manager)

    with self.assertRaisesRegex(
        AttributeError,
        re.escape(
            "attribute 'fingerprint' cannot be used with the dot syntax. Use"
            " self.update('fingerprint', ...) instead"
        ),
    ):
      root.fingerprint = kd.item(123)

    root.update('fingerprint', kd.item(123))

    with self.assertRaisesRegex(
        AttributeError,
        re.escape(
            "attribute 'fingerprint' cannot be used with the dot syntax. Use"
            " self.get_attr('fingerprint') instead"
        ),
    ):
      _ = root.fingerprint

    fingerprint = root.get_attr('fingerprint')
    self.assertEqual(fingerprint.get_data_slice(), kd.item(123))

    # IPython auto-complete should not suggest the reserved attribute names.
    self.assertEqual(
        kd.dir(root.get_data_slice(populate_including_descendants=[root])),
        ['fingerprint'],
    )
    self.assertNotIn('fingerprint', root.__all__)
    self.assertEqual(
        root.__all__,
        [
            'find_descendants',
            'get',
            'get_ancestor',
            'get_attr',
            'get_children',
            'get_data_slice',
            'get_manager',
            'get_path_from_root',
            'get_root',
            'get_schema',
            'grep_descendants',
            'is_view_valid',
            'update',
        ],
    )

  def test_getitem_raises_error_for_invalid_argument(self):
    manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path
    )
    root = DataSliceManagerView(manager)
    root.query = kd.list([
        kd.named_schema('query').new(query_id=0, text='How tall is Obama'),
        kd.named_schema('query').new(
            query_id=1, text='How high is the Eiffel tower'
        ),
    ])

    with self.assertRaisesRegex(
        ValueError,
        re.escape('only the [:] syntax is supported; got a request for [1]'),
    ):
      _ = root.query[1]

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'only the [:] syntax is supported; got a request for [slice(0, 2,'
            ' None)]'
        ),
    ):
      _ = root.query[0:2]

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'only the [:] syntax is supported; got a request for [slice(0, 2,'
            ' 1)]'
        ),
    ):
      _ = root.query[0:2:1]

  def test_find_and_grep_descendants(self):
    # Set up a plain Koda DataSlice with query and doc data.
    query_schema = kd.named_schema('query')
    new_query = query_schema.new
    doc_schema = kd.named_schema('doc')
    new_doc = doc_schema.new
    query_ds = kd.slice([
        new_query(
            id=1,
            text='How high is the Eiffel tower',
            doc=kd.list([
                new_doc(
                    id=10, title='Attractions of Paris', content='foo' * 10000
                )
            ]),
        ),
        new_query(
            id=2,
            text='How high is the empire state building',
            doc=kd.list([
                new_doc(
                    id=11,
                    title='Attractions of New York',
                    content='bar' * 10000,
                ),
                new_doc(
                    id=12,
                    title="The world's tallest buildings",
                    content='baz' * 10000,
                ),
            ]),
        ),
    ])

    # Set up a DataSliceManager with the data from above.
    manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path
    )
    root = DataSliceManagerView(manager)
    root.query = query_ds.implode(ndim=-1)

    with self.subTest('grep'):
      self.assertEqual(
          list(root.grep_descendants(r'id')),
          [root.query[:].id, root.query[:].doc[:].id],
      )
      self.assertEqual(
          list(root.grep_descendants(r'text')),
          [root.query[:].text],
      )
      self.assertEqual(
          list(root.grep_descendants(r't')),
          [
              root.query[:].text,
              root.query[:].doc[:].content,
              root.query[:].doc[:].title,
          ],
      )
      self.assertEqual(
          list(root.grep_descendants(r't', max_delta_depth=3)),
          [root.query[:].text],
      )
      self.assertEqual(
          list(root.query[:].grep_descendants(r't', max_delta_depth=3)),
          [
              root.query[:].text,
              root.query[:].doc[:].content,
              root.query[:].doc[:].title,
          ],
      )
      self.assertEqual(
          list(root.query[:].doc[:].grep_descendants(r't')),
          [root.query[:].doc[:].content, root.query[:].doc[:].title],
      )
      self.assertEqual(
          list(root.grep_descendants(r'\.t')),
          [root.query[:].text, root.query[:].doc[:].title],
      )
      self.assertEqual(
          list(root.grep_descendants('doc.*title')),
          [root.query[:].doc[:].title],
      )
      self.assertEqual(
          list(root.grep_descendants('query.*t')),
          [
              root.query[:].text,
              root.query[:].doc[:].content,
              root.query[:].doc[:].title,
          ],
      )
      no_explode = r'[^\[]*'  # match anything except [ zero or more times
      self.assertEqual(
          # After exploding the query list, find all views that contain a 't'
          # character somewhere in the subsequent path part, but do not yield
          # views that explode more lists.
          list(
              root.grep_descendants(
                  re.escape('query[:].') + f'{no_explode}t{no_explode}$'
              )
          ),
          [root.query[:].text],
      )
      self.assertEqual(
          list(root.grep_descendants(re.escape('doc[:].title'))),
          [root.query[:].doc[:].title],
      )
      self.assertEqual(
          # Find all views that end with [:]
          list(root.grep_descendants(re.escape('[:]') + '$')),
          [root.query[:], root.query[:].doc[:]],
      )

    with self.subTest('find'):
      self.assertEqual(
          list(root.find_descendants(lambda v: v.get_schema() == kd.INT32)),
          [root.query[:].id, root.query[:].doc[:].id],
      )
      self.assertEqual(
          list(root.find_descendants(lambda v: v.get_schema() == kd.STRING)),
          [
              root.query[:].text,
              root.query[:].doc[:].content,
              root.query[:].doc[:].title,
          ],
      )
      self.assertEqual(
          list(
              root.query[:].doc.find_descendants(
                  lambda v: v.get_schema() == kd.STRING
              )
          ),
          [
              root.query[:].doc[:].content,
              root.query[:].doc[:].title,
          ],
      )
      self.assertEqual(
          list(
              root.find_descendants(
                  lambda v: v.get_schema() == kd.STRING, max_delta_depth=3
              )
          ),
          [root.query[:].text],
      )
      self.assertEqual(
          list(root.find_descendants(lambda v: v.get_schema() == doc_schema)),
          [root.query[:].doc[:]],
      )

  def test_repr(self):
    # Set up a plain Koda DataSlice with query and doc data.
    query_schema = kd.named_schema('query')
    new_query = query_schema.new
    doc_schema = kd.named_schema('doc')
    new_doc = doc_schema.new
    query_ds = kd.slice([
        new_query(
            id=1,
            text='How high is the Eiffel tower',
            doc=kd.list([
                new_doc(
                    id=10, title='Attractions of Paris', content='foo' * 10000
                )
            ]),
        ),
        new_query(
            id=2,
            text='How high is the empire state building',
            doc=kd.list([
                new_doc(
                    id=11,
                    title='Attractions of New York',
                    content='bar' * 10000,
                ),
                new_doc(
                    id=12,
                    title="The world's tallest buildings",
                    content='baz' * 10000,
                ),
            ]),
        ),
    ])

    # Set up a DataSliceManager with the data from above.
    manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path
    )
    root = DataSliceManagerView(manager)
    root.query = query_ds.implode(ndim=-1)

    manager_repr = repr(manager)
    self.assertRegex(
        manager_repr,
        re.escape(
            '<koladata.ext.persisted_data.persisted_incremental_data_slice_manager.PersistedIncrementalDataSliceManager'
            ' object at '
        )
        + r'\w+'
        + re.escape('>'),
    )

    self.assertEqual(
        repr(root),
        f"DataSliceManagerView({manager_repr}, DataSlicePath(''))",
    )
    self.assertEqual(
        repr(root.query[:]),
        f"DataSliceManagerView({manager_repr}, DataSlicePath('.query[:]'))",
    )
    self.assertEqual(
        repr(root.query[:].text),
        (
            f'DataSliceManagerView({manager_repr},'
            " DataSlicePath('.query[:].text'))"
        ),
    )
    self.assertEqual(
        repr(root.query[:].doc[:].title),
        (
            f'DataSliceManagerView({manager_repr},'
            " DataSlicePath('.query[:].doc[:].title'))"
        ),
    )

  def test_get_with_populate_arguments(self):
    token_info_schema = kd.schema.new_schema(is_noun=kd.BOOLEAN)
    doc_schema = kd.schema.new_schema(
        doc_id=kd.INT32,
        title=kd.STRING,
        tokens=kd.dict_schema(kd.STRING, token_info_schema),
    )
    query_metadata_schema = kd.schema.new_schema(
        locale=kd.STRING,
    )
    query_schema = kd.schema.new_schema(
        query_id=kd.INT32,
        text=kd.STRING,
        doc=kd.list_schema(doc_schema),
        metadata=query_metadata_schema,
    )
    query_list = kd.list([
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
                        'How': token_info_schema.new(is_noun=False),
                        'Eiffel tower': token_info_schema.new(is_noun=True),
                    }),
                ),
                doc_schema.new(doc_id=4, title='Tower of London'),
            ]),
            metadata=query_metadata_schema.new(locale='en-GB'),
        ),
        query_schema.new(
            query_id=2,
            text='How old is Bush?',
            doc=kd.list([
                doc_schema.new(
                    doc_id=5,
                    title='George W. Bush hands over the baton',
                    tokens=kd.dict({
                        'George W. Bush': token_info_schema.new(is_noun=True),
                        'hands': token_info_schema.new(is_noun=False),
                    }),
                ),
                doc_schema.new(doc_id=6, title='The oldest bushes'),
                doc_schema.new(doc_id=7, title='Ancient bushes'),
            ]),
            metadata=query_metadata_schema.new(locale='en-US'),
        ),
    ])

    manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        self.create_tempdir().full_path
    )
    root_view = DataSliceManagerView(manager)
    root_view.query = query_list, 'Added queries populated with data'
    query_view = root_view.query[:]
    doc_view = query_view.doc[:]

    kd.testing.assert_equivalent(
        # This is a simple case where we want queries with selected query
        # features populated in the result.
        query_view.get(
            populate=[query_view.query_id, query_view.text],
        ),
        query_list[:]
        .with_bag(kd.bag())
        .with_attrs(query_id=query_list[:].query_id, text=query_list[:].text),
        ids_equality=True,
    )

    kd.testing.assert_equivalent(
        doc_view.doc_id.get(
            # Since the query text is not a descendant of the doc_id, only the
            # doc_id will be visible in the result.
            populate=[query_view.text],
        ),
        query_list[:].doc[:].doc_id,
    )
    kd.testing.assert_equivalent(
        root_view.get(
            # This time we request the result to start from the root, so the
            # query text will be visible in the result.
            populate=[query_view.text, doc_view.doc_id],
        ),
        kd.new(
            query=kd.list([
                query_schema.new(
                    text='How tall is Obama',
                    doc=kd.list([
                        doc_schema.new(doc_id=0),
                        doc_schema.new(doc_id=1),
                        doc_schema.new(doc_id=2),
                    ]),
                ),
                query_schema.new(
                    text='How high is the Eiffel tower',
                    doc=kd.list([
                        doc_schema.new(doc_id=3),
                        doc_schema.new(doc_id=4),
                    ]),
                ),
                query_schema.new(
                    text='How old is Bush?',
                    doc=kd.list([
                        doc_schema.new(doc_id=5),
                        doc_schema.new(doc_id=6),
                        doc_schema.new(doc_id=7),
                    ]),
                ),
            ])
        ),
        schemas_equality=False,
    )

    kd.testing.assert_equivalent(
        doc_view.get(populate_including_descendants=[query_view]),
        doc_view.get(populate_including_descendants=[doc_view]),
        ids_equality=True,
    )
    kd.testing.assert_equivalent(
        doc_view.get(populate=[query_view]),
        doc_view.get(),
        ids_equality=True,
    )
    kd.testing.assert_equivalent(
        root_view.get(
            populate=[doc_view], populate_including_descendants=[query_view]
        ),
        root_view.get(populate_including_descendants=[root_view]),
        ids_equality=True,
    )

  def test_views_are_hashable(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidsm.PersistedIncrementalDataSliceManager.create_new(
        persistence_dir
    )
    root = DataSliceManagerView(manager)
    root.query = (
        kd.list([
            kd.named_schema('query').new(query_id=0, text='How tall is Obama'),
            kd.named_schema('query').new(
                query_id=1, text='How high is the Eiffel tower'
            ),
        ]),
        'Added queries with query_id and text',
    )
    query = root.query[:]
    query.doc = (
        kd.list([
            kd.named_schema('doc').new(doc_id=0, title='Barack Obama'),
            kd.named_schema('doc').new(doc_id=1, title='Michelle Obama'),
            kd.named_schema('doc').new(doc_id=2, title='George W. Bush'),
        ]),
        'Added docs with doc_id and title',
    )
    doc = query.doc[:]

    self.assertEqual(hash(root), hash(root))
    self.assertEqual(hash(root), hash(root.get_root()))
    self.assertEqual(hash(root.query[:]), hash(query))
    self.assertEqual(hash(root.query[:]), hash(root.query[:]))
    self.assertEqual(hash(root.query[:].text), hash(query.text))
    self.assertEqual(hash(root.query[:].text), hash(root.query[:].text))
    self.assertEqual(hash(root.query[:].doc[:].title), hash(doc.title))

    my_view_set = {root, query, doc, doc.title, root.query[:].doc[:]}
    self.assertLen(my_view_set, 4)

    another_manager = (
        pidsm.PersistedIncrementalDataSliceManager.create_from_dir(
            persistence_dir
        )
    )
    another_root = DataSliceManagerView(another_manager)

    self.assertNotEqual(hash(another_root), hash(root))
    self.assertNotEqual(hash(another_root), hash(root.get_root()))
    self.assertNotEqual(hash(another_root.query[:]), hash(query))
    self.assertNotEqual(hash(another_root.query[:]), hash(root.query[:]))
    self.assertNotEqual(hash(another_root.query[:].text), hash(query.text))
    self.assertNotEqual(
        hash(another_root.query[:].text), hash(root.query[:].text)
    )
    self.assertNotEqual(
        hash(another_root.query[:].doc[:].title), hash(doc.title)
    )


if __name__ == '__main__':
  absltest.main()
