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
from koladata.ext.persisted_data import data_slice_manager_view as data_slice_manager_view_lib
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import persisted_incremental_data_slice_manager as pidsm


DataSlicePath = data_slice_path_lib.DataSlicePath
DataSliceManagerView = data_slice_manager_view_lib.DataSliceManagerView


class DataSliceManagerViewTest(absltest.TestCase):

  def test_typical_usage(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidsm.PersistedIncrementalDataSliceManager(
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
    kd.testing.assert_equivalent(
        queries.get_schema(), expected_query_schema
    )

    kd.testing.assert_equivalent(
        queries.text.get_data_slice(),
        kd.slice(['How tall is Obama', 'How high is the Eiffel tower']),
    )
    kd.testing.assert_equivalent(
        queries.get_data_slice(with_descendants=True),
        kd.slice([
            expected_query_schema.new(query_id=0, text='How tall is Obama'),
            expected_query_schema.new(
                query_id=1, text='How high is the Eiffel tower'
            ),
        ]),
    )
    restricted_query_schema = kd.named_schema('query', query_id=kd.INT32)
    kd.testing.assert_equivalent(
        queries.query_id.get_data_slice(with_ancestors=True),
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
        docs.title.get_data_slice(),
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

    with self.subTest('__iter__'):
      for some_view in [root, root.query, queries, docs, docs.title]:
        self.assertEqual(
            [child for child in some_view],
            some_view.get_children(),
        )

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
          [set(t) for t in tokens.get_dict_keys().get_data_slice().to_py()],
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
                  tokens.get_dict_keys().get_data_slice().to_py(),
                  tokens.get_dict_values()
                  .part_of_speech.get_data_slice()
                  .to_py(),
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
    manager = pidsm.PersistedIncrementalDataSliceManager(
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
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)

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
    with self.assertRaisesRegex(
        ValueError,
        re.escape("invalid data slice path: '.query[:].tokens'"),
    ):
      for _ in tokens:
        pass

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
              'find_descendants',
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
              'find_descendants',
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

  def test_branch_and_filter_recipe(self):

    def create_initial_manager() -> pidsm.PersistedIncrementalDataSliceManager:
      persistence_dir = self.create_tempdir().full_path
      manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
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
        root.get_data_slice(with_descendants=True),
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
        filtered_root.get_data_slice(with_descendants=True),
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
            'Initial state with an empty root DataSlice',
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

  def test_error_messages_when_sugar_does_not_apply_to_attribute_name(self):
    manager = pidsm.PersistedIncrementalDataSliceManager(
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
        kd.dir(root.get_data_slice(with_descendants=True)), ['fingerprint']
    )
    self.assertNotIn('fingerprint', root.__all__)
    self.assertEqual(
        root.__all__,
        [
            'find_descendants',
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
    manager = pidsm.PersistedIncrementalDataSliceManager(
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
    manager = pidsm.PersistedIncrementalDataSliceManager(
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
    manager = pidsm.PersistedIncrementalDataSliceManager(
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


if __name__ == '__main__':
  absltest.main()
