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
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    root = DataSliceManagerView(manager)

    root.query = kd.list([
        kd.named_schema('query').new(query_id=0, text='How tall is Obama'),
        kd.named_schema('query').new(
            query_id=1, text='How high is the Eiffel tower'
        ),
    ])

    queries = root.query.get_list_items()

    expected_query_schema = kd.named_schema(
        'query', query_id=kd.INT32, text=kd.STRING
    )
    kd.testing.assert_deep_equivalent(
        queries.get_schema(), expected_query_schema
    )

    kd.testing.assert_deep_equivalent(
        queries.text.get_data_slice(),
        kd.slice(['How tall is Obama', 'How high is the Eiffel tower']),
    )
    kd.testing.assert_deep_equivalent(
        queries.get_data_slice(with_descendants=True),
        kd.slice([
            expected_query_schema.new(query_id=0, text='How tall is Obama'),
            expected_query_schema.new(
                query_id=1, text='How high is the Eiffel tower'
            ),
        ]),
    )
    restricted_query_schema = kd.named_schema('query', query_id=kd.INT32)
    kd.testing.assert_deep_equivalent(
        queries.query_id.get_data_slice(with_ancestors=True),
        kd.new(
            query=kd.list([
                restricted_query_schema.new(query_id=0),
                restricted_query_schema.new(query_id=1),
            ])
        ),
    )

    queries.doc = kd.slice([
        kd.list([
            kd.named_schema('doc').new(doc_id=1, title='Barack Obama'),
            kd.named_schema('doc').new(doc_id=2, title='Michelle Obama'),
        ]),
        kd.list(
            [kd.named_schema('doc').new(doc_id=2, title='Tower of London')]
        ),
    ])

    docs = queries.doc.get_list_items()

    self.assertEqual(
        docs.title.get_schema(),
        kd.STRING,
    )
    kd.testing.assert_deep_equivalent(
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
      queries.token = kd.slice([
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

      tokens = root.query.get_list_items().token

      kd.testing.assert_deep_equivalent(
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
      kd.testing.assert_deep_equivalent(
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
    query = root.query.get_list_items()

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
    doc = doc_list.get_list_items()
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
      assert hasattr(root, '__all__')  # to please pytype

      # A valid entity view:
      self.assertEqual(
          root.__all__,
          # Note no get_parent() or get_grandparent() or get_list_items() here:
          [
              'get_ancestor',
              'get_attr',
              'get_children',
              'get_data_slice',
              'get_manager',
              'get_path_from_root',
              'get_root',
              'get_schema',
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
              'get_ancestor',
              'get_children',
              'get_data_slice',
              'get_list_items',
              'get_manager',
              'get_parent',
              'get_path_from_root',
              'get_root',
              'get_schema',
              'is_view_valid',
          ],
      )
      # A valid dict view:
      self.assertEqual(
          root.query.__all__,
          [
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

  def test_filtering_recipe(self):
    persistence_dir = self.create_tempdir().full_path
    manager = pidsm.PersistedIncrementalDataSliceManager(persistence_dir)
    root = DataSliceManagerView(manager)

    root.query = kd.list([
        kd.named_schema('query').new(query_id=0, text='How tall is Obama'),
        kd.named_schema('query').new(
            query_id=1, text='How high is the Eiffel tower'
        ),
    ])

    queries = root.query.get_list_items()

    queries.doc = kd.slice([
        kd.list([
            kd.named_schema('doc').new(doc_id=1, title='Barack Obama'),
            kd.named_schema('doc').new(doc_id=2, title='Michelle Obama'),
        ]),
        kd.list(
            [kd.named_schema('doc').new(doc_id=2, title='Tower of London')]
        ),
    ])

    docs = queries.doc.get_list_items()

    # In practice, we would likely do the filtering on a copy of the
    # persistence_dir in order not to overwrite the original data. However, for
    # this test we will simply overwrite the original data and use the variables
    # defined above.

    # Filter the docs to only keep the ones with "Barack" in the title, and
    # filter the queries to only keep the ones with at least one such doc.
    new_docs = docs.get_data_slice().select(
        kd.strings.contains(docs.title.get_data_slice(), 'Barack')
    )
    queries.doc = new_docs.implode()
    new_queries = queries.get_data_slice().select(kd.agg_any(kd.has(new_docs)))
    root.query = new_queries.implode()

    # The filtering recipe above results in one query with one doc:
    kd.testing.assert_deep_equivalent(
        root.get_data_slice(with_descendants=True),
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
            " self.update('fingerprint', attr_value) instead"
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
    assert hasattr(root, '__all__')  # to please pytype
    self.assertNotIn('fingerprint', root.__all__)
    self.assertEqual(
        root.__all__,
        [
            'get_ancestor',
            'get_attr',
            'get_children',
            'get_data_slice',
            'get_manager',
            'get_path_from_root',
            'get_root',
            'get_schema',
            'is_view_valid',
            'update',
        ],
    )


if __name__ == '__main__':
  absltest.main()
