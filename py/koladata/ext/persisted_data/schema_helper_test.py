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
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib
from koladata.ext.persisted_data import schema_helper
from koladata.ext.persisted_data import test_only_schema_node_name_helper


DataSlicePath = data_slice_path_lib.DataSlicePath
DictGetKeys = data_slice_path_lib.DictGetKeys
DictGetValues = data_slice_path_lib.DictGetValues
ListExplode = data_slice_path_lib.ListExplode
GetAttr = data_slice_path_lib.GetAttr

schema_node_name = test_only_schema_node_name_helper.schema_node_name
to_same_len_set = test_only_schema_node_name_helper.to_same_len_set


class SchemaHelperTest(absltest.TestCase):

  def test_list_schema(self):
    list_schema = kd.list_schema(kd.STRING)
    helper = schema_helper.SchemaHelper(list_schema)

    self.assertEqual(
        helper.get_all_schema_node_names(),
        to_same_len_set([
            schema_node_name(list_schema),
            schema_node_name(list_schema, action=ListExplode()),
        ]),
    )
    self.assertEqual(
        helper.get_leaf_schema_node_names(),
        {schema_node_name(list_schema, action=ListExplode())},
    )
    self.assertEqual(
        helper.get_non_leaf_schema_node_names(), {schema_node_name(list_schema)}
    )

    self.assertEqual(
        helper.get_descendant_schema_node_names(
            {schema_node_name(list_schema)}
        ),
        {schema_node_name(list_schema, action=ListExplode())},
    )
    self.assertEmpty(
        helper.get_descendant_schema_node_names(
            {schema_node_name(list_schema, action=ListExplode())}
        )
    )

    self.assertEmpty(
        helper.get_ancestor_schema_node_names({schema_node_name(list_schema)})
    )
    self.assertEqual(
        helper.get_ancestor_schema_node_names(
            {schema_node_name(list_schema, action=ListExplode())}
        ),
        {schema_node_name(list_schema)},
    )

  def test_dict_schema(self):
    dict_schema = kd.dict_schema(kd.STRING, kd.STRING)
    helper = schema_helper.SchemaHelper(dict_schema)

    self.assertEqual(
        helper.get_all_schema_node_names(),
        to_same_len_set([
            schema_node_name(dict_schema),
            schema_node_name(dict_schema, action=DictGetKeys()),
            schema_node_name(dict_schema, action=DictGetValues()),
        ]),
    )
    self.assertEqual(
        helper.get_leaf_schema_node_names(),
        to_same_len_set([
            schema_node_name(dict_schema, action=DictGetKeys()),
            schema_node_name(dict_schema, action=DictGetValues()),
        ]),
    )
    self.assertEqual(
        helper.get_non_leaf_schema_node_names(), {schema_node_name(dict_schema)}
    )

    self.assertEqual(
        helper.get_descendant_schema_node_names(
            {schema_node_name(dict_schema)}
        ),
        to_same_len_set([
            schema_node_name(dict_schema, action=DictGetKeys()),
            schema_node_name(dict_schema, action=DictGetValues()),
        ]),
    )
    self.assertEmpty(
        helper.get_descendant_schema_node_names(
            {schema_node_name(dict_schema, action=DictGetKeys())}
        )
    )
    self.assertEmpty(
        helper.get_descendant_schema_node_names(
            {schema_node_name(dict_schema, action=DictGetValues())}
        )
    )

    self.assertEmpty(
        helper.get_ancestor_schema_node_names({schema_node_name(dict_schema)})
    )
    self.assertEqual(
        helper.get_ancestor_schema_node_names(
            {schema_node_name(dict_schema, action=DictGetKeys())}
        ),
        {schema_node_name(dict_schema)},
    )
    self.assertEqual(
        helper.get_ancestor_schema_node_names(
            {schema_node_name(dict_schema, action=DictGetValues())}
        ),
        {schema_node_name(dict_schema)},
    )

  def test_entity_schema(self):
    entity_schema = kd.schema.new_schema(x=kd.STRING, y=kd.INT32)
    helper = schema_helper.SchemaHelper(entity_schema)

    self.assertEqual(
        helper.get_all_schema_node_names(),
        to_same_len_set([
            schema_node_name(entity_schema),
            schema_node_name(entity_schema, action=GetAttr('x')),
            schema_node_name(entity_schema, action=GetAttr('y')),
        ]),
    )
    self.assertEqual(
        helper.get_leaf_schema_node_names(),
        to_same_len_set([
            schema_node_name(entity_schema, action=GetAttr('x')),
            schema_node_name(entity_schema, action=GetAttr('y')),
        ]),
    )
    self.assertEqual(
        helper.get_non_leaf_schema_node_names(),
        {schema_node_name(entity_schema)},
    )

    self.assertEqual(
        helper.get_descendant_schema_node_names(
            {schema_node_name(entity_schema)}
        ),
        to_same_len_set([
            schema_node_name(entity_schema, action=GetAttr('x')),
            schema_node_name(entity_schema, action=GetAttr('y')),
        ]),
    )
    self.assertEmpty(
        helper.get_descendant_schema_node_names(
            {schema_node_name(entity_schema, action=GetAttr('x'))}
        )
    )
    self.assertEmpty(
        helper.get_descendant_schema_node_names(
            {schema_node_name(entity_schema, action=GetAttr('y'))}
        )
    )

    self.assertEmpty(
        helper.get_ancestor_schema_node_names({schema_node_name(entity_schema)})
    )
    self.assertEqual(
        helper.get_ancestor_schema_node_names(
            {schema_node_name(entity_schema, action=GetAttr('x'))}
        ),
        {schema_node_name(entity_schema)},
    )
    self.assertEqual(
        helper.get_ancestor_schema_node_names(
            {schema_node_name(entity_schema, action=GetAttr('y'))}
        ),
        {schema_node_name(entity_schema)},
    )

  def test_basic_recursive_schema(self):
    tree_node_schema = kd.named_schema(
        'TreeNode',
        value=kd.STRING,
        children=kd.list_schema(kd.named_schema('TreeNode')),
    )

    helper = schema_helper.SchemaHelper(tree_node_schema)

    self.assertEqual(
        helper.get_all_schema_node_names(),
        to_same_len_set([
            schema_node_name(tree_node_schema),
            schema_node_name(tree_node_schema, action=GetAttr('value')),
            schema_node_name(tree_node_schema.children),
            # Note that we do not have a separate schema node name for
            # tree_node_schema.children.get_item_schema(), because its schema
            # node name is the same as that of tree_node_schema, as we show
            # below.
        ]),
    )
    self.assertEqual(
        schema_node_name(tree_node_schema.children.get_item_schema()),
        schema_node_name(tree_node_schema),
    )
    kd.testing.assert_equal(
        tree_node_schema.children.get_item_schema(),
        tree_node_schema,
    )

    self.assertEqual(
        helper.get_leaf_schema_node_names(),
        {
            schema_node_name(tree_node_schema, action=GetAttr('value')),
        },
    )
    self.assertEqual(
        helper.get_non_leaf_schema_node_names(),
        to_same_len_set([
            schema_node_name(tree_node_schema),
            schema_node_name(tree_node_schema.children),
        ]),
    )

    self.assertEqual(
        helper.get_descendant_schema_node_names(
            {schema_node_name(tree_node_schema)}
        ),
        to_same_len_set([
            schema_node_name(tree_node_schema),
            schema_node_name(tree_node_schema, action=GetAttr('value')),
            schema_node_name(tree_node_schema.children),
        ]),
    )
    self.assertEmpty(
        helper.get_descendant_schema_node_names(
            {schema_node_name(tree_node_schema, action=GetAttr('value'))}
        )
    )
    self.assertEqual(
        helper.get_descendant_schema_node_names(
            {schema_node_name(tree_node_schema.children)}
        ),
        to_same_len_set([
            schema_node_name(tree_node_schema),
            schema_node_name(tree_node_schema, action=GetAttr('value')),
            schema_node_name(tree_node_schema.children),
        ]),
    )

    self.assertEqual(
        helper.get_ancestor_schema_node_names(
            {schema_node_name(tree_node_schema)}
        ),
        to_same_len_set([
            schema_node_name(tree_node_schema),
            schema_node_name(tree_node_schema.children),
        ]),
    )
    self.assertEqual(
        helper.get_ancestor_schema_node_names(
            {schema_node_name(tree_node_schema, action=GetAttr('value'))}
        ),
        to_same_len_set([
            schema_node_name(tree_node_schema),
            schema_node_name(tree_node_schema.children),
        ]),
    )
    self.assertEqual(
        helper.get_ancestor_schema_node_names(
            {schema_node_name(tree_node_schema.children)}
        ),
        to_same_len_set([
            schema_node_name(tree_node_schema),
            schema_node_name(tree_node_schema.children),
        ]),
    )

  def test_aliased_recursive_schema(self):
    tree_node_schema = kd.named_schema(
        'TreeNode',
        value=kd.STRING,
        children=kd.list_schema(kd.named_schema('TreeNode')),
    )
    root_schema = kd.schema.new_schema(
        some_tree=tree_node_schema,
        another_tree=tree_node_schema,
    )
    helper = schema_helper.SchemaHelper(root_schema)

    self.assertEqual(
        helper.get_all_schema_node_names(),
        to_same_len_set([
            schema_node_name(root_schema),
            schema_node_name(root_schema.some_tree),
            schema_node_name(root_schema.some_tree, action=GetAttr('value')),
            schema_node_name(root_schema.some_tree.children),
        ]),
    )

    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions([])
        ),
        schema_node_name(root_schema),
    )
    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions([GetAttr('some_tree')])
        ),
        schema_node_name(root_schema.some_tree),
    )
    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions([GetAttr('some_tree'), GetAttr('value')])
        ),
        schema_node_name(root_schema.some_tree, action=GetAttr('value')),
    )
    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions(
                [GetAttr('some_tree'), GetAttr('children')]
            )
        ),
        schema_node_name(root_schema.some_tree.children),
    )
    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions(
                [GetAttr('some_tree'), GetAttr('children'), ListExplode()]
            )
        ),
        schema_node_name(root_schema.some_tree),
    )
    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('value'),
            ])
        ),
        schema_node_name(root_schema.some_tree, action=GetAttr('value')),
    )
    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
                ListExplode(),
                GetAttr('value'),
            ])
        ),
        schema_node_name(root_schema.some_tree, action=GetAttr('value')),
    )

  def test_entity_and_list_interactions(self):
    query_schema = kd.named_schema('query')
    doc_schema = kd.named_schema('doc')
    new_query = query_schema.new
    new_doc = doc_schema.new
    root = kd.new(
        query=kd.list([
            new_query(
                query_id='q1',
                doc=new_doc(doc_id=kd.slice([0, 1, 2, 3])).implode(),
            ),
            new_query(
                query_id='q2',
                doc=new_doc(doc_id=kd.slice([4, 5, 6])).implode(),
            ),
        ])
    )
    root_schema = root.get_schema()
    query_schema = root_schema.query.get_item_schema()
    doc_schema = query_schema.doc.get_item_schema()
    helper = schema_helper.SchemaHelper(root_schema)

    self.assertEqual(
        helper.get_all_schema_node_names(),
        to_same_len_set([
            schema_node_name(root_schema),
            schema_node_name(root_schema.query),
            schema_node_name(query_schema),
            schema_node_name(query_schema, action=GetAttr('query_id')),
            schema_node_name(query_schema.doc),
            schema_node_name(doc_schema),
            schema_node_name(doc_schema, action=GetAttr('doc_id')),
        ]),
    )

    kd.testing.assert_equal(
        helper.get_subschema_at(schema_node_name(root_schema)),
        root_schema,
    )
    kd.testing.assert_equal(
        helper.get_subschema_at(schema_node_name(root_schema.query)),
        root_schema.query,
    )
    kd.testing.assert_equal(
        helper.get_subschema_at(schema_node_name(query_schema)),
        root_schema.query.get_item_schema(),
    )
    kd.testing.assert_equal(
        helper.get_subschema_at(
            schema_node_name(query_schema, action=GetAttr('query_id'))
        ),
        root_schema.query.get_item_schema().query_id,
    )
    kd.testing.assert_equal(
        helper.get_subschema_at(schema_node_name(query_schema.doc)),
        root_schema.query.get_item_schema().doc,
    )
    kd.testing.assert_equal(
        helper.get_subschema_at(schema_node_name(doc_schema)),
        root_schema.query.get_item_schema().doc.get_item_schema(),
    )
    kd.testing.assert_equal(
        helper.get_subschema_at(
            schema_node_name(doc_schema, action=GetAttr('doc_id'))
        ),
        root_schema.query.get_item_schema().doc.get_item_schema().doc_id,
    )

  def test_more_entity_list_interactions(self):
    bag = kd.mutable_bag()
    o = bag.new(x=bag.list([1, 2, 3]), y=bag.list([4, 5, 6]))
    o_schema = o.get_schema()
    helper = schema_helper.SchemaHelper(o_schema)
    self.assertEqual(
        helper.get_all_schema_node_names(),
        to_same_len_set([
            schema_node_name(o_schema),
            schema_node_name(o_schema.x),
            schema_node_name(o_schema.x, action=ListExplode()),
        ]),
    )
    kd.testing.assert_equal(o_schema.x, o_schema.y)

    # And indeed, the two might be aliased in actual objects:
    o2 = bag.new(x=bag.list([1, 2, 3]), schema=o_schema)
    o2.y = kd.stub(o2.x)
    self.assertEqual(o2.x, o2.y)
    kd.testing.assert_equal(o2.get_schema(), o.get_schema())
    self.assertEqual(o2.to_py(obj_as_dict=True), dict(x=[1, 2, 3], y=[1, 2, 3]))

    o2.x[0] = 7  # Mutating one alias affects the other.
    self.assertEqual(o2.x, o2.y)
    kd.testing.assert_equal(o2.get_schema(), o.get_schema())
    self.assertEqual(o2.to_py(obj_as_dict=True), dict(x=[7, 2, 3], y=[7, 2, 3]))

  def test_not_all_string_features_have_the_same_schema_node_name(self):
    query_schema = kd.named_schema('query')
    doc_schema = kd.named_schema('doc')
    new_query = query_schema.new
    new_doc = doc_schema.new
    root = kd.new(
        query=kd.list([
            new_query(
                query_id='q1',
                doc=new_doc(
                    doc_id=kd.slice(['d0', 'd1', 'd2', 'd3'])
                ).implode(),
            ),
            new_query(
                query_id='q2',
                doc=new_doc(doc_id=kd.slice(['d4', 'd5', 'd6'])).implode(),
            ),
        ])
    )
    root_schema = root.get_schema()
    query_schema = root_schema.query.get_item_schema()
    doc_schema = query_schema.doc.get_item_schema()
    helper = schema_helper.SchemaHelper(root_schema)

    self.assertEqual(
        helper.get_all_schema_node_names(),
        to_same_len_set([
            schema_node_name(root_schema),
            schema_node_name(root_schema.query),
            schema_node_name(query_schema),
            schema_node_name(query_schema, action=GetAttr('query_id')),
            schema_node_name(query_schema.doc),
            schema_node_name(doc_schema),
            schema_node_name(doc_schema, action=GetAttr('doc_id')),
        ]),
    )
    # query_id and doc_id are both string features, but they have different
    # schema node names.
    self.assertEqual(
        root_schema.query.get_item_schema().query_id,
        root_schema.query.get_item_schema().doc.get_item_schema().doc_id,
    )
    self.assertEqual(root_schema.query.get_item_schema().query_id, kd.STRING)
    self.assertNotEqual(
        schema_node_name(query_schema, action=GetAttr('query_id')),
        schema_node_name(doc_schema, action=GetAttr('doc_id')),
    )
    self.assertEqual(
        helper.get_subschema_at(
            schema_node_name(query_schema, action=GetAttr('query_id'))
        ),
        helper.get_subschema_at(
            schema_node_name(doc_schema, action=GetAttr('doc_id'))
        ),
    )
    self.assertEqual(
        helper.get_subschema_at(
            schema_node_name(query_schema, action=GetAttr('query_id'))
        ),
        kd.STRING,
    )

    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions([GetAttr('query'), ListExplode()])
        ),
        schema_node_name(query_schema),
    )
    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions(
                [GetAttr('query'), ListExplode(), GetAttr('query_id')]
            )
        ),
        schema_node_name(query_schema, action=GetAttr('query_id')),
    )
    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions([
                GetAttr('query'),
                ListExplode(),
                GetAttr('doc'),
                ListExplode(),
                GetAttr('doc_id'),
            ])
        ),
        schema_node_name(doc_schema, action=GetAttr('doc_id')),
    )
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            "invalid data slice path: '.query[:].query_id[:].foo'. The actual"
            " schema at prefix '.query[:].query_id' is STRING, so we cannot"
            " process the remaining part '[:].foo'"
        ),
    ):
      helper.get_schema_node_name_for_data_slice_path(
          DataSlicePath.from_actions([
              GetAttr('query'),
              ListExplode(),
              GetAttr('query_id'),
              ListExplode(),
              GetAttr('foo'),
          ])
      )

  def test_generate_available_data_paths(self):
    self.maxDiff = None
    tree_node_schema = kd.named_schema(
        'TreeNode',
        value=kd.STRING,
        children=kd.list_schema(kd.named_schema('TreeNode')),
    )
    initial_root_schema = kd.schema.new_schema(
        some_tree=tree_node_schema,
    )
    helper = schema_helper.SchemaHelper(initial_root_schema)

    self.assertEqual(
        list(helper.generate_available_data_slice_paths(max_depth=5)),
        [
            DataSlicePath.from_actions([]),
            DataSlicePath.from_actions([GetAttr('some_tree')]),
            DataSlicePath.from_actions(
                [GetAttr('some_tree'), GetAttr('children')]
            ),
            DataSlicePath.from_actions(
                [GetAttr('some_tree'), GetAttr('value')]
            ),
            DataSlicePath.from_actions(
                [GetAttr('some_tree'), GetAttr('children'), ListExplode()]
            ),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
            ]),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('value'),
            ]),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
                ListExplode(),
            ]),
        ],
    )

    self.assertEqual(
        list(helper.generate_available_data_slice_paths(max_depth=7)),
        [
            DataSlicePath.from_actions([]),
            DataSlicePath.from_actions([GetAttr('some_tree')]),
            DataSlicePath.from_actions(
                [GetAttr('some_tree'), GetAttr('children')]
            ),
            DataSlicePath.from_actions(
                [GetAttr('some_tree'), GetAttr('value')]
            ),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
            ]),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
            ]),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('value'),
            ]),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
                ListExplode(),
            ]),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
            ]),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
                ListExplode(),
                GetAttr('value'),
            ]),
            DataSlicePath.from_actions([
                GetAttr('some_tree'),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
                ListExplode(),
                GetAttr('children'),
                ListExplode(),
            ]),
        ],
    )

    for data_path in helper.generate_available_data_slice_paths(max_depth=7):
      self.assertTrue(helper.is_valid_data_slice_path(data_path))

  def test_generate_available_data_paths_for_dict_schema(self):
    dict_schema = kd.dict_schema(kd.STRING, kd.STRING)
    helper = schema_helper.SchemaHelper(dict_schema)
    self.assertEqual(
        list(helper.generate_available_data_slice_paths(max_depth=5)),
        [
            DataSlicePath.from_actions([]),
            DataSlicePath.from_actions([DictGetKeys()]),
            DataSlicePath.from_actions([DictGetValues()]),
        ],
    )

    for data_path in helper.generate_available_data_slice_paths(max_depth=7):
      self.assertTrue(helper.is_valid_data_slice_path(data_path))

  def test_get_affected_schema_node_names(self):
    root = kd.new()
    helper1 = schema_helper.SchemaHelper(root.get_schema())

    o = kd.new(x=kd.new(z=1))
    actual_affected_schema_node_names = helper1.get_affected_schema_node_names(
        at_schema_node_name=schema_node_name(root.get_schema()),
        attr_name='foo',
        attr_value_schema=o.get_schema(),
    )

    def get_expected_affected_schema_node_names():
      h = schema_helper.SchemaHelper(
          root.get_schema().with_attrs(foo=o.get_schema())
      )
      h_schema = h.get_schema()
      result = h.get_all_schema_node_names() - {schema_node_name(h_schema)}
      self.assertEqual(
          result,
          to_same_len_set([
              schema_node_name(h_schema.foo),
              schema_node_name(h_schema.foo.x),
              schema_node_name(h_schema.foo.x, action=GetAttr('z')),
          ]),
      )
      return result

    self.assertEqual(
        actual_affected_schema_node_names,
        get_expected_affected_schema_node_names(),
    )

    updated_root = root.updated(kd.attrs(root, foo=o))
    helper2 = schema_helper.SchemaHelper(updated_root.get_schema())
    h2_schema = helper2.get_schema()

    self.assertEqual(
        helper2.get_affected_schema_node_names(
            at_schema_node_name=schema_node_name(h2_schema),
            attr_name='bar',
            attr_value_schema=o.get_schema(),
        ),
        to_same_len_set([
            schema_node_name(h2_schema.foo),
            schema_node_name(h2_schema.foo.x),
            schema_node_name(h2_schema.foo.x, action=GetAttr('z')),
        ]),
    )

    self.assertEqual(
        helper2.get_affected_schema_node_names(
            at_schema_node_name=schema_node_name(h2_schema.foo),
            attr_name='x',
            attr_value_schema=o.x.get_schema(),
        ),
        to_same_len_set([
            schema_node_name(h2_schema.foo.x),
            schema_node_name(h2_schema.foo.x, action=GetAttr('z')),
        ]),
    )

    self.assertEqual(
        helper2.get_affected_schema_node_names(
            at_schema_node_name=schema_node_name(h2_schema.foo.x),
            attr_name='z',
            attr_value_schema=o.x.z.get_schema(),
        ),
        {schema_node_name(h2_schema.foo.x, action=GetAttr('z'))},
    )

  def test_get_affected_schema_node_names_when_pointing_back_to_root(self):
    tree_node_schema = kd.named_schema(
        'TreeNode',
        value=kd.STRING,
    )
    helper = schema_helper.SchemaHelper(tree_node_schema)

    actual_affected_schema_node_names = helper.get_affected_schema_node_names(
        at_schema_node_name=schema_node_name(tree_node_schema),
        attr_name='children',
        attr_value_schema=kd.list_schema(tree_node_schema),
    )

    def get_expected_affected_schema_node_names():
      h = schema_helper.SchemaHelper(
          tree_node_schema.with_attrs(children=kd.list_schema(tree_node_schema))
      )
      h_schema = h.get_schema()
      result = h.get_all_schema_node_names()
      self.assertEqual(
          schema_node_name(h_schema),
          schema_node_name(h_schema.children.get_item_schema()),
      )
      self.assertEqual(
          result,
          to_same_len_set([
              schema_node_name(h_schema),
              schema_node_name(h_schema, action=GetAttr('value')),
              schema_node_name(h_schema.children),
          ]),
      )
      return result

    self.assertEqual(
        actual_affected_schema_node_names,
        get_expected_affected_schema_node_names(),
    )

  def test_get_schema_node_name(self):
    foo_schema = kd.named_schema('foo')
    foo_schema_node_name = schema_node_name(foo_schema)
    self.assertEqual(
        schema_helper.get_schema_node_name(
            parent_schema_item=foo_schema,
            action=GetAttr('bar'),
            child_schema_item=kd.INT32,
        ),
        f'{foo_schema_node_name}.bar:INT32',
    )
    self.assertEqual(
        schema_helper.get_schema_node_name(
            parent_schema_item=foo_schema,
            action=GetAttr('bar'),
            # kd.named_schema will use a fixed item id:
            child_schema_item=kd.named_schema('whatever'),
        ),
        kd.ids.encode_itemid(kd.named_schema('whatever').get_itemid()).to_py(),
    )

  def test_schema_node_name_for_leaf_schemas_that_cannot_be_aliased(self):
    foo_schema = kd.named_schema('foo')
    foo_schema_node_name = schema_node_name(foo_schema)
    # See koladata/internal/dtype.h for leaf schemas.
    for value in [
        kd.int32(1),
        kd.float32(2.0),
        kd.int64(3),
        kd.float64(4),
        kd.present,
        kd.missing,
        kd.schema.to_bool(True),
        kd.bytes(b'foo'),
        kd.item('bar'),
        kd.expr.pack_expr(kd.I.x + kd.I.y),
        kd.new_itemid(),
        kd.item(None, schema=kd.NONE),
    ]:
      # Make sure the schema of value is a leaf schema:
      self.assertIsNone(schema_helper._get_item_id(value.get_schema()))
      root = kd.new()
      root_with_value = root.with_attrs(value=value)
      root_with_aliased_value = root.with_attrs(value=kd.stub(value))
      kd.testing.assert_equivalent(
          root_with_value.value.get_schema(),
          root_with_aliased_value.value.get_schema(),
      )
      self.assertEqual(
          schema_helper.get_schema_node_name(
              parent_schema_item=foo_schema,
              action=GetAttr('bar'),
              child_schema_item=value.get_schema(),
          ),
          f'{foo_schema_node_name}.bar:{value.get_schema()}',
      )

    for value in [
        kd.slice(None, schema=kd.NONE),
        kd.slice([[]], schema=kd.NONE),
        kd.slice([None, None], schema=kd.NONE),
    ]:
      stub_value = kd.stub(value).no_bag()  # Drop the empty bag.
      kd.testing.assert_equivalent(
          value.get_schema(),
          stub_value.get_schema(),
      )

  def test_schema_schema_is_not_supported(self):
    for schema in [
        kd.SCHEMA,
        kd.schema.new_schema(x=kd.SCHEMA),
        kd.dict_schema(kd.INT32, kd.SCHEMA),
    ]:
      with self.assertRaisesRegex(
          ValueError, re.escape('SCHEMA schemas are not supported')
      ):
        schema_helper.SchemaHelper(schema)

  def test_object_schema_is_not_supported(self):
    for schema in [
        kd.OBJECT,
        kd.schema.new_schema(x=kd.OBJECT),
        kd.dict_schema(kd.INT32, kd.OBJECT),
    ]:
      with self.assertRaisesRegex(
          ValueError, re.escape('OBJECT schemas are not supported')
      ):
        schema_helper.SchemaHelper(schema)

  def test_schema_helper_constructor_expects_schema_item(self):
    with self.assertRaisesRegex(
        ValueError, re.escape('expected a SCHEMA item. Got: 1')
    ):
      schema_helper.SchemaHelper(kd.int32(1))

    with self.assertRaisesRegex(
        ValueError, re.escape('expected a SCHEMA item. Got: [1, 2, 3]')
    ):
      schema_helper.SchemaHelper(kd.slice([1, 2, 3]))

  def test_schema_helper_get_schema(self):
    schema = kd.dict_schema(key_schema=kd.INT32, value_schema=kd.INT64)
    helper = schema_helper.SchemaHelper(schema)
    kd.testing.assert_equivalent(helper.get_schema(), schema)

  def test_schema_helper_is_valid_schema_node_name(self):
    schema = kd.schema.new_schema(
        foo=kd.INT32,
        bar=kd.list_schema(kd.STRING),
    )
    helper = schema_helper.SchemaHelper(schema)
    self.assertFalse(helper.is_valid_schema_node_name('.foo'))
    self.assertFalse(helper.is_valid_schema_node_name('.foo:INT32'))
    self.assertTrue(helper.is_valid_schema_node_name(schema_node_name(schema)))
    self.assertTrue(
        helper.is_valid_schema_node_name(
            schema_node_name(schema, action=GetAttr('foo'))
        )
    )
    self.assertTrue(
        helper.is_valid_schema_node_name(schema_node_name(schema.bar))
    )
    self.assertTrue(
        helper.is_valid_schema_node_name(
            schema_node_name(schema.bar, action=ListExplode())
        )
    )

  def test_schema_helper_is_valid_data_path(self):
    schema = kd.schema.new_schema(
        foo=kd.INT32,
        bar=kd.list_schema(kd.STRING),
    )
    helper = schema_helper.SchemaHelper(schema)

    self.assertTrue(
        helper.is_valid_data_slice_path(DataSlicePath.from_actions([]))
    )
    self.assertTrue(
        helper.is_valid_data_slice_path(
            DataSlicePath.from_actions([GetAttr('foo')])
        )
    )
    self.assertTrue(
        helper.is_valid_data_slice_path(
            DataSlicePath.from_actions([GetAttr('bar')])
        )
    )
    self.assertTrue(
        helper.is_valid_data_slice_path(
            DataSlicePath.from_actions([GetAttr('bar'), ListExplode()])
        )
    )

    # These are not valid data slice paths for the schema.
    self.assertFalse(
        helper.is_valid_data_slice_path(
            DataSlicePath.from_actions([GetAttr('foo'), ListExplode()])
        )
    )
    self.assertFalse(
        helper.is_valid_data_slice_path(
            DataSlicePath.from_actions(
                [GetAttr('bar'), ListExplode(), DictGetKeys()]
            )
        )
    )

  def test_generate_all_available_data_paths(self):
    schema = kd.schema.new_schema(
        foo=kd.INT32,
        bar=kd.list_schema(kd.STRING),
    )
    helper = schema_helper.SchemaHelper(schema)
    self.assertEqual(
        set(helper.generate_available_data_slice_paths(max_depth=-1)),
        set([
            DataSlicePath.from_actions([]),
            DataSlicePath.from_actions([GetAttr('foo')]),
            DataSlicePath.from_actions([GetAttr('bar')]),
            DataSlicePath.from_actions([GetAttr('bar'), ListExplode()]),
        ]),
    )

  def test_generate_available_data_paths_for_negative_but_not_minus_one_max_depth(
      self,
  ):
    schema = kd.schema.new_schema(
        foo=kd.INT32,
        bar=kd.list_schema(kd.STRING),
    )
    helper = schema_helper.SchemaHelper(schema)
    self.assertEmpty(
        set(helper.generate_available_data_slice_paths(max_depth=-2)),
    )

  def test_get_schema_node_name_for_top_level_primitive_schema(self):
    helper = schema_helper.SchemaHelper(kd.INT32)
    self.assertEqual(
        helper.get_schema_node_name_for_data_slice_path(
            DataSlicePath.from_actions([])
        ),
        '.:INT32',
    )

  def test_get_schema_node_name_for_invalid_data_slice_path(self):
    schema = kd.schema.named_schema('SomeSchema', foo=kd.INT32)
    helper = schema_helper.SchemaHelper(schema)

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid data slice path: '.bar'")
    ):
      helper.get_schema_node_name_for_data_slice_path(
          DataSlicePath.from_actions([GetAttr('bar')])
      )

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid data slice path: '.foo[:]'")
    ):
      helper.get_schema_node_name_for_data_slice_path(
          DataSlicePath.from_actions([GetAttr('foo'), ListExplode()])
      )

  def test_use_invalid_schema_node_name(self):
    schema = kd.schema.named_schema('SomeSchema', foo=kd.INT32)
    helper = schema_helper.SchemaHelper(schema)

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid schema node name: '.foo'")
    ):
      helper.get_ancestor_schema_node_names({'.foo'})

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid schema node name: '.foo:INT32'")
    ):
      helper.get_descendant_schema_node_names({'.foo:INT32'})

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid schema node name: 'abcde'")
    ):
      helper.get_affected_schema_node_names(
          at_schema_node_name='abcde',
          attr_name='bar',
          attr_value_schema=kd.INT32,
      )

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid schema node name: 'bar'")
    ):
      helper.get_subschema_at('bar')

  def test_is_leaf_and_non_leaf_schema_node_name(self):
    schema = kd.schema.named_schema(
        'SomeSchema',
        foo=kd.INT32,
        bar=kd.schema.named_schema('InnerSchema', zoo=kd.INT32),
    )
    helper = schema_helper.SchemaHelper(schema)

    for non_leaf_schema_node_name in [
        schema_node_name(schema),
        schema_node_name(schema.bar),
    ]:
      self.assertTrue(
          helper.is_non_leaf_schema_node_name(non_leaf_schema_node_name)
      )
      self.assertFalse(
          helper.is_leaf_schema_node_name(non_leaf_schema_node_name)
      )

    for leaf_schema_node_name in [
        schema_node_name(schema, action=GetAttr('foo')),
        schema_node_name(schema.bar, action=GetAttr('zoo')),
    ]:
      self.assertTrue(helper.is_leaf_schema_node_name(leaf_schema_node_name))
      self.assertFalse(
          helper.is_non_leaf_schema_node_name(leaf_schema_node_name)
      )

    for invalid_schema_node_name in [
        '.foo',
        '.foo:INT32',
        'abcde',
    ]:
      with self.assertRaisesRegex(
          ValueError,
          re.escape(f"invalid schema node name: '{invalid_schema_node_name}'"),
      ):
        helper.is_leaf_schema_node_name(invalid_schema_node_name)
      with self.assertRaisesRegex(
          ValueError,
          re.escape(f"invalid schema node name: '{invalid_schema_node_name}'"),
      ):
        helper.is_non_leaf_schema_node_name(invalid_schema_node_name)

  def test_get_schema_bag(self):
    schema = kd.named_schema(
        'SomeSchema',
        foo=kd.INT32,
        bar=kd.named_schema('InnerSchema', zoo=kd.INT32),
    )
    helper = schema_helper.SchemaHelper(schema)

    schema_no_bag = schema.no_bag()
    kd.testing.assert_equivalent(
        schema_no_bag.with_bag(
            helper.get_schema_bag({schema_node_name(schema)})
        ),
        kd.named_schema('SomeSchema'),
    )
    kd.testing.assert_equivalent(
        schema_no_bag.with_bag(
            helper.get_schema_bag(
                to_same_len_set([
                    schema_node_name(schema),
                    schema_node_name(schema, action=GetAttr('foo')),
                ])
            )
        ),
        kd.named_schema('SomeSchema', foo=kd.INT32),
    )
    kd.testing.assert_equivalent(
        schema_no_bag.with_bag(
            helper.get_schema_bag(
                to_same_len_set([
                    schema_node_name(schema),
                    schema_node_name(schema.bar),
                ])
            )
        ),
        kd.named_schema(
            'SomeSchema',
            bar=kd.named_schema('InnerSchema'),
        ),
    )
    kd.testing.assert_equivalent(
        schema_no_bag.with_bag(
            helper.get_schema_bag(
                to_same_len_set([
                    schema_node_name(schema),
                    schema_node_name(schema, action=GetAttr('foo')),
                    schema_node_name(schema.bar),
                ])
            )
        ),
        kd.named_schema(
            'SomeSchema',
            foo=kd.INT32,
            bar=kd.named_schema('InnerSchema'),
        ),
    )
    kd.testing.assert_equivalent(
        schema_no_bag.with_bag(
            helper.get_schema_bag(
                to_same_len_set([
                    schema_node_name(schema),
                    schema_node_name(schema.bar),
                    schema_node_name(schema.bar, action=GetAttr('zoo')),
                ])
            )
        ),
        kd.named_schema(
            'SomeSchema',
            bar=kd.named_schema('InnerSchema', zoo=kd.INT32),
        ),
    )
    kd.testing.assert_equivalent(
        schema_no_bag.with_bag(
            helper.get_schema_bag(
                to_same_len_set([
                    schema_node_name(schema),
                    schema_node_name(schema, action=GetAttr('foo')),
                    schema_node_name(schema.bar),
                    schema_node_name(schema.bar, action=GetAttr('zoo')),
                ])
            )
        ),
        schema,  # The full schema
    )

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid schema node name: 'abcde'")
    ):
      helper.get_schema_bag({'abcde'})

  def test_same_list_schema_in_several_places(self):
    doc_schema = kd.named_schema(
        'Doc',
        salient_terms=kd.list_schema(kd.STRING),
    )
    query_schema = kd.named_schema(
        'Query',
        # This has the same schema as the salient_terms list in the doc_schema:
        a=kd.list_schema(kd.STRING),
        doc=kd.list_schema(doc_schema),
    )
    helper = schema_helper.SchemaHelper(query_schema)
    kd.testing.assert_equivalent(
        helper.get_schema_bag(helper.get_all_schema_node_names()),
        query_schema.get_bag(),
    )

  def test_get_parent_schema_node_names(self):
    schema = kd.schema.named_schema(
        'SomeSchema',
        foo=kd.INT32,
        bar=kd.schema.named_schema('InnerSchema', zoo=kd.INT32),
    )
    helper = schema_helper.SchemaHelper(schema)
    self.assertEmpty(
        helper.get_parent_schema_node_names(schema_node_name(schema))
    )

    schema = schema.with_attrs(loop=schema)
    schema = schema.with_attrs(loop_via_list=kd.list_schema(schema))
    helper = schema_helper.SchemaHelper(schema)
    self.assertEqual(
        helper.get_parent_schema_node_names(
            schema_node_name(schema, action=GetAttr('foo'))
        ),
        {schema_node_name(schema)},
    )
    self.assertEqual(
        helper.get_parent_schema_node_names(schema_node_name(schema.bar)),
        {schema_node_name(schema)},
    )
    self.assertEqual(
        helper.get_parent_schema_node_names(schema_node_name(schema.loop)),
        to_same_len_set(
            [schema_node_name(schema), schema_node_name(schema.loop_via_list)]
        ),
    )
    self.assertEqual(
        helper.get_parent_schema_node_names(
            schema_node_name(schema.bar, action=GetAttr('zoo'))
        ),
        {schema_node_name(schema.bar)},
    )
    self.assertEqual(
        helper.get_parent_schema_node_names(schema_node_name(schema.loop.bar)),
        {schema_node_name(schema)},
    )

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid schema node name: 'abcde'")
    ):
      helper.get_parent_schema_node_names('abcde')

  def test_get_child_schema_node_names(self):
    schema = kd.schema.named_schema(
        'SomeSchema',
        foo=kd.INT32,
        bar=kd.schema.named_schema('InnerSchema', zoo=kd.INT32),
    )
    schema = schema.with_attrs(loop=schema)
    schema = schema.with_attrs(loop_via_list=kd.list_schema(schema))
    helper = schema_helper.SchemaHelper(schema)
    self.assertEmpty(
        helper.get_child_schema_node_names(
            schema_node_name(schema, action=GetAttr('foo'))
        )
    )
    self.assertEqual(
        helper.get_child_schema_node_names(schema_node_name(schema)),
        to_same_len_set([
            schema_node_name(schema, action=GetAttr('foo')),
            schema_node_name(schema.bar),
            schema_node_name(schema),  # Via the 'loop' attribute.
            schema_node_name(schema.loop_via_list),
        ]),
    )
    self.assertEqual(
        helper.get_child_schema_node_names(
            schema_node_name(schema.loop_via_list)
        ),
        {schema_node_name(schema)},
    )
    self.assertEqual(
        helper.get_child_schema_node_names(schema_node_name(schema.bar)),
        {schema_node_name(schema.bar, action=GetAttr('zoo'))},
    )
    self.assertEqual(
        helper.get_child_schema_node_names(schema_node_name(schema.loop.bar)),
        {schema_node_name(schema.bar, action=GetAttr('zoo'))},
    )

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid schema node name: 'abcde'")
    ):
      helper.get_child_schema_node_names('abcde')

  def test_get_minimal_schema_bag_for_parent_child_relationship(self):
    schema = kd.schema.named_schema(
        'SomeSchema',
        foo=kd.INT32,
        bar=kd.schema.named_schema('InnerSchema', zoo=kd.INT32),
    )
    helper = schema_helper.SchemaHelper(schema)

    kd.testing.assert_equivalent(
        helper.get_minimal_schema_bag_for_parent_child_relationship(
            parent_schema_node_name=schema_node_name(schema),
            child_schema_node_name=schema_node_name(
                schema, action=GetAttr('foo')
            ),
        ),
        # The bag does not mention 'bar' or 'zoo':
        kd.attrs(kd.schema.named_schema('SomeSchema'), foo=kd.INT32),
    )
    kd.testing.assert_equivalent(
        helper.get_minimal_schema_bag_for_parent_child_relationship(
            parent_schema_node_name=schema_node_name(schema),
            child_schema_node_name=schema_node_name(schema.bar),
        ),
        # Note that the inner schema is minimal: it does not mention 'zoo':
        kd.attrs(
            kd.schema.named_schema('SomeSchema'),
            bar=kd.schema.named_schema('InnerSchema'),
        ),
    )

    schema = schema.with_attrs(bar2=schema.bar)
    helper = schema_helper.SchemaHelper(schema)
    kd.testing.assert_equivalent(
        helper.get_minimal_schema_bag_for_parent_child_relationship(
            parent_schema_node_name=schema_node_name(schema),
            child_schema_node_name=schema_node_name(schema.bar),
        ),
        # Similar to the previous case, but now the bag also contains 'bar2':
        kd.attrs(
            kd.schema.named_schema('SomeSchema'),
            bar=kd.schema.named_schema('InnerSchema'),
            bar2=kd.schema.named_schema('InnerSchema')
        ),
    )

    schema = schema.with_attrs(loop=schema)
    helper = schema_helper.SchemaHelper(schema)
    kd.testing.assert_equivalent(
        helper.get_minimal_schema_bag_for_parent_child_relationship(
            parent_schema_node_name=schema_node_name(schema),
            child_schema_node_name=schema_node_name(schema),
        ),
        kd.attrs(
            kd.schema.named_schema('SomeSchema'),
            loop=kd.schema.named_schema('SomeSchema'),
        ),
    )

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid schema node name: 'abcde'")
    ):
      helper.get_minimal_schema_bag_for_parent_child_relationship(
          parent_schema_node_name='abcde',
          child_schema_node_name=schema_node_name(schema.bar),
      )

    with self.assertRaisesRegex(
        ValueError, re.escape("invalid schema node name: 'abcde'")
    ):
      helper.get_minimal_schema_bag_for_parent_child_relationship(
          parent_schema_node_name=schema_node_name(schema),
          child_schema_node_name='abcde',
      )


if __name__ == '__main__':
  absltest.main()
