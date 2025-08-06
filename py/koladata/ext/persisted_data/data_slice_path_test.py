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

import itertools
import re
from absl.testing import absltest
from koladata import kd
from koladata.ext.persisted_data import data_slice_path as data_slice_path_lib


DataSlicePath = data_slice_path_lib.DataSlicePath
DictGetKeys = data_slice_path_lib.DictGetKeys
DictGetValues = data_slice_path_lib.DictGetValues
ListExplode = data_slice_path_lib.ListExplode
GetAttr = data_slice_path_lib.GetAttr


class DataSlicePathTest(absltest.TestCase):

  def test_can_be_used_with_dot_syntax_in_data_slice_path_string(self):
    for attr_name in [
        'foo',
        'foo_bar',
        'FooBar',
        'foo_bar_FooBar123',
        '__metadata__',
    ]:
      self.assertTrue(
          data_slice_path_lib.can_be_used_with_dot_syntax_in_data_slice_path_string(
              attr_name
          )
      )

    for attr_name in [
        '',
        '.',
        'foo.bar',
        '123foo',
        'foo()',
        '42',
        'foo\nbar',
        '$foo',
        '@bar',
        'foo@bar',
    ]:
      self.assertFalse(
          data_slice_path_lib.can_be_used_with_dot_syntax_in_data_slice_path_string(
              attr_name
          )
      )
    for action in [
        DictGetKeys(),
        DictGetValues(),
        ListExplode(),
        GetAttr('bar'),
    ]:
      self.assertFalse(
          data_slice_path_lib.can_be_used_with_dot_syntax_in_data_slice_path_string(
              DataSlicePath.from_actions([action]).to_string()
          )
      )

  def test_base64_encoded_attr_name(self):
    self.assertEqual(
        data_slice_path_lib.base64_encoded_attr_name('foo'), 'Zm9v'
    )
    self.assertEqual(
        data_slice_path_lib.base64_encoded_attr_name('foo_bar'), 'Zm9vX2Jhcg=='
    )
    self.assertEqual(
        data_slice_path_lib.base64_encoded_attr_name('FooBar'), 'Rm9vQmFy'
    )
    self.assertEqual(
        data_slice_path_lib.base64_encoded_attr_name('foo_bar_FooBar123'),
        'Zm9vX2Jhcl9Gb29CYXIxMjM=',
    )
    self.assertEqual(
        data_slice_path_lib.base64_encoded_attr_name(''),
        '',
    )
    self.assertEqual(
        data_slice_path_lib.base64_encoded_attr_name('foo.bar$#$ ()'),
        'Zm9vLmJhciQjJCAoKQ==',
    )

  def test_decode_base64_encoded_attr_name(self):
    self.assertEqual(
        data_slice_path_lib.decode_base64_encoded_attr_name('Zm9v'), 'foo'
    )
    self.assertEqual(
        data_slice_path_lib.decode_base64_encoded_attr_name('Zm9vX2Jhcg=='),
        'foo_bar',
    )
    self.assertEqual(
        data_slice_path_lib.decode_base64_encoded_attr_name('Rm9vQmFy'),
        'FooBar',
    )
    self.assertEqual(
        data_slice_path_lib.decode_base64_encoded_attr_name(
            'Zm9vX2Jhcl9Gb29CYXIxMjM='
        ),
        'foo_bar_FooBar123',
    )
    self.assertEqual(
        data_slice_path_lib.decode_base64_encoded_attr_name(''),
        '',
    )
    self.assertEqual(
        data_slice_path_lib.decode_base64_encoded_attr_name(
            'Zm9vLmJhciQjJCAoKQ=='
        ),
        'foo.bar$#$ ()',
    )

  def test_base64_encode_decode_round_trip(self):
    for s in [
        'foo',
        'foo_bar',
        'FooBar',
        'foo_bar_FooBar123',
        '',
        'foo.bar$#$ ()',
        '  ',
        '  foo  bar  ',
        '\n\t###""',
    ]:
      self.assertEqual(
          data_slice_path_lib.decode_base64_encoded_attr_name(
              data_slice_path_lib.base64_encoded_attr_name(s)
          ),
          s,
      )

  def test_data_slice_path_parse_from_string(self):
    self.assertEqual(
        DataSlicePath.parse_from_string('.foo'),
        DataSlicePath.from_actions([GetAttr('foo')]),
    )
    self.assertEqual(
        DataSlicePath.parse_from_string('.foo.bar'),
        DataSlicePath.from_actions([GetAttr('foo'), GetAttr('bar')]),
    )
    self.assertEqual(
        DataSlicePath.parse_from_string(
            '.foo[:][:].bar.get_values().get_keys().zoo'
        ),
        DataSlicePath.from_actions([
            GetAttr('foo'),
            ListExplode(),
            ListExplode(),
            GetAttr('bar'),
            DictGetValues(),
            DictGetKeys(),
            GetAttr('zoo'),
        ]),
    )
    for attr_name in ['foo', '.', '', 'foo.bar', '.foo()', 'moo123.foo']:
      encoded_attr_name = data_slice_path_lib.base64_encoded_attr_name(
          attr_name
      )
      self.assertEqual(
          DataSlicePath.parse_from_string(f'.get_attr("{encoded_attr_name}")'),
          DataSlicePath.from_actions([GetAttr(attr_name)]),
      )
      self.assertEqual(
          DataSlicePath.parse_from_string(f".get_attr('{encoded_attr_name}')"),
          DataSlicePath.from_actions([GetAttr(attr_name)]),
      )
      self.assertEqual(
          DataSlicePath.parse_from_string(
              f'[:].get_attr("{encoded_attr_name}")[:].get_keys()'
          ),
          DataSlicePath.from_actions(
              [ListExplode(), GetAttr(attr_name), ListExplode(), DictGetKeys()]
          ),
      )

  def test_data_slice_path_parse_from_string_with_invalid_input(self):
    for invalid_input in [
        '[:',
        '[',
        '.foo.get_attr(',
        '.foo.get_attr(a',
        '.foo.get_attr("$@ ")',
        '.foo.get_attr("a)',
        '.foo.get_attr("a\')',
        '.foo.get_attr("a".bar',
        'bar',
        '.foo.get_keys()zoo',
        '..',
        '.foo..bar',
        '.bar.__metadata __',
        '.bar.my_strange_attr_name().zoo',
    ]:
      with self.assertRaisesRegex(
          ValueError, re.escape(f"invalid data slice path '{invalid_input}'")
      ):
        DataSlicePath.parse_from_string(invalid_input)

  def test_data_slice_path_parse_from_string_to_string_round_trip(self):
    for data_slice_path_string in [
        '.foo',
        '.foo.bar',
        '.foo[:][:].bar.get_values().get_keys().zoo',
        '.foo[:]',
        '.foo[:][:]',
        '.get_attr("")',
        '.get_attr("Zm9vLmJhciQjJCAoKQ==")',
        '[:].get_keys().get_attr("Zm9vLmJhciQjJCAoKQ==").get_values().zoo',
    ]:
      self.assertEqual(
          DataSlicePath.parse_from_string(data_slice_path_string).to_string(),
          data_slice_path_string,
      )
      self.assertEqual(
          f'{DataSlicePath.parse_from_string(data_slice_path_string)}',
          data_slice_path_string,
      )

  def test_data_slice_path_extended_with(self):
    self.assertEqual(
        DataSlicePath.from_actions([GetAttr('foo')]).concat(
            DataSlicePath.from_actions([GetAttr('bar'), ListExplode()])
        ),
        DataSlicePath.from_actions(
            [GetAttr('foo'), GetAttr('bar'), ListExplode()]
        ),
    )

    for dsp1_string, dsp2_string in itertools.product(
        [
            '',
            '.foo',
            '.foo.bar',
            '.foo[:][:].bar.get_values().get_keys().zoo',
            '.foo[:]',
            '.foo[:][:]',
            '.get_attr("")',
            '.get_attr("Zm9vLmJhciQjJCAoKQ==")',
            '[:].get_keys().get_attr("Zm9vLmJhciQjJCAoKQ==").get_values().zoo',
        ],
        repeat=2,
    ):
      dsp1 = DataSlicePath.parse_from_string(dsp1_string)
      dsp2 = DataSlicePath.parse_from_string(dsp2_string)
      self.assertEqual(
          dsp1.concat(dsp2),
          DataSlicePath(dsp1.actions + dsp2.actions),
      )

  def test_data_slice_path_extended_with_action(self):
    self.assertEqual(
        DataSlicePath.from_actions([GetAttr('foo')]).extended_with_action(
            GetAttr('bar')
        ),
        DataSlicePath.from_actions([GetAttr('foo'), GetAttr('bar')]),
    )

    for data_slice_path_string in [
        '.foo',
        '.foo.bar',
        '.foo[:][:].bar.get_values().get_keys().zoo',
        '.foo[:]',
        '.foo[:][:]',
        '.get_attr("")',
        '.get_attr("Zm9vLmJhciQjJCAoKQ==")',
        '[:].get_keys().get_attr("Zm9vLmJhciQjJCAoKQ==").get_values().zoo',
    ]:
      dsp = DataSlicePath.parse_from_string(data_slice_path_string)
      for action in [
          GetAttr('foo'),
          GetAttr('bar asdf\n'),
          DictGetKeys(),
          DictGetValues(),
          ListExplode(),
      ]:
        self.assertEqual(
            dsp.extended_with_action(action),
            DataSlicePath.from_actions(list(dsp.actions) + [action]),
        )

  def test_get_subslices_of_entities_and_lists(self):
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

    kd.testing.assert_equivalent(
        data_slice_path_lib.get_subslice(
            root,
            DataSlicePath.from_actions([
                GetAttr('query'),
                ListExplode(),
                GetAttr('doc'),
                ListExplode(),
                GetAttr('doc_id'),
            ]),
        ),
        root.query[:].doc[:].doc_id,
    )
    kd.testing.assert_equivalent(
        data_slice_path_lib.get_subslice(
            root,
            DataSlicePath.from_actions(
                [GetAttr('query'), ListExplode(), GetAttr('doc')]
            ),
        ),
        root.query[:].doc,
    )

  def test_get_subslices_of_dict(self):
    ds = kd.dict({1: 'a', 2: 'b', 3: 'c'})

    kd.testing.assert_equivalent(
        data_slice_path_lib.get_subslice(ds, DataSlicePath.from_actions([])),
        ds,
    )
    kd.testing.assert_equivalent(
        data_slice_path_lib.get_subslice(
            ds, DataSlicePath.from_actions([DictGetKeys()])
        ),
        ds.get_keys(),
    )
    kd.testing.assert_equivalent(
        data_slice_path_lib.get_subslice(
            ds, DataSlicePath.from_actions([DictGetValues()])
        ),
        ds.get_values(),
    )

  def test_get_subslices_with_incompatible_data_slices_and_paths(self):
    ds = kd.dict({1: 'a', 2: 'b', 3: 'c'})
    with self.assertRaisesRegex(
        ValueError,
        re.escape('cannot get or set attributes on schema: INT32'),
    ):
      data_slice_path_lib.get_subslice(
          ds, DataSlicePath.from_actions([DictGetKeys(), DictGetValues()])
      )
    with self.assertRaisesRegex(
        ValueError,
        re.escape('cannot get or set attributes on schema: STRING'),
    ):
      data_slice_path_lib.get_subslice(
          ds, DataSlicePath.from_actions([DictGetValues(), DictGetKeys()])
      )
    with self.assertRaisesRegex(
        ValueError,
        re.escape('primitives do not have attributes, got STRING'),
    ):
      data_slice_path_lib.get_subslice(
          ds, DataSlicePath.from_actions([DictGetValues(), GetAttr('foo')])
      )
    with self.assertRaisesRegex(
        ValueError,
        re.escape("the attribute 'bar' is missing on the schema"),
    ):
      data_slice_path_lib.get_subslice(
          ds, DataSlicePath.from_actions([GetAttr('bar')])
      )
    with self.assertRaisesRegex(
        ValueError,
        re.escape('cannot explode'),
    ):
      data_slice_path_lib.get_subslice(
          ds, DataSlicePath.from_actions([ListExplode()])
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

    self.assertEqual(
        list(
            data_slice_path_lib.generate_data_slice_paths_for_arbitrary_data_slice_with_schema(
                initial_root_schema, max_depth=5
            )
        ),
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
        list(
            data_slice_path_lib.generate_data_slice_paths_for_arbitrary_data_slice_with_schema(
                initial_root_schema, max_depth=7
            )
        ),
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

  def test_generate_available_data_paths_for_dict_schema(self):
    dict_schema = kd.dict_schema(kd.STRING, kd.STRING)
    self.assertEqual(
        list(
            data_slice_path_lib.generate_data_slice_paths_for_arbitrary_data_slice_with_schema(
                dict_schema, max_depth=5
            )
        ),
        [
            DataSlicePath.from_actions([]),
            DataSlicePath.from_actions([DictGetKeys()]),
            DataSlicePath.from_actions([DictGetValues()]),
        ],
    )

  def test_generate_available_data_paths_for_negative_max_depth(self):
    schema = kd.schema.new_schema(
        foo=kd.INT32,
        bar=kd.list_schema(kd.STRING),
    )
    self.assertEmpty(
        set(
            data_slice_path_lib.generate_data_slice_paths_for_arbitrary_data_slice_with_schema(
                schema, max_depth=-1
            )
        )
    )

  def test_get_subschema_bag(self):
    dict_schema = kd.dict_schema(
        kd.INT32, kd.named_schema('MyDictValue', x=kd.INT32)
    )
    kd.testing.assert_equivalent(
        DictGetKeys().get_subschema_bag(dict_schema),
        # The bag contains exactly one entity-attribute-value triple, namely:
        # * entity = dict_schema.get_itemid()
        # * attribute = '__keys__'
        # * value = kd.INT32.
        kd.attrs(dict_schema, **{'__keys__': kd.INT32}),
    )
    kd.testing.assert_equivalent(
        DictGetValues().get_subschema_bag(dict_schema),
        # The bag contains exactly one entity-attribute-value triple, namely:
        # * entity = dict_schema.get_itemid()
        # * attribute = '__values__'
        # * value = kd.named_schema('MyDictValue'), i.e. the value captures
        #   not only the itemid of the value schema, but also the name of the
        #   value schema. However, it captures nothing else about the value
        #   schema, in particular it does not capture its attribute 'x' or its
        #   schema kd.INT32. In this sense it is minimal.
        kd.attrs(dict_schema, **{'__values__': kd.named_schema('MyDictValue')}),
    )

    list_schema = kd.list_schema(kd.named_schema('MyListItem', x=kd.INT32))
    kd.testing.assert_equivalent(
        ListExplode().get_subschema_bag(list_schema),
        # Again, the subschema bag is minimal for the action - it does not
        # mention 'x' or its schema kd.INT32:
        kd.attrs(list_schema, **{'__items__': kd.named_schema('MyListItem')}),
    )

    entity_schema = kd.named_schema(
        'MyEntity', x=kd.INT32, y=kd.named_schema('MyInnerEntity', z=kd.STRING)
    )
    kd.testing.assert_equivalent(
        GetAttr('x').get_subschema_bag(entity_schema),
        # The subschema bag mentions only attribute 'x' and not 'y':
        kd.attrs(entity_schema, **{'x': kd.INT32}),
    )
    kd.testing.assert_equivalent(
        GetAttr('y').get_subschema_bag(entity_schema),
        # The subschema bag mentions only attribute 'y' and not 'x'. Moreover,
        # it does not mention 'z' or its schema:
        kd.attrs(entity_schema, **{'y': kd.named_schema('MyInnerEntity')}),
    )

  def test_subschema_bags_are_complete_for_primitive_subschemas(self):
    # By this we mean that the subschema bag contains all information needed
    # to construct Koda items when the subschemas are schemas of primitives.

    with self.subTest('dict'):
      dict_schema = kd.dict_schema(kd.STRING, kd.INT32)
      reconstructed_dict_schema = dict_schema.no_bag().with_bag(
          # Note that we need to combine the two minimal bags in order to
          # reconstruct the original dict schema.
          kd.bags.updated(
              DictGetKeys().get_subschema_bag(dict_schema),
              DictGetValues().get_subschema_bag(dict_schema),
          )
      )
      d = kd.dict({'foo': 123}, schema=reconstructed_dict_schema)
      self.assertEqual(
          d.to_pytree(),
          {'foo': 123},
      )
      self.assertEqual(d.get_schema().get_itemid(), dict_schema.get_itemid())

    with self.subTest('list'):
      list_schema = kd.list_schema(kd.INT32)
      reconstructed_list_schema = list_schema.no_bag().with_bag(
          ListExplode().get_subschema_bag(list_schema)
      )
      l = kd.list([123, 456], schema=reconstructed_list_schema)
      self.assertEqual(
          l.to_pytree(),
          [123, 456],
      )
      self.assertEqual(l.get_schema().get_itemid(), list_schema.get_itemid())

    with self.subTest('entity'):
      entity_schema = kd.named_schema('MyEntity', x=kd.INT32)
      reconstructed_entity_schema = entity_schema.no_bag().with_bag(
          GetAttr('x').get_subschema_bag(entity_schema)
      )
      e = reconstructed_entity_schema.new(x=123)
      self.assertEqual(
          e.to_pytree(),
          {'x': 123},
      )
      self.assertEqual(e.get_schema().get_itemid(), entity_schema.get_itemid())


if __name__ == '__main__':
  absltest.main()
