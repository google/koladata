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


schema_node_name = test_only_schema_node_name_helper.schema_node_name
to_same_len_set = test_only_schema_node_name_helper.to_same_len_set

DictGetKeys = data_slice_path_lib.DictGetKeys
DictGetValues = data_slice_path_lib.DictGetValues
ListExplode = data_slice_path_lib.ListExplode
GetAttr = data_slice_path_lib.GetAttr


class TestOnlySchemaNodeNameHelperTest(absltest.TestCase):

  def test_schema_node_name_first_argument_must_have_item_id(self):
    for schema in [kd.INT32, kd.OBJECT, kd.FLOAT32, kd.SCHEMA]:
      with self.assertRaises(AssertionError):
        schema_node_name(schema)

  def test_schema_node_name_second_argument_is_none(self):
    for schema in [
        kd.list_schema(kd.INT32),
        kd.dict_schema(kd.INT32, kd.INT32),
        kd.named_schema('foo', x=kd.INT32),
        kd.new(a=1, b=2).get_schema(),
    ]:
      self.assertEqual(
          schema_node_name(schema),
          schema_helper.get_schema_node_name_from_schema_having_an_item_id(
              schema
          ),
      )

  def test_schema_node_name_second_argument_is_not_none(self):
    list_schema = kd.list_schema(kd.INT32)
    self.assertEqual(
        schema_node_name(
            list_schema,
            action=ListExplode(),
        ),
        schema_helper.get_schema_node_name(
            parent_schema_item=list_schema,
            action=ListExplode(),
            child_schema_item=kd.INT32,
        ),
    )

    dict_schema = kd.dict_schema(key_schema=kd.STRING, value_schema=kd.FLOAT32)
    self.assertEqual(
        schema_node_name(
            dict_schema,
            action=DictGetKeys(),
        ),
        schema_helper.get_schema_node_name(
            parent_schema_item=dict_schema,
            action=DictGetKeys(),
            child_schema_item=kd.STRING,
        ),
    )
    self.assertEqual(
        schema_node_name(
            dict_schema,
            action=DictGetValues(),
        ),
        schema_helper.get_schema_node_name(
            parent_schema_item=dict_schema,
            action=DictGetValues(),
            child_schema_item=kd.FLOAT32,
        ),
    )

    named_schema = kd.named_schema('foo', x=kd.INT32)
    self.assertEqual(
        schema_node_name(
            named_schema,
            action=GetAttr('x'),
        ),
        schema_helper.get_schema_node_name(
            parent_schema_item=named_schema,
            action=GetAttr('x'),
            child_schema_item=kd.INT32,
        ),
    )

    schema = kd.new(a=1.0, b=2).get_schema()
    self.assertEqual(
        schema_node_name(
            schema,
            action=GetAttr('a'),
        ),
        schema_helper.get_schema_node_name(
            parent_schema_item=schema,
            action=GetAttr('a'),
            child_schema_item=kd.FLOAT32,
        ),
    )
    self.assertEqual(
        schema_node_name(
            schema,
            action=GetAttr('b'),
        ),
        schema_helper.get_schema_node_name(
            parent_schema_item=schema,
            action=GetAttr('b'),
            child_schema_item=kd.INT32,
        ),
    )

  def test_schema_node_name_raises_when_child_schema_item_has_item_id(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the child schema ENTITY(b=INT32) has an item id. Please use'
            ' schema_node_name(child_schema) instead, i.e. without the "action"'
            ' kwarg'
        ),
    ):
      schema_node_name(
          kd.new(a=kd.new(b=1)).get_schema(),
          action=GetAttr('a'),
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'the child schema LIST[INT32] has an item id. Please use'
            ' schema_node_name(child_schema) instead, i.e. without the "action"'
            ' kwarg'
        ),
    ):
      schema_node_name(
          kd.list_schema(item_schema=kd.list_schema(kd.INT32)),
          action=ListExplode(),
      )

  def test_to_same_len_set_result(self):
    self.assertEqual(
        to_same_len_set(['a', 'b']),
        {'a', 'b'},
    )

    self.assertEqual(
        to_same_len_set(['a12', 'b', 'c']),
        {'a12', 'b', 'c'},
    )

  def test_to_same_len_set_checks_cardinality(self):
    with self.assertRaises(AssertionError):
      to_same_len_set(['a', 'a'])

    with self.assertRaises(AssertionError):
      to_same_len_set(['a', 'b', 'c', 'b'])


if __name__ == '__main__':
  absltest.main()
