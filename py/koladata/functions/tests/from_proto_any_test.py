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
from google.protobuf import descriptor_pool
from koladata.expr import expr_eval
from koladata.functions import proto_conversions
from koladata.functions.tests import test_pb2
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants
from google.protobuf import any_pb2

ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class FromProtoAnyTest(absltest.TestCase):

  def test_zero_messages(self):
    x = proto_conversions.from_proto_any([])
    testing.assert_equal(x.no_bag(), ds([], schema_constants.OBJECT))

  def test_single_none(self):
    x = proto_conversions.from_proto_any(None)
    testing.assert_equal(x.no_bag(), ds(None, schema_constants.OBJECT))
    self.assertFalse(x.get_bag().is_mutable())

  def test_invalid_input_primitive(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'from_proto_any expects a nested container of google.protobuf.Any'
        " messages, got <class 'int'>",
    ):
      proto_conversions.from_proto_any(1)  # pytype: disable=wrong-arg-types

  def test_list_with_none(self):
    x = proto_conversions.from_proto_any([None])
    testing.assert_equal(x.no_bag(), ds([None], schema_constants.OBJECT))

  def test_invalid_input_list_primitive(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'from_proto_any expects a nested container of google.protobuf.Any'
        " messages, got <class 'int'>",
    ):
      proto_conversions.from_proto_any([1])  # pytype: disable=wrong-arg-types

  def test_invalid_input_wrong_proto_type(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'from_proto_any expects a nested container of google.protobuf.Any'
        f' messages, got {repr(test_pb2.MessageA)}',
    ):
      proto_conversions.from_proto_any(test_pb2.MessageA())  # pytype: disable=wrong-arg-types

  def test_single_message(self):
    m = test_pb2.MessageA(some_text='thing 1')
    any_m = any_pb2.Any()
    any_m.Pack(m)
    x = proto_conversions.from_proto_any(any_m)
    self.assertFalse(x.get_bag().is_mutable())
    self.assertEqual(x.get_ndim(), 0)
    self.assertEqual(x.some_text, 'thing 1')
    self.assertEqual(x.get_schema(), schema_constants.OBJECT)

  def test_single_message_with_message_type(self):
    m = test_pb2.MessageA(some_text='thing 1')
    any_m = any_pb2.Any()
    any_m.Pack(m)
    x = proto_conversions.from_proto_any(any_m, message_type=test_pb2.MessageA)
    self.assertFalse(x.get_bag().is_mutable())
    self.assertEqual(x.get_ndim(), 0)
    self.assertEqual(x.some_text, 'thing 1')
    self.assertEqual(x.get_schema().no_bag(), schema_constants.OBJECT)

  def test_multiple_messages_same_type(self):
    m1 = test_pb2.MessageA(some_text='thing 1')
    any_m1 = any_pb2.Any()
    any_m1.Pack(m1)
    m2 = test_pb2.MessageA(some_text='thing 2')
    any_m2 = any_pb2.Any()
    any_m2.Pack(m2)

    x = proto_conversions.from_proto_any([any_m1, any_m2])
    self.assertFalse(x.get_bag().is_mutable())
    self.assertEqual(x.get_ndim(), 1)
    testing.assert_equal(x.some_text.no_bag(), ds(['thing 1', 'thing 2']))
    self.assertEqual(x.get_schema(), schema_constants.OBJECT)

  def test_multiple_messages_different_types(self):
    m1 = test_pb2.MessageA(some_text='thing 1')
    any_m1 = any_pb2.Any()
    any_m1.Pack(m1)
    m2 = test_pb2.MessageB(text='thing 2')
    any_m2 = any_pb2.Any()
    any_m2.Pack(m2)

    x = proto_conversions.from_proto_any([any_m1, any_m2])
    self.assertFalse(x.get_bag().is_mutable())
    self.assertEqual(x.get_ndim(), 1)
    testing.assert_equal(x.maybe('some_text').no_bag(), ds(['thing 1', None]))
    testing.assert_equal(x.maybe('text').no_bag(), ds([None, 'thing 2']))
    self.assertEqual(x.get_schema(), schema_constants.OBJECT)

  def test_nested_list_input(self):
    m1 = test_pb2.MessageA(some_text='1')
    any_m1 = any_pb2.Any()
    any_m1.Pack(m1)
    m2 = test_pb2.MessageA(some_text='2')
    any_m2 = any_pb2.Any()
    any_m2.Pack(m2)
    m3 = test_pb2.MessageB(text='3')
    any_m3 = any_pb2.Any()
    any_m3.Pack(m3)

    x = proto_conversions.from_proto_any([[any_m1, None, any_m2], [], [any_m3]])
    self.assertEqual(x.get_ndim(), 2)
    self.assertEqual(
        x.get_shape(), ds([[None, None, None], [], [None]]).get_shape()
    )
    testing.assert_equal(
        x.maybe('some_text').no_bag(), ds([['1', None, '2'], [], [None]])
    )
    testing.assert_equal(
        x.maybe('text').no_bag(), ds([[None, None, None], [], ['3']])
    )

  def test_explicit_schema(self):
    m1 = test_pb2.MessageA(some_text='thing 1')
    any_m1 = any_pb2.Any()
    any_m1.Pack(m1)
    m2 = test_pb2.MessageB(text='thing 2')
    any_m2 = any_pb2.Any()
    any_m2.Pack(m2)

    schema = expr_eval.eval(
        kde.schema.new_schema(some_text=schema_constants.STRING)
    )
    x = proto_conversions.from_proto_any([any_m1, any_m2], schema=schema)
    self.assertFalse(x.get_bag().is_mutable())
    testing.assert_equal(x.some_text.no_bag(), ds(['thing 1', None]))
    self.assertCountEqual(x.get_attr_names(intersection=True), ['some_text'])

  def test_extensions(self):
    m = test_pb2.MessageA(some_text='thing 1')
    m.Extensions[test_pb2.MessageAExtension.message_a_extension].extra = 2
    any_m = any_pb2.Any()
    any_m.Pack(m)

    ext_name = (
        '(koladata.functions.testing.MessageAExtension.message_a_extension)'
    )
    x = proto_conversions.from_proto_any(
        any_m,
        extensions=[ext_name],
    )
    self.assertFalse(x.get_bag().is_mutable())
    self.assertEqual(x.get_attr(ext_name).extra, 2)

  def test_empty_descriptor_pool(self):
    m = test_pb2.MessageA(some_text='thing 1')
    any_m = any_pb2.Any()
    any_m.Pack(m)
    pool = descriptor_pool.DescriptorPool()
    with self.assertRaisesRegex(KeyError, 'MessageA'):
      proto_conversions.from_proto_any(any_m, descriptor_pool=pool)

  def test_invalid_type_url(self):
    m = test_pb2.MessageA(some_text='thing 1')
    any_m = any_pb2.Any()
    any_m.Pack(m)
    any_m.type_url = 'type.googleapis.com/invalid.Type'
    with self.assertRaisesRegex(KeyError, 'invalid.Type'):
      proto_conversions.from_proto_any(any_m)


if __name__ == '__main__':
  absltest.main()
