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
from koladata.functions import functions as fns
from koladata.functions.tests import test_pb2
from koladata.operators import kde_operators
from koladata.testing import testing
from koladata.types import data_slice
from koladata.types import schema_constants


ds = data_slice.DataSlice.from_vals
kde = kde_operators.kde


class FromProtoTest(absltest.TestCase):

  def test_zero_messages(self):
    x = fns.from_proto([])
    testing.assert_equal(x.no_bag(), ds([], schema_constants.OBJECT))
    self.assertFalse(x.get_bag().is_mutable())

  def test_single_none(self):
    x = fns.from_proto(None)
    testing.assert_equal(x.no_bag(), ds(None, schema_constants.OBJECT))
    self.assertFalse(x.get_bag().is_mutable())

  def test_invalid_input_primitive(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'messages must be Message or list of Message, got 1'
    ):
      fns.from_proto(1)  # pytype: disable=wrong-arg-types

  def test_list_with_none(self):
    x = fns.from_proto([None])
    testing.assert_equal(x.no_bag(), ds([None], schema_constants.OBJECT))

  def test_invalid_input_list_primitive(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'messages must be Message or list of Message, got [1]'
    ):
      fns.from_proto([1])  # pytype: disable=wrong-arg-types

  def test_invalid_different_types(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'expected all messages to have the same type, got'
            ' koladata.functions.testing.MessageA and'
            ' koladata.functions.testing.MessageB'
        ),
    ):
      fns.from_proto([test_pb2.MessageA(), test_pb2.MessageB()])

  def test_invalid_input_mismatched_itemid_ndim(self):
    with self.assertRaisesWithLiteralMatch(
        ValueError, 'itemid must be a DataItem if messages is a single message'
    ):
      fns.from_proto(test_pb2.MessageA(), itemid=ds([None]))

    with self.assertRaisesWithLiteralMatch(
        ValueError,
        'itemid must be a 1-D DataSlice if messages is a list of messages',
    ):
      fns.from_proto([], itemid=ds(None))

  def test_single_empty_message(self):
    x = fns.from_proto(test_pb2.MessageA())
    self.assertEqual(x.get_ndim(), 0)
    self.assertFalse(x.get_bag().is_mutable())

  def test_single_empty_message_object_schema(self):
    x = fns.from_proto(test_pb2.MessageA(), schema=schema_constants.OBJECT)
    self.assertEqual(x.get_ndim(), 0)
    self.assertEqual(x.get_schema(), schema_constants.OBJECT)

  def test_single_empty_message_invalid_schema(self):
    with self.assertRaisesRegex(
        ValueError, re.escape("schema's schema must be SCHEMA, got: INT32")
    ):
      _ = fns.from_proto(test_pb2.MessageA(), schema=ds(123))
    with self.assertRaisesRegex(
        ValueError,
        re.escape("schema's schema must be SCHEMA, got: "),
    ):
      _ = fns.from_proto(test_pb2.MessageA(), schema=fns.list([1, 2, 3]))

  def test_single_empty_message_itemid(self):
    x_itemid = fns.uu(seed='').get_itemid()
    x = fns.from_proto(test_pb2.MessageA(), itemid=x_itemid)
    testing.assert_equal(x.get_itemid().no_bag(), x_itemid.no_bag())
    self.assertFalse(x.get_bag().is_mutable())

  def test_single_message(self):
    m = test_pb2.MessageA(
        some_text='thing 1',
        some_float=1.0,
        message_b_list=[
            test_pb2.MessageB(text='a'),
            test_pb2.MessageB(text='b'),
            test_pb2.MessageB(text='c'),
        ],
    )
    x = fns.from_proto(m)
    self.assertFalse(x.get_bag().is_mutable())
    s = x.get_schema()
    self.assertEqual(x.get_ndim(), 0)

    self.assertEqual(x.some_text, 'thing 1')
    self.assertEqual(s.some_text, schema_constants.STRING)
    self.assertEqual(x.some_float, 1.0)
    self.assertEqual(s.some_float, schema_constants.FLOAT32)
    testing.assert_equal(x.message_b_list[:].text.no_bag(), ds(['a', 'b', 'c']))

    # Check message schema metadata.
    s_metadata = s.get_attr('__schema_metadata__')
    self.assertEqual(
        s_metadata.get_attr('__proto_schema_metadata_full_name__'),
        'koladata.functions.testing.MessageA',
    )
    self.assertEqual(
        s_metadata.get_attr('proto_default_value_some_text'), 'aaa'
    )
    self.assertEqual(
        s_metadata.get_attr('proto_default_value_some_float'), 123.4
    )

    message_b_s_metadata = s.message_b_list.get_attr('__items__').get_attr(
        '__schema_metadata__'
    )
    self.assertEqual(
        message_b_s_metadata.get_attr('__proto_schema_metadata_full_name__'),
        'koladata.functions.testing.MessageB',
    )
    self.assertIsNone(
        message_b_s_metadata.maybe('_proto_default_value_text').to_py()
    )

  def test_single_message_explicit_schema(self):
    s = fns.schema_from_proto(test_pb2.MessageA)
    m = test_pb2.MessageA(
        some_text='thing 1',
        some_float=1.0,
        message_b_list=[
            test_pb2.MessageB(text='a'),
            test_pb2.MessageB(text='b'),
            test_pb2.MessageB(text='c'),
        ],
    )
    x = fns.from_proto(m, schema=s)
    self.assertFalse(x.get_bag().is_mutable())
    self.assertEqual(x.get_ndim(), 0)

    self.assertEqual(x.some_text, 'thing 1')
    self.assertEqual(s.some_text, schema_constants.STRING)
    self.assertEqual(x.some_float, 1.0)
    self.assertEqual(s.some_float, schema_constants.FLOAT32)
    testing.assert_equal(x.message_b_list[:].text.no_bag(), ds(['a', 'b', 'c']))

  def test_multiple_message(self):
    m1 = test_pb2.MessageA(
        some_text='thing 1',
        some_float=1.0,
        message_b_list=[
            test_pb2.MessageB(text='a'),
            test_pb2.MessageB(text='b'),
            test_pb2.MessageB(text='c'),
        ],
    )
    m2 = test_pb2.MessageA(
        some_text='thing 2',
        message_b_list=[
            test_pb2.MessageB(),
            test_pb2.MessageB(text='d'),
        ],
    )

    x = fns.from_proto([m1, m2])
    s = x.get_schema()
    self.assertEqual(x.get_ndim(), 1)

    testing.assert_equal(x.some_text.no_bag(), ds(['thing 1', 'thing 2']))
    self.assertEqual(s.some_text, schema_constants.STRING)
    testing.assert_equal(x.some_float.no_bag(), ds([1.0, None]))
    self.assertEqual(s.some_float, schema_constants.FLOAT32)
    testing.assert_equal(
        x.message_b_list[:].text.no_bag(), ds([['a', 'b', 'c'], [None, 'd']])
    )
    self.assertFalse(x.get_bag().is_mutable())

  def test_extensions(self):
    m = test_pb2.MessageA(
        some_text='thing 1',
        some_float=1.0,
        message_b_list=[
            test_pb2.MessageB(text='a'),
            test_pb2.MessageB(text='b'),
            test_pb2.MessageB(text='c'),
        ],
    )

    m.message_set_extensions.Extensions[
        test_pb2.MessageAExtension.message_set_extension
    ].extra = 1
    m.Extensions[test_pb2.MessageAExtension.message_a_extension].extra = 2
    m.Extensions[test_pb2.MessageAExtension.message_a_extension].Extensions[
        test_pb2.MessageAExtensionExtension.message_a_extension_extension
    ].extra = 3
    m.message_b_list[0].Extensions[
        test_pb2.MessageBExtension.message_b_extension
    ].extra = 4
    m.message_b_list[2].Extensions[
        test_pb2.MessageBExtension.message_b_extension
    ].extra = 5

    x_noext = fns.from_proto(m)
    self.assertCountEqual(
        x_noext.get_attr_names(intersection=True),
        [
            'some_text',
            'some_float',
            'message_b_list',
            'message_set_extensions',
        ],
    )

    x = fns.from_proto(
        m,
        extensions=[
            '(koladata.functions.testing.MessageAExtension.message_a_extension)',
            '(koladata.functions.testing.MessageAExtension.message_a_extension).(koladata.functions.testing.MessageAExtensionExtension.message_a_extension_extension)',
            'message_set_extensions.(koladata.functions.testing.MessageAExtension.message_set_extension)',
            'message_b_list.(koladata.functions.testing.MessageBExtension.message_b_extension)',
        ],
    )

    self.assertCountEqual(
        x.get_attr_names(intersection=True),
        [
            'some_text',
            'some_float',
            'message_b_list',
            'message_set_extensions',
            '(koladata.functions.testing.MessageAExtension.message_a_extension)',
        ],
    )

    self.assertCountEqual(
        x.message_set_extensions.get_attr_names(intersection=True),
        [
            '(koladata.functions.testing.MessageAExtension.message_set_extension)'
        ],
    )
    self.assertEqual(
        x.message_set_extensions.get_attr(
            '(koladata.functions.testing.MessageAExtension.message_set_extension)'
        ).extra,
        1,
    )

    self.assertCountEqual(
        x.get_attr(
            '(koladata.functions.testing.MessageAExtension.message_a_extension)'
        ).get_attr_names(intersection=True),
        [
            'extra',
            '(koladata.functions.testing.MessageAExtensionExtension.message_a_extension_extension)',
        ],
    )
    self.assertEqual(
        x.get_attr(
            '(koladata.functions.testing.MessageAExtension.message_a_extension)'
        ).extra,
        2,
    )

    self.assertCountEqual(
        x.get_attr(
            '(koladata.functions.testing.MessageAExtension.message_a_extension)'
        ).get_attr_names(intersection=True),
        [
            'extra',
            '(koladata.functions.testing.MessageAExtensionExtension.message_a_extension_extension)',
        ],
    )
    self.assertEqual(
        x.get_attr(
            '(koladata.functions.testing.MessageAExtension.message_a_extension)'
        )
        .get_attr(
            '(koladata.functions.testing.MessageAExtensionExtension.message_a_extension_extension)'
        )
        .extra,
        3,
    )

    self.assertCountEqual(
        x.message_b_list[:].get_attr_names(intersection=True),
        [
            'text',
            '(koladata.functions.testing.MessageBExtension.message_b_extension)',
        ],
    )
    testing.assert_equal(
        x.message_b_list[:]
        .get_attr(
            '(koladata.functions.testing.MessageBExtension.message_b_extension)'
        )
        .extra.no_bag(),
        ds([4, None, 5]),
    )

    self.assertEqual(
        x.get_attr(
            '(koladata.functions.testing.MessageAExtension.message_a_extension)'
        )
        .get_schema()
        .get_attr('__schema_metadata__')
        .get_attr('__proto_schema_metadata_full_name__'),
        'koladata.functions.testing.MessageAExtension',
    )

  def test_extension_on_wrong_message_error(self):
    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'extension'
            ' "koladata.functions.testing.MessageBExtension.message_b_extension"'
            " exists, but isn't an extension on target message type"
            ' "koladata.functions.testing.MessageA", expected'
            ' "koladata.functions.testing.MessageB"'
        ),
    ):
      _ = fns.from_proto(
          test_pb2.MessageA(),
          extensions=[
              '(koladata.functions.testing.MessageBExtension.message_b_extension)'
          ],
      )

    with self.assertRaisesRegex(
        ValueError,
        re.escape(
            'extension'
            ' "koladata.functions.testing.MessageBExtension.message_b_extension"'
            " exists, but isn't an extension on target message type"
            ' "koladata.functions.testing.MessageA", expected'
            ' "koladata.functions.testing.MessageB"'
        ),
    ):
      _ = fns.from_proto(
          test_pb2.MessageA(),
          schema=kde.schema.new_schema(**{
              '(koladata.functions.testing.MessageBExtension.message_b_extension)': (
                  schema_constants.OBJECT
              )
          }).eval(),
      )


if __name__ == '__main__':
  absltest.main()
